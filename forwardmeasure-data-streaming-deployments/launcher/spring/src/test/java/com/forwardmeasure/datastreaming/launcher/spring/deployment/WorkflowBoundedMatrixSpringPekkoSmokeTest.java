/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.datastreaming.launcher.spring.deployment;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.launcher.application.fowf.RealFowfWorkflowFixture;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;

/**
 * The Spring sibling of {@code WorkflowBoundedMatrixQuarkusPekkoSmokeTest} (launcher/quarkus) -
 * Phase G's real per-framework proof for workflow-bounded mode: a real HTTP {@code POST
 * /workflow-runs} against a real, booted Spring app dispatches a real fowf workflow using the
 * {@code correlated-worker}/{@code kubernetes-job} construct.
 *
 * <p>Reuses the exact {@code @TestConfiguration}+{@code @Primary}+{@code @Import} {@code
 * KubernetesClient} override {@code DirectIngestionMatrixSpringPekkoSmokeTest} already proved
 * necessary - this test never dispatches through {@code DirectIngestionLauncher}, but Spring
 * eagerly constructs every {@code @Bean} regardless of whether it's ever injected somewhere
 * actually used, so the same real ambient-production-cluster risk applies here too.
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.context.annotation.Import(
    WorkflowBoundedMatrixSpringPekkoSmokeTest.TestKubernetesClientConfiguration.class)
class WorkflowBoundedMatrixSpringPekkoSmokeTest {

  private static final String ROLE = "spring-workflow-smoke-role";
  private static final String NAMESPACE = "fds-spring-workflow-smoke";
  private static final String PEKKO_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-pekko:1.1.0";

  private static RealFowfWorkflowFixture fixture;
  private static com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer opensearch;
  private static GenericContainer<?> enginePekko;
  private static GenericContainer<?> operationAdapter;
  private static GenericContainer<?> executionManagement;
  private static WorkflowDefinition published;
  private static String pekkoImage;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() throws Exception {
    fixture = RealFowfWorkflowFixture.start("fds-spring-wf-smoke", ROLE);
    try (var k8s = fixture.kubernetes().createClient()) {
      k8s.namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }
    pekkoImage = fixture.kubernetes().loadImageAndPinDigest(PEKKO_LOCAL_IMAGE);

    opensearch = new com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer().start();
    String openSearchUrlForPod =
        "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
    String gatewayIp = dockerBridgeGatewayIp();

    executionManagement = fixture.startExecutionManagement("pekko");
    enginePekko = fixture.startEnginePekko();
    GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
    operationAdapter =
        fixture.startOperationAdapterWithKubernetesJobAllowlist(
            NAMESPACE, "unused", 1, true, NAMESPACE, pekkoImage, 1);
    fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);

    String asyncApiUrl =
        fixture.startAsyncApiDocumentServer(
            "/ingestion-worker-kubernetes-job.yaml", asyncApiDocument());

    var spec =
        WorldCheckFixtures.boundedFileSpec(
            "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
            openSearchUrlForPod,
            Path.of(""));
    String specBase64 =
        Base64.getEncoder().encodeToString(spec.toYaml().getBytes(StandardCharsets.UTF_8));
    String[] tsvLines = WorldCheckFixtures.SAMPLE_TSV.split("\n");
    String seedTsvBase64 =
        Base64.getEncoder()
            .encodeToString(
                (tsvLines[0] + "\n" + tsvLines[3] + "\n").getBytes(StandardCharsets.UTF_8));

    published =
        publish(
            fixture,
            definitionManagement,
            asyncApiUrl,
            specBase64,
            seedTsvBase64,
            gatewayIp,
            pekkoImage);

    fixture
        .keycloak()
        .grantResourceAuthorization(
            fixture.organizationId(),
            "datastreaming-workflow-run",
            "workflow-runs",
            "workflow-bounded-permission",
            ROLE,
            Set.of(
                AuthorizationAction.WORKFLOW_RUN_LAUNCH.scope(),
                AuthorizationAction.WORKFLOW_RUN_READ.scope()));
  }

  @AfterAll
  static void stopFixtures() {
    if (opensearch != null) {
      opensearch.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @DynamicPropertySource
  static void registerDynamicProperties(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.security.oauth2.resourceserver.jwt.issuer-uri",
        () -> fixture.keycloak().issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.organization-client-id",
        () -> AuthzenKeycloakFixture.CLIENT_ID);
    registry.add(
        "datastreaming.launcher.authorization.issuer",
        () -> fixture.keycloak().issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.client-id",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID);
    registry.add(
        "datastreaming.launcher.authorization.client-secret",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET);
    registry.add(
        "datastreaming.launcher.fowf.base-url",
        () ->
            "http://"
                + executionManagement.getHost()
                + ":"
                + executionManagement.getMappedPort(8080));
    registry.add(
        "datastreaming.launcher.fowf.keycloak.token-url",
        () -> fixture.keycloak().issuer() + "/protocol/openid-connect/token");
    registry.add(
        "datastreaming.launcher.fowf.keycloak.client-id",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID);
    registry.add(
        "datastreaming.launcher.fowf.keycloak.client-secret",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET);
    registry.add("datastreaming.launcher.k8s.namespaces", () -> NAMESPACE);
    registry.add("datastreaming.launcher.k8s.images", () -> pekkoImage);
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    registry.add(
        "datastreaming.launcher.k8s.host-aliases",
        () -> {
          try {
            return "host.docker.internal=" + dockerBridgeGatewayIp();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
    registry.add("datastreaming.launcher.pekko.image", () -> pekkoImage);
    registry.add("datastreaming.launcher.pekko.command", () -> "true #");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> pekkoImage);
    registry.add("datastreaming.launcher.kafka-streams.command", () -> "true #");
  }

  @Test
  @Timeout(300)
  void
      realHttpCallThroughFowfDispatchesARealWorldCheckRowThroughACorrelatedWorkerKubernetesJobStep()
          throws Exception {
    String correlationId = "wf-smoke-" + UUID.randomUUID();
    String token = fixture.keycloak().mintUserToken();

    Map<String, Object> requestBody =
        Map.of(
            "revisionId",
            published.getId(),
            "input",
            Map.of(),
            "idempotencyKey",
            "idempotency-" + correlationId,
            "correlationId",
            correlationId);

    String executionId =
        given()
            .port(port)
            .header("Authorization", "Bearer " + token)
            .contentType("application/json")
            .body(requestBody)
            .when()
            .post("/workflow-runs")
            .then()
            .statusCode(202)
            .extract()
            .path("id");

    String state = pollUntilTerminal(token, executionId);
    assertEquals(
        "COMPLETED",
        state,
        "expected the real correlated-worker/kubernetes-job workflow to complete");

    String documentUri = opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-3";
    HttpResponse<String> document =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document written by the fowf-dispatched Job: " + document.body());
    assertTrue(
        document.body().contains("\"entity_kind\":\"organization\""),
        "unexpected document body: " + document.body());
  }

  private String pollUntilTerminal(String token, String executionId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
    String state = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      var response =
          given()
              .port(port)
              .header("Authorization", "Bearer " + fixture.keycloak().mintUserToken())
              .when()
              .get("/workflow-runs/" + executionId);
      if (response.statusCode() == 200) {
        state = response.jsonPath().getString("state");
        if ("COMPLETED".equals(state) || "FAILED".equals(state)) {
          return state;
        }
      }
      Thread.sleep(1000);
    }
    return state;
  }

  private static WorkflowDefinition publish(
      RealFowfWorkflowFixture fixture,
      GenericContainer<?> definitionManagement,
      String asyncApiUrl,
      String specBase64,
      String seedTsvBase64,
      String gatewayIp,
      String pekkoImage)
      throws Exception {
    String baseUrl =
        "http://" + definitionManagement.getHost() + ":" + definitionManagement.getMappedPort(8080);

    var authorApiClient =
        new com.forwardmeasure.openworkflow.definition.management.client.ApiClient();
    authorApiClient.setBasePath(baseUrl);
    authorApiClient.setBearerToken(fixture.keycloak().mintUserToken());

    WorkflowsApi workflows = new WorkflowsApi(authorApiClient);
    WorkflowDefinitionsApi definitions = new WorkflowDefinitionsApi(authorApiClient);
    WorkflowDefinitionGovernanceApi governance =
        new WorkflowDefinitionGovernanceApi(authorApiClient);

    Workflow workflow =
        workflows.createWorkflow(
            new CreateWorkflowRequest()
                .name("fds-spring-workflow-bounded-worldcheck")
                .title("FDS Spring workflow-bounded WorldCheck ingestion")
                .description(
                    "Real Phase G matrix cell - WorkflowBoundedMatrixSpringPekkoSmokeTest."));

    WorkflowDefinition created =
        definitions.createWorkflowDefinition(
            workflow.getId(),
            new CreateWorkflowDefinitionRequest()
                .version("1.0.0")
                .source(
                    workflowSource(asyncApiUrl, specBase64, seedTsvBase64, gatewayIp, pekkoImage)));

    WorkflowDefinitionValidation validation =
        governance.validateWorkflowDefinition(
            ifMatch(created.getRevision()), workflow.getId(), created.getId());
    assertTrue(
        Boolean.TRUE.equals(validation.getValid()),
        "expected the real workflow document to compile cleanly: " + validation.getViolations());

    WorkflowDefinition published =
        governance.publishWorkflowDefinition(
            ifMatch(created.getRevision()), workflow.getId(), created.getId());
    assertEquals("PUBLISHED", published.getStatus().name());
    return published;
  }

  private static String ifMatch(Long revision) {
    return "\"" + revision + "\"";
  }

  private static String workflowSource(
      String asyncApiUrl,
      String specBase64,
      String seedTsvBase64,
      String gatewayIp,
      String pekkoImage) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-spring-workflow-bounded
      name: worldcheck-ingestion
      version: '1.0.0'
    do:
      - runIngestion:
          call: com.forwardmeasure.openworkflow.correlated-worker
          with:
            document:
              endpoint: %s
            command:
              channel: ingestion.worldcheck.commands
              message:
                payload:
                  namespace: fds-spring-workflow-smoke
                  image: %s
                  parallelism: 1
                  completions: 1
                  hostAliases:
                    host.docker.internal: %s
                  command: ["sh", "-c"]
                  args:
                    - "echo %s | base64 -d > /tmp/source.csv && echo %s | base64 -d > /tmp/ingestion-spec.yaml && exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml"
            events:
              channel: ingestion.worldcheck.events
              subscription:
                namespace: fds-spring-workflow-smoke
                consume:
                  until: '${ .payload.status == "SUCCEEDED" or .payload.status == "FAILED" or .payload.status == "CANCELLED" }'
                  for: PT5M
            cancellation:
              channel: ingestion.worldcheck.cancellations
              message:
                payload:
                  namespace: fds-spring-workflow-smoke
    """
        .formatted(asyncApiUrl, pekkoImage, gatewayIp, seedTsvBase64, specBase64);
  }

  private static String asyncApiDocument() {
    return """
    asyncapi: 2.6.0
    info:
      title: WorldCheck Ingestion Worker (kubernetes-job)
      version: 1.0.0
      description: Phase G Spring matrix cell.
    servers:
      cluster:
        url: k8s://in-cluster
        protocol: kubernetes-job
    channels:
      ingestion.worldcheck.commands:
        servers: [cluster]
        publish:
          message: {name: LaunchIngestionJob}
      ingestion.worldcheck.events:
        servers: [cluster]
        subscribe:
          message: {name: IngestionJobEvent}
      ingestion.worldcheck.cancellations:
        servers: [cluster]
        publish:
          message: {name: CancelIngestionJob}
    """;
  }

  private static String dockerBridgeGatewayIp() throws Exception {
    Process process =
        new ProcessBuilder(
                "docker",
                "network",
                "inspect",
                "bridge",
                "--format",
                "{{(index .IPAM.Config 0).Gateway}}")
            .redirectErrorStream(true)
            .start();
    String output =
        new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
    process.waitFor();
    return output;
  }

  /**
   * Same real, live-verified-necessary override {@code DirectIngestionMatrixSpringPekkoSmokeTest}
   * already documents - see this class's own javadoc.
   */
  @TestConfiguration
  static class TestKubernetesClientConfiguration {
    @Bean
    @Primary
    KubernetesClient testKubernetesClient() {
      return new KubernetesClientBuilder()
          .withConfig(Config.fromKubeconfig(fixture.kubernetes().kubeConfigYaml()))
          .build();
    }
  }
}
