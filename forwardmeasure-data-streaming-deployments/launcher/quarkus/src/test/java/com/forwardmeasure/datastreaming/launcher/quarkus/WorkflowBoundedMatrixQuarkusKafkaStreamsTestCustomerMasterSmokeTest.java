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
package com.forwardmeasure.datastreaming.launcher.quarkus;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.launcher.application.fowf.RealFowfWorkflowFixture;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * The Kafka-Streams-engine sibling of {@code
 * WorkflowBoundedMatrixQuarkusPekkoTestCustomerMasterSmokeTest} - mirrors {@code
 * WorkflowBoundedMatrixQuarkusKafkaStreamsSmokeTest}'s own engine-swap pattern exactly (not blocked
 * on anything as of 2026-09-25 - both fowf bugs already fixed and verified).
 */
@QuarkusTest
@QuarkusTestResource(
    value = WorkflowBoundedMatrixQuarkusKafkaStreamsTestCustomerMasterSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class WorkflowBoundedMatrixQuarkusKafkaStreamsTestCustomerMasterSmokeTest {

  @Test
  @Timeout(300)
  void
      realHttpCallThroughFowfDispatchesARealTestCustomerMasterRowThroughACorrelatedWorkerKubernetesJobStep()
          throws Exception {
    String correlationId = "wf-cm-ks-smoke-" + UUID.randomUUID();
    String token = SmokeResource.fixture.keycloak().mintUserToken();

    Map<String, Object> requestBody =
        Map.of(
            "revisionId",
            SmokeResource.published.getId(),
            "input",
            Map.of(),
            "idempotencyKey",
            "idempotency-" + correlationId,
            "correlationId",
            correlationId);

    String executionId =
        given()
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

    String documentUri =
        SmokeResource.opensearch.hostEndpoint()
            + "/test-customer-master-screening-records/_doc/KYC22438AML";
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

  private static String pollUntilTerminal(String token, String executionId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
    String state = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      var response =
          given()
              .header("Authorization", "Bearer " + SmokeResource.fixture.keycloak().mintUserToken())
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

  public static final class SmokeResource implements QuarkusTestResourceLifecycleManager {
    static final String ROLE = "quarkus-workflow-ks-cm-smoke-role";
    static final String NAMESPACE = "fds-quarkus-workflow-ks-cm-smoke";
    static final String PEKKO_LOCAL_IMAGE = "forwardmeasure/data-streaming-executor-pekko:1.1.0";

    static volatile RealFowfWorkflowFixture fixture;
    static volatile com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer opensearch;
    static volatile WorkflowDefinition published;

    @Override
    public Map<String, String> start() {
      fixture = RealFowfWorkflowFixture.start("fds-quarkus-wf-ks-cm-smoke", ROLE);
      try (var k8s = fixture.kubernetes().createClient()) {
        k8s.namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }
      String pekkoImage = fixture.kubernetes().loadImageAndPinDigest(PEKKO_LOCAL_IMAGE);

      opensearch =
          new com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer().start();
      String openSearchUrlForPod =
          "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
      String gatewayIp = dockerBridgeGatewayIp();

      GenericContainer<?> executionManagement = fixture.startExecutionManagement("kafka-streams");
      fixture.startEngineKafkaStreams();
      GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
      // joinPekkoCluster=false - the Kafka-Streams engine has no Pekko WorkflowCommand entities at
      // all, so operation-adapter's own cluster must stay self-joined.
      fixture.startOperationAdapterWithKubernetesJobAllowlist(
          NAMESPACE, "unused", 1, false, NAMESPACE, pekkoImage, 1);

      String asyncApiUrl =
          fixture.startAsyncApiDocumentServer(
              "/ingestion-worker-kubernetes-job.yaml", asyncApiDocument());

      var spec =
          TestCustomerMasterFixtures.boundedFileSpec(
              "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
              openSearchUrlForPod,
              Path.of(""));
      String[] lines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
      String seedCsvBase64 =
          Base64.getEncoder()
              .encodeToString((lines[0] + "\n" + lines[3] + "\n").getBytes(StandardCharsets.UTF_8));

      try {
        String specBase64 =
            Base64.getEncoder().encodeToString(spec.toYaml().getBytes(StandardCharsets.UTF_8));
        published =
            publish(
                fixture,
                definitionManagement,
                asyncApiUrl,
                specBase64,
                seedCsvBase64,
                gatewayIp,
                pekkoImage);
      } catch (Exception failure) {
        throw new IllegalStateException("failed to publish the real workflow definition", failure);
      }

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

      String executionManagementBaseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

      return Map.ofEntries(
          Map.entry("quarkus.oidc.auth-server-url", fixture.keycloak().issuer().toString()),
          Map.entry(
              "datastreaming.launcher.authorization.organization-client-id",
              com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.CLIENT_ID),
          Map.entry(
              "datastreaming.launcher.authorization.issuer",
              fixture.keycloak().issuer().toString()),
          Map.entry(
              "datastreaming.launcher.authorization.client-id",
              com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
          Map.entry(
              "datastreaming.launcher.authorization.client-secret",
              com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
          Map.entry("datastreaming.launcher.fowf.base-url", executionManagementBaseUrl),
          Map.entry(
              "datastreaming.launcher.fowf.keycloak.token-url",
              fixture.keycloak().issuer() + "/protocol/openid-connect/token"),
          Map.entry(
              "datastreaming.launcher.fowf.keycloak.client-id",
              com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
          Map.entry(
              "datastreaming.launcher.fowf.keycloak.client-secret",
              com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
          Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
          Map.entry("datastreaming.launcher.k8s.images", pekkoImage),
          Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
          Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
          Map.entry("datastreaming.launcher.pekko.image", pekkoImage),
          Map.entry("datastreaming.launcher.pekko.command", "true #"),
          Map.entry("datastreaming.launcher.kafka-streams.image", pekkoImage),
          Map.entry("datastreaming.launcher.kafka-streams.command", "true #"),
          Map.entry("datastreaming.launcher.spark.image", "unused"),
          Map.entry("datastreaming.launcher.spark.command", "unused"),
          Map.entry("datastreaming.launcher.kafka.bootstrap-servers", "unused"));
    }

    @Override
    public void stop() {
      if (opensearch != null) {
        opensearch.close();
      }
      if (fixture != null) {
        fixture.close();
      }
    }

    private static WorkflowDefinition publish(
        RealFowfWorkflowFixture fixture,
        GenericContainer<?> definitionManagement,
        String asyncApiUrl,
        String specBase64,
        String seedCsvBase64,
        String gatewayIp,
        String pekkoImage)
        throws Exception {
      String baseUrl =
          "http://"
              + definitionManagement.getHost()
              + ":"
              + definitionManagement.getMappedPort(8080);

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
                  .name("fds-quarkus-workflow-bounded-ks-cm-customer-master")
                  .title("FDS Quarkus workflow-bounded Customer Master ingestion (Kafka-Streams)")
                  .description(
                      "Real Phase H matrix cell -"
                          + " WorkflowBoundedMatrixQuarkusKafkaStreamsTestCustomerMasterSmokeTest."));

      WorkflowDefinition created =
          definitions.createWorkflowDefinition(
              workflow.getId(),
              new CreateWorkflowDefinitionRequest()
                  .version("1.0.0")
                  .source(
                      workflowSource(
                          asyncApiUrl, specBase64, seedCsvBase64, gatewayIp, pekkoImage)));

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
        String seedCsvBase64,
        String gatewayIp,
        String pekkoImage) {
      return """
      document:
        dsl: '1.0.3'
        namespace: fds-quarkus-workflow-bounded-ks-ss
        name: test-customer-master-ingestion
        version: '1.0.0'
      do:
        - runIngestion:
            call: com.forwardmeasure.openworkflow.correlated-worker
            with:
              document:
                endpoint: %s
              command:
                channel: ingestion.testcustomermaster.commands
                message:
                  payload:
                    namespace: fds-quarkus-workflow-ks-cm-smoke
                    image: %s
                    parallelism: 1
                    completions: 1
                    hostAliases:
                      host.docker.internal: %s
                    command: ["sh", "-c"]
                    args:
                      - "echo %s | base64 -d > /tmp/source.csv && echo %s | base64 -d > /tmp/ingestion-spec.yaml && exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml"
              events:
                channel: ingestion.testcustomermaster.events
                subscription:
                  namespace: fds-quarkus-workflow-ks-cm-smoke
                  consume:
                    until: '${ .payload.status == "SUCCEEDED" or .payload.status == "FAILED" or .payload.status == "CANCELLED" }'
                    for: PT5M
              cancellation:
                channel: ingestion.testcustomermaster.cancellations
                message:
                  payload:
                    namespace: fds-quarkus-workflow-ks-cm-smoke
      """
          .formatted(asyncApiUrl, pekkoImage, gatewayIp, seedCsvBase64, specBase64);
    }

    private static String asyncApiDocument() {
      return """
      asyncapi: 2.6.0
      info:
        title: Customer Master Ingestion Worker (kubernetes-job)
        version: 1.0.0
        description: Phase H Quarkus matrix cell (Kafka-Streams engine).
      servers:
        cluster:
          url: k8s://in-cluster
          protocol: kubernetes-job
      channels:
        ingestion.testcustomermaster.commands:
          servers: [cluster]
          publish:
            message: {name: LaunchIngestionJob}
        ingestion.testcustomermaster.events:
          servers: [cluster]
          subscribe:
            message: {name: IngestionJobEvent}
        ingestion.testcustomermaster.cancellations:
          servers: [cluster]
          publish:
            message: {name: CancelIngestionJob}
      """;
    }

    private static String dockerBridgeGatewayIp() {
      try {
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
      } catch (Exception e) {
        throw new IllegalStateException("failed to discover the real Docker bridge gateway IP", e);
      }
    }
  }
}
