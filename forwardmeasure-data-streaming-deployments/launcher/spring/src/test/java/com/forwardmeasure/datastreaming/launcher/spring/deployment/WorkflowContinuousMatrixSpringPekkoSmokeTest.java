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
import java.time.Duration;
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
 * The Spring sibling of {@code WorkflowContinuousMatrixQuarkusPekkoSmokeTest} (launcher/quarkus) -
 * Phase G's real per-framework proof for continuous mode. See that class's own javadoc for the real
 * finding this confirms a second time: continuous mode needs no new dispatch code, only a different
 * published {@code WorkflowDefinition}.
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.context.annotation.Import(
    WorkflowContinuousMatrixSpringPekkoSmokeTest.TestKubernetesClientConfiguration.class)
class WorkflowContinuousMatrixSpringPekkoSmokeTest {

  private static final String ROLE = "spring-workflow-continuous-smoke-role";
  private static final String NAMESPACE = "fds-spring-workflow-continuous-smoke";
  private static final String CORRELATION_NAME = "fds-spring-cont-worker";
  private static final String IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";

  private static RealFowfWorkflowFixture fixture;
  private static GenericContainer<?> enginePekko;
  private static GenericContainer<?> operationAdapter;
  private static GenericContainer<?> executionManagement;
  private static WorkflowDefinition published;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() throws Exception {
    fixture = RealFowfWorkflowFixture.start("fds-spring-wf-cont-smoke", ROLE);
    try (var k8s = fixture.kubernetes().createClient()) {
      k8s.namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }

    executionManagement = fixture.startExecutionManagement("pekko");
    enginePekko = fixture.startEnginePekko();
    GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
    operationAdapter = fixture.startOperationAdapter(NAMESPACE, IMAGE, 1, true);
    fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);

    String asyncApiUrl =
        fixture.startAsyncApiDocumentServer(
            "/stream-worker-kubernetes-deployment.yaml", asyncApiDocument());

    published = publish(fixture, definitionManagement, asyncApiUrl);

    fixture
        .keycloak()
        .grantResourceAuthorization(
            fixture.organizationId(),
            "datastreaming-workflow-run",
            "workflow-runs",
            "workflow-continuous-permission",
            ROLE,
            Set.of(
                AuthorizationAction.WORKFLOW_RUN_LAUNCH.scope(),
                AuthorizationAction.WORKFLOW_RUN_READ.scope()));
  }

  @AfterAll
  static void stopFixtures() {
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
    registry.add("datastreaming.launcher.k8s.images", () -> IMAGE);
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    registry.add("datastreaming.launcher.k8s.host-aliases", () -> "unused=127.0.0.1");
    registry.add("datastreaming.launcher.pekko.image", () -> IMAGE);
    registry.add("datastreaming.launcher.pekko.command", () -> "true #");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> IMAGE);
    registry.add("datastreaming.launcher.kafka-streams.command", () -> "true #");
  }

  @Test
  @Timeout(300)
  void realHttpCallThroughFowfAppliesARealDeploymentThatReachesAvailable() throws Exception {
    String correlationId = "wf-cont-smoke-" + UUID.randomUUID();
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
    assertEquals("COMPLETED", state, "expected the real 2-step apply+watch workflow to complete");

    try (KubernetesClient k8s = fixture.kubernetes().createClient()) {
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      Integer readyReplicas = null;
      while (System.nanoTime() < deadline) {
        var deployments =
            k8s.apps()
                .deployments()
                .inNamespace(NAMESPACE)
                .withLabel("openworkflow.io/execution-id", executionId)
                .list()
                .getItems();
        assertEquals(
            1, deployments.size(), "expected exactly one real Deployment for this execution");
        var deployment = deployments.get(0);
        readyReplicas =
            deployment.getStatus() == null ? null : deployment.getStatus().getReadyReplicas();
        if (Integer.valueOf(1).equals(readyReplicas)) {
          return;
        }
        Thread.sleep(500);
      }
      assertTrue(
          Integer.valueOf(1).equals(readyReplicas),
          "the real Deployment must actually be Available (readyReplicas=1) - last observed "
              + readyReplicas);
    }
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
      RealFowfWorkflowFixture fixture, GenericContainer<?> definitionManagement, String asyncApiUrl)
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
                .name("fds-spring-workflow-continuous-stream-worker")
                .title("FDS Spring workflow-continuous stream worker")
                .description(
                    "Real Phase G matrix cell - WorkflowContinuousMatrixSpringPekkoSmokeTest."));

    WorkflowDefinition created =
        definitions.createWorkflowDefinition(
            workflow.getId(),
            new CreateWorkflowDefinitionRequest()
                .version("1.0.0")
                .source(workflowSource(asyncApiUrl)));

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

  private static String workflowSource(String asyncApiUrl) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-spring-workflow-continuous
      name: stream-worker
      version: '1.0.0'
    do:
      - applyStreamWorker:
          call: asyncapi
          with:
            document:
              endpoint: %s
            channel: stream-worker.commands
            message:
              payload:
                namespace: fds-spring-workflow-continuous-smoke
                name: %s
                image: docker.io/library/busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
                replicas: 1
                command: ["sh", "-c", "sleep 300"]
      - watchStreamWorker:
          call: asyncapi
          with:
            document:
              endpoint: %s
            channel: stream-worker.events
            subscription:
              consume:
                amount: 1
              filter: '${ {namespace: "fds-spring-workflow-continuous-smoke", name: "%s", readinessTimeoutSeconds: 120} }'
    """
        .formatted(asyncApiUrl, CORRELATION_NAME, asyncApiUrl, CORRELATION_NAME);
  }

  private static String asyncApiDocument() {
    return """
    asyncapi: 2.6.0
    info:
      title: Stream Worker Deployment (kubernetes-deployment)
      version: 1.0.0
    servers:
      cluster:
        url: k8s://in-cluster
        protocol: kubernetes-deployment
    channels:
      stream-worker.commands:
        servers: [cluster]
        publish:
          message: {name: ApplyStreamWorkerDeployment}
      stream-worker.events:
        servers: [cluster]
        subscribe:
          message: {name: StreamWorkerDeploymentReadiness}
    """;
  }

  /**
   * Same real, live-verified-necessary override {@code DirectIngestionMatrixSpringPekkoSmokeTest}
   * already documents.
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
