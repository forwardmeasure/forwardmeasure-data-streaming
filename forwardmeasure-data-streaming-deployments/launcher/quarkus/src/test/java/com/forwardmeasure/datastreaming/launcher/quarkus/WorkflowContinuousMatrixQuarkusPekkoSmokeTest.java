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
import com.forwardmeasure.datastreaming.launcher.application.fowf.ContinuousDeliveryAcceptanceFixture;
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
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/** Public workflow admission, real worker readiness, and two waves of persisted input. */
@QuarkusTest
@QuarkusTestResource(
    value = WorkflowContinuousMatrixQuarkusPekkoSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class WorkflowContinuousMatrixQuarkusPekkoSmokeTest {

  @Test
  @Timeout(600)
  void realHttpCallThroughFowfDeploysAWorkerThatKeepsIngesting() throws Exception {
    String correlationId = "wf-cont-smoke-" + UUID.randomUUID();
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
    assertEquals("COMPLETED", state, "expected the real 2-step apply+watch workflow to complete");

    try (var k8s = SmokeResource.fixture.kubernetes().createClient()) {
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      Integer readyReplicas = null;
      while (System.nanoTime() < deadline) {
        var deployments =
            k8s.apps()
                .deployments()
                .inNamespace(SmokeResource.NAMESPACE)
                .withLabel("openworkflow.io/execution-id", executionId)
                .list()
                .getItems();
        assertEquals(
            1, deployments.size(), "expected exactly one real Deployment for this execution");
        var deployment = deployments.get(0);
        readyReplicas =
            deployment.getStatus() == null ? null : deployment.getStatus().getReadyReplicas();
        if (Integer.valueOf(1).equals(readyReplicas)) {
          SmokeResource.delivery.verifyOngoingDelivery();
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
    static final String ROLE = "quarkus-workflow-continuous-smoke-role";
    static final String NAMESPACE = "fds-quarkus-workflow-continuous-smoke";
    static final String CORRELATION_NAME = "fds-quarkus-cont-worker";
    static String IMAGE;
    static ContinuousDeliveryAcceptanceFixture delivery;

    static volatile RealFowfWorkflowFixture fixture;
    static volatile GenericContainer<?> enginePekko;
    static volatile GenericContainer<?> operationAdapter;
    static volatile WorkflowDefinition published;

    @Override
    public Map<String, String> start() {
      fixture =
          RealFowfWorkflowFixture.start(
              "fds-quarkus-wf-cont-smoke", ROLE, RealFowfWorkflowFixture.Framework.QUARKUS);
      delivery = new ContinuousDeliveryAcceptanceFixture(fixture, "pekko");
      IMAGE = delivery.image();
      try (var k8s = fixture.kubernetes().createClient()) {
        k8s.namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }

      GenericContainer<?> executionManagement = fixture.startExecutionManagement("pekko");
      enginePekko = fixture.startEnginePekko();
      GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
      operationAdapter = fixture.startOperationAdapter(NAMESPACE, IMAGE, 1, true);
      try {
        fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "interrupted awaiting Pekko cluster readiness", interrupted);
      }

      String asyncApiUrl =
          fixture.startAsyncApiDocumentServer(
              "/stream-worker-kubernetes-deployment.yaml", asyncApiDocument());

      try {
        published = publish(fixture, definitionManagement, asyncApiUrl);
      } catch (Exception failure) {
        throw new IllegalStateException("failed to publish the real workflow definition", failure);
      }

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
          // This test never dispatches through the Direct-Job path, but DirectIngestionLauncher's
          // own producer is eagerly created regardless.
          Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
          Map.entry("datastreaming.launcher.k8s.images", IMAGE),
          // Leave optional pull secrets, host aliases and Spark settings at production defaults.
          Map.entry("datastreaming.launcher.pekko.image", IMAGE),
          Map.entry("datastreaming.launcher.pekko.command", "false"),
          Map.entry("datastreaming.launcher.kafka-streams.image", IMAGE),
          Map.entry("datastreaming.launcher.kafka-streams.command", "false"));
    }

    @Override
    public void stop() {
      if (fixture != null) {
        fixture.close();
        if (delivery != null) delivery.close();
      }
    }

    private static WorkflowDefinition publish(
        RealFowfWorkflowFixture fixture,
        GenericContainer<?> definitionManagement,
        String asyncApiUrl)
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
                  .name("fds-quarkus-workflow-continuous-stream-worker")
                  .title("FDS Quarkus workflow-continuous stream worker")
                  .description(
                      "Real Phase G matrix cell - WorkflowContinuousMatrixQuarkusPekkoSmokeTest."));

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
        namespace: fds-quarkus-workflow-continuous
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
                  namespace: fds-quarkus-workflow-continuous-smoke
                  name: %s
                  image: %s
                  replicas: 1
                  command: %s
        - watchStreamWorker:
            call: asyncapi
            with:
              document:
                endpoint: %s
              channel: stream-worker.events
              subscription:
                consume:
                  amount: 1
                filter: '${ {namespace: "fds-quarkus-workflow-continuous-smoke", name: "%s", readinessTimeoutSeconds: 120} }'
      """
          .formatted(
              asyncApiUrl,
              CORRELATION_NAME,
              IMAGE,
              delivery.commandJson(),
              asyncApiUrl,
              CORRELATION_NAME);
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
  }
}
