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
 * Phase G's own workflow-bounded matrix cell (Quarkus + Pekko) - the real per-framework proof this
 * plan's own gap-bridging doc calls for: a real HTTP call against a real, booted Quarkus launcher
 * app (not a direct Java call, unlike {@code -launcher-application}'s own {@code
 * RealFowfWorkflowBoundedIngestionTest}, which this test's own {@code SmokeResource} reuses
 * directly - via the new {@code -launcher-application} test-jar - for the real 6-service fowf boot
 * machinery only) dispatches a real fowf workflow using the {@code correlated-worker}/{@code
 * kubernetes-job} construct, which in turn dispatches a real Kubernetes Job running this repo's own
 * Pekko executor image, which writes a real row into a real OpenSearch index.
 *
 * <p>Mirrors {@code DirectIngestionMatrixQuarkusPekkoSmokeTest}'s own shape (a {@code
 * QuarkusTestResourceLifecycleManager} boots everything once, real REST calls against the live app,
 * independent OpenSearch verification) but through {@code POST /workflow-runs} (fowf) not {@code
 * POST /ingestion-runs} (direct K8s) - proving {@code WorkflowRunResource}/{@code
 * WorkflowIngestionLauncher}'s own real CDI wiring under Quarkus, which no existing test exercises.
 */
@QuarkusTest
@QuarkusTestResource(
    value = WorkflowBoundedMatrixQuarkusPekkoSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class WorkflowBoundedMatrixQuarkusPekkoSmokeTest {

  @Test
  @Timeout(300)
  void
      realHttpCallThroughFowfDispatchesARealWorldCheckRowThroughACorrelatedWorkerKubernetesJobStep()
          throws Exception {
    String correlationId = "wf-smoke-" + UUID.randomUUID();
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
        SmokeResource.opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-3";
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
    static final String ROLE = "quarkus-workflow-smoke-role";
    static final String NAMESPACE = "fds-quarkus-workflow-smoke";
    static final String PEKKO_LOCAL_IMAGE = "forwardmeasure/data-streaming-executor-pekko:1.1.0";

    static volatile RealFowfWorkflowFixture fixture;
    static volatile com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer opensearch;
    static volatile GenericContainer<?> enginePekko;
    static volatile GenericContainer<?> operationAdapter;
    static volatile WorkflowDefinition published;

    @Override
    public Map<String, String> start() {
      fixture =
          RealFowfWorkflowFixture.start(
              "fds-quarkus-wf-smoke", ROLE, RealFowfWorkflowFixture.Framework.QUARKUS);
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

      GenericContainer<?> executionManagement = fixture.startExecutionManagement("pekko");
      enginePekko = fixture.startEnginePekko();
      GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
      operationAdapter =
          fixture.startOperationAdapterWithKubernetesJobAllowlist(
              NAMESPACE, "unused", 1, true, NAMESPACE, pekkoImage, 1);
      try {
        fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "interrupted awaiting Pekko cluster readiness", interrupted);
      }

      String asyncApiUrl =
          fixture.startAsyncApiDocumentServer(
              "/ingestion-worker-kubernetes-job.yaml", asyncApiDocument());

      var spec =
          WorldCheckFixtures.boundedFileSpec(
              "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
              openSearchUrlForPod,
              Path.of(""));
      String[] tsvLines = WorldCheckFixtures.SAMPLE_TSV.split("\n");
      String seedTsvBase64 =
          Base64.getEncoder()
              .encodeToString(
                  (tsvLines[0] + "\n" + tsvLines[3] + "\n").getBytes(StandardCharsets.UTF_8));

      try {
        String specBase64 =
            Base64.getEncoder().encodeToString(spec.toYaml().getBytes(StandardCharsets.UTF_8));
        published =
            publish(
                fixture,
                definitionManagement,
                asyncApiUrl,
                specBase64,
                seedTsvBase64,
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
          // This test never dispatches through the Direct-Job path, but DirectIngestionLauncher's
          // own producer is eagerly created regardless (same reason RealFowfWorkflowFixture's own
          // callers set these) - non-blank values are all that's required.
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
        String seedTsvBase64,
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
                  .name("fds-quarkus-workflow-bounded-worldcheck")
                  .title("FDS Quarkus workflow-bounded WorldCheck ingestion")
                  .description(
                      "Real Phase G matrix cell - WorkflowBoundedMatrixQuarkusPekkoSmokeTest."));

      WorkflowDefinition created =
          definitions.createWorkflowDefinition(
              workflow.getId(),
              new CreateWorkflowDefinitionRequest()
                  .version("1.0.0")
                  .source(
                      workflowSource(
                          asyncApiUrl, specBase64, seedTsvBase64, gatewayIp, pekkoImage)));

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
        namespace: fds-quarkus-workflow-bounded
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
                    namespace: fds-quarkus-workflow-smoke
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
                  namespace: fds-quarkus-workflow-smoke
                  consume:
                    until: '${ .payload.status == "SUCCEEDED" or .payload.status == "FAILED" or .payload.status == "CANCELLED" }'
                    for: PT5M
              cancellation:
                channel: ingestion.worldcheck.cancellations
                message:
                  payload:
                    namespace: fds-quarkus-workflow-smoke
      """
          .formatted(asyncApiUrl, pekkoImage, gatewayIp, seedTsvBase64, specBase64);
    }

    private static String asyncApiDocument() {
      return """
      asyncapi: 2.6.0
      info:
        title: WorldCheck Ingestion Worker (kubernetes-job)
        version: 1.0.0
        description: Phase G Quarkus matrix cell - command/event/cancellation contract, protocol
          kubernetes-job, mirrors -launcher-application's own real
          RealFowfWorkflowBoundedIngestionTest fixture document exactly.
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
