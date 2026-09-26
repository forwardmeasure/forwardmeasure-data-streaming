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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.KeycloakOrganizationClaims;
import com.forwardmeasure.authzen.client.AuthzenAuthorizationFactory;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecution;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionState;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import java.io.IOException;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

/**
 * Workflow-bounded mode's real proof: a real fowf workflow execution, using the mature, proven
 * {@code correlated-worker}/{@code kubernetes-job} construct (not the newer, still-stalled two-step
 * {@code asyncapi}/{@code kubernetes-deployment} pattern {@code
 * RealFowfWorkflowFixtureEndToEndTest} exercises - see that class's own javadoc and the handover
 * doc it references), dispatches this repo's own real Pekko executor image, and the row lands in a
 * real OpenSearch index - the same business-logic proof {@code
 * DirectIngestionMatrixQuarkusPekkoSmokeTest}/etc. already give for the Direct-Job path, this time
 * via a real fowf-triggered workflow instead.
 *
 * <p><b>Parameterized over both real fowf engines</b> (2026-09-24 - the Kafka Streams engine's own
 * gap this class's own earlier javadoc used to cite, {@code KafkaOperationAdapterDispatcher} never
 * handling {@code UPSERT_ASYNC_API_SUBSCRIPTION}, is now fixed upstream in fowf - see {@code
 * docs/kafka-streams-async-api-subscription-dispatch-gap-2026-09-24.md} - so, mirroring {@code
 * RealFowfWorkflowFixtureEndToEndTest}'s own parity fix, this is no longer a Pekko-only proof).
 * {@code fixture.startEngineKafkaStreams()}/{@code startEnginePekko()} and whether
 * operation-adapter joins the Pekko cluster are the only per-engine branches; everything else is
 * identical.
 *
 * <p>Reuses {@link RealFowfWorkflowFixture}'s Stage 1/2/3/5/6 boot machinery unchanged (Postgres,
 * Keycloak, execution-management, definition-management, operation-adapter) plus {@code
 * startEnginePekko()}/{@code startEngineKafkaStreams()} from the continuous-mode end-to-end test.
 * New here: a real {@code kubernetes-job} AsyncAPI document (FDS has no existing one, mirroring
 * forwardmeasure-entity-intelligence's own real {@code ingestion-worker-kubernetes-job.yaml}), the
 * {@code correlated-worker} workflow shape, and the real {@code
 * OPENWORKFLOW_OPERATIONS_KUBERNETES_JOB_*} allowlist wiring added via {@link
 * RealFowfWorkflowFixture#startOperationAdapterWithKubernetesJobAllowlist} for this test.
 *
 * <p>Needs an {@code operation-adapter} image carrying 2026-09-23's real {@code hostAliases} fix to
 * {@code AsyncApiKubernetesJobOperationExecutor} (that executor never read a {@code hostAliases}
 * payload field before, even though {@code KubernetesJobSpec} itself already supported one - the
 * exact same real gap FDS's own {@code DirectIngestionLauncher} already had and fixed for its own
 * direct-Job path) - without it, the dispatched pod has no route to the sibling OpenSearch
 * testcontainer at all.
 */
class RealFowfWorkflowBoundedIngestionTest {

  private static final String ROLE = "workflow-bounded-launcher";
  private static final String NAMESPACE = "streaming";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * The real, locally-built image (this repo's own {@code ${project.version}}, no Docker Hub
   * reference needed - real, live-caught 2026-09-24: the previous Docker Hub digest pull is public
   * and succeeds fine on the host, but K3s's own containerd pulls over its own network path,
   * independent of the host Docker daemon's cache, and a live {@code kubectl get pods} taken while
   * this test was stuck confirmed the dispatched Job's pod sat in ImagePullBackOff/ErrImagePull the
   * entire run. There is no reason to depend on any registry at all for an image this repo already
   * builds itself - {@link KubernetesTestContainer#loadImageAndPinDigest} loads this straight from
   * the host's local image cache into K3s (docker save + ctr images import, no pull of any kind)
   * and hands back a real, verified-resolvable {@code @sha256:}-pinned reference - required because
   * {@code AsyncApiKubernetesJobOperationExecutor} enforces this org's own standing digest-pinning
   * policy on every dispatched Job image (confirmed live 2026-09-24: a plain {@code :1.1.0} tag was
   * rejected outright with "Kubernetes Job image must be pinned by sha256 digest," repeatedly,
   * before the command leg ever got to create anything).
   */
  private static final String PEKKO_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-pekko:1.1.0";

  @ParameterizedTest(name = "engine={0}")
  @ValueSource(strings = {"kafka-streams", "pekko"})
  @Timeout(600)
  void engineDispatchesARealWorldCheckRowThroughACorrelatedWorkerKubernetesJobStep(String engine)
      throws Exception {
    try (RealFowfWorkflowFixture fixture =
        RealFowfWorkflowFixture.start("fds-wf-bounded-" + engine, ROLE)) {
      try (var k8s = fixture.kubernetes().createClient()) {
        k8s.namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }
      // See PEKKO_LOCAL_IMAGE's own javadoc for why this is needed - K3s's containerd can't see the
      // host daemon's cache on its own, and the dispatched Job's own image must be digest-pinned.
      String pekkoImage = fixture.kubernetes().loadImageAndPinDigest(PEKKO_LOCAL_IMAGE);

      com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer opensearch =
          new com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer().start();
      try {
        String openSearchUrlForPod =
            "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
        String gatewayIp = dockerBridgeGatewayIp();

        boolean isPekko = engine.equals("pekko");
        GenericContainer<?> executionManagement = fixture.startExecutionManagement(engine);
        GenericContainer<?> enginePekko = isPekko ? fixture.startEnginePekko() : null;
        if (!isPekko) {
          fixture.startEngineKafkaStreams();
        }
        GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
        GenericContainer<?> operationAdapter =
            fixture.startOperationAdapterWithKubernetesJobAllowlist(
                NAMESPACE, "unused", 1, isPekko, NAMESPACE, pekkoImage, 1);
        if (isPekko) {
          fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);
        }

        String asyncApiUrl =
            fixture.startAsyncApiDocumentServer(
                "/ingestion-worker-kubernetes-job.yaml", ASYNCAPI_DOCUMENT);

        IngestionSpec spec =
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

        WorkflowDefinition published =
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

        String accessToken = fixture.keycloak().mintUserToken();
        ActiveOrganization actor =
            KeycloakOrganizationClaims.extract(
                decodeClaims(accessToken), AuthzenKeycloakFixture.CLIENT_ID);
        AuthorizationService authorization =
            AuthzenAuthorizationFactory.create(
                MAPPER,
                fixture.keycloak().issuer(),
                AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID,
                AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET,
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                "fds-wf-bounded-test-v1");

        String executionManagementBaseUrl =
            "http://"
                + executionManagement.getHost()
                + ":"
                + executionManagement.getMappedPort(8080);
        var executionApiClient = new com.forwardmeasure.openworkflow.execution.client.ApiClient();
        executionApiClient.setBasePath(executionManagementBaseUrl);
        executionApiClient.setBearerToken(accessToken);
        WorkflowExecutionsApi executionsApi = new WorkflowExecutionsApi(executionApiClient);

        WorkflowIngestionLauncher launcher =
            new WorkflowIngestionLauncher(executionsApi, authorization);

        String correlationId = "fds-wf-bounded-" + UUID.randomUUID();
        WorkflowExecution started =
            launcher.launch(
                new WorkflowLaunchRequest(
                    published.getId(), Map.of(), "idempotency-" + correlationId, correlationId),
                actor);
        assertNotNull(started.getId(), "a real fowf execution must have been admitted");

        WorkflowExecution finalState;
        try {
          finalState = awaitTerminal(fixture, executionApiClient, launcher, started.getId(), actor);
        } catch (AssertionError neverTerminal) {
          // Same real, live-confirmed reason RealFowfWorkflowFixtureEndToEndTest dumps raw docker
          // logs on this exact failure mode - the fixture's own piped console capture has
          // repeatedly, silently truncated mid-run for reasons unrelated to this test's own logic.
          dumpContainerLogs("engine-pekko", enginePekko);
          dumpContainerLogs("operation-adapter", operationAdapter);
          dumpContainerLogs("execution-management", executionManagement);
          dumpContainerLogs("definition-management", definitionManagement);
          throw neverTerminal;
        }
        assertEquals(
            WorkflowExecutionState.COMPLETED,
            finalState.getState(),
            "expected the real correlated-worker/kubernetes-job workflow to complete - got "
                + finalState.getState()
                + ", error="
                + finalState.getError());

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
      } finally {
        opensearch.close();
      }
    }
  }

  /** Same re-mint-per-poll shape as {@code RealFowfWorkflowFixtureEndToEndTest#awaitTerminal}. */
  private static WorkflowExecution awaitTerminal(
      RealFowfWorkflowFixture fixture,
      com.forwardmeasure.openworkflow.execution.client.ApiClient executionApiClient,
      WorkflowIngestionLauncher launcher,
      UUID executionId,
      ActiveOrganization actor)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
    WorkflowExecution last = null;
    while (System.nanoTime() < deadline) {
      executionApiClient.setBearerToken(fixture.keycloak().mintUserToken());
      last = launcher.observe(executionId, actor);
      System.out.println(
          "RealFowfWorkflowBoundedIngestionTest: execution state = " + last.getState());
      if (last.getState() == WorkflowExecutionState.COMPLETED
          || last.getState() == WorkflowExecutionState.FAILED) {
        return last;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError(
        "WorkflowExecution "
            + executionId
            + " never reached a terminal state - last seen: "
            + last);
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
    var reviewerApiClient =
        new com.forwardmeasure.openworkflow.definition.management.client.ApiClient();
    reviewerApiClient.setBasePath(baseUrl);
    reviewerApiClient.setBearerToken(fixture.provisionReviewerToken(ROLE));

    WorkflowsApi workflows = new WorkflowsApi(authorApiClient);
    WorkflowDefinitionsApi definitions = new WorkflowDefinitionsApi(authorApiClient);
    WorkflowDefinitionGovernanceApi governance =
        new WorkflowDefinitionGovernanceApi(authorApiClient);
    WorkflowDefinitionGovernanceApi reviewerGovernance =
        new WorkflowDefinitionGovernanceApi(reviewerApiClient);

    Workflow workflow =
        workflows.createWorkflow(
            new CreateWorkflowRequest()
                .name("fds-workflow-bounded-worldcheck")
                .title("FDS workflow-bounded WorldCheck ingestion")
                .description(
                    "Real workflow-bounded proof - RealFowfWorkflowBoundedIngestionTest."));

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

    WorkflowDefinition submitted =
        governance.submitWorkflowDefinition(
            ifMatch(created.getRevision()), workflow.getId(), created.getId());

    WorkflowDefinition published =
        reviewerGovernance.publishWorkflowDefinition(
            ifMatch(submitted.getRevision()), workflow.getId(), created.getId());
    assertEquals("PUBLISHED", published.getStatus().name());
    return published;
  }

  /**
   * Writes {@code container}'s complete, Docker-API-buffered log (immune to this fixture's own
   * repeatedly-truncating live console capture) to {@code /tmp/fds-wf-bounded-<name>-docker.log} -
   * same mechanism as {@code RealFowfWorkflowFixtureEndToEndTest#dumpContainerLogs}.
   */
  private static void dumpContainerLogs(String name, GenericContainer<?> container) {
    if (container == null) return;
    try {
      Process process =
          new ProcessBuilder("docker", "logs", container.getContainerId())
              .redirectOutput(new java.io.File("/tmp/fds-wf-bounded-" + name + "-docker.log"))
              .redirectErrorStream(true)
              .start();
      process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS);
    } catch (java.io.IOException | InterruptedException failure) {
      System.err.println("Failed to dump logs for " + name + ": " + failure);
    }
  }

  private static String ifMatch(Long revision) {
    return "\"" + revision + "\"";
  }

  private static Map<String, Object> decodeClaims(String jwt) {
    String[] segments = jwt.split("\\.");
    byte[] payload = Base64.getUrlDecoder().decode(segments[1]);
    try {
      return MAPPER.readValue(payload, new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
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

  /**
   * The dispatched pod decodes both base64 blobs the same way {@code
   * DirectIngestionLauncher#reconstructSpecAndRunCommand} does for the Direct-Job path (a plain
   * shell one-liner - {@code KubernetesJobSpec} has no volume-mounting concept either), then execs
   * the real Pekko runner against the reconstructed spec file - byte-for-byte the same real
   * dispatch shape the Direct-Job matrix cells already prove, just launched by fowf instead of this
   * repo's own launcher.
   */
  private static String workflowSource(
      String asyncApiUrl,
      String specBase64,
      String seedTsvBase64,
      String gatewayIp,
      String pekkoImage) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-workflow-bounded
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
                  namespace: streaming
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
                namespace: streaming
                consume:
                  until: '${ .payload.status == "SUCCEEDED" or .payload.status == "FAILED" or .payload.status == "CANCELLED" }'
                  for: PT5M
            cancellation:
              channel: ingestion.worldcheck.cancellations
              message:
                payload:
                  namespace: streaming
    """
        .formatted(asyncApiUrl, pekkoImage, gatewayIp, seedTsvBase64, specBase64);
  }

  private static final String ASYNCAPI_DOCUMENT =
      """
      asyncapi: 2.6.0
      info:
        title: WorldCheck Ingestion Worker (kubernetes-job)
        version: 1.0.0
        description: >
          Command/event/cancellation contract for launching this repo's own real Pekko executor
          image as a Kubernetes Job via fowf's correlated-worker construct, protocol kubernetes-job.
          AsyncAPI 2.6 (channel-embedded publish/subscribe), not 3.0 - mirrors
          forwardmeasure-entity-intelligence's own real ingestion-worker-kubernetes-job.yaml exactly.
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
