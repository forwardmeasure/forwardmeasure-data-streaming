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
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
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
 * Phase D's real final proof: every stage this fixture built (1 through 6) running together as real
 * separate processes, driving one real {@code CONTINUOUS}-shaped {@code kubernetes-deployment}
 * workflow execution, through FDS's own real {@link WorkflowIngestionLauncher} - not a hand-built
 * {@code ProtocolOperationDescriptor} (that's what {@code
 * RealAsyncApiKubernetesDeploymentDispatchTest} in forwardmeasure-openworkflow already proves), and
 * not a partial-stage boot proof (that's what Stages 1-6 each already proved individually). This is
 * the first time this exact combination - FDS's own launcher, real fowf
 * definition-management/execution-management/engine/operation-adapter, and a real K3s cluster - has
 * ever been driven together end to end.
 *
 * <p>Reuses Stage 5's exact, already-schema-fixed workflow/AsyncAPI document shape (see {@code
 * RealFowfWorkflowFixtureStage5Test}) and Stage 6's exact operation-adapter wiring (see {@code
 * RealFowfWorkflowFixtureStage6Test}) - this test's own value is wiring all of it together and
 * driving it from FDS's real launcher, not reinventing either half.
 *
 * <p><b>Parameterized over both real fowf engines</b> (added 2026-09-21, after the first version of
 * this test/fixture only ever wired the Kafka Streams one - a real parity gap, not a deliberate
 * scope decision: both engines dispatch {@code kubernetes-deployment} operations through the
 * identical {@code KafkaProtocolOperationExecutors.create(...)} factory, so both need the same real
 * proof, not just the one built first). {@code fixture.startEngineKafkaStreams()}/{@code
 * startEnginePekko()} are the only per-engine branch; everything else in this test is identical.
 */
class RealFowfWorkflowFixtureEndToEndTest {

  private static final String ROLE = "workflow-run-launcher";
  private static final String NAMESPACE = "streaming";
  private static final String CORRELATION_NAME = "fds-e2e-worker";
  private static final String IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ParameterizedTest(name = "engine={0}")
  @ValueSource(strings = {"kafka-streams", "pekko"})
  @Timeout(600)
  void workflowIngestionLauncherDrivesARealDeploymentToAvailableThroughEverySixStages(String engine)
      throws Exception {
    try (RealFowfWorkflowFixture fixture =
        RealFowfWorkflowFixture.start("fds-e2e-" + engine, ROLE)) {
      try (var k8s = fixture.kubernetes().createClient()) {
        k8s.namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }

      GenericContainer<?> executionManagement = fixture.startExecutionManagement(engine);
      boolean isPekko = engine.equals("pekko");
      GenericContainer<?> enginePekko = isPekko ? fixture.startEnginePekko() : null;
      if (!isPekko) {
        fixture.startEngineKafkaStreams();
      }
      GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
      GenericContainer<?> operationAdapter =
          fixture.startOperationAdapter(NAMESPACE, IMAGE, 5, isPekko);
      if (isPekko) {
        // Both nodes are now running - only now can Cluster Bootstrap actually converge (see
        // awaitPekkoClusterReady's own javadoc for why this can't be checked per-container
        // during either individual startOperationAdapter/startEnginePekko call).
        fixture.awaitPekkoClusterReady(enginePekko, operationAdapter);
      }

      String asyncApiUrl =
          fixture.startAsyncApiDocumentServer(
              "/stream-worker-kubernetes-deployment.yaml", ASYNCAPI_DOCUMENT);
      WorkflowDefinition published = publish(fixture, definitionManagement, asyncApiUrl);

      // FDS's own launcher-side AuthZEN check (WorkflowIngestionLauncher#launch) is a SEPARATE
      // mechanism from fowf's own server-side check RealFowfWorkflowFixture#start already granted
      // (openworkflow-execution/executions) - a real, distinct resource this fixture has not
      // granted for any earlier stage, since no earlier stage exercised this launcher at all.
      fixture
          .keycloak()
          .grantResourceAuthorization(
              fixture.organizationId(),
              "datastreaming-workflow-run",
              "workflow-runs",
              "e2e-workflow-run-permission",
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
              "fds-e2e-test-v1");

      String executionManagementBaseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);
      var executionApiClient = new com.forwardmeasure.openworkflow.execution.client.ApiClient();
      executionApiClient.setBasePath(executionManagementBaseUrl);
      executionApiClient.setBearerToken(accessToken);
      ExecutionsApi executionsApi = new ExecutionsApi(executionApiClient);

      WorkflowIngestionLauncher launcher =
          new WorkflowIngestionLauncher(executionsApi, authorization);

      String correlationId = "fds-e2e-" + UUID.randomUUID();
      Execution started =
          launcher.launch(
              new WorkflowLaunchRequest(
                  published.getId(), Map.of(), "idempotency-" + correlationId, correlationId),
              actor);
      assertNotNull(started.getId(), "a real fowf execution must have been admitted");

      Execution finalState;
      try {
        finalState = awaitTerminal(fixture, executionApiClient, launcher, started.getId(), actor);
      } catch (AssertionError neverTerminal) {
        // Real, live-confirmed 2026-09-23: this fixture's own console log capture (Slf4jLogConsumer
        // piped through the test harness's own tee/grep chain) has repeatedly, silently truncated
        // mid-run for reasons unrelated to this test's own logic - Testcontainers' own internal
        // per-container log buffer (GenericContainer#getLogs, pulled directly from the Docker API,
        // independent of any live-streaming consumer) is the one capture mechanism proven immune to
        // that truncation. Dumped here, once, straight to disk, so a stalled/never-terminal
        // execution's real root cause is diagnosable even when the piped console capture fails.
        dumpContainerLogs("engine-" + engine, isPekko ? enginePekko : null);
        dumpContainerLogs("operation-adapter-" + engine, operationAdapter);
        dumpContainerLogs("execution-management-" + engine, executionManagement);
        throw neverTerminal;
      }
      assertEquals(
          ExecutionState.COMPLETED,
          finalState.getState(),
          "expected the real 2-step apply+watch workflow to complete - got "
              + finalState.getState()
              + ", error="
              + finalState.getError());

      // Independent verification, not just trusting the engine's own COMPLETED claim: the real
      // Deployment this execution applied carries a real openworkflow.io/execution-id label
      // (AsyncApiKubernetesDeploymentOperationExecutor#apply's own real spec) - list by that label
      // rather than re-deriving the deterministic name, so this assertion is decoupled from that
      // internal naming scheme.
      //
      // Real, live-confirmed 2026-09-23: fowf's own AsyncApiKubernetesDeploymentOperationExecutor
      // (shared, unmodified, by both engines) only ever declares the workflow COMPLETED after its
      // own bounded poll observes the Deployment's real status.conditions[Available]=True - it
      // never re-derives readiness itself, so there is no fowf-side logic bug to fix here. This
      // check used to be a single, un-retried GET run immediately after COMPLETED - a real gap in
      // THIS TEST, not fowf: a fresh, independent client's GET has no guarantee of being
      // instantaneously consistent with whatever GET fowf's own poll loop last saw, and there is
      // no reason to assume zero propagation delay. Retrying briefly here (mirroring
      // awaitTerminal's own poll shape) is the correct fix - not a production code change.
      try (var k8s = fixture.kubernetes().createClient()) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        Deployment deployment = null;
        Integer readyReplicas = null;
        while (System.nanoTime() < deadline) {
          var deployments =
              k8s.apps()
                  .deployments()
                  .inNamespace(NAMESPACE)
                  .withLabel("openworkflow.io/execution-id", started.getId().toString())
                  .list()
                  .getItems();
          assertEquals(
              1, deployments.size(), "expected exactly one real Deployment for this execution");
          deployment = deployments.get(0);
          readyReplicas =
              deployment.getStatus() == null ? null : deployment.getStatus().getReadyReplicas();
          if (Integer.valueOf(1).equals(readyReplicas)) {
            return;
          }
          Thread.sleep(500);
        }
        assertTrue(
            deployment != null && Integer.valueOf(1).equals(readyReplicas),
            "the real Deployment must actually be Available (readyReplicas=1), not just applied"
                + " - last observed readyReplicas="
                + readyReplicas);
      }
    }
  }

  /**
   * Real access tokens from this fixture's own Keycloak realm are short-lived (a real, standard
   * default TTL, not something this test controls) - a single token minted once up front would go
   * stale mid-poll on a slow run and turn a real terminal-state check into a spurious 401
   * (confirmed live 2026-09-21: the first version of this loop reused one token for up to 5 minutes
   * of polling and hit exactly that). Re-minting on every poll keeps this loop testing the real
   * execution state, not Keycloak's own token lifetime.
   */
  private static Execution awaitTerminal(
      RealFowfWorkflowFixture fixture,
      com.forwardmeasure.openworkflow.execution.client.ApiClient executionApiClient,
      WorkflowIngestionLauncher launcher,
      UUID executionId,
      ActiveOrganization actor)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
    Execution last = null;
    while (System.nanoTime() < deadline) {
      executionApiClient.setBearerToken(fixture.keycloak().mintUserToken());
      last = launcher.observe(executionId, actor);
      System.out.println(
          "RealFowfWorkflowFixtureEndToEndTest: execution state = " + last.getState());
      if (last.getState() == ExecutionState.COMPLETED || last.getState() == ExecutionState.FAILED) {
        return last;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError(
        "Execution " + executionId + " never reached a terminal state - last seen: " + last);
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
                .name("fds-phase-d-e2e-stream-worker")
                .title("FDS Phase D end-to-end stream worker")
                .description("Real Phase D final proof - RealFowfWorkflowFixtureEndToEndTest."));

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
   * repeatedly-truncating live console capture - see the {@code AssertionError} catch above) to
   * {@code /tmp/fds-e2e-<name>.log}. {@code container} is {@code null} for the engine flavor this
   * invocation didn't start (e.g. engine-pekko during a kafka-streams run).
   */
  private static void dumpContainerLogs(String name, GenericContainer<?> container) {
    if (container == null) return;
    // Shells out to the real docker CLI directly, bypassing both this fixture's own console
    // capture (proven, live, repeatedly truncated for reasons unrelated to this test - confirmed
    // 2026-09-23 by a run that cut off within the first few seconds, during Liquibase migration
    // output, nowhere near the actual scenario) and Testcontainers' own GenericContainer#getLogs()
    // (also tried, also came back empty for reasons not yet root-caused). A raw "docker logs"
    // process, writing straight to a file, is the same mechanism a human would use by hand.
    try {
      Process process =
          new ProcessBuilder("docker", "logs", container.getContainerId())
              .redirectOutput(new java.io.File("/tmp/fds-e2e-" + name + "-docker.log"))
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
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  // Byte-for-byte the same real, schema-fixed shape as RealFowfWorkflowFixtureStage5Test - see
  // that class's own javadoc for why (subscription.filter, not literal fields).
  private static String workflowSource(String asyncApiUrl) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-phase-d
      name: e2e-stream-worker
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
                namespace: streaming
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
              filter: '${ {namespace: "streaming", name: "%s", readinessTimeoutSeconds: 120} }'
    """
        .formatted(asyncApiUrl, CORRELATION_NAME, asyncApiUrl, CORRELATION_NAME);
  }

  private static final String ASYNCAPI_DOCUMENT =
      """
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
