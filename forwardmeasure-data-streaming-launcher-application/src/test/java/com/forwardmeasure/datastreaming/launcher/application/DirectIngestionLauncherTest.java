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
package com.forwardmeasure.datastreaming.launcher.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationDecision;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import com.forwardmeasure.testcontainers.junit.kubernetes.WithKubernetesContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real, no-mocks proof against an actual single-node K3s cluster that {@link
 * DirectIngestionLauncher} correctly encodes an {@link IngestionSpec} into a Job's env var, that
 * the pod-side reconstruction (base64 decode -> file -> exec) genuinely works, and that the
 * planner- driven engine dispatch (see {@code ExecutionPlanCompiler}) genuinely picks the right
 * runner command - using stand-in {@code pekkoRunnerCommand}/{@code kafkaStreamsRunnerCommand}
 * values ({@code grep}, not real packaged runner images; see this class's own javadoc for why no
 * such image exists for every engine yet) that verify a marker value from the spec's own source URI
 * survived the whole round trip.
 */
@WithKubernetesContainer
final class DirectIngestionLauncherTest {

  private static final String NAMESPACE = "launcher-test";
  private static final String STAND_IN_IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final String MARKER = "launcher-test-marker-9f3c";
  private static final AuthorizationService AUTHORIZATION = new PermitAllAuthorizationService();
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  @BeforeAll
  static void createNamespace(KubernetesTestContainer kubernetes) {
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }
  }

  @Test
  @Timeout(180)
  void launchReconstructsTheIngestionSpecInsideThePod(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q " + MARKER,
            STAND_IN_IMAGE,
            "grep -q this-marker-only-exists-in-the-kafka-streams-path-never-in-this-test");
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            ingestionSpecWithMarker("file"),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      String jobName = launcher.launch(client, request, ACTOR);
      assertEquals(
          DirectIngestionLauncher.deterministicJobName(ACTOR, request.correlationId()), jobName);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(180)
  void launchDispatchesToTheKafkaStreamsRunnerForAKafkaSourcedSpec(
      KubernetesTestContainer kubernetes) throws InterruptedException {
    // Deliberately distinguishing stand-in commands, not the same one for both engines: the pekko
    // command looks for a marker that never appears in this spec's own source URI, so if dispatch
    // ever picked the wrong (pekko) command by mistake, the Job would FAIL, not just "happen to
    // also succeed" - a real, not incidental, proof that ExecutionPlanCompiler's own
    // kafka-source-in-BOUNDED-mode -> KAFKA_STREAMS rule actually drove dispatch.
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q this-marker-only-exists-in-the-pekko-path-never-in-this-test",
            STAND_IN_IMAGE,
            "grep -q " + MARKER,
            List.of());
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            ingestionSpecWithMarker("kafka"),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      launcher.launch(client, request, ACTOR);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(60)
  void launchRejectsAContinuousSpecWithoutTouchingTheCluster(KubernetesTestContainer kubernetes) {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q " + MARKER,
            STAND_IN_IMAGE,
            "grep -q " + MARKER);
    IngestionSpec continuousSpec = ingestionSpecWithMarker("kafka", ExecutionMode.CONTINUOUS);
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(), NAMESPACE, continuousSpec, Map.of(), Map.of(), null);

    try (KubernetesClient client = kubernetes.createClient()) {
      assertThrows(
          UnsupportedOperationException.class, () -> launcher.launch(client, request, ACTOR));
      assertTrue(
          client.batch().v1().jobs().inNamespace(NAMESPACE).list().getItems().stream()
              .noneMatch(
                  job ->
                      job.getMetadata()
                          .getName()
                          .equals(
                              DirectIngestionLauncher.deterministicJobName(
                                  ACTOR, request.correlationId()))));
    }
  }

  @Test
  @Timeout(180)
  void cancelDeletesARealRunningJob(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "sh -c 'sleep 300 #'",
            STAND_IN_IMAGE,
            "sh -c 'sleep 300 #'");
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            ingestionSpecWithMarker("file"),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      launcher.launch(client, request, ACTOR);

      Optional<KubernetesJobObservation> beforeCancel;
      do {
        beforeCancel = launcher.observe(client, NAMESPACE, request.correlationId(), ACTOR);
        Thread.sleep(200);
      } while (beforeCancel.isEmpty());

      launcher.cancel(client, NAMESPACE, request.correlationId(), ACTOR);

      long deadline = System.currentTimeMillis() + 60_000;
      Optional<KubernetesJobObservation> afterCancel;
      do {
        afterCancel = launcher.observe(client, NAMESPACE, request.correlationId(), ACTOR);
        if (afterCancel.isEmpty()) {
          break;
        }
        Thread.sleep(200);
      } while (System.currentTimeMillis() < deadline);

      assertTrue(afterCancel.isEmpty(), "expected the Job to be gone after cancel()");
    }
  }

  @Test
  @Timeout(180)
  void replacementLauncherPreservesPlannedDeliveryAndRejectsChangedIdempotencyPayload(
      KubernetesTestContainer kubernetes) throws Exception {
    var spec =
        com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures
            .boundedKafkaSpecWithScreening(
                "unused:9092", MARKER, "http://unused:9200", java.nio.file.Path.of(""));
    var request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            spec,
            Map.of("memory", "16Mi"),
            Map.of("memory", "64Mi"),
            120L);
    try (KubernetesClient client = kubernetes.createClient()) {
      stagedLauncher().launch(client, request, ACTOR);
      var changed =
          new DirectLaunchRequest(
              request.correlationId(),
              NAMESPACE,
              spec,
              request.resourceRequests(),
              request.resourceLimits(),
              121L);
      assertThrows(
          IllegalArgumentException.class, () -> stagedLauncher().launch(client, changed, ACTOR));
      // The replacement has no in-memory record of the original launch.
      var replacement = stagedLauncher();
      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          pollUntilTerminal(replacement, client, request.correlationId()).phase());
      var delivery =
          client
              .batch()
              .v1()
              .jobs()
              .inNamespace(NAMESPACE)
              .withName(
                  DirectIngestionLauncher.deterministicJobName(
                      ACTOR, request.correlationId() + ":delivery"))
              .get();
      assertEquals(120L, delivery.getSpec().getActiveDeadlineSeconds());
      var container = delivery.getSpec().getTemplate().getSpec().getContainers().getFirst();
      assertEquals(
          new io.fabric8.kubernetes.api.model.Quantity("64Mi"),
          container.getResources().getLimits().get("memory"));
      assertTrue(
          container.getEnv().stream()
              .anyMatch(env -> "MERGE_POLICY_YAML_BASE64".equals(env.getName())));
    }
  }

  @Test
  void stagedLaunchChecksNamespaceBeforeAnyClusterMutation() {
    var spec =
        com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures
            .boundedKafkaSpecWithScreening(
                "unused:9092", MARKER, "http://unused:9200", java.nio.file.Path.of(""));
    var request =
        new DirectLaunchRequest("denied", "not-authorized", spec, Map.of(), Map.of(), null);
    assertThrows(SecurityException.class, () -> stagedLauncher().launch(null, request, ACTOR));
  }

  private static DirectIngestionLauncher stagedLauncher() {
    return new DirectIngestionLauncher(
        IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
        AUTHORIZATION,
        STAND_IN_IMAGE,
        "true",
        STAND_IN_IMAGE,
        "true",
        List.of(),
        Map.of(),
        STAND_IN_IMAGE,
        "grep -q " + MARKER,
        "unused:9092");
  }

  private static IngestionSpec ingestionSpecWithMarker(String sourceConnector) {
    return ingestionSpecWithMarker(sourceConnector, ExecutionMode.BOUNDED);
  }

  private static IngestionSpec ingestionSpecWithMarker(
      String sourceConnector, ExecutionMode executionMode) {
    return new IngestionSpec(
        List.of(
            new SourcePlan(
                "single",
                new SourceSpec(
                    sourceConnector, "file:///" + MARKER, new SourceSpec.FormatSpec("csv"), null),
                new TransformSpec("party", List.of()),
                1.0)),
        null,
        null,
        new SinkSpec("opensearch", "test-index", null, null),
        executionMode,
        new DeliverySemantics(true, new ConcurrencySpec(1, 1), null),
        null);
  }

  private static KubernetesJobObservation pollUntilTerminal(
      DirectIngestionLauncher launcher, KubernetesClient client, String correlationId)
      throws InterruptedException {
    while (true) {
      Optional<KubernetesJobObservation> observation =
          launcher.observe(client, NAMESPACE, correlationId, ACTOR);
      if (observation.isPresent() && isTerminal(observation.get().phase())) {
        return observation.get();
      }
      Thread.sleep(500);
    }
  }

  private static boolean isTerminal(KubernetesJobObservation.Phase phase) {
    return phase == KubernetesJobObservation.Phase.SUCCEEDED
        || phase == KubernetesJobObservation.Phase.FAILED;
  }

  /**
   * A local, file-scoped stand-in for {@link AuthorizationService} - deliberately not a shared,
   * importable-from-anywhere stub class. The shared {@code StubAuthorizationService} this class
   * used to import was removed repo-wide 2026-09-20: it masked a real Keycloak Organizations-group
   * authorization bug elsewhere in the product (native Role policies don't see roles granted only
   * via Organization membership). What's under test here is real Kubernetes Job launch/observe/
   * cancel mechanics against a real K3s cluster, not the authorization decision itself, so a
   * permissive local fake - not a real Keycloak-backed check - is the right amount of realism.
   */
  private static final class PermitAllAuthorizationService implements AuthorizationService {
    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      return new AuthorizationDecision(true, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }
}
