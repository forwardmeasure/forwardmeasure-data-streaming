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
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.testkit.StubAuthorizationService;
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
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
 * DirectCorrelationLauncher} correctly encodes a {@link CorrelationSpec} into a Job's env var and
 * that the pod-side reconstruction (base64 decode -> file -> exec) genuinely works - same stand-in
 * {@code grep} approach as {@link DirectIngestionLauncherTest}, since no real Spark runner image
 * exists yet either (see this repo's own live gap tracker).
 */
@WithKubernetesContainer
final class DirectCorrelationLauncherTest {

  private static final String NAMESPACE = "correlation-launcher-test";
  private static final String STAND_IN_IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final String MARKER = "correlation-launcher-marker-71ab";
  private static final AuthorizationService AUTHORIZATION = StubAuthorizationService.permitAll();
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          TenantDatabase.forAlias("test-tenant"),
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
  void launchReconstructsTheCorrelationSpecInsideThePod(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    DirectCorrelationLauncher launcher =
        new DirectCorrelationLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q " + MARKER);
    DirectCorrelationLaunchRequest request =
        new DirectCorrelationLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            correlationSpecWithMarker(),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      String jobName = launcher.launch(client, request, ACTOR);
      assertEquals(
          DirectCorrelationLauncher.deterministicJobName(request.correlationId()), jobName);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(180)
  void launchDispatchesToThePekkoRunnerWhenTheSpecAsksForIt(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    // Deliberately distinguishing stand-in commands, same technique as
    // DirectIngestionLauncherTest's own equivalent test - if dispatch ever picked the wrong
    // (spark) command by mistake, the Job would FAIL, not just "happen to also succeed".
    DirectCorrelationLauncher launcher =
        new DirectCorrelationLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q this-marker-only-exists-in-the-spark-path-never-in-this-test",
            STAND_IN_IMAGE,
            "grep -q " + MARKER,
            List.of());
    DirectCorrelationLaunchRequest request =
        new DirectCorrelationLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            new CorrelationSpec(
                List.of(
                    new CorrelationSpec.SourceEntry(
                        "core",
                        new SourceSpec("file", "file:///" + MARKER, null, null),
                        new TransformSpec("party", List.of()),
                        1.0)),
                "uid",
                new SinkSpec("file", "/tmp/out", null, null, null),
                new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(1, 1), null, null)),
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
  void launchRejectsPekkoEngineWhenNotConfiguredWithoutTouchingTheCluster(
      KubernetesTestContainer kubernetes) {
    // Before 2026-09-13's fix, this launcher never checked execution.engine() at all - it would
    // have silently run this "pekko" spec through the spark stand-in command instead of rejecting
    // it. This test only became possible to write once that real, quiet bug was fixed.
    DirectCorrelationLauncher launcher =
        new DirectCorrelationLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            AUTHORIZATION,
            STAND_IN_IMAGE,
            "grep -q " + MARKER);
    DirectCorrelationLaunchRequest request =
        new DirectCorrelationLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            new CorrelationSpec(
                List.of(
                    new CorrelationSpec.SourceEntry(
                        "core",
                        new SourceSpec("file", "file:///" + MARKER, null, null),
                        new TransformSpec("party", List.of()),
                        1.0)),
                "uid",
                new SinkSpec("file", "/tmp/out", null, null, null),
                new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(1, 1), null, null)),
            Map.of(),
            Map.of(),
            null);

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
                              DirectCorrelationLauncher.deterministicJobName(
                                  request.correlationId()))));
    }
  }

  private static CorrelationSpec correlationSpecWithMarker() {
    return new CorrelationSpec(
        List.of(
            new CorrelationSpec.SourceEntry(
                "core",
                new SourceSpec("file", "file:///" + MARKER, null, null),
                new TransformSpec("party", List.of()),
                1.0)),
        "uid",
        new SinkSpec("file", "/tmp/out", null, null, null),
        new ExecutionSpec("spark", new ExecutionSpec.ConcurrencySpec(1, 1), null, null));
  }

  private static KubernetesJobObservation pollUntilTerminal(
      DirectCorrelationLauncher launcher, KubernetesClient client, String correlationId)
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
}
