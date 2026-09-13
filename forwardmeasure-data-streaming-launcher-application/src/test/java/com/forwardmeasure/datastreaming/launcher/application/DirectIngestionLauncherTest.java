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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
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
 * DirectIngestionLauncher} correctly encodes an {@link IngestionSpec} into a Job's env var and that
 * the pod-side reconstruction (base64 decode -> file -> exec) genuinely works - using a stand-in
 * {@code pekkoRunnerCommand} ({@code grep}, not a real packaged runner image; see this class's own
 * javadoc for why no such image exists yet) that verifies a marker value from the spec's own {@code
 * source.uri} survived the whole round trip.
 */
@WithKubernetesContainer
final class DirectIngestionLauncherTest {

  private static final String NAMESPACE = "launcher-test";
  private static final String STAND_IN_IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final String MARKER = "launcher-test-marker-9f3c";

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
            STAND_IN_IMAGE,
            "grep -q " + MARKER);
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            ingestionSpecWithMarker(),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      String jobName = launcher.launch(client, request);
      assertEquals(DirectIngestionLauncher.deterministicJobName(request.correlationId()), jobName);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(60)
  void launchRejectsANonPekkoEngineWithoutTouchingTheCluster(KubernetesTestContainer kubernetes) {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            STAND_IN_IMAGE,
            "grep -q " + MARKER);
    IngestionSpec sparkSpec =
        new IngestionSpec(
            new SourceSpec("file", "file:///" + MARKER, null, null),
            new TransformSpec("party", List.of()),
            new SinkSpec("opensearch", "test-index", null, null),
            new ExecutionSpec("spark", null, null, null));
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(), NAMESPACE, sparkSpec, Map.of(), Map.of(), null);

    try (KubernetesClient client = kubernetes.createClient()) {
      assertThrows(UnsupportedOperationException.class, () -> launcher.launch(client, request));
      assertTrue(
          client.batch().v1().jobs().inNamespace(NAMESPACE).list().getItems().stream()
              .noneMatch(
                  job ->
                      job.getMetadata()
                          .getName()
                          .equals(
                              DirectIngestionLauncher.deterministicJobName(
                                  request.correlationId()))));
    }
  }

  @Test
  @Timeout(180)
  void cancelDeletesARealRunningJob(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
            STAND_IN_IMAGE,
            "sh -c 'sleep 300 #'");
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            UUID.randomUUID().toString(),
            NAMESPACE,
            ingestionSpecWithMarker(),
            Map.of(),
            Map.of(),
            null);

    try (KubernetesClient client = kubernetes.createClient()) {
      launcher.launch(client, request);

      Optional<KubernetesJobObservation> beforeCancel;
      do {
        beforeCancel = launcher.observe(client, NAMESPACE, request.correlationId());
        Thread.sleep(200);
      } while (beforeCancel.isEmpty());

      launcher.cancel(client, NAMESPACE, request.correlationId());

      long deadline = System.currentTimeMillis() + 60_000;
      Optional<KubernetesJobObservation> afterCancel;
      do {
        afterCancel = launcher.observe(client, NAMESPACE, request.correlationId());
        if (afterCancel.isEmpty()) {
          break;
        }
        Thread.sleep(200);
      } while (System.currentTimeMillis() < deadline);

      assertTrue(afterCancel.isEmpty(), "expected the Job to be gone after cancel()");
    }
  }

  private static IngestionSpec ingestionSpecWithMarker() {
    return new IngestionSpec(
        new SourceSpec("file", "file:///" + MARKER, new SourceSpec.FormatSpec("csv"), null),
        new TransformSpec("party", List.of()),
        new SinkSpec("opensearch", "test-index", null, null),
        new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(1, 1), null, null));
  }

  private static KubernetesJobObservation pollUntilTerminal(
      DirectIngestionLauncher launcher, KubernetesClient client, String correlationId)
      throws InterruptedException {
    while (true) {
      Optional<KubernetesJobObservation> observation =
          launcher.observe(client, NAMESPACE, correlationId);
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
