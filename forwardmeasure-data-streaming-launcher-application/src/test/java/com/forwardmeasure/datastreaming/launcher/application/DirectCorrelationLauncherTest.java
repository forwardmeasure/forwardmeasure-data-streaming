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

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
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
      String jobName = launcher.launch(client, request);
      assertEquals(
          DirectCorrelationLauncher.deterministicJobName(request.correlationId()), jobName);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
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
