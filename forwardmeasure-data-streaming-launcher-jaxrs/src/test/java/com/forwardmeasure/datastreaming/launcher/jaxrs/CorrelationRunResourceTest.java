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
package com.forwardmeasure.datastreaming.launcher.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLauncher;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.RunAccepted;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import com.forwardmeasure.testcontainers.junit.kubernetes.WithKubernetesContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real, no-mocks proof against an actual single-node K3s cluster that {@link
 * CorrelationRunResource} correctly wires HTTP semantics around {@link DirectCorrelationLauncher} -
 * the exact sibling of {@code IngestionRunResourceTest} for the Spark/{@link CorrelationSpec} path.
 */
@WithKubernetesContainer
final class CorrelationRunResourceTest {

  private static final String NAMESPACE = "correlation-run-resource-test";
  private static final String STAND_IN_IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final String MARKER = "correlation-run-resource-marker-4e91";

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
  void createReturnsAcceptedAndGetPollsThroughToSucceeded(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    try (KubernetesClient client = kubernetes.createClient()) {
      CorrelationRunResource resource =
          new CorrelationRunResource(
              new DirectCorrelationLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "grep -q " + MARKER),
              client);
      DirectCorrelationLaunchRequest request =
          new DirectCorrelationLaunchRequest(
              UUID.randomUUID().toString(),
              NAMESPACE,
              correlationSpecWithMarker(),
              Map.of(),
              Map.of(),
              null);

      Response created = resource.create(request);
      assertEquals(202, created.getStatus());
      assertNotNull(created.getHeaderString("Location"));
      RunAccepted accepted = (RunAccepted) created.getEntity();
      assertEquals(request.correlationId(), accepted.correlationId());
      assertEquals(
          DirectCorrelationLauncher.deterministicJobName(request.correlationId()),
          accepted.jobName());

      KubernetesJobObservation observation = pollUntilTerminal(resource, request.correlationId());
      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(60)
  void getReturnsNotFoundForAnUnknownCorrelationId(KubernetesTestContainer kubernetes) {
    try (KubernetesClient client = kubernetes.createClient()) {
      CorrelationRunResource resource =
          new CorrelationRunResource(
              new DirectCorrelationLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "grep -q " + MARKER),
              client);

      Response response = resource.get("no-such-correlation-id", NAMESPACE);

      assertEquals(404, response.getStatus());
    }
  }

  @Test
  @Timeout(60)
  void getAndCancelRejectAMissingNamespace(KubernetesTestContainer kubernetes) {
    try (KubernetesClient client = kubernetes.createClient()) {
      CorrelationRunResource resource =
          new CorrelationRunResource(
              new DirectCorrelationLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "grep -q " + MARKER),
              client);

      assertThrows(BadRequestException.class, () -> resource.get("some-id", null));
      assertThrows(BadRequestException.class, () -> resource.cancel("some-id", " "));
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
      CorrelationRunResource resource, String correlationId) throws InterruptedException {
    while (true) {
      Response response = resource.get(correlationId, NAMESPACE);
      if (response.getStatus() == 200) {
        KubernetesJobObservation observation = (KubernetesJobObservation) response.getEntity();
        if (observation.phase() == KubernetesJobObservation.Phase.SUCCEEDED
            || observation.phase() == KubernetesJobObservation.Phase.FAILED) {
          return observation;
        }
      }
      Thread.sleep(500);
    }
  }
}
