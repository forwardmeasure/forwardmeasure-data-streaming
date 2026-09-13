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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectLaunchRequest;
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
 * Real, no-mocks proof against an actual single-node K3s cluster that {@link IngestionRunResource}
 * correctly wires HTTP semantics (status codes, {@code Location}, response bodies) around {@link
 * DirectIngestionLauncher} - same stand-in image/command approach {@code
 * DirectIngestionLauncherTest} already uses, since no real Pekko runner image exists yet either.
 */
@WithKubernetesContainer
final class IngestionRunResourceTest {

  private static final String NAMESPACE = "ingestion-run-resource-test";
  private static final String STAND_IN_IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final String MARKER = "ingestion-run-resource-marker-2b6d";

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
      IngestionRunResource resource =
          new IngestionRunResource(
              new DirectIngestionLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "grep -q " + MARKER),
              client);
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(),
              NAMESPACE,
              ingestionSpecWithMarker(),
              Map.of(),
              Map.of(),
              null);

      Response created = resource.create(request);
      assertEquals(202, created.getStatus());
      assertNotNull(created.getHeaderString("Location"));
      RunAccepted accepted = (RunAccepted) created.getEntity();
      assertEquals(request.correlationId(), accepted.correlationId());
      assertEquals(
          DirectIngestionLauncher.deterministicJobName(request.correlationId()),
          accepted.jobName());

      KubernetesJobObservation observation = pollUntilTerminal(resource, request.correlationId());
      assertEquals(KubernetesJobObservation.Phase.SUCCEEDED, observation.phase());
    }
  }

  @Test
  @Timeout(60)
  void getReturnsNotFoundForAnUnknownCorrelationId(KubernetesTestContainer kubernetes) {
    try (KubernetesClient client = kubernetes.createClient()) {
      IngestionRunResource resource =
          new IngestionRunResource(
              new DirectIngestionLauncher(
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
      IngestionRunResource resource =
          new IngestionRunResource(
              new DirectIngestionLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "grep -q " + MARKER),
              client);

      assertThrows(BadRequestException.class, () -> resource.get("some-id", null));
      assertThrows(BadRequestException.class, () -> resource.cancel("some-id", " "));
    }
  }

  @Test
  @Timeout(180)
  void cancelDeletesARealRunningJob(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    try (KubernetesClient client = kubernetes.createClient()) {
      IngestionRunResource resource =
          new IngestionRunResource(
              new DirectIngestionLauncher(
                  IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(STAND_IN_IMAGE)),
                  STAND_IN_IMAGE,
                  "sh -c 'sleep 300 #'"),
              client);
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(),
              NAMESPACE,
              ingestionSpecWithMarker(),
              Map.of(),
              Map.of(),
              null);

      resource.create(request);

      Response beforeCancel;
      do {
        beforeCancel = resource.get(request.correlationId(), NAMESPACE);
        Thread.sleep(200);
      } while (beforeCancel.getStatus() == 404);

      Response cancelled = resource.cancel(request.correlationId(), NAMESPACE);
      assertEquals(204, cancelled.getStatus());

      long deadline = System.currentTimeMillis() + 60_000;
      Response afterCancel;
      do {
        afterCancel = resource.get(request.correlationId(), NAMESPACE);
        if (afterCancel.getStatus() == 404) {
          break;
        }
        Thread.sleep(200);
      } while (System.currentTimeMillis() < deadline);

      assertEquals(404, afterCancel.getStatus(), "expected the Job to be gone after cancel()");
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
      IngestionRunResource resource, String correlationId) throws InterruptedException {
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
