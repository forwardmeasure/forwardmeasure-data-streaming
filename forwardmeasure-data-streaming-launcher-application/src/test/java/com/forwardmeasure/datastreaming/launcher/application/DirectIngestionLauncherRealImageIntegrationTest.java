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
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The real thing, not a stand-in: pulls the actual {@code
 * docker.io/forwardmeasure/data-streaming-executor-pekko} image (private - built and pushed
 * 2026-09-13, see this repo's own live gap tracker) into a real K3s cluster via a real {@code
 * imagePullSecret}, runs its real {@code PekkoIngestionRunner.main()}, and proves a real CSV row
 * flows through a real Camel source, the real transform engine, and the real (async, non-lossy)
 * Camel sink this session fixed - the same class of proof {@code DirectIngestionLauncherTest}
 * already gives for the launch/observe/cancel mechanics using a {@code busybox} stand-in, this time
 * for the actual runner image with nothing substituted.
 *
 * <p>Needs real Docker Hub credentials the test never hardcodes - skipped if {@code
 * DOCKER_HUB_USERNAME}/{@code DOCKER_HUB_TOKEN} aren't set (the same two environment variables
 * {@code KubernetesJobLifecycleTest}'s own private-registry proof uses).
 *
 * <p>The Job's {@code command} always replaces the image's own {@code ENTRYPOINT} (real Kubernetes
 * semantics - see {@link DirectIngestionLauncher}'s own {@code reconstructSpecAndRunCommand}), so
 * this test's {@code pekkoRunnerCommand} first seeds a tiny real CSV fixture at {@code
 * /tmp/source.csv} before invoking the real {@code java -jar /deployments/application.jar} - there
 * is no volume-mounting concept in {@code KubernetesJobSpec} (deliberate, see its own javadoc), so
 * this is the only way to give the real pod real input data without adding one.
 *
 * <p><b>2026-09-21</b>: this class's former Spark sibling ({@code
 * DirectCorrelationLauncherRealImageIntegrationTest}, which pulled the real {@code
 * data-streaming-executor-spark} image and ran {@code SparkCorrelationRunner.main()} directly) was
 * removed, not merged in - {@link DirectIngestionLauncher} no longer accepts a raw Spark image at
 * all now that Spark is an optional compute stage, never a delivery engine (see this class's own
 * javadoc). A real second image proof belongs here once a real, pushed Kafka Streams executor image
 * exists (see the repo's own gap-bridging plan, Phase C) - not before, since there is nothing real
 * yet to point it at.
 */
@WithKubernetesContainer
final class DirectIngestionLauncherRealImageIntegrationTest {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DirectIngestionLauncherRealImageIntegrationTest.class);
  private static final String NAMESPACE = "real-pekko-image-test";

  /**
   * Re-pinned 2026-09-21: the previous digest's image had no real SLF4J provider on its runtime
   * classpath ({@code slf4j-simple} was {@code test}-scoped only in {@code -executor-pekko}'s own
   * pom) - Pekko's own logging bootstrap fails hard with no provider bound, causing an immediate
   * {@code CoordinatedShutdown} before any real work ran, which surfaced as this exact test's own
   * {@code BackoffLimitExceeded} failure the first time real credentials let it actually run
   * instead of skip. Fixed and re-pushed under the same {@code 1.1.0} tag.
   */
  private static final String PEKKO_IMAGE =
      "docker.io/forwardmeasure/data-streaming-executor-pekko@sha256:"
          + "a1eab0f12073b4b2f0215fd350061522ffa01aca08ab259a81e8053ee04a90e3";

  private static final String PULL_SECRET_NAME = "dockerhub-pull-secret";

  /**
   * No real, pushed Kafka Streams executor image exists yet (see this repo's own gap-bridging plan,
   * Phase C) - this placeholder is never actually dispatched, since {@link #realIngestionSpec()}'s
   * file-sourced, all-{@code LIGHT}-transform spec always resolves to {@code PEKKO_STREAMS} via
   * {@code ExecutionPlanCompiler}. Revisit once that image is real.
   */
  private static final String KAFKA_STREAMS_IMAGE_PLACEHOLDER = "unused";

  private static final String SEED_AND_RUN_COMMAND =
      "sh -c 'printf \"id,name\\nS1,Alice Anderson\\n\" > /tmp/source.csv && "
          + "exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml'";
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  @Test
  @Timeout(300)
  void launchesTheRealPekkoImageAndProcessesARealRowEndToEnd(KubernetesTestContainer kubernetes)
      throws InterruptedException {
    String username = System.getenv("DOCKER_HUB_USERNAME");
    String token = System.getenv("DOCKER_HUB_TOKEN");
    Assumptions.assumeTrue(
        username != null && token != null,
        "DOCKER_HUB_USERNAME/DOCKER_HUB_TOKEN not set - skipping the real private-image proof");

    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
      client
          .secrets()
          .inNamespace(NAMESPACE)
          .resource(dockerConfigSecret(username, token))
          .create();

      DirectIngestionLauncher launcher =
          new DirectIngestionLauncher(
              IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(PEKKO_IMAGE)),
              new PermitAllAuthorizationService(),
              PEKKO_IMAGE,
              SEED_AND_RUN_COMMAND,
              KAFKA_STREAMS_IMAGE_PLACEHOLDER,
              KAFKA_STREAMS_IMAGE_PLACEHOLDER,
              List.of(PULL_SECRET_NAME));
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(),
              NAMESPACE,
              realIngestionSpec(),
              Map.of(),
              Map.of(),
              null);

      String jobName = launcher.launch(client, request, ACTOR);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          observation.phase(),
          () ->
              "job did not succeed (" + observation + ") - pod logs:\n" + podLogs(client, jobName));
    }
  }

  private static IngestionSpec realIngestionSpec() {
    return new IngestionSpec(
        List.of(
            new SourcePlan(
                "single",
                new SourceSpec(
                    "file",
                    "file:/tmp?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                    new SourceSpec.FormatSpec("csv"),
                    null),
                new TransformSpec(
                    "party",
                    List.of(
                        new TransformSpec.FieldRule("uid", "id", null, null, null, null, null),
                        new TransformSpec.FieldRule("name", "name", null, null, null, null, null))),
                1.0)),
        null,
        null,
        new SinkSpec("file", "file:/tmp?fileName=output.jsonl&fileExist=Append", null, null, null),
        ExecutionMode.BOUNDED,
        new DeliverySemantics(true, new ConcurrencySpec(1, 1), null),
        null);
  }

  private static Secret dockerConfigSecret(String username, String token) {
    String auth =
        Base64.getEncoder()
            .encodeToString((username + ":" + token).getBytes(StandardCharsets.UTF_8));
    String dockerConfigJson =
        "{\"auths\":{\"https://index.docker.io/v1/\":{\"username\":\""
            + username
            + "\",\"password\":\""
            + token
            + "\",\"auth\":\""
            + auth
            + "\"}}}";
    return new SecretBuilder()
        .withNewMetadata()
        .withName(PULL_SECRET_NAME)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withType("kubernetes.io/dockerconfigjson")
        .addToData(
            ".dockerconfigjson",
            Base64.getEncoder().encodeToString(dockerConfigJson.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

  /**
   * Real diagnostic, not a guess: the actual stdout/stderr of the pod the Job ran, fetched while
   * the cluster still exists - {@code @WithKubernetesContainer} tears the cluster down once the
   * test method returns, so a failure with no captured logs is a dead end after the fact.
   */
  private static String podLogs(KubernetesClient client, String jobName) {
    List<Pod> pods =
        client.pods().inNamespace(NAMESPACE).withLabel("job-name", jobName).list().getItems();
    StringBuilder logs = new StringBuilder();
    for (Pod pod : pods) {
      logs.append("--- ").append(pod.getMetadata().getName()).append(" ---\n");
      try {
        logs.append(
            client.pods().inNamespace(NAMESPACE).withName(pod.getMetadata().getName()).getLog());
      } catch (RuntimeException e) {
        logs.append("(failed to fetch logs: ").append(e).append(")\n");
      }
    }
    return logs.toString();
  }

  private static KubernetesJobObservation pollUntilTerminal(
      DirectIngestionLauncher launcher, KubernetesClient client, String correlationId)
      throws InterruptedException {
    KubernetesJobObservation.Phase lastLoggedPhase = null;
    while (true) {
      Optional<KubernetesJobObservation> observation =
          launcher.observe(client, NAMESPACE, correlationId, ACTOR);
      if (observation.isPresent() && observation.get().phase() != lastLoggedPhase) {
        lastLoggedPhase = observation.get().phase();
        LOGGER.info("observed {}", observation.get());
      }
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
   * via Organization membership). What's under test here is the real, private Pekko runner image
   * against a real K3s cluster, not the authorization decision itself, so a permissive local fake -
   * not a real Keycloak-backed check - is the right amount of realism.
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
