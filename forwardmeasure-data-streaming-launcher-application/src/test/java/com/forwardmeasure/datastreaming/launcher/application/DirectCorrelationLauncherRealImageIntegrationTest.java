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
 * The Spark sibling of {@code DirectIngestionLauncherRealImageIntegrationTest}: pulls the actual
 * private {@code docker.io/forwardmeasure/data-streaming-executor-spark} image into a real K3s
 * cluster via a real {@code imagePullSecret} and runs its real {@code SparkIngestionRunner.main()}
 * against one real, seeded CSV row - the same real-image proof, this time for the Spark/{@link
 * CorrelationSpec} path.
 *
 * <p>Needs real Docker Hub credentials the test never hardcodes - skipped if {@code
 * DOCKER_HUB_USERNAME}/{@code DOCKER_HUB_TOKEN} aren't set.
 */
@WithKubernetesContainer
final class DirectCorrelationLauncherRealImageIntegrationTest {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DirectCorrelationLauncherRealImageIntegrationTest.class);
  private static final String NAMESPACE = "real-spark-image-test";
  private static final String SPARK_IMAGE =
      "docker.io/forwardmeasure/data-streaming-executor-spark@sha256:"
          + "bc04957003c4796c0e08d27b06de524a20d61862f639d48fdecd602290d6f966";
  private static final String PULL_SECRET_NAME = "dockerhub-pull-secret";
  private static final String SEED_AND_RUN_COMMAND =
      "sh -c 'printf \"ID,FULL_NAME\\nS1,Alice Anderson\\n\" > /tmp/source.csv && "
          + "exec java -jar /deployments/application.jar /tmp/correlation-spec.yaml'";

  @Test
  @Timeout(300)
  void launchesTheRealSparkImageAndProcessesARealRowEndToEnd(KubernetesTestContainer kubernetes)
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

      DirectCorrelationLauncher launcher =
          new DirectCorrelationLauncher(
              IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(SPARK_IMAGE)),
              SPARK_IMAGE,
              SEED_AND_RUN_COMMAND,
              List.of(PULL_SECRET_NAME));
      DirectCorrelationLaunchRequest request =
          new DirectCorrelationLaunchRequest(
              UUID.randomUUID().toString(),
              NAMESPACE,
              realCorrelationSpec(),
              Map.of(),
              Map.of(),
              null);

      String jobName = launcher.launch(client, request);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          observation.phase(),
          () ->
              "job did not succeed (" + observation + ") - pod logs:\n" + podLogs(client, jobName));
    }
  }

  private static CorrelationSpec realCorrelationSpec() {
    return new CorrelationSpec(
        List.of(
            new CorrelationSpec.SourceEntry(
                "core",
                new SourceSpec("file", "/tmp/source.csv", null, null),
                new TransformSpec(
                    "party",
                    List.of(
                        new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                        new TransformSpec.FieldRule(
                            "name", "FULL_NAME", null, null, null, null, null))),
                1.0)),
        "uid",
        new SinkSpec("file", "/tmp/out", null, null, null),
        new ExecutionSpec("spark", new ExecutionSpec.ConcurrencySpec(1, 1), null, null));
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
      DirectCorrelationLauncher launcher, KubernetesClient client, String correlationId)
      throws InterruptedException {
    KubernetesJobObservation.Phase lastLoggedPhase = null;
    while (true) {
      Optional<KubernetesJobObservation> observation =
          launcher.observe(client, NAMESPACE, correlationId);
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
}
