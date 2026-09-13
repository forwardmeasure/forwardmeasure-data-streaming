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

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobLifecycle;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Direct mode's Spark-correlation counterpart to {@link DirectIngestionLauncher} - runs a {@link
 * CorrelationSpec} as one real Kubernetes Job (the {@code SparkIngestionRunner} entrypoint), with
 * no fowf workflow engine involved, closing the "spark engine unlaunchable" gap {@link
 * DirectIngestionLauncher}'s own javadoc documented: {@code CorrelationSpec} and a real Spark
 * runner entrypoint both exist now, so the launcher can dispatch a correlation run the same way it
 * already dispatches a single-source Pekko one - same reconstruction mechanism (spec → YAML →
 * base64 env var → pod-side shell decode → exec), same {@link KubernetesJobLifecycle} delegation,
 * same {@link IngestionJobPolicy} authorization discipline, deliberately kept as its own class
 * rather than folded into {@link DirectIngestionLauncher} since the two run genuinely different
 * spec types against genuinely different runner images.
 */
public final class DirectCorrelationLauncher {

  private static final String JOB_NAME_PREFIX = "fds-corr-";
  private static final String SPEC_ENV_VAR = "CORRELATION_SPEC_YAML_BASE64";
  private static final String SPEC_FILE_PATH = "/tmp/correlation-spec.yaml";
  private static final String CORRELATION_ID_LABEL =
      "data-streaming.forwardmeasure.com/correlation-id";

  private final IngestionJobPolicy policy;
  private final String sparkRunnerImage;
  private final String sparkRunnerCommand;
  private final List<String> imagePullSecretNames;

  /**
   * @param sparkRunnerCommand the shell command that runs the reconstructed spec (e.g. {@code "java
   *     -jar runner.jar"} in a real deployment) - the spec's own file path is appended as its final
   *     argument, same convention as {@link DirectIngestionLauncher}'s own constructor.
   */
  public DirectCorrelationLauncher(
      IngestionJobPolicy policy, String sparkRunnerImage, String sparkRunnerCommand) {
    this(policy, sparkRunnerImage, sparkRunnerCommand, List.of());
  }

  /**
   * @param imagePullSecretNames names of existing {@code kubernetes.io/dockerconfigjson} Secrets in
   *     the target namespace - see {@link DirectIngestionLauncher}'s own matching constructor.
   */
  public DirectCorrelationLauncher(
      IngestionJobPolicy policy,
      String sparkRunnerImage,
      String sparkRunnerCommand,
      List<String> imagePullSecretNames) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.sparkRunnerImage = Objects.requireNonNull(sparkRunnerImage, "sparkRunnerImage");
    this.sparkRunnerCommand = Objects.requireNonNull(sparkRunnerCommand, "sparkRunnerCommand");
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }

  /**
   * Launches {@code request.correlationSpec()} as one Job. Returns the deterministic Job name
   * ({@link #deterministicJobName}) a caller uses with {@link #observe}/{@link #cancel}.
   */
  public String launch(KubernetesClient client, DirectCorrelationLaunchRequest request) {
    policy.authorizeNamespace(request.namespace());
    policy.authorizeImage(sparkRunnerImage);

    String jobName = deterministicJobName(request.correlationId());
    KubernetesJobSpec jobSpec =
        new KubernetesJobSpec(
            request.namespace(),
            jobName,
            Map.of(CORRELATION_ID_LABEL, labelSafe(request.correlationId())),
            sparkRunnerImage,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(sparkRunnerCommand)),
            Map.of(SPEC_ENV_VAR, encodeSpecYaml(request.correlationSpec())),
            request.resourceRequests(),
            request.resourceLimits(),
            1,
            1,
            0,
            request.activeDeadlineSeconds(),
            imagePullSecretNames);
    KubernetesJobLifecycle.launch(client, jobSpec);
    return jobName;
  }

  public Optional<KubernetesJobObservation> observe(
      KubernetesClient client, String namespace, String correlationId) {
    return KubernetesJobLifecycle.observe(client, namespace, deterministicJobName(correlationId));
  }

  public void cancel(KubernetesClient client, String namespace, String correlationId) {
    KubernetesJobLifecycle.cancel(client, namespace, deterministicJobName(correlationId));
  }

  /**
   * The same correlation id always resolves to the same Job name - see {@link
   * DirectCorrelationLaunchRequest}.
   */
  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(JOB_NAME_PREFIX, correlationId);
  }

  private static String reconstructSpecAndRunCommand(String runnerCommand) {
    return "echo \"$"
        + SPEC_ENV_VAR
        + "\" | base64 -d > "
        + SPEC_FILE_PATH
        + " && exec "
        + runnerCommand
        + " "
        + SPEC_FILE_PATH;
  }

  private static String encodeSpecYaml(CorrelationSpec spec) {
    try {
      return Base64.getEncoder().encodeToString(spec.toYaml().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("failed to serialize CorrelationSpec to YAML", e);
    }
  }

  private static String labelSafe(String value) {
    String sanitized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "-");
    return sanitized.length() > 63 ? sanitized.substring(0, 63) : sanitized;
  }
}
