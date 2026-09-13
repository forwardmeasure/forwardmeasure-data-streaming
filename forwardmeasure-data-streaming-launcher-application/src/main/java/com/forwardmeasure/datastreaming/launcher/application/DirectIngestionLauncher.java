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

import com.forwardmeasure.datastreaming.api.IngestionSpec;
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
 * Direct mode: runs an {@link IngestionSpec} as one real Kubernetes Job, with no fowf workflow
 * engine involved at all - the async-REST, invoke-ingestion-from-outside-fowf capability in its
 * bypass form (see {@code WorkflowIngestionLauncher} for the other, through-fowf form).
 *
 * <p>Delegates the actual Job manifest/status/deletion mechanics to {@link KubernetesJobLifecycle}
 * (the same library {@code AsyncApiKubernetesJobOperationExecutor} uses in fowf) - this class owns
 * only what's specific to launching an ingestion run: {@link IngestionJobPolicy} authorization,
 * translating an {@link IngestionSpec} into a container command that reconstructs it inside the
 * pod, and the correlation-id/Job-name convention.
 *
 * <p>Only {@code execution.engine() == "pekko"} is supported here - the Spark correlation
 * equivalent (several sources, a blocking field, per-source trust weights) is a genuinely different
 * spec type ({@code CorrelationSpec}) run against a genuinely different runner image, so it's its
 * own sibling class, {@link DirectCorrelationLauncher}, not a branch inside this one.
 *
 * <p><b>Still a real, separate follow-up, not yet done</b>: neither {@code
 * forwardmeasure-data-streaming-executor-pekko} nor {@code -executor-spark} has a packaged,
 * pushable container image *pushed anywhere* yet - the {@code Dockerfile}/{@code
 * docker-maven-plugin} packaging exists in both modules now, but nothing has built/pushed a real
 * image from it. This class's own real Kubernetes-Job mechanics are fully correct and tested
 * against a real cluster regardless (see this module's own test, which proves the
 * encode/launch/observe/cancel mechanism end to end using a stand-in image, the same way {@code
 * KubernetesJobLifecycleTest}/{@code RealKubernetesJobOperationExecutorTest} already prove the
 * shared library itself without needing a real production image) - but a caller pointing this class
 * at a real deployment needs a real, pushed image first.
 */
public final class DirectIngestionLauncher {

  private static final String PEKKO_ENGINE = "pekko";
  private static final String JOB_NAME_PREFIX = "fds-";
  private static final String SPEC_ENV_VAR = "INGESTION_SPEC_YAML_BASE64";
  private static final String SPEC_FILE_PATH = "/tmp/ingestion-spec.yaml";
  private static final String CORRELATION_ID_LABEL =
      "data-streaming.forwardmeasure.com/correlation-id";

  private final IngestionJobPolicy policy;
  private final String pekkoRunnerImage;
  private final String pekkoRunnerCommand;
  private final List<String> imagePullSecretNames;

  /**
   * @param pekkoRunnerCommand the shell command that actually runs the reconstructed spec (e.g.
   *     {@code "java -jar runner.jar"} in a real deployment) - the spec's own file path is appended
   *     as its final argument. Configurable (rather than hardcoded) so this class's own real
   *     Job-launch mechanics are independently testable with a stand-in verification command,
   *     without needing a real, packaged runner image to exist first.
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy, String pekkoRunnerImage, String pekkoRunnerCommand) {
    this(policy, pekkoRunnerImage, pekkoRunnerCommand, List.of());
  }

  /**
   * @param imagePullSecretNames names of existing {@code kubernetes.io/dockerconfigjson} Secrets in
   *     the target namespace (added 2026-09-13, once {@code pekkoRunnerImage} became a real,
   *     private registry image rather than only the public {@code busybox} stand-in) - see {@link
   *     com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobSpec}'s own javadoc for why
   *     this type never carries credentials itself.
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      List<String> imagePullSecretNames) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.pekkoRunnerImage = Objects.requireNonNull(pekkoRunnerImage, "pekkoRunnerImage");
    this.pekkoRunnerCommand = Objects.requireNonNull(pekkoRunnerCommand, "pekkoRunnerCommand");
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }

  /**
   * Launches {@code request.ingestionSpec()} as one Job. Returns the deterministic Job name ({@link
   * #deterministicJobName}) a caller uses with {@link #observe}/{@link #cancel}.
   *
   * @throws UnsupportedOperationException if the spec's engine isn't {@code pekko} (see this
   *     class's own javadoc for why)
   */
  public String launch(KubernetesClient client, DirectLaunchRequest request) {
    IngestionSpec spec = request.ingestionSpec();
    String engine = spec.execution().engine();
    if (!PEKKO_ENGINE.equals(engine)) {
      throw new UnsupportedOperationException(
          "DirectIngestionLauncher: engine '"
              + engine
              + "' is not yet supported - only 'pekko' has a real single-source runner today; see"
              + " this class's own javadoc for why 'spark' correlation is a real, separate,"
              + " tracked follow-up rather than a silent gap");
    }
    policy.authorizeNamespace(request.namespace());
    policy.authorizeImage(pekkoRunnerImage);

    String jobName = deterministicJobName(request.correlationId());
    KubernetesJobSpec jobSpec =
        new KubernetesJobSpec(
            request.namespace(),
            jobName,
            Map.of(CORRELATION_ID_LABEL, labelSafe(request.correlationId())),
            pekkoRunnerImage,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(pekkoRunnerCommand)),
            Map.of(SPEC_ENV_VAR, encodeSpecYaml(spec)),
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
   * The same correlation id always resolves to the same Job name - see {@link DirectLaunchRequest}.
   */
  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(JOB_NAME_PREFIX, correlationId);
  }

  /**
   * The pod's own container reconstructs the spec from the env var - a plain shell one-liner, not a
   * ConfigMap/volume mount: {@link KubernetesJobSpec} (the shared library's own contract)
   * deliberately has no volume-mounting concept, since the two real callers (this launcher and
   * fowf's executor) have never needed one; an env var is well within Kubernetes' real size limits
   * for an {@code IngestionSpec} document (source/mapper/sink/execution config, typically a few
   * KB).
   */
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

  private static String encodeSpecYaml(IngestionSpec spec) {
    try {
      return Base64.getEncoder().encodeToString(spec.toYaml().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("failed to serialize IngestionSpec to YAML", e);
    }
  }

  private static String labelSafe(String value) {
    String sanitized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "-");
    return sanitized.length() > 63 ? sanitized.substring(0, 63) : sanitized;
  }
}
