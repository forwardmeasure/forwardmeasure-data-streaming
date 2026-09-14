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

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobLifecycle;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
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
 * <p>{@code execution.engine()} may be {@code "pekko"} (always supported - {@code
 * PekkoIngestionRunner}) or {@code "spark"} (supported when constructed with a {@code
 * sparkRunnerImage} - {@code SparkIngestionRunner}, added 2026-09-13, genuinely no-correlation
 * single-source Spark run, for a case like one very large file where Spark's own partitioned reader
 * earns its keep). Multi-source correlation (several sources, a blocking field, per-source trust
 * weights) is still a genuinely different spec type ({@code CorrelationSpec}) with its own
 * launcher, {@link DirectCorrelationLauncher} - that boundary is about spec *shape* (one source vs.
 * several to merge), not about which engine may run a single source.
 *
 * <p><b>Refactored 2026-09-13</b> to a thin adapter over {@link EnvConfiguredJobLauncher} rather
 * than an independent implementation of Job-manifest/status/deletion mechanics: this class's real,
 * distinct value is exactly two things - picking which configured runner image/command a spec's
 * engine maps to (rejecting anything neither configured nor recognized), and the {@code
 * IngestionSpec} → base64-env-var → pod-side shell reconstruction convention - not the K8s API
 * calls underneath, which are identical to every other direct-mode launcher and now live once, in
 * {@link EnvConfiguredJobLauncher}. Public API (constructors, {@link #launch}/{@link #observe}/
 * {@link #cancel}/{@link #deterministicJobName}) is unchanged, including the real, established
 * {@code fds-} Job-name prefix (preserved via {@link EnvConfiguredJobLauncher}'s prefix-overriding
 * constructor, not silently renamed to its own default).
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
  private static final String SPARK_ENGINE = "spark";
  private static final String JOB_NAME_PREFIX = "fds-";
  private static final String SPEC_ENV_VAR = "INGESTION_SPEC_YAML_BASE64";
  private static final String SPEC_FILE_PATH = "/tmp/ingestion-spec.yaml";

  private final EnvConfiguredJobLauncher delegate;
  private final AuthorizationService authorization;
  private final String pekkoRunnerImage;
  private final String pekkoRunnerCommand;
  private final String sparkRunnerImage;
  private final String sparkRunnerCommand;
  private final List<String> imagePullSecretNames;

  /**
   * @param pekkoRunnerCommand the shell command that actually runs the reconstructed spec (e.g.
   *     {@code "java -jar runner.jar"} in a real deployment) - the spec's own file path is appended
   *     as its final argument. Configurable (rather than hardcoded) so this class's own real
   *     Job-launch mechanics are independently testable with a stand-in verification command,
   *     without needing a real, packaged runner image to exist first.
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand) {
    this(policy, authorization, pekkoRunnerImage, pekkoRunnerCommand, List.of());
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
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      List<String> imagePullSecretNames) {
    this(
        policy,
        authorization,
        pekkoRunnerImage,
        pekkoRunnerCommand,
        null,
        null,
        imagePullSecretNames);
  }

  /**
   * @param sparkRunnerImage enables {@code execution.engine() == "spark"} for a single-source run
   *     when non-null (added 2026-09-13, together with {@code sparkRunnerCommand}) - both {@code
   *     null} (the other constructors' behavior) means this instance only supports {@code pekko},
   *     matching this class's own original scope exactly; a caller that needs both engines uses
   *     this constructor, one that only ever needs {@code pekko} keeps using the shorter ones
   *     unchanged.
   * @param sparkRunnerCommand the shell command that runs the reconstructed spec via {@code
   *     SparkIngestionRunner} (e.g. {@code "java -jar spark-runner.jar"}), same
   *     spec-file-as-final-argument convention as {@code pekkoRunnerCommand}.
   * @param authorization real, fail-closed caller authorization, added 2026-09-14 (see
   *     docs/fds-authorization-remediation-guide.md) - checked at the top of {@link #launch}/{@link
   *     #observe}/{@link #cancel}, before {@link IngestionJobPolicy} ever runs: policy is a
   *     request-payload allowlist (is this namespace/image approved at all), not a substitute for
   *     "is this caller allowed to act."
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      String sparkRunnerImage,
      String sparkRunnerCommand,
      List<String> imagePullSecretNames) {
    this.delegate =
        new EnvConfiguredJobLauncher(Objects.requireNonNull(policy, "policy"), JOB_NAME_PREFIX);
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.pekkoRunnerImage = Objects.requireNonNull(pekkoRunnerImage, "pekkoRunnerImage");
    this.pekkoRunnerCommand = Objects.requireNonNull(pekkoRunnerCommand, "pekkoRunnerCommand");
    this.sparkRunnerImage = sparkRunnerImage;
    this.sparkRunnerCommand = sparkRunnerCommand;
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }

  /**
   * Launches {@code request.ingestionSpec()} as one Job. Returns the deterministic Job name ({@link
   * #deterministicJobName}) a caller uses with {@link #observe}/{@link #cancel}.
   *
   * @throws UnsupportedOperationException if the spec's engine is {@code spark} but this instance
   *     wasn't constructed with a {@code sparkRunnerImage}, or if it's anything other than {@code
   *     pekko}/{@code spark} (see this class's own javadoc for the real, current engine scope)
   */
  public String launch(
      KubernetesClient client, DirectLaunchRequest request, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.ingestionRun(request.correlationId()),
            AuthorizationAction.INGESTION_RUN_LAUNCH,
            request.correlationId(),
            Map.of()));
    IngestionSpec spec = request.ingestionSpec();
    String engine = spec.execution().engine();
    String image;
    String command;
    if (PEKKO_ENGINE.equals(engine)) {
      image = pekkoRunnerImage;
      command = pekkoRunnerCommand;
    } else if (SPARK_ENGINE.equals(engine) && sparkRunnerImage != null) {
      image = sparkRunnerImage;
      command = sparkRunnerCommand;
    } else {
      throw new UnsupportedOperationException(
          "DirectIngestionLauncher: engine '"
              + engine
              + "' is not supported by this instance - 'pekko' is always supported; 'spark' needs"
              + " this instance constructed with a sparkRunnerImage (see this class's own"
              + " javadoc)");
    }
    EnvLaunchRequest envRequest =
        new EnvLaunchRequest(
            request.correlationId(),
            request.namespace(),
            image,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(command)),
            Map.of(SPEC_ENV_VAR, encodeSpecYaml(spec)),
            request.resourceRequests(),
            request.resourceLimits(),
            request.activeDeadlineSeconds(),
            imagePullSecretNames);
    return delegate.launch(client, envRequest);
  }

  public Optional<KubernetesJobObservation> observe(
      KubernetesClient client, String namespace, String correlationId, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.ingestionRun(correlationId),
            AuthorizationAction.INGESTION_RUN_READ,
            correlationId,
            Map.of()));
    return delegate.observe(client, namespace, correlationId);
  }

  public void cancel(
      KubernetesClient client, String namespace, String correlationId, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.ingestionRun(correlationId),
            AuthorizationAction.INGESTION_RUN_CANCEL,
            correlationId,
            Map.of()));
    delegate.cancel(client, namespace, correlationId);
  }

  /**
   * The same correlation id always resolves to the same Job name - see {@link DirectLaunchRequest}.
   * Computed directly (not via {@link EnvConfiguredJobLauncher#deterministicJobName}, which uses
   * that class's own default prefix) since this class's real, established prefix is {@code fds-},
   * not {@code fds-env-} - must stay in lockstep with the prefix passed to this class's own {@code
   * delegate} above.
   */
  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(JOB_NAME_PREFIX, correlationId);
  }

  /**
   * The pod's own container reconstructs the spec from the env var - a plain shell one-liner, not a
   * ConfigMap/volume mount: {@code KubernetesJobSpec} (the shared library's own contract)
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
}
