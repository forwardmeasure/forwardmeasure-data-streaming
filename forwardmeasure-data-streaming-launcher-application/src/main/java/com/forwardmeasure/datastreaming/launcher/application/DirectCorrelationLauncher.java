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
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
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
 * Direct mode's correlation counterpart to {@link DirectIngestionLauncher} - runs a {@link
 * CorrelationSpec} as one real Kubernetes Job, with no fowf workflow engine involved, closing the
 * "spark engine unlaunchable" gap {@link DirectIngestionLauncher}'s own javadoc documented.
 *
 * <p>{@code execution.engine()} may be {@code "spark"} (always supported - {@code
 * SparkCorrelationRunner}) or {@code "pekko"} (supported when constructed with a {@code
 * pekkoRunnerImage} - {@code PekkoCorrelationRunner}, added 2026-09-13 for a bounded correlation
 * run small enough that Spark's own cluster/shuffle machinery is genuine overkill; see {@code
 * PekkoCorrelationEngine}'s own javadoc for why it produces the identical merge result, just
 * without a real distributed shuffle). Before 2026-09-13 this class never actually checked {@code
 * execution.engine()} at all - it silently ran the Spark runner regardless of what the spec's own
 * engine field said, which a caller passing a typo'd or unimplemented engine value would never have
 * been told about; that's fixed here too, not just the symmetry.
 *
 * <p><b>Refactored 2026-09-13</b> to a thin adapter over {@link EnvConfiguredJobLauncher}, same as
 * {@link DirectIngestionLauncher} - this class's real, distinct value is picking which configured
 * runner image/command a spec's engine maps to, and the {@code CorrelationSpec} → base64-env-var →
 * pod-side shell reconstruction convention, not the K8s API calls underneath (now shared, not
 * duplicated a third time). Public API and the real, established {@code fds-corr-} Job-name prefix
 * are unchanged.
 */
public final class DirectCorrelationLauncher {

  private static final String SPARK_ENGINE = "spark";
  private static final String PEKKO_ENGINE = "pekko";
  private static final String JOB_NAME_PREFIX = "fds-corr-";
  private static final String SPEC_ENV_VAR = "CORRELATION_SPEC_YAML_BASE64";
  private static final String SPEC_FILE_PATH = "/tmp/correlation-spec.yaml";

  private final EnvConfiguredJobLauncher delegate;
  private final AuthorizationService authorization;
  private final String sparkRunnerImage;
  private final String sparkRunnerCommand;
  private final String pekkoRunnerImage;
  private final String pekkoRunnerCommand;
  private final List<String> imagePullSecretNames;

  /**
   * @param sparkRunnerCommand the shell command that runs the reconstructed spec (e.g. {@code "java
   *     -jar runner.jar"} in a real deployment) - the spec's own file path is appended as its final
   *     argument, same convention as {@link DirectIngestionLauncher}'s own constructor.
   */
  public DirectCorrelationLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String sparkRunnerImage,
      String sparkRunnerCommand) {
    this(policy, authorization, sparkRunnerImage, sparkRunnerCommand, List.of());
  }

  /**
   * @param imagePullSecretNames names of existing {@code kubernetes.io/dockerconfigjson} Secrets in
   *     the target namespace - see {@link DirectIngestionLauncher}'s own matching constructor.
   */
  public DirectCorrelationLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String sparkRunnerImage,
      String sparkRunnerCommand,
      List<String> imagePullSecretNames) {
    this(
        policy,
        authorization,
        sparkRunnerImage,
        sparkRunnerCommand,
        null,
        null,
        imagePullSecretNames);
  }

  /**
   * @param pekkoRunnerImage enables {@code execution.engine() == "pekko"} when non-null (added
   *     2026-09-13, together with {@code pekkoRunnerCommand}) - both {@code null} (the other
   *     constructors' behavior) means this instance only supports {@code spark}, matching this
   *     class's own original scope exactly.
   * @param pekkoRunnerCommand the shell command that runs the reconstructed spec via {@code
   *     PekkoCorrelationRunner}, same spec-file-as-final-argument convention as {@code
   *     sparkRunnerCommand}.
   * @param authorization real, fail-closed caller authorization, added 2026-09-14 (see
   *     docs/fds-authorization-remediation-guide.md) - checked at the top of {@link #launch}/{@link
   *     #observe}/{@link #cancel}, before {@link IngestionJobPolicy} ever runs; see {@link
   *     DirectIngestionLauncher}'s own matching constructor javadoc for why both checks are needed.
   */
  public DirectCorrelationLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String sparkRunnerImage,
      String sparkRunnerCommand,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      List<String> imagePullSecretNames) {
    this.delegate =
        new EnvConfiguredJobLauncher(Objects.requireNonNull(policy, "policy"), JOB_NAME_PREFIX);
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.sparkRunnerImage = Objects.requireNonNull(sparkRunnerImage, "sparkRunnerImage");
    this.sparkRunnerCommand = Objects.requireNonNull(sparkRunnerCommand, "sparkRunnerCommand");
    this.pekkoRunnerImage = pekkoRunnerImage;
    this.pekkoRunnerCommand = pekkoRunnerCommand;
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }

  /**
   * Launches {@code request.correlationSpec()} as one Job. Returns the deterministic Job name
   * ({@link #deterministicJobName}) a caller uses with {@link #observe}/{@link #cancel}.
   *
   * @throws UnsupportedOperationException if the spec's engine is {@code pekko} but this instance
   *     wasn't constructed with a {@code pekkoRunnerImage}, or if it's anything other than {@code
   *     spark}/{@code pekko} (see this class's own javadoc for the real, current engine scope)
   */
  public String launch(
      KubernetesClient client, DirectCorrelationLaunchRequest request, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.correlationRun(request.correlationId()),
            AuthorizationAction.CORRELATION_RUN_LAUNCH,
            request.correlationId(),
            Map.of()));
    String engine = request.correlationSpec().execution().engine();
    String image;
    String command;
    if (SPARK_ENGINE.equals(engine)) {
      image = sparkRunnerImage;
      command = sparkRunnerCommand;
    } else if (PEKKO_ENGINE.equals(engine) && pekkoRunnerImage != null) {
      image = pekkoRunnerImage;
      command = pekkoRunnerCommand;
    } else {
      throw new UnsupportedOperationException(
          "DirectCorrelationLauncher: engine '"
              + engine
              + "' is not supported by this instance - 'spark' is always supported; 'pekko' needs"
              + " this instance constructed with a pekkoRunnerImage (see this class's own"
              + " javadoc)");
    }
    EnvLaunchRequest envRequest =
        new EnvLaunchRequest(
            request.correlationId(),
            request.namespace(),
            image,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(command)),
            Map.of(SPEC_ENV_VAR, encodeSpecYaml(request.correlationSpec())),
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
            DataStreamingAuthorizationResources.correlationRun(correlationId),
            AuthorizationAction.CORRELATION_RUN_READ,
            correlationId,
            Map.of()));
    return delegate.observe(client, namespace, correlationId);
  }

  public void cancel(
      KubernetesClient client, String namespace, String correlationId, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.correlationRun(correlationId),
            AuthorizationAction.CORRELATION_RUN_CANCEL,
            correlationId,
            Map.of()));
    delegate.cancel(client, namespace, correlationId);
  }

  /**
   * The same correlation id always resolves to the same Job name - see {@link
   * DirectCorrelationLaunchRequest}. Computed directly, not via {@link EnvConfiguredJobLauncher
   * #deterministicJobName} (that method's default prefix isn't this class's real {@code fds-corr-}
   * one) - must stay in lockstep with the prefix passed to this class's own {@code delegate} above.
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
}
