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
import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
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
 * <p><b>Collapsed 2026-09-21</b> from two classes ({@code DirectIngestionLauncher}, {@code
 * DirectCorrelationLauncher}) into one: the unified {@link IngestionSpec} already expresses both
 * single-source and correlated runs via {@code sources.size()} - there is no longer a real spec-
 * shape difference to justify two launcher classes, and which engine runs a spec is resolved
 * automatically by {@link ExecutionPlanCompiler} (see the repo's own gap-bridging plan, "the
 * planner" section), not read off a raw {@code engine} string the author never writes anymore. This
 * class dispatches strictly to a {@link DeliveryEngineKind}-specific runner image - never Spark
 * directly (Spark is an optional compute stage a {@link ExecutionPlan} may carry, never a delivery
 * engine; see {@link ExecutionPlan#sparkStage()}'s own javadoc). A plan whose {@code sparkStage} is
 * present is deliberately rejected here (see {@link #launch}) - a real two-Job Spark-then-delivery
 * pipeline is genuine new plumbing no in-scope spec needs yet (no registered transform is {@code
 * HEAVY}), so this class doesn't speculatively build a dispatch path nothing can reach.
 *
 * <p>Only {@link ExecutionMode#BOUNDED} specs are dispatched here - {@code CONTINUOUS} specs
 * resolve to a Kubernetes {@code Deployment} via fowf's own {@code kubernetes-deployment} protocol
 * instead (see the repo's own gap-bridging plan, Phase D) - {@link #launch} rejects anything else
 * up front with a clear message pointing at that path.
 */
public final class DirectIngestionLauncher {

  private static final String JOB_NAME_PREFIX = "fds-";
  private static final String SPEC_ENV_VAR = "INGESTION_SPEC_YAML_BASE64";
  private static final String SPEC_FILE_PATH = "/tmp/ingestion-spec.yaml";

  private final EnvConfiguredJobLauncher delegate;
  private final AuthorizationService authorization;
  private final String pekkoRunnerImage;
  private final String pekkoRunnerCommand;
  private final String kafkaStreamsRunnerImage;
  private final String kafkaStreamsRunnerCommand;
  private final List<String> imagePullSecretNames;

  /**
   * @param pekkoRunnerCommand / {@code kafkaStreamsRunnerCommand} the shell command that actually
   *     runs the reconstructed spec on that engine (e.g. {@code "java -jar runner.jar"} in a real
   *     deployment) - the spec's own file path is appended as its final argument. Configurable
   *     (rather than hardcoded) so this class's own real Job-launch mechanics are independently
   *     testable with a stand-in verification command, without needing a real, packaged runner
   *     image to exist first.
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      String kafkaStreamsRunnerImage,
      String kafkaStreamsRunnerCommand) {
    this(
        policy,
        authorization,
        pekkoRunnerImage,
        pekkoRunnerCommand,
        kafkaStreamsRunnerImage,
        kafkaStreamsRunnerCommand,
        List.of());
  }

  /**
   * @param imagePullSecretNames names of existing {@code kubernetes.io/dockerconfigjson} Secrets in
   *     the target namespace - see {@link
   *     com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobSpec}'s own javadoc for why
   *     this type never carries credentials itself.
   * @param authorization real, fail-closed caller authorization (see
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
      String kafkaStreamsRunnerImage,
      String kafkaStreamsRunnerCommand,
      List<String> imagePullSecretNames) {
    this(
        policy,
        authorization,
        pekkoRunnerImage,
        pekkoRunnerCommand,
        kafkaStreamsRunnerImage,
        kafkaStreamsRunnerCommand,
        imagePullSecretNames,
        Map.of());
  }

  /**
   * @param hostAliases written into every dispatched Job as real {@code pod.spec.hostAliases}
   *     entries (see {@link com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobSpec}'s own
   *     javadoc) - empty in every real deployment; only a test environment whose dispatch target (a
   *     Testcontainers-managed K3s node) can't resolve a sibling Testcontainers-managed service
   *     (OpenSearch, Kafka) through cluster DNS needs this at all.
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      String kafkaStreamsRunnerImage,
      String kafkaStreamsRunnerCommand,
      List<String> imagePullSecretNames,
      Map<String, String> hostAliases) {
    this.delegate =
        new EnvConfiguredJobLauncher(
            Objects.requireNonNull(policy, "policy"), JOB_NAME_PREFIX, hostAliases);
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.pekkoRunnerImage = Objects.requireNonNull(pekkoRunnerImage, "pekkoRunnerImage");
    this.pekkoRunnerCommand = Objects.requireNonNull(pekkoRunnerCommand, "pekkoRunnerCommand");
    this.kafkaStreamsRunnerImage =
        Objects.requireNonNull(kafkaStreamsRunnerImage, "kafkaStreamsRunnerImage");
    this.kafkaStreamsRunnerCommand =
        Objects.requireNonNull(kafkaStreamsRunnerCommand, "kafkaStreamsRunnerCommand");
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }

  /**
   * Compiles {@code request.ingestionSpec()} via {@link ExecutionPlanCompiler}, launches it as one
   * Job on whichever {@link DeliveryEngineKind} the plan resolved to, and returns the deterministic
   * Job name ({@link #deterministicJobName}) a caller uses with {@link #observe}/{@link #cancel}.
   *
   * @throws UnsupportedOperationException if the spec's {@code executionMode} isn't {@code
   *     BOUNDED}, or its compiled plan carries a {@code sparkStage} (see this class's own javadoc
   *     for why neither is dispatched here)
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
    if (spec.executionMode() != ExecutionMode.BOUNDED) {
      throw new UnsupportedOperationException(
          "DirectIngestionLauncher: executionMode '"
              + spec.executionMode()
              + "' is not supported here - only BOUNDED specs dispatch as a Kubernetes Job; a"
              + " CONTINUOUS spec dispatches as a Kubernetes Deployment via the fowf workflow"
              + " path instead");
    }
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);
    if (plan.sparkStage().isPresent()) {
      throw new UnsupportedOperationException(
          "DirectIngestionLauncher: this spec's compiled plan requires a Spark compute stage,"
              + " which this launcher does not dispatch yet - a real Spark-stage-then-delivery"
              + " pipeline is a separate, not-yet-built two-Job path (see this class's own"
              + " javadoc)");
    }
    String image;
    String command;
    if (plan.profile().deliveryEngine() == DeliveryEngineKind.KAFKA_STREAMS) {
      image = kafkaStreamsRunnerImage;
      command = kafkaStreamsRunnerCommand;
    } else {
      image = pekkoRunnerImage;
      command = pekkoRunnerCommand;
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
   * for an {@code IngestionSpec} document (sources/transforms/sink/delivery config, typically a few
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
