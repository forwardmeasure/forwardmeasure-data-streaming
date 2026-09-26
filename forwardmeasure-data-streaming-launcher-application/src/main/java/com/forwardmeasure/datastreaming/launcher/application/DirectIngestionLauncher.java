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
import com.forwardmeasure.datastreaming.core.SparkHandoffSpecs;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobLifecycle;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
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
 * planner" section), not read off a raw {@code engine} string the author never writes anymore.
 *
 * <p><b>Two-Job Spark-then-delivery pipeline (added 2026-09-25)</b>: a plan whose {@code
 * sparkStage} is present dispatches as two real, sequential Kubernetes Jobs - Spark first (writing
 * every mapped row to {@code plan.sparkStage().get().handoffTopic()}, per {@code
 * SparkIngestionRunner}/{@code SparkCorrelationRunner}'s own contract), then a delivery-engine Job
 * (Pekko or Kafka Streams, whichever {@link SparkHandoffSpecs#deliveryStageSpec} resolves to) that
 * reads that topic and finishes the write to {@code plan.destination()}. The two Jobs are chained
 * by lazy reconciliation inside {@link #observe} - {@link #launch} only ever creates the Spark Job
 * and returns immediately, exactly like a non-staged run; {@link #observe} advances the pipeline
 * each time a caller polls it: once the Spark Job reaches {@code SUCCEEDED}, the next {@link
 * #observe} call creates the delivery Job (idempotently, via the same deterministic-naming
 * guarantee every Job this class launches already has) if it doesn't exist yet. No background
 * thread/watcher is involved - this launcher stays exactly as stateless as it already was; the only
 * new durable state is one small {@link ConfigMap} (created alongside the Spark Job, deleted once
 * the delivery Job succeeds) carrying the delivery stage's own {@link IngestionSpec} YAML, so a
 * later {@link #observe} call - however long after {@link #launch}, even across a launcher-process
 * restart - can re-derive exactly what the delivery Job needs without this class ever persisting
 * anything itself. A caller that only ever launches non-staged plans is completely unaffected -
 * {@link #launch}/{@link #observe}/{@link #cancel}'s own single-Job behavior for those plans is
 * byte-for-byte unchanged from before this feature existed.
 *
 * <p>Requires the {@link #DirectIngestionLauncher(IngestionJobPolicy, AuthorizationService, String,
 * String, String, String, List, Map, String, String, String) fullest constructor} (real {@code
 * sparkRunnerImage}/{@code sparkRunnerCommand}/{@code kafkaBootstrapServers}) to actually dispatch
 * a Spark-staged plan - {@link #launch} throws a clear {@link IllegalStateException} (not {@link
 * UnsupportedOperationException} - the capability exists, this specific instance just wasn't
 * configured for it) if a Spark-staged plan reaches an instance constructed without them.
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
  private static final String KAFKA_BOOTSTRAP_SERVERS_ENV_VAR = "KAFKA_BOOTSTRAP_SERVERS";
  private static final String SPARK_HANDOFF_TOPIC_ENV_VAR = "SPARK_HANDOFF_TOPIC";
  private static final String SPARK_STAGE_SUFFIX = ":spark";
  private static final String DELIVERY_STAGE_SUFFIX = ":delivery";
  private static final String HANDOFF_CONFIGMAP_PREFIX = "fds-handoff-";
  private static final String HANDOFF_SPEC_YAML_KEY = "delivery-spec.yaml";

  private final EnvConfiguredJobLauncher delegate;
  private final AuthorizationService authorization;
  private final String pekkoRunnerImage;
  private final String pekkoRunnerCommand;
  private final String kafkaStreamsRunnerImage;
  private final String kafkaStreamsRunnerCommand;
  private final List<String> imagePullSecretNames;
  private final String sparkRunnerImage;
  private final String sparkRunnerCommand;
  private final String kafkaBootstrapServers;

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
   *     <p>Leaves this instance unable to dispatch a Spark-staged plan (see this class's own
   *     javadoc) - use the {@linkplain #DirectIngestionLauncher(IngestionJobPolicy,
   *     AuthorizationService, String, String, String, String, List, Map, String, String, String)
   *     fullest constructor} for that.
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
    this(
        policy,
        authorization,
        pekkoRunnerImage,
        pekkoRunnerCommand,
        kafkaStreamsRunnerImage,
        kafkaStreamsRunnerCommand,
        imagePullSecretNames,
        hostAliases,
        null,
        null,
        null);
  }

  /**
   * @param sparkRunnerImage / {@code sparkRunnerCommand} the Spark stage's own runner image/command
   *     (mirrors {@code pekkoRunnerCommand}'s own shape exactly - {@code SparkIngestionRunner}/
   *     {@code SparkCorrelationRunner}'s real {@code main()} reads the reconstructed spec's file
   *     path from {@code args[0]}, appended the same way for every runner). Required (along with
   *     {@code kafkaBootstrapServers}) to dispatch a Spark-staged plan at all - {@code null} is
   *     accepted here (every shorter constructor passes it) for a caller that never needs to.
   * @param kafkaBootstrapServers the real Kafka cluster's own bootstrap servers - passed to the
   *     Spark Job as {@code KAFKA_BOOTSTRAP_SERVERS} (matching {@code
   *     SparkIngestionRunner#kafkaBootstrapServersFromEnv}'s own required env var) and to {@link
   *     SparkHandoffSpecs#deliveryStageSpec} when building the delivery Job's own kafka-sourced
   *     spec - deliberately a runtime address supplied here, not carried by any compiled {@link
   *     ExecutionPlan} (see that class's own javadoc for why).
   */
  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      String pekkoRunnerImage,
      String pekkoRunnerCommand,
      String kafkaStreamsRunnerImage,
      String kafkaStreamsRunnerCommand,
      List<String> imagePullSecretNames,
      Map<String, String> hostAliases,
      String sparkRunnerImage,
      String sparkRunnerCommand,
      String kafkaBootstrapServers) {
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
    this.sparkRunnerImage = sparkRunnerImage;
    this.sparkRunnerCommand = sparkRunnerCommand;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  /**
   * Compiles {@code request.ingestionSpec()} via {@link ExecutionPlanCompiler}. A non-staged plan
   * launches its one delivery Job exactly as before. A Spark-staged plan launches only the Spark
   * Job (see this class's own javadoc for why the delivery Job comes later, via {@link #observe})
   * plus a small handoff {@link ConfigMap} the later delivery-Job creation reads. Returns the
   * deterministic Job name a caller uses with {@link #observe}/{@link #cancel} - the Spark Job's
   * own name for a staged plan (the one actually running immediately after this call returns).
   *
   * @throws UnsupportedOperationException if the spec's {@code executionMode} isn't {@code BOUNDED}
   *     (a real CONTINUOUS spec dispatches via a completely different path, not something this
   *     instance could ever be configured to support)
   * @throws IllegalStateException if the compiled plan carries a {@code sparkStage} and this
   *     instance wasn't constructed with real Spark runner settings (a configuration gap this
   *     instance genuinely could be given, unlike the {@code executionMode} case above)
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
      return launchSparkStage(client, request, plan);
    }
    return launchDeliveryJob(
        client, request.correlationId(), request.namespace(), spec, plan, request);
  }

  private String launchSparkStage(
      KubernetesClient client, DirectLaunchRequest request, ExecutionPlan plan) {
    if (sparkRunnerImage == null || kafkaBootstrapServers == null) {
      throw new IllegalStateException(
          "DirectIngestionLauncher: this spec's compiled plan requires a Spark compute stage, but"
              + " this instance was constructed without real sparkRunnerImage/"
              + "kafkaBootstrapServers settings - use the fullest constructor (see this class's"
              + " own javadoc)");
    }
    IngestionSpec deliverySpec = SparkHandoffSpecs.deliveryStageSpec(plan, kafkaBootstrapServers);
    String deliverySpecYaml;
    try {
      deliverySpecYaml = deliverySpec.toYaml();
    } catch (IOException e) {
      throw new UncheckedIOException("failed to serialize the derived delivery-stage spec", e);
    }
    createHandoffConfigMap(client, request.namespace(), request.correlationId(), deliverySpecYaml);

    IngestionSpec originalSpec = request.ingestionSpec();
    Map<String, String> env =
        Map.of(
            SPEC_ENV_VAR,
            encodeSpecYaml(originalSpec),
            KAFKA_BOOTSTRAP_SERVERS_ENV_VAR,
            kafkaBootstrapServers,
            // Real, live-found bug fix (2026-09-25): the Spark pod recompiles its own
            // ExecutionPlan fresh from the (base64/YAML round-tripped) spec above, which
            // ExecutionPlanCompiler.resolveSparkStage derives handoffTopic from independently, via
            // spec.hashCode() - not guaranteed to agree with the handoffTopic this method already
            // computed (via `plan`, straight from the in-memory spec) for the delivery Job's own
            // ConfigMap. This launcher is the single source of truth for the topic both stages must
            // agree on, so it hands the Spark pod its own already-computed value explicitly - see
            // SparkIngestionRunner#withHandoffTopicOverride's own javadoc for the live failure this
            // closes (Spark wrote 5 real rows to its own re-derived topic while the delivery Job's
            // consumer, reading this launcher's independently-derived topic name, read zero).
            SPARK_HANDOFF_TOPIC_ENV_VAR,
            plan.sparkStage().get().handoffTopic());
    EnvLaunchRequest envRequest =
        new EnvLaunchRequest(
            request.correlationId() + SPARK_STAGE_SUFFIX,
            request.namespace(),
            sparkRunnerImage,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(sparkRunnerCommand)),
            env,
            request.resourceRequests(),
            request.resourceLimits(),
            request.activeDeadlineSeconds(),
            imagePullSecretNames);
    return delegate.launch(client, envRequest);
  }

  private String launchDeliveryJob(
      KubernetesClient client,
      String correlationId,
      String namespace,
      IngestionSpec spec,
      ExecutionPlan plan,
      DirectLaunchRequest request) {
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
            correlationId,
            namespace,
            image,
            List.of("sh", "-c"),
            List.of(reconstructSpecAndRunCommand(command)),
            Map.of(SPEC_ENV_VAR, encodeSpecYaml(spec)),
            request == null ? Map.of() : request.resourceRequests(),
            request == null ? Map.of() : request.resourceLimits(),
            request == null ? null : request.activeDeadlineSeconds(),
            imagePullSecretNames);
    return delegate.launch(client, envRequest);
  }

  /**
   * For a non-staged plan, an immediate one-shot check of that plan's one Job - byte-for-byte the
   * same behavior as before Spark-staged plans existed. For a Spark-staged plan, lazy
   * reconciliation (see this class's own javadoc): while the Spark Job hasn't reached {@code
   * SUCCEEDED} yet, its own observation *is* the overall result; once it has, this call creates the
   * delivery Job (idempotently) the first time it's asked, using the handoff {@link ConfigMap}
   * {@link #launch} left behind, then reports the delivery Job's own observation (creating it and
   * observing a just-created Job in the same call reports {@code RUNNING}/{@code PENDING}, not a
   * separate "not started yet" state - there is no meaningful difference for a caller polling this
   * method either way). The handoff {@link ConfigMap} is deleted once the delivery Job reaches
   * {@code SUCCEEDED}, so this method's own real, no-persisted-state footprint returns to zero once
   * a Spark-staged run finishes.
   */
  public Optional<KubernetesJobObservation> observe(
      KubernetesClient client, String namespace, String correlationId, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.ingestionRun(correlationId),
            AuthorizationAction.INGESTION_RUN_READ,
            correlationId,
            Map.of()));

    Optional<KubernetesJobObservation> sparkObservation =
        delegate.observe(client, namespace, correlationId + SPARK_STAGE_SUFFIX);
    if (sparkObservation.isEmpty()) {
      // Not a Spark-staged run at all (or the Spark Job was already cleaned up) - the plain,
      // non-staged single-Job path, unchanged.
      return delegate.observe(client, namespace, correlationId);
    }
    KubernetesJobObservation spark = sparkObservation.get();
    if (spark.phase() != KubernetesJobObservation.Phase.SUCCEEDED) {
      return Optional.of(spark);
    }

    Optional<KubernetesJobObservation> deliveryObservation =
        delegate.observe(client, namespace, correlationId + DELIVERY_STAGE_SUFFIX);
    if (deliveryObservation.isEmpty()) {
      return Optional.of(reconcileDeliveryJob(client, namespace, correlationId));
    }
    KubernetesJobObservation delivery = deliveryObservation.get();
    if (delivery.phase() == KubernetesJobObservation.Phase.SUCCEEDED) {
      deleteHandoffConfigMap(client, namespace, correlationId);
    }
    return Optional.of(delivery);
  }

  /**
   * The Spark Job just reached SUCCEEDED and the delivery Job doesn't exist yet - create it now.
   */
  private KubernetesJobObservation reconcileDeliveryJob(
      KubernetesClient client, String namespace, String correlationId) {
    String deliverySpecYaml = readHandoffConfigMap(client, namespace, correlationId);
    IngestionSpec deliverySpec = IngestionSpec.parseYaml(deliverySpecYaml);
    ExecutionPlan deliveryPlan = ExecutionPlanCompiler.compile(deliverySpec);
    launchDeliveryJob(
        client, correlationId + DELIVERY_STAGE_SUFFIX, namespace, deliverySpec, deliveryPlan, null);
    return delegate
        .observe(client, namespace, correlationId + DELIVERY_STAGE_SUFFIX)
        .orElse(KubernetesJobObservation.running(0, 1, 0));
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
    delegate.cancel(client, namespace, correlationId + SPARK_STAGE_SUFFIX);
    delegate.cancel(client, namespace, correlationId + DELIVERY_STAGE_SUFFIX);
    deleteHandoffConfigMap(client, namespace, correlationId);
  }

  /**
   * The same correlation id always resolves to the same Job name - see {@link DirectLaunchRequest}.
   * Computed directly (not via {@link EnvConfiguredJobLauncher#deterministicJobName}, which uses
   * that class's own default prefix) since this class's real, established prefix is {@code fds-},
   * not {@code fds-env-} - must stay in lockstep with the prefix passed to this class's own {@code
   * delegate} above. For a Spark-staged run, this names the *delivery* Job specifically (the one a
   * successful run's own real output/side effects come from) - use {@link #observe} to learn which
   * stage is actually active right now rather than assuming this name is already running.
   */
  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(JOB_NAME_PREFIX, correlationId);
  }

  private void createHandoffConfigMap(
      KubernetesClient client, String namespace, String correlationId, String deliverySpecYaml) {
    ConfigMap configMap =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName(handoffConfigMapName(correlationId))
            .withNamespace(namespace)
            .endMetadata()
            .addToData(HANDOFF_SPEC_YAML_KEY, deliverySpecYaml)
            .build();
    try {
      client.configMaps().inNamespace(namespace).resource(configMap).create();
    } catch (io.fabric8.kubernetes.client.KubernetesClientException conflict) {
      if (conflict.getCode() != 409) {
        throw conflict;
      }
      // Already exists - a retried launch() call for the same correlationId, same real
      // idempotency guarantee KubernetesJobLifecycle.launch already gives every Job.
    }
  }

  private String readHandoffConfigMap(
      KubernetesClient client, String namespace, String correlationId) {
    ConfigMap configMap =
        client
            .configMaps()
            .inNamespace(namespace)
            .withName(handoffConfigMapName(correlationId))
            .get();
    if (configMap == null || configMap.getData() == null) {
      throw new IllegalStateException(
          "DirectIngestionLauncher: no handoff ConfigMap found for correlationId '"
              + correlationId
              + "' - the Spark stage's own launch() call should have created one");
    }
    String yaml = configMap.getData().get(HANDOFF_SPEC_YAML_KEY);
    if (yaml == null) {
      throw new IllegalStateException(
          "DirectIngestionLauncher: handoff ConfigMap for correlationId '"
              + correlationId
              + "' has no '"
              + HANDOFF_SPEC_YAML_KEY
              + "' key");
    }
    return yaml;
  }

  private void deleteHandoffConfigMap(
      KubernetesClient client, String namespace, String correlationId) {
    client
        .configMaps()
        .inNamespace(namespace)
        .withName(handoffConfigMapName(correlationId))
        .delete();
  }

  private static String handoffConfigMapName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(HANDOFF_CONFIGMAP_PREFIX, correlationId);
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
