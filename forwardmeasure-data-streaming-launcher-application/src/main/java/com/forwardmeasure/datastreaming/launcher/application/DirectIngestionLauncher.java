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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobLifecycle;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.io.UncheckedIOException;
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
 * shape difference to justify two launcher classes. Which engine runs a spec is never resolved per
 * spec: it is the installation's one engine, {@link RunnerImages#engine()}.
 *
 * <p><b>Staged runs.</b> Two kinds of plan dispatch as two sequential Kubernetes Jobs: a plan with
 * a {@code sparkStage} (the Spark Job writes every mapped row to its handoff topic), and, on a
 * Kafka Streams installation, a bounded plan reading non-Kafka sources (the loader Job writes each
 * source's rows to a per-run topic, see {@code LoaderHandoffSpecs}). The delivery-engine Job then
 * reads those topics and finishes the write to {@code plan.destination()}. The two Jobs are chained
 * by lazy reconciliation inside {@link #observe}: {@link #launch} creates only the first Job and a
 * small handoff {@link ConfigMap} holding the complete delivery request; once the first Job reaches
 * {@code SUCCEEDED}, the next {@link #observe} call creates the delivery Job (idempotently, by
 * deterministic name), and the ConfigMap is deleted when delivery succeeds. No background thread is
 * involved, and the launcher persists nothing itself, so this survives a launcher restart.
 *
 * <p>Only {@link ExecutionMode#BOUNDED} specs are dispatched here - {@code CONTINUOUS} specs
 * resolve to a Kubernetes {@code Deployment} via fowf's own {@code kubernetes-deployment} protocol
 * instead (see the repo's own gap-bridging plan, Phase D) - {@link #launch} rejects anything else
 * up front with a clear message pointing at that path.
 */
public final class DirectIngestionLauncher {

  private static final String JOB_NAME_PREFIX = "fds-";
  private static final String SPARK_STAGE_SUFFIX = ":spark";
  private static final String LOAD_STAGE_SUFFIX = ":load";
  private static final String DELIVERY_STAGE_SUFFIX = ":delivery";
  private static final String HANDOFF_CONFIGMAP_PREFIX = "fds-handoff-";
  private static final String HANDOFF_REQUEST_KEY = "delivery-job.json";
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private final IngestionJobPolicy policy;

  private final EnvConfiguredJobLauncher delegate;
  private final IngestionLaunchPlanner planner;
  private final AuthorizationService authorization;
  private final RunnerImages runners;
  private final List<String> imagePullSecretNames;

  public DirectIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      RunnerImages runners,
      List<String> imagePullSecretNames,
      Map<String, String> hostAliases) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.delegate =
        new EnvConfiguredJobLauncher(
            Objects.requireNonNull(policy, "policy"),
            JOB_NAME_PREFIX,
            hostAliases == null ? Map.of() : hostAliases);
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.runners = Objects.requireNonNull(runners, "runners");
    this.imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
    this.planner = new IngestionLaunchPlanner(policy, runners, this.imagePullSecretNames);
  }

  public IngestionLaunchPlanner planner() {
    return planner;
  }

  public String launch(
      KubernetesClient client, DirectLaunchRequest request, ActiveOrganization actor) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.ingestionRun(request.correlationId()),
            AuthorizationAction.INGESTION_RUN_LAUNCH,
            request.correlationId(),
            Map.of()));
    request =
        new DirectLaunchRequest(
            scopedCorrelation(actor, request.correlationId()),
            request.namespace(),
            request.ingestionSpec(),
            request.resourceRequests(),
            request.resourceLimits(),
            request.activeDeadlineSeconds());
    IngestionSpec spec = request.ingestionSpec();
    if (spec.executionMode() != ExecutionMode.BOUNDED) {
      throw new UnsupportedOperationException(
          "DirectIngestionLauncher: executionMode '"
              + spec.executionMode()
              + "' is not supported here - only BOUNDED specs dispatch as a Kubernetes Job; a"
              + " CONTINUOUS spec dispatches as a Kubernetes Deployment via the fowf workflow"
              + " path instead");
    }
    policy.authorizeNamespace(request.namespace());
    ExecutionPlan plan = planner.compile(spec, request.correlationId());
    IngestionLaunchPlanner.Stage first = IngestionLaunchPlanner.firstStage(plan);
    if (first != IngestionLaunchPlanner.Stage.DELIVERY) {
      return launchFirstStage(client, request, plan, first);
    }
    return launchDeliveryJob(
        client, request.correlationId(), request.namespace(), spec, plan, request);
  }

  /**
   * Launches a staged run's first Job - the Spark stage or the Kafka Streams loader - and leaves
   * its planned delivery Job in a handoff {@link ConfigMap}; {@link #observe} creates that Job once
   * the first stage succeeds.
   */
  private String launchFirstStage(
      KubernetesClient client,
      DirectLaunchRequest request,
      ExecutionPlan plan,
      IngestionLaunchPlanner.Stage stage) {
    IngestionSpec deliverySpec =
        planner.deliveryStageSpec(plan, request.correlationId()).orElseThrow();
    ExecutionPlan deliveryPlan = planner.deliveryPlan(deliverySpec, plan, request.correlationId());
    EnvLaunchRequest deliveryRequest =
        deliveryRequest(
            request.correlationId() + DELIVERY_STAGE_SUFFIX,
            request.namespace(),
            deliverySpec,
            deliveryPlan,
            request);
    RunnerImages.Runner runner = planner.runner(stage);
    policy.authorizeNamespace(request.namespace());
    policy.authorizeImage(runner.image());
    policy.authorizeImage(deliveryRequest.image());
    createHandoffConfigMap(client, request, deliveryRequest);

    Map<String, String> env =
        planner.stageEnvironment(request.ingestionSpec(), plan, request.correlationId(), stage);
    EnvLaunchRequest envRequest =
        new EnvLaunchRequest(
            request.correlationId() + stageSuffix(stage),
            request.namespace(),
            runner.image(),
            List.of("sh", "-c"),
            List.of(IngestionLaunchPlanner.command(runner.command())),
            env,
            request.resourceRequests(),
            request.resourceLimits(),
            request.activeDeadlineSeconds(),
            imagePullSecretNames);
    return delegate.launch(client, envRequest);
  }

  private static String stageSuffix(IngestionLaunchPlanner.Stage stage) {
    return switch (stage) {
      case SPARK -> SPARK_STAGE_SUFFIX;
      case LOAD -> LOAD_STAGE_SUFFIX;
      case DELIVERY -> DELIVERY_STAGE_SUFFIX;
    };
  }

  private String launchDeliveryJob(
      KubernetesClient client,
      String correlationId,
      String namespace,
      IngestionSpec spec,
      ExecutionPlan plan,
      DirectLaunchRequest request) {
    return delegate.launch(client, deliveryRequest(correlationId, namespace, spec, plan, request));
  }

  private EnvLaunchRequest deliveryRequest(
      String correlationId,
      String namespace,
      IngestionSpec spec,
      ExecutionPlan plan,
      DirectLaunchRequest request) {
    RunnerImages.Runner runner = runners.delivery();
    Map<String, String> env =
        planner.stageEnvironment(spec, plan, correlationId, IngestionLaunchPlanner.Stage.DELIVERY);
    return new EnvLaunchRequest(
        correlationId,
        namespace,
        runner.image(),
        List.of("sh", "-c"),
        List.of(IngestionLaunchPlanner.command(runner.command())),
        env,
        request.resourceRequests(),
        request.resourceLimits(),
        request.activeDeadlineSeconds(),
        imagePullSecretNames);
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

    correlationId = scopedCorrelation(actor, correlationId);
    Optional<KubernetesJobObservation> firstObservation =
        delegate.observe(client, namespace, correlationId + SPARK_STAGE_SUFFIX);
    if (firstObservation.isEmpty()) {
      firstObservation = delegate.observe(client, namespace, correlationId + LOAD_STAGE_SUFFIX);
    }
    if (firstObservation.isEmpty()) {
      // Not a staged run at all (or its first Job was already cleaned up) - the plain,
      // single-Job path.
      return delegate.observe(client, namespace, correlationId);
    }
    KubernetesJobObservation first = firstObservation.get();
    if (first.phase() != KubernetesJobObservation.Phase.SUCCEEDED) {
      return Optional.of(first);
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
    EnvLaunchRequest delivery = readHandoffConfigMap(client, namespace, correlationId);
    if (!namespace.equals(delivery.namespace())
        || !(correlationId + DELIVERY_STAGE_SUFFIX).equals(delivery.correlationId())) {
      throw new IllegalStateException("Spark handoff belongs to a different run");
    }
    delegate.launch(client, delivery);
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
    correlationId = scopedCorrelation(actor, correlationId);
    delegate.cancel(client, namespace, correlationId);
    delegate.cancel(client, namespace, correlationId + SPARK_STAGE_SUFFIX);
    delegate.cancel(client, namespace, correlationId + LOAD_STAGE_SUFFIX);
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
  static String scopedCorrelation(ActiveOrganization actor, String correlationId) {
    return actor.tenantId() + ":" + correlationId;
  }

  public static String deterministicJobName(ActiveOrganization actor, String correlationId) {
    return deterministicJobName(scopedCorrelation(actor, correlationId));
  }

  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(JOB_NAME_PREFIX, correlationId);
  }

  private void createHandoffConfigMap(
      KubernetesClient client, DirectLaunchRequest request, EnvLaunchRequest delivery) {
    String namespace = request.namespace();
    String correlationId = request.correlationId();
    Map<String, String> data;
    try {
      data =
          Map.of(
              HANDOFF_REQUEST_KEY,
              JSON.writeValueAsString(delivery),
              "original-request.json",
              JSON.writeValueAsString(request));
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot serialize Spark handoff", failure);
    }
    ConfigMap configMap =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName(handoffConfigMapName(correlationId))
            .withNamespace(namespace)
            .endMetadata()
            .withData(data)
            .build();
    try {
      client.configMaps().inNamespace(namespace).resource(configMap).create();
    } catch (io.fabric8.kubernetes.client.KubernetesClientException conflict) {
      if (conflict.getCode() != 409) {
        throw conflict;
      }
      ConfigMap existing =
          client
              .configMaps()
              .inNamespace(namespace)
              .withName(handoffConfigMapName(correlationId))
              .get();
      if (existing == null || !Objects.equals(existing.getData(), configMap.getData())) {
        throw new IllegalArgumentException(
            "Correlation id already belongs to a different ingestion spec");
      }
    }
  }

  private EnvLaunchRequest readHandoffConfigMap(
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
    String json = configMap.getData().get(HANDOFF_REQUEST_KEY);
    if (json == null) {
      throw new IllegalStateException("Spark handoff is missing its planned delivery request");
    }
    try {
      return JSON.readValue(json, EnvLaunchRequest.class);
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot read Spark handoff", failure);
    }
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
}
