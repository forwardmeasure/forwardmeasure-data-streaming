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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.LoaderHandoffSpecs;
import com.forwardmeasure.datastreaming.core.SparkHandoffSpecs;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One planner for direct Jobs and FOWF-owned Jobs/Deployments. No remote action is performed here.
 */
public final class IngestionLaunchPlanner {
  private final RunnerImages runners;
  private final List<String> pullSecrets;
  private final IngestionJobPolicy policy;

  /** Which runner a Job of a staged run uses. */
  enum Stage {
    SPARK,
    LOAD,
    DELIVERY
  }

  public IngestionLaunchPlanner(
      IngestionJobPolicy policy, RunnerImages runners, List<String> pullSecrets) {
    this.policy = policy;
    this.runners = java.util.Objects.requireNonNull(runners, "runners");
    this.pullSecrets = List.copyOf(pullSecrets);
  }

  public RunnerImages runners() {
    return runners;
  }

  public ExecutionPlan compile(IngestionSpec spec, String executionKey) {
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec, runners.engine());
    if (plan.sparkStage().isEmpty()) return plan;
    if (spec.executionMode() == ExecutionMode.CONTINUOUS) {
      for (SourcePlan source : spec.sources()) {
        if (!"kafka".equals(source.source().connector())
            || !KafkaConnectorUri.parse(source.source().uri())
                .bootstrapServers()
                .equals(runners.kafkaBootstrapServers())) {
          throw new IllegalArgumentException(
              "Continuous Spark sources and handoff must share the configured Kafka cluster");
        }
      }
    }
    return new ExecutionPlan(
        plan.profile(),
        plan.sources(),
        plan.blockingField(),
        Optional.of(
            plan.sparkStage()
                .orElseThrow()
                .withHandoffTopic("fds-spark-handoff-" + digest(executionKey))),
        plan.transforms(),
        plan.destination(),
        plan.delivery(),
        plan.errors(),
        plan.mergePolicy());
  }

  /** The base topic a run's loader stage writes each source to. */
  public static String loadTopic(String executionKey) {
    return "fds-load-" + digest(executionKey);
  }

  /** The delivery stage of a staged {@code plan}, keeping the original plan's merge policy. */
  ExecutionPlan deliveryPlan(IngestionSpec delivery, ExecutionPlan plan, String executionKey) {
    ExecutionPlan compiled = compile(delivery, executionKey);
    return new ExecutionPlan(
        compiled.profile(),
        compiled.sources(),
        compiled.blockingField(),
        compiled.sparkStage(),
        compiled.transforms(),
        compiled.destination(),
        compiled.delivery(),
        compiled.errors(),
        plan.mergePolicy());
  }

  /** The delivery-stage spec of a staged plan, or empty when {@code plan} runs as one Job. */
  Optional<IngestionSpec> deliveryStageSpec(ExecutionPlan plan, String executionKey) {
    if (plan.sparkStage().isPresent()) {
      return Optional.of(
          SparkHandoffSpecs.deliveryStageSpec(plan, runners.kafkaBootstrapServers()));
    }
    if (LoaderHandoffSpecs.needsLoader(plan)) {
      return Optional.of(
          LoaderHandoffSpecs.deliveryStageSpec(
              plan, runners.kafkaBootstrapServers(), loadTopic(executionKey)));
    }
    return Optional.empty();
  }

  static Stage firstStage(ExecutionPlan plan) {
    if (plan.sparkStage().isPresent()) return Stage.SPARK;
    if (LoaderHandoffSpecs.needsLoader(plan)) return Stage.LOAD;
    return Stage.DELIVERY;
  }

  public Map<String, Object> workflowInput(DirectLaunchRequest request, String executionKey) {
    policy.authorizeNamespace(request.namespace());
    ExecutionPlan plan = compile(request.ingestionSpec(), executionKey);
    Optional<IngestionSpec> delivery = deliveryStageSpec(plan, executionKey);
    Map<String, Object> first =
        worker(request, request.ingestionSpec(), plan, executionKey, firstStage(plan));
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("primary", first);
    result.put("namespace", request.namespace());
    result.put("continuous", request.ingestionSpec().executionMode() == ExecutionMode.CONTINUOUS);
    result.put("hasDelivery", delivery.isPresent());
    result.put("deploymentName", "fds-stream-" + digest(executionKey));
    first.put("name", result.get("deploymentName"));
    if (delivery.isPresent()) {
      ExecutionPlan deliveryPlan = deliveryPlan(delivery.get(), plan, executionKey);
      Map<String, Object> deliveryWorker =
          worker(request, delivery.get(), deliveryPlan, executionKey, Stage.DELIVERY);
      deliveryWorker.put("name", result.get("deploymentName") + "-delivery");
      result.put("delivery", deliveryWorker);
    }
    return result;
  }

  private Map<String, Object> worker(
      DirectLaunchRequest request,
      IngestionSpec spec,
      ExecutionPlan plan,
      String executionKey,
      Stage stage) {
    RunnerImages.Runner runner = runner(stage);
    String image = runner.image();
    String command = runner.command();
    policy.authorizeImage(image);
    Map<String, Object> environment =
        new LinkedHashMap<>(stageEnvironment(spec, plan, executionKey, stage));
    Map<String, Object> worker = new LinkedHashMap<>();
    if (stage == Stage.DELIVERY
        && plan.profile().executionMode() == ExecutionMode.CONTINUOUS
        && runners.engine() == DeliveryEngineKind.PEKKO_STREAMS
        && (plan.sources().size() > 1 || plan.transforms() != null)) {
      // A continuous Pekko worker that correlates or runs a graph keeps its state on the
      // installation's journal.
      RunnerImages.PekkoJournal journal = runners.journal();
      if (journal == null || journal.env().isEmpty())
        throw new IllegalStateException(
            "This continuous run keeps state: configure datastreaming.launcher.journal.env and"
                + " secret-env for the installation's journal");
      environment.put("FDS_PLATFORM_ENGINE", journal.platformEngine());
      environment.putAll(journal.env());
      journal
          .secretEnv()
          .forEach(
              (name, ref) ->
                  environment.put(
                      name, Map.of("secretKeyRef", Map.of("name", ref.name(), "key", ref.key()))));
      RunnerImages.DatabaseProxy proxy = journal.databaseProxy();
      if (proxy != null) {
        policy.authorizeImage(proxy.image());
        worker.put(
            "databaseProxy",
            Map.of(
                "image", proxy.image(),
                "instanceSecretName", proxy.instance().name(),
                "instanceSecretKey", proxy.instance().key(),
                "port", 5432));
        worker.put("serviceAccountName", proxy.serviceAccountName());
      }
    }
    worker.put("namespace", request.namespace());
    worker.put("image", image);
    worker.put("command", List.of("sh", "-c"));
    worker.put("args", List.of(command(command)));
    worker.put("env", environment);
    worker.put("parallelism", 1);
    worker.put("completions", 1);
    worker.put("replicas", 1);
    worker.put("backoffLimit", 0);
    worker.put(
        "resources",
        Map.of("requests", request.resourceRequests(), "limits", request.resourceLimits()));
    worker.put("imagePullSecretNames", pullSecrets);
    if (request.activeDeadlineSeconds() != null)
      worker.put("activeDeadlineSeconds", request.activeDeadlineSeconds());
    return worker;
  }

  /**
   * Snapshot the resolved policy with the worker spec; mounted launcher paths need not exist in its
   * pod.
   */
  RunnerImages.Runner runner(Stage stage) {
    return switch (stage) {
      case SPARK -> runners.spark();
      case LOAD -> runners.loader();
      case DELIVERY -> runners.delivery();
    };
  }

  /** Everything a stage's Job needs in its environment: the spec, its engine and its handoff. */
  Map<String, String> stageEnvironment(
      IngestionSpec spec, ExecutionPlan plan, String executionKey, Stage stage) {
    Map<String, String> environment = new LinkedHashMap<>(environment(spec, plan));
    environment.put("FDS_EXECUTION_ID", executionKey);
    environment.put(ExecutionPlanCompiler.DELIVERY_ENGINE_ENV, runners.engine().name());
    switch (stage) {
      case SPARK -> {
        environment.put("KAFKA_BOOTSTRAP_SERVERS", runners.kafkaBootstrapServers());
        environment.put("SPARK_HANDOFF_TOPIC", plan.sparkStage().orElseThrow().handoffTopic());
      }
      case LOAD -> {
        environment.put("KAFKA_BOOTSTRAP_SERVERS", runners.kafkaBootstrapServers());
        environment.put("FDS_LOAD_TOPIC", loadTopic(executionKey));
      }
      case DELIVERY -> {}
    }
    return environment;
  }

  static Map<String, String> environment(IngestionSpec spec, ExecutionPlan plan) {
    IngestionSpec resolved =
        new IngestionSpec(
            spec.sources(),
            spec.blockingField(),
            spec.transforms(),
            spec.sink(),
            spec.executionMode(),
            spec.delivery(),
            spec.errors(),
            "file:/tmp/merge-policy.yaml");
    try {
      return Map.of(
          "MERGE_POLICY_ROOT",
          "/tmp",
          "INGESTION_SPEC_YAML_BASE64",
          encode(resolved.toYaml()),
          "MERGE_POLICY_YAML_BASE64",
          encode(new ObjectMapper(new YAMLFactory()).writeValueAsString(plan.mergePolicy())));
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot serialize planned ingestion", failure);
    }
  }

  static String command(String runner) {
    return "printf '%s' \"$MERGE_POLICY_YAML_BASE64\" | base64 -d > /tmp/merge-policy.yaml &&"
        + " printf '%s' \"$INGESTION_SPEC_YAML_BASE64\" | base64 -d >"
        + " /tmp/ingestion-spec.yaml && exec "
        + runner
        + " /tmp/ingestion-spec.yaml";
  }

  private static String encode(String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String digest(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)),
              0,
              16);
    } catch (java.security.NoSuchAlgorithmException failure) {
      throw new IllegalStateException(failure);
    }
  }
}
