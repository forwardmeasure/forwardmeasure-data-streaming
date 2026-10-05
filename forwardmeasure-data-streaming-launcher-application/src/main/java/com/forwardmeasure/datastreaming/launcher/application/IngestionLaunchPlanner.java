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
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
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
  private final String pekkoImage,
      pekkoCommand,
      kafkaImage,
      kafkaCommand,
      sparkImage,
      sparkCommand,
      brokers;
  private final List<String> pullSecrets;
  private final IngestionJobPolicy policy;

  public IngestionLaunchPlanner(
      IngestionJobPolicy policy,
      String pekkoImage,
      String pekkoCommand,
      String kafkaImage,
      String kafkaCommand,
      String sparkImage,
      String sparkCommand,
      String brokers,
      List<String> pullSecrets) {
    this.policy = policy;
    this.pekkoImage = pekkoImage;
    this.pekkoCommand = pekkoCommand;
    this.kafkaImage = kafkaImage;
    this.kafkaCommand = kafkaCommand;
    this.sparkImage = sparkImage;
    this.sparkCommand = sparkCommand;
    this.brokers = brokers;
    this.pullSecrets = List.copyOf(pullSecrets);
  }

  public ExecutionPlan compile(IngestionSpec spec, String executionKey) {
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);
    if (plan.sparkStage().isEmpty()) return plan;
    if (spec.executionMode() == ExecutionMode.CONTINUOUS) {
      for (SourcePlan source : spec.sources()) {
        if (!"kafka".equals(source.source().connector())
            || !KafkaConnectorUri.parse(source.source().uri()).bootstrapServers().equals(brokers)) {
          throw new IllegalArgumentException(
              "Continuous Spark sources and handoff must share the configured Kafka cluster");
        }
      }
    }
    if (sparkImage == null || sparkCommand == null || brokers == null) {
      throw new IllegalStateException("Spark runner and Kafka handoff settings are required");
    }
    return new ExecutionPlan(
        plan.profile(),
        plan.sources(),
        plan.blockingField(),
        Optional.of(
            new SparkStagePlan(
                plan.sparkStage().orElseThrow().transformNames(),
                "fds-spark-handoff-" + digest(executionKey))),
        plan.transforms(),
        plan.destination(),
        plan.delivery(),
        plan.errors(),
        plan.mergePolicy());
  }

  public Map<String, Object> workflowInput(DirectLaunchRequest request, String executionKey) {
    policy.authorizeNamespace(request.namespace());
    ExecutionPlan plan = compile(request.ingestionSpec(), executionKey);
    Map<String, Object> first =
        worker(request, request.ingestionSpec(), plan, executionKey, plan.sparkStage().isPresent());
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("primary", first);
    result.put("namespace", request.namespace());
    result.put("continuous", request.ingestionSpec().executionMode() == ExecutionMode.CONTINUOUS);
    result.put("hasDelivery", plan.sparkStage().isPresent());
    result.put("deploymentName", "fds-stream-" + digest(executionKey));
    first.put("name", result.get("deploymentName"));
    if (plan.sparkStage().isPresent()) {
      IngestionSpec delivery = SparkHandoffSpecs.deliveryStageSpec(plan, brokers);
      ExecutionPlan compiled = compile(delivery, executionKey);
      ExecutionPlan deliveryPlan =
          new ExecutionPlan(
              compiled.profile(),
              compiled.sources(),
              compiled.blockingField(),
              compiled.sparkStage(),
              compiled.transforms(),
              compiled.destination(),
              compiled.delivery(),
              compiled.errors(),
              plan.mergePolicy());
      Map<String, Object> deliveryWorker =
          worker(request, delivery, deliveryPlan, executionKey, false);
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
      boolean spark) {
    boolean kafka = plan.profile().deliveryEngine() == DeliveryEngineKind.KAFKA_STREAMS;
    String image = spark ? sparkImage : kafka ? kafkaImage : pekkoImage;
    String command = spark ? sparkCommand : kafka ? kafkaCommand : pekkoCommand;
    policy.authorizeImage(image);
    Map<String, String> environment = new LinkedHashMap<>(environment(spec, plan));
    environment.put("FDS_EXECUTION_ID", executionKey);
    if (spark) {
      environment.put("KAFKA_BOOTSTRAP_SERVERS", brokers);
      environment.put("SPARK_HANDOFF_TOPIC", plan.sparkStage().orElseThrow().handoffTopic());
    }
    Map<String, Object> worker = new LinkedHashMap<>();
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
