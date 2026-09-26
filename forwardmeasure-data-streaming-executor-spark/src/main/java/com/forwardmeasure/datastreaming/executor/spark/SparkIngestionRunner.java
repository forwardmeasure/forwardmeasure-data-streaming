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
package com.forwardmeasure.datastreaming.executor.spark;

import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExecutionPlan}'s ({@code sources.size() == 1}, no correlation) Spark entrypoint - only
 * ever invoked for a plan the planner gave a {@link
 * com.forwardmeasure.datastreaming.api.SparkStagePlan} (see {@link ExecutionPlanCompiler}); {@link
 * #run} rejects a plan without one rather than guessing a destination.
 *
 * <p>Sink writing (retargeted 2026-09-21, see {@link SparkSinks} for the full retirement of its old
 * {@code file}/{@code jdbc}/{@code opensearch} direct-write paths) always delegates to {@link
 * SparkSinks#write} against the plan's own {@code sparkStage().handoffTopic()} - never {@code
 * plan.destination()} directly. A real business-destination write is a {@code DeliveryEngine}'s
 * job, picked up from that topic afterward (a real two-Job pipeline this repo's launcher layer
 * deliberately doesn't dispatch yet - see {@code DirectIngestionLauncher}'s own javadoc).
 */
public final class SparkIngestionRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionRunner.class);

  private SparkIngestionRunner() {}

  /** Summary of one single-source run - how many rows were mapped and written. */
  public record IngestionResult(long recordsWritten) {}

  /**
   * Runs {@code plan}: reads and maps its one declared source (see {@link
   * SparkCorrelationEngine#readAndMapSingleSource} for why this is genuinely not "correlation with
   * one source" - no grouping/merging step exists here at all), and writes every mapped row to
   * {@code plan.sparkStage().get().handoffTopic()} on the real Kafka cluster reachable at {@code
   * kafkaBootstrapServers} via {@link SparkSinks#write}.
   */
  public static IngestionResult run(
      SparkSession spark, ExecutionPlan plan, String kafkaBootstrapServers) {
    if (plan.sources().size() != 1) {
      throw new IllegalArgumentException(
          "SparkIngestionRunner: expected exactly one source, got "
              + plan.sources().size()
              + " - use SparkCorrelationRunner for a correlated plan");
    }
    if (plan.sparkStage().isEmpty()) {
      throw new IllegalStateException(
          "SparkIngestionRunner: plan has no sparkStage - it should never have been dispatched to"
              + " Spark at all (see this class's own javadoc)");
    }
    SourcePlan sourcePlan = plan.sources().get(0);
    IngestionPipeline.MalformedRecordPolicy malformedRecordPolicy =
        IngestionPipeline.MalformedRecordPolicy.from(plan.errors());
    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
                spark, sourcePlan.source(), sourcePlan.mapper(), Map.of(), malformedRecordPolicy)
            .cache();
    long recordsWritten = mapped.count();
    SinkSpec handoffSink = handoffSink(plan, kafkaBootstrapServers);
    SparkSinks.write(spark, mapped, handoffSink, plan.errors());
    return new IngestionResult(recordsWritten);
  }

  static SinkSpec handoffSink(ExecutionPlan plan, String kafkaBootstrapServers) {
    return new SinkSpec(
        "kafka",
        kafkaBootstrapServers,
        plan.sparkStage().get().handoffTopic(),
        null,
        null,
        Map.of());
  }

  /**
   * Real, live-found bug fix (2026-09-25): {@code ExecutionPlanCompiler.resolveSparkStage} derives
   * {@code handoffTopic} from {@code spec.hashCode()} - a value {@link DirectIngestionLauncher}
   * (fowf-free direct mode) computes twice, independently, in two different processes: once from
   * the in-memory {@code IngestionSpec} object when building the delivery Job's own handoff
   * ConfigMap, and again here, from the same spec after a base64/YAML round-trip. Nothing
   * guarantees those two {@code hashCode()} calls agree (a YAML round-trip is not contractually
   * required to be hashCode-preserving), and a live run confirmed they genuinely can disagree:
   * Spark wrote 5 real rows to its own (correctly, locally re-derived) topic while the delivery
   * Job's consumer, reading the launcher's independently-derived topic name, saw {@code
   * recordsRead=0} - a real, silent data loss, not a hypothetical one. The launcher is the one
   * process that must actually agree with itself (it derives the topic once, for the ConfigMap, and
   * needs the dispatched pod to write to that exact same topic) - so it now passes its own
   * already-computed topic explicitly via {@code SPARK_HANDOFF_TOPIC}, and this override always
   * wins over any value {@code ExecutionPlanCompiler} would derive fresh inside the pod.
   * Package-visible so {@link SparkCorrelationRunner#main} applies the identical override.
   */
  static final String SPARK_HANDOFF_TOPIC_ENV_VAR = "SPARK_HANDOFF_TOPIC";

  static ExecutionPlan withHandoffTopicOverride(ExecutionPlan plan) {
    String override = System.getenv(SPARK_HANDOFF_TOPIC_ENV_VAR);
    if (override == null || override.isBlank() || plan.sparkStage().isEmpty()) {
      return plan;
    }
    SparkStagePlan overridden =
        new SparkStagePlan(plan.sparkStage().get().transformNames(), override.trim());
    return new ExecutionPlan(
        plan.profile(),
        plan.sources(),
        plan.blockingField(),
        java.util.Optional.of(overridden),
        plan.transforms(),
        plan.destination(),
        plan.delivery(),
        plan.errors());
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = withHandoffTopicOverride(ExecutionPlanCompiler.compile(spec));
    String kafkaBootstrapServers = kafkaBootstrapServersFromEnv();

    SparkSession spark =
        SparkSessionFactory.create(
            "forwardmeasure-data-streaming-ingestion", sparkExecutorConfigFromEnv());
    int exitCode = 0;
    try {
      IngestionResult result = run(spark, plan, kafkaBootstrapServers);
      LOGGER.info("SparkIngestionRunner: recordsWritten={}", result.recordsWritten());
    } catch (Exception e) {
      LOGGER.error("SparkIngestionRunner: run failed", e);
      exitCode = 1;
    } finally {
      spark.stop();
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /**
   * {@code SPARK_MASTER} defaults to {@code local[*]} (single-pod, embedded driver) - a caller
   * wanting real distributed Spark sets it to {@code k8s://...} and also provides {@code
   * SPARK_KUBERNETES_CONTAINER_IMAGE}/{@code SPARK_KUBERNETES_NAMESPACE} (required in that mode -
   * see {@link SparkSessionFactory#sparkConfigProperties}); {@code
   * SPARK_KUBERNETES_SERVICE_ACCOUNT}/ {@code SPARK_EXECUTOR_POD_TEMPLATE_FILE} are always
   * optional. Package-visible so {@link SparkCorrelationRunner} can build the identical config
   * shape from the identical env vars.
   */
  static SparkExecutorConfig sparkExecutorConfigFromEnv() {
    return new SparkExecutorConfig(
        System.getenv().getOrDefault("SPARK_MASTER", "local[*]"),
        System.getenv("SPARK_KUBERNETES_CONTAINER_IMAGE"),
        System.getenv("SPARK_KUBERNETES_NAMESPACE"),
        System.getenv("SPARK_KUBERNETES_SERVICE_ACCOUNT"),
        System.getenv("SPARK_EXECUTOR_POD_TEMPLATE_FILE"));
  }

  /**
   * The real Kafka cluster's own bootstrap servers - deliberately not part of the compiled {@link
   * ExecutionPlan} (the planner has no business knowing a runtime infra address; {@code
   * SparkStagePlan#handoffTopic()} is just a topic name). Package-visible so {@link
   * SparkCorrelationRunner} reads the identical env var.
   */
  static String kafkaBootstrapServersFromEnv() {
    return requiredEnv("KAFKA_BOOTSTRAP_SERVERS");
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
