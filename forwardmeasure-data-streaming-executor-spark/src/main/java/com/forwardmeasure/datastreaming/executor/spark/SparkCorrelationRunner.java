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
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExecutionPlan}'s ({@code sources.size() > 1}, multi-source correlation) Spark entrypoint -
 * only ever invoked for a plan the planner gave a {@link
 * com.forwardmeasure.datastreaming.api.SparkStagePlan}, mirroring {@link SparkIngestionRunner}'s
 * own single-source counterpart exactly. Drives {@link SparkCorrelationEngine}'s real
 * read/map/correlate/merge logic across however many sources the plan declares and hands the merged
 * result off to a real Kafka topic - never a real business destination directly, see {@link
 * SparkSinks}' own javadoc for why.
 */
public final class SparkCorrelationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkCorrelationRunner.class);

  private SparkCorrelationRunner() {}

  /** Summary of one correlation run - how many sources were read and how many groups resulted. */
  public record CorrelationResult(long sourceCount, long groupCount) {}

  /**
   * Runs {@code plan}: reads and maps every declared source, correlates them on {@code
   * plan.blockingField()}, merges each group by trust weight (see {@link
   * SparkCorrelationEngine#correlate}), and writes the merged rows to {@code
   * plan.sparkStage().get().handoffTopic()} on the real Kafka cluster reachable at {@code
   * kafkaBootstrapServers} via {@link SparkSinks#write}.
   */
  public static CorrelationResult run(
      SparkSession spark, ExecutionPlan plan, String kafkaBootstrapServers) {
    if (plan.sources().size() <= 1) {
      throw new IllegalArgumentException(
          "SparkCorrelationRunner: expected more than one source, got "
              + plan.sources().size()
              + " - use SparkIngestionRunner for a single-source plan");
    }
    if (plan.sparkStage().isEmpty()) {
      throw new IllegalStateException(
          "SparkCorrelationRunner: plan has no sparkStage - it should never have been dispatched"
              + " to Spark at all (see this class's own javadoc)");
    }
    IngestionPipeline.MalformedRecordPolicy malformedRecordPolicy =
        IngestionPipeline.MalformedRecordPolicy.from(plan.errors());
    List<JavaRDD<SparkCorrelationRecord>> mapped =
        plan.sources().stream()
            .map(
                entry ->
                    SparkCorrelationEngine.readAndMap(
                        spark,
                        new SparkSourceConfig(
                            entry.sourceKey(), entry.source(), entry.mapper(), entry.trustWeight()),
                        plan.blockingField(),
                        Map.of(),
                        malformedRecordPolicy))
            .toList();

    // Cached deliberately: count() below and the write() call are two separate actions on the
    // same lazily-evaluated RDD - without caching, Spark would recompute the entire
    // read/map/correlate/merge pipeline (across every source) a second time for the write alone.
    JavaRDD<Map<String, Object>> merged = SparkCorrelationEngine.correlate(mapped).cache();
    long groupCount = merged.count();
    SinkSpec handoffSink = SparkIngestionRunner.handoffSink(plan, kafkaBootstrapServers);
    SparkSinks.write(spark, merged, handoffSink, plan.errors());
    return new CorrelationResult(plan.sources().size(), groupCount);
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("CORRELATION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);
    String kafkaBootstrapServers = SparkIngestionRunner.kafkaBootstrapServersFromEnv();

    SparkSession spark =
        SparkSessionFactory.create(
            "forwardmeasure-data-streaming-correlation",
            SparkIngestionRunner.sparkExecutorConfigFromEnv());
    int exitCode = 0;
    try {
      CorrelationResult result = run(spark, plan, kafkaBootstrapServers);
      LOGGER.info(
          "SparkCorrelationRunner: sourceCount={} groupCount={}",
          result.sourceCount(),
          result.groupCount());
    } catch (Exception e) {
      LOGGER.error("SparkCorrelationRunner: run failed", e);
      exitCode = 1;
    } finally {
      spark.stop();
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
