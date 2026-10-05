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
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, live-found gap fixed (2026-09-25): the packaged {@code data-streaming-executor-spark}
 * image's entrypoint was hardcoded to {@link SparkIngestionRunner#main}, which explicitly rejects
 * any plan with more than one source - {@code DirectIngestionLauncher} dispatches the same image/
 * command for a Spark stage regardless of {@code plan.sources().size()}, so a real correlated,
 * {@code HEAVY}-transform spec would have reached the pod and crashed immediately, exactly the
 * class of half-built dispatch path this org's own standing discipline (see {@code
 * feedback_ask_before_deferring_any_capability}) exists to catch before it ships. This class is now
 * the real image entrypoint (see {@code -executor-spark/pom.xml}'s {@code mainClass}) - it branches
 * on cardinality the same way {@code PekkoStreamsDeliveryEngine}/{@code KafkaStreamsDeliveryEngine}
 * already do for their own delivery dispatch, delegating to the already-real, already-tested {@link
 * SparkIngestionRunner#run}/{@link SparkCorrelationRunner#run} - a pure relocation of the dispatch
 * decision, not new engine logic.
 *
 * <p>{@link SparkIngestionRunner#main}/{@link SparkCorrelationRunner#main} are unchanged and still
 * directly invokable (e.g. a manual {@code spark-submit} outside the Job-dispatch flow) - this
 * class does not replace them, it's the one thing {@code java -jar} actually runs by default now.
 */
public final class SparkStageRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkStageRunner.class);

  private SparkStageRunner() {}

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan =
        SparkIngestionRunner.withHandoffTopicOverride(ExecutionPlanCompiler.compile(spec));
    String kafkaBootstrapServers = SparkIngestionRunner.kafkaBootstrapServersFromEnv();

    SparkSession spark =
        SparkSessionFactory.create(
            "forwardmeasure-data-streaming-spark-stage",
            SparkIngestionRunner.sparkExecutorConfigFromEnv());
    int exitCode = 0;
    try {
      if (plan.profile().executionMode()
          == com.forwardmeasure.datastreaming.api.ExecutionMode.CONTINUOUS) {
        ContinuousSparkStageRunner.run(spark, plan, kafkaBootstrapServers);
      } else if (plan.sources().size() == 1) {
        SparkIngestionRunner.IngestionResult result =
            SparkIngestionRunner.run(spark, plan, kafkaBootstrapServers);
        LOGGER.info("SparkStageRunner: single-source recordsWritten={}", result.recordsWritten());
      } else {
        SparkCorrelationRunner.CorrelationResult result =
            SparkCorrelationRunner.run(spark, plan, kafkaBootstrapServers);
        LOGGER.info(
            "SparkStageRunner: correlated sourceCount={} groupCount={}",
            result.sourceCount(),
            result.groupCount());
      }
    } catch (Exception e) {
      LOGGER.error("SparkStageRunner: run failed", e);
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
