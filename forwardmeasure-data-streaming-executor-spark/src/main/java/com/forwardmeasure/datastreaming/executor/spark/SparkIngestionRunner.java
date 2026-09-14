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

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IngestionSpec}'s (single-source, no correlation) Spark entrypoint - renamed 2026-09-13
 * from {@code SparkSourceIngestionRunner} once its only reason to exist as a class separate from
 * this one (housing a distinct {@code main()} apart from a class that also carried {@code
 * CorrelationSpec} handling) went away: this class now owns that {@code main()} directly, and the
 * {@code CorrelationSpec} path this class used to also carry moved out to its own {@link
 * SparkCorrelationRunner}. Mirrors {@code forwardmeasure-data-streaming-executor-pekko}'s own
 * {@code PekkoIngestionRunner}/{@code PekkoCorrelationRunner} split - one {@code
 * {Engine}{SpecShape}Runner} class per (engine, spec-shape) pair, consistently, rather than one
 * class holding both spec types' logic plus a second class that exists only to give one of them its
 * own {@code main()}.
 *
 * <p>Sink writing (generalized 2026-09-13, see {@link SparkSinks} for the full dispatch and why it
 * was extracted into its own shared class once it stopped being {@code file}-only) delegates
 * entirely to {@link SparkSinks#write} - this class's own job is just reading/mapping the one
 * declared source and handing the result off.
 */
public final class SparkIngestionRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionRunner.class);

  private SparkIngestionRunner() {}

  /** Summary of one single-source run - how many rows were mapped and written. */
  public record IngestionResult(long recordsWritten) {}

  /**
   * Runs {@code spec}: reads and maps its one declared source (see {@link
   * SparkCorrelationEngine#readAndMapSingleSource} for why this is genuinely not "correlation with
   * one source" - no grouping/merging step exists here at all), and writes every mapped row to
   * {@code spec.sink()} via {@link SparkSinks#write}. The Spark-engine counterpart to {@code
   * PekkoIngestionRunner}'s own Pekko-engine single-source run - same {@link IngestionSpec} input,
   * same "every row independent, nothing ever merged" semantics, different execution engine.
   */
  public static IngestionResult run(SparkSession spark, IngestionSpec spec) {
    IngestionPipeline.MalformedRecordPolicy malformedRecordPolicy =
        IngestionPipeline.MalformedRecordPolicy.from(spec.execution().failure());
    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
                spark, spec.source(), spec.mapper(), Map.of(), malformedRecordPolicy)
            .cache();
    long recordsWritten = mapped.count();
    List<String> columns =
        spec.mapper().fields().stream().map(TransformSpec.FieldRule::target).distinct().toList();
    SparkSinks.write(spark, mapped, spec.sink(), spec.execution(), columns);
    return new IngestionResult(recordsWritten);
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));

    SparkSession spark =
        SparkSessionFactory.create(
            "forwardmeasure-data-streaming-ingestion", sparkExecutorConfigFromEnv());
    int exitCode = 0;
    try {
      IngestionResult result = run(spark, spec);
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

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
