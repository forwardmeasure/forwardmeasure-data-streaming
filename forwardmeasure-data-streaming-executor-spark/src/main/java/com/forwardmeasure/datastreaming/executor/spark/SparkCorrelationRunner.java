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

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
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
 * {@link CorrelationSpec}'s (multi-source correlation) Spark entrypoint - split out 2026-09-13 from
 * what used to be {@code SparkIngestionRunner}'s own second {@code run()} overload plus a second
 * {@code main()}, once that arrangement was called out as an inconsistency next to {@code
 * forwardmeasure-data-streaming-executor-pekko}'s own clean {@code PekkoIngestionRunner}/{@code
 * PekkoCorrelationRunner} split: this module now has the same one-class-per-(engine, spec-shape)
 * shape. Drives {@link SparkCorrelationEngine}'s real read/map/correlate/merge logic across however
 * many sources the spec declares and writes the merged result to a real sink - the same "spec in,
 * real Job out" shape {@code PekkoCorrelationRunner} has, generalized to Spark.
 *
 * <p>Sink writing (generalized 2026-09-13) delegates entirely to {@link SparkSinks#write} - see
 * that class's own javadoc for the full dispatch (file/kafka/jdbc/opensearch/any other Spark-native
 * format).
 */
public final class SparkCorrelationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkCorrelationRunner.class);

  private SparkCorrelationRunner() {}

  /** Summary of one correlation run - how many sources were read and how many groups resulted. */
  public record CorrelationResult(long sourceCount, long groupCount) {}

  /**
   * Runs {@code spec}: reads and maps every declared source, correlates them on {@code
   * spec.blockingField()}, merges each group by trust weight (see {@link
   * SparkCorrelationEngine#correlate}), and writes the merged rows to {@code spec.sink()} via
   * {@link SparkSinks#write}.
   */
  public static CorrelationResult run(SparkSession spark, CorrelationSpec spec) {
    IngestionPipeline.MalformedRecordPolicy malformedRecordPolicy =
        IngestionPipeline.MalformedRecordPolicy.from(spec.execution().failure());
    List<JavaRDD<SparkCorrelationRecord>> mapped =
        spec.sources().stream()
            .map(
                entry ->
                    SparkCorrelationEngine.readAndMap(
                        spark,
                        new SparkSourceConfig(
                            entry.sourceKey(), entry.source(), entry.mapper(), entry.trustWeight()),
                        spec.blockingField(),
                        Map.of(),
                        malformedRecordPolicy))
            .toList();

    // Cached deliberately: count() below and the saveAsTextFile() write are two separate actions
    // on the same lazily-evaluated RDD - without caching, Spark would recompute the entire
    // read/map/correlate/merge pipeline (across every source) a second time for the write alone.
    JavaRDD<Map<String, Object>> merged = SparkCorrelationEngine.correlate(mapped).cache();
    long groupCount = merged.count();
    // The merged rows' own key set is exactly the union of every source's own mapper().fields()
    // target names (see SparkCorrelationEngine#mergeGroup - each merged row's fields all come from
    // some source's own TransformSpec output) - declared statically here, not scanned from the
    // data, for the same reason SparkIngestionRunner derives its own columns from spec.mapper().
    List<String> columns =
        spec.sources().stream()
            .flatMap(entry -> entry.mapper().fields().stream())
            .map(TransformSpec.FieldRule::target)
            .distinct()
            .toList();
    SparkSinks.write(spark, merged, spec.sink(), spec.execution(), columns);
    return new CorrelationResult(spec.sources().size(), groupCount);
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("CORRELATION_SPEC_PATH");
    CorrelationSpec spec = CorrelationSpec.load(Path.of(specPath));

    SparkSession spark =
        SparkSessionFactory.create(
            "forwardmeasure-data-streaming-correlation",
            SparkIngestionRunner.sparkExecutorConfigFromEnv());
    int exitCode = 0;
    try {
      CorrelationResult result = run(spark, spec);
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
