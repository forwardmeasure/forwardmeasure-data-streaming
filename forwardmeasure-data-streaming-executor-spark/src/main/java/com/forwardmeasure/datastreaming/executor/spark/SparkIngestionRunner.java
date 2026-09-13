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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standalone runnable entrypoint {@code forwardmeasure-data-streaming-executor-spark} never had
 * (added 2026-09-13, once the launcher work surfaced the gap - see the design plan's own live gap
 * tracker): reads a {@link CorrelationSpec}, drives {@link SparkCorrelationEngine}'s real
 * read/map/correlate/merge logic across however many sources the spec declares, and writes the
 * merged result to a real sink - the same "spec in, real Job out" shape {@code
 * forwardmeasure-data-streaming-executor-pekko}'s own {@code IngestionPipelineRunner} already has
 * for the single-source path, generalized to Spark's own multi-source correlation.
 *
 * <p><b>Deliberate scope for this first cut, not a silent gap</b>: only a {@code file} sink is
 * supported, writing NDJSON via Spark's own native, already-distributed {@code
 * JavaRDD#saveAsTextFile} - not Camel. This is a deliberate divergence from the Pekko runner's own
 * sink (which does use Camel, via a real {@code ProducerTemplate}): the *source* side here already
 * uses Spark's own native readers (see {@link SparkCorrelationEngine}'s own {@code readSource}),
 * not Camel, so writing through Spark's own native, distributed writer for the sink too is the
 * consistent choice - introducing Camel into this module only for one connector's sink would mean a
 * `CamelContext` per partition across a whole cluster for no real benefit over Spark's own writer.
 * Any other {@code SinkSpec.connector} throws {@link UnsupportedOperationException} rather than
 * silently writing nowhere real.
 */
public final class SparkIngestionRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionRunner.class);
  private static final String FILE_CONNECTOR = "file";

  private SparkIngestionRunner() {}

  /** Summary of one correlation run - how many sources were read and how many groups resulted. */
  public record CorrelationResult(long sourceCount, long groupCount) {}

  /**
   * Runs {@code spec}: reads and maps every declared source, correlates them on {@code
   * spec.blockingField()}, merges each group by trust weight (see {@link
   * SparkCorrelationEngine#correlate}), and writes the merged rows to {@code spec.sink()}.
   *
   * @throws UnsupportedOperationException if the sink's connector isn't {@code file} (see this
   *     class's own javadoc for why)
   */
  public static CorrelationResult run(SparkSession spark, CorrelationSpec spec) {
    String connector = spec.sink().connector();
    if (!FILE_CONNECTOR.equals(connector)) {
      throw new UnsupportedOperationException(
          "SparkIngestionRunner: sink connector '"
              + connector
              + "' is not yet supported - only 'file' (Spark's own native distributed writer)"
              + " today; see this class's own javadoc for why that's a real, deliberate scope"
              + " boundary, not a silent gap");
    }

    List<JavaRDD<SparkCorrelationRecord>> mapped =
        spec.sources().stream()
            .map(
                entry ->
                    SparkCorrelationEngine.readAndMap(
                        spark,
                        new SparkSourceConfig(
                            entry.sourceKey(), entry.source(), entry.mapper(), entry.trustWeight()),
                        spec.blockingField(),
                        Map.of()))
            .toList();

    // Cached deliberately: count() below and the saveAsTextFile() write are two separate actions
    // on the same lazily-evaluated RDD - without caching, Spark would recompute the entire
    // read/map/correlate/merge pipeline (across every source) a second time for the write alone.
    JavaRDD<Map<String, Object>> merged = SparkCorrelationEngine.correlate(mapped).cache();
    long groupCount = merged.count();
    serializeToNdjson(merged).saveAsTextFile(spec.sink().uri());
    return new CorrelationResult(spec.sources().size(), groupCount);
  }

  private static JavaRDD<String> serializeToNdjson(JavaRDD<Map<String, Object>> merged) {
    return merged.mapPartitions(
        (FlatMapFunction<java.util.Iterator<Map<String, Object>>, String>)
            rows -> {
              ObjectMapper objectMapper = new ObjectMapper();
              List<String> serialized = new ArrayList<>();
              while (rows.hasNext()) {
                serialized.add(writeValueAsString(rows.next(), objectMapper));
              }
              return serialized.iterator();
            });
  }

  private static String writeValueAsString(Map<String, Object> row, ObjectMapper objectMapper) {
    try {
      return objectMapper.writeValueAsString(row);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("CORRELATION_SPEC_PATH");
    CorrelationSpec spec = CorrelationSpec.load(Path.of(specPath));

    // A plain `java -jar` process builds its own embedded SparkSession, never a spark-submit-
    // orchestrated cluster deployment (see this module's own Dockerfile.jvm) - so `master` must be
    // set here explicitly; nothing else (no spark-submit, no spark-defaults.conf) will supply one,
    // and SparkSession.getOrCreate() throws immediately without it.
    SparkSession spark =
        SparkSession.builder()
            .appName("forwardmeasure-data-streaming-correlation")
            .master("local[*]")
            .getOrCreate();
    int exitCode = 0;
    try {
      CorrelationResult result = run(spark, spec);
      LOGGER.info(
          "SparkIngestionRunner: sourceCount={} groupCount={}",
          result.sourceCount(),
          result.groupCount());
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

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
