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
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Writes a mapped {@code JavaRDD<Map<String,Object>>} to a real Kafka topic - the single, shared
 * sink dispatch both {@link SparkIngestionRunner} and {@link SparkCorrelationRunner} call.
 *
 * <p><b>Retired 2026-09-21</b> (per the repo's own gap-bridging plan, "Spark: an optional
 * distributed compute stage, never a delivery engine"): this class used to also write directly to
 * {@code file}/{@code jdbc}/{@code opensearch} - real business destinations. That violated the
 * invariant that Spark may compute but ownership of final delivery always returns to a {@code
 * DeliveryEngine}. Those three write paths (and their own tests) are deleted, not deprecated -
 * {@link #write} now refuses anything but {@code kafka} outright, so a caller cannot silently
 * regress back to a direct write. {@code sink} here is never the spec's real destination; it is
 * always the {@link com.forwardmeasure.datastreaming.api.SparkStagePlan#handoffTopic()} a {@code
 * DeliveryEngine} picks up afterward - see {@link SparkIngestionRunner#run}/{@link
 * SparkCorrelationRunner#run} for how that handoff {@link SinkSpec} is built.
 *
 * <p>Spark's own native {@code kafka} write format needs a single {@code value} column (confirmed
 * by Spark's own documented Kafka sink contract) - this class builds one from each row's own JSON
 * serialization, the same body shape the Pekko sink's own kafka path already sends.
 *
 * <p>{@code errors.sinkFailure()}, wired for real 2026-09-13: the terminal write action goes
 * through {@link IngestionPipeline#withSinkFailureHandlingBlocking} (retry with backoff when {@code
 * sinkFailure: retry}, fail-fast otherwise) - the retry *mechanism* itself is proven generically by
 * {@code IngestionPipelineTest} in {@code forwardmeasure-data-streaming-core}, not re-proven per
 * connector here.
 *
 * <p><b>Widened to public 2026-09-28</b>: {@link #write} was package-private until now, reachable
 * only by {@link SparkIngestionRunner}/{@link SparkCorrelationRunner} in this same package. A real
 * external caller with its own already-correct, non-{@code IngestionSpec}-driven read/map/correlate
 * pipeline (FEI's own {@code CorrelatedSourceIngestionWorker}, adopting this project's own "Spark:
 * an optional distributed compute stage, never a delivery engine" rule for its own merged RDD)
 * needs exactly this method - the terminal Kafka handoff write - without adopting this module's
 * entire {@code IngestionSpec}/{@code ExecutionPlanCompiler} planner subsystem just to reach it.
 * {@link SinkSpec}/{@link ErrorPolicy} were already public; only this method's own visibility was
 * the real blocker.
 */
public final class SparkSinks {

  private static final String KAFKA_CONNECTOR = "kafka";

  private SparkSinks() {}

  public static void write(
      SparkSession spark, JavaRDD<Map<String, Object>> mapped, SinkSpec sink, ErrorPolicy errors) {
    if (!KAFKA_CONNECTOR.equals(sink.connector())) {
      throw new IllegalArgumentException(
          "SparkSinks: '"
              + sink.connector()
              + "' is not a real business destination Spark may write to directly - Spark only"
              + " ever hands off via a kafka topic to a DeliveryEngine (see this class's own"
              + " javadoc)");
    }
    IngestionPipeline.SinkFailurePolicy sinkFailurePolicy =
        IngestionPipeline.SinkFailurePolicy.from(errors);
    JavaRDD<Row> rows =
        serializeToNdjson(mapped).map((Function<String, Row>) json -> RowFactory.create(json));
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("value", DataTypes.StringType, false)});
    Dataset<Row> dataFrame = spark.createDataFrame(rows, schema);

    Map<String, String> options = new LinkedHashMap<>(sink.options());
    options.putIfAbsent("kafka.bootstrap.servers", sink.uri());
    options.putIfAbsent("topic", sink.index());
    IngestionPipeline.withSinkFailureHandlingBlocking(
        sinkFailurePolicy, () -> dataFrame.write().format("kafka").options(options).save());
  }

  static JavaRDD<String> serializeToNdjson(JavaRDD<Map<String, Object>> mapped) {
    return mapped.mapPartitions(
        (FlatMapFunction<Iterator<Map<String, Object>>, String>)
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
}
