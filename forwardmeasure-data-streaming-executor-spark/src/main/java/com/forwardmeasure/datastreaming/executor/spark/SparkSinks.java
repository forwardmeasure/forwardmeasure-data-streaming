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
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.OpenSearchIndexInitializer;
import com.forwardmeasure.datastreaming.api.SecretRefs;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Writes a mapped {@code JavaRDD<Map<String,Object>>} to {@code sink} - the single, shared sink
 * dispatch both {@link SparkIngestionRunner} and {@link SparkCorrelationRunner} call (extracted
 * 2026-09-13 alongside genuinely generalizing it beyond the previous {@code file}-only restriction,
 * which both of those classes' own {@code run()} methods used to enforce independently).
 *
 * <p>Dispatch: {@code file} keeps Spark's own native, already-distributed {@code
 * JavaRDD#saveAsTextFile} NDJSON writer - unchanged. {@code kafka} uses Spark's own native {@code
 * kafka} write format, which needs a single {@code value} column (confirmed by Spark's own
 * documented Kafka sink contract) - this class builds one from each row's own JSON serialization,
 * the same body shape the Pekko sink's own kafka path already sends. Every other connector (e.g.
 * {@code jdbc}, or any further Spark-native format like {@code parquet}) goes through a genuinely
 * structured {@link Dataset} built with one nullable string column per {@code columns} entry - the
 * caller's own declared output field names (each spec's {@code TransformSpec.fields()}' own {@code
 * target} names - the union across every source for {@link SparkCorrelationRunner}'s multi-source
 * case), not inferred from the data. An earlier version derived {@code columns} from the RDD itself
 * ({@code distinct()} + {@code collect()} over every row's own key set) - dropped 2026-09-13: a
 * full extra distributed action over the whole dataset just to learn field names that the spec
 * already declares statically, and (found the same day) not even reliably mutable once collected on
 * every Spark version. Deliberately not attempting numeric/date type inference, which would be a
 * guess this class has no real basis for. {@code opensearch} has no Spark-native writer at all (it
 * isn't a Spark ecosystem format), so it gets the one genuinely bespoke per-connector path here: a
 * real {@code foreachPartition} PUT-per- document loop using the JDK's own {@code
 * java.net.http.HttpClient} - built fresh inside the closure (never captured from the driver), the
 * same "no live object crosses the wire" discipline {@link SparkCorrelationEngine} already
 * established for {@code FieldMappingEngine}.
 *
 * <p>{@code jdbc}'s own {@code user}/{@code password} and {@code opensearch}'s own basic-auth
 * credentials go through {@link SparkSecretOptions}/{@link com.forwardmeasure.datastreaming.api
 * .SecretRefs} first (added 2026-09-13) - a reference like {@code env:DB_PASSWORD} is resolved
 * before Spark or the HTTP client ever sees it, not handed through as that literal string.
 *
 * <p>{@code execution.failure().sinkFailure()}/{@code sink.batching()}, wired for real 2026-09-13:
 * every terminal write action goes through {@link
 * IngestionPipeline#withSinkFailureHandlingBlocking} (retry with backoff when {@code sinkFailure:
 * retry}, a real gap - a single transient write failure used to fail the whole Spark job outright,
 * every connector, with no way to opt out). {@code batching.maxRecords()} maps onto {@code jdbc}'s
 * own real, native {@code batchsize} write option (a genuine JDBC batch, not invented); {@code
 * batching.maxWait()} has no meaningful equivalent here - Spark writes a whole {@code Dataset} in
 * one {@code .save()} call, not a continuous, time-windowed stream the way Pekko's own sink is, so
 * a wall-clock wait has nothing to bound - stated plainly rather than silently ignored.
 */
final class SparkSinks {

  private static final String FILE_CONNECTOR = "file";
  private static final String JDBC_CONNECTOR = "jdbc";
  private static final String KAFKA_CONNECTOR = "kafka";
  private static final String OPENSEARCH_CONNECTOR = "opensearch";

  private SparkSinks() {}

  static void write(
      SparkSession spark,
      JavaRDD<Map<String, Object>> mapped,
      SinkSpec sink,
      ExecutionSpec execution,
      List<String> columns) {
    IngestionPipeline.SinkFailurePolicy sinkFailurePolicy =
        IngestionPipeline.SinkFailurePolicy.from(execution.failure());
    String connector = sink.connector();
    if (FILE_CONNECTOR.equals(connector)) {
      writeFile(mapped, sink);
    } else if (KAFKA_CONNECTOR.equals(connector)) {
      writeKafka(spark, mapped, sink, sinkFailurePolicy);
    } else if (OPENSEARCH_CONNECTOR.equals(connector)) {
      writeOpenSearch(mapped, sink, sinkFailurePolicy);
    } else {
      writeStructured(spark, mapped, sink, sinkFailurePolicy, columns);
    }
  }

  /**
   * Deliberately never retried, regardless of {@code sinkFailurePolicy}: {@code
   * JavaRDD#saveAsTextFile} refuses to write to a path that already exists, so retrying after a
   * partial failure would not re-attempt the same write - it would fail immediately with a
   * confusing "path already exists" error instead of the original real failure, which is worse than
   * not retrying at all. Unlike {@code kafka}/{@code jdbc} (an at-least-once retry may duplicate a
   * row, an accepted, standard tradeoff) or {@code opensearch} (a per-document PUT is genuinely
   * idempotent), there is no safe way to retry a whole-dataset file write in place.
   */
  private static void writeFile(JavaRDD<Map<String, Object>> mapped, SinkSpec sink) {
    serializeToNdjson(mapped).saveAsTextFile(sink.uri());
  }

  private static void writeKafka(
      SparkSession spark,
      JavaRDD<Map<String, Object>> mapped,
      SinkSpec sink,
      IngestionPipeline.SinkFailurePolicy sinkFailurePolicy) {
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

  private static void writeStructured(
      SparkSession spark,
      JavaRDD<Map<String, Object>> mapped,
      SinkSpec sink,
      IngestionPipeline.SinkFailurePolicy sinkFailurePolicy,
      List<String> columns) {
    StructType schema =
        new StructType(
            columns.stream()
                .map(column -> DataTypes.createStructField(column, DataTypes.StringType, true))
                .toArray(StructField[]::new));
    JavaRDD<Row> rows =
        mapped.map((Function<Map<String, Object>, Row>) row -> rowFor(row, columns));
    Dataset<Row> dataFrame = spark.createDataFrame(rows, schema);

    Map<String, String> options = SparkSecretOptions.resolve(sink.options());
    String connector = sink.connector();
    if (JDBC_CONNECTOR.equals(connector)) {
      options.putIfAbsent("url", sink.uri());
      options.putIfAbsent("dbtable", sink.index());
      options.putIfAbsent("driver", "org.postgresql.Driver");
      SinkSpec.BatchingSpec batching = sink.batching();
      if (batching != null && batching.maxRecords() != null) {
        options.putIfAbsent("batchsize", String.valueOf(batching.maxRecords()));
      }
    }
    IngestionPipeline.withSinkFailureHandlingBlocking(
        sinkFailurePolicy,
        () -> dataFrame.write().format(connector).options(options).mode(SaveMode.Append).save());
  }

  private static Row rowFor(Map<String, Object> row, List<String> columns) {
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Object value = row.get(columns.get(i));
      values[i] = value == null ? null : String.valueOf(value);
    }
    return RowFactory.create(values);
  }

  /**
   * Retries at the per-*row* granularity (not the whole partition) - unlike a whole-dataset {@code
   * file}/{@code kafka}/{@code jdbc} write, one document {@code PUT} by id is genuinely idempotent
   * (indexing the same id twice just overwrites), so retrying it in place is safe with no
   * duplicate-row tradeoff to accept.
   */
  private static void writeOpenSearch(
      JavaRDD<Map<String, Object>> mapped,
      SinkSpec sink,
      IngestionPipeline.SinkFailurePolicy sinkFailurePolicy) {
    String idField = requiredOption(sink.options(), "idField");
    String baseUrl = sink.uri();
    String index = sink.index();
    Map<String, String> options = sink.options();
    OpenSearchIndexInitializer.ensureIndex(baseUrl, index, options);
    mapped.foreachPartition(
        (VoidFunction<Iterator<Map<String, Object>>>)
            rows -> {
              ObjectMapper objectMapper = new ObjectMapper();
              HttpClient client = HttpClient.newHttpClient();
              String authHeader = basicAuthHeader(options);
              while (rows.hasNext()) {
                Map<String, Object> row = rows.next();
                Object idValue = row.get(idField);
                if (idValue == null) {
                  throw new IllegalArgumentException(
                      "opensearch sink: a row is missing its own id field '" + idField + "'");
                }
                String docUri =
                    baseUrl
                        + "/"
                        + index
                        + "/_doc/"
                        + URLEncoder.encode(String.valueOf(idValue), StandardCharsets.UTF_8);
                String json = objectMapper.writeValueAsString(row);
                IngestionPipeline.withSinkFailureHandlingBlocking(
                    sinkFailurePolicy, () -> putOneDocument(client, docUri, json, authHeader));
              }
            });
  }

  private static void putOneDocument(
      HttpClient client, String docUri, String json, String authHeader) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(docUri))
            .PUT(HttpRequest.BodyPublishers.ofString(json))
            .header("Content-Type", "application/json");
    if (authHeader != null) {
      requestBuilder.header("Authorization", authHeader);
    }
    HttpResponse<String> response;
    try {
      response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException("opensearch sink: PUT " + docUri + " failed", e);
    }
    if (response.statusCode() >= 300) {
      throw new IllegalStateException(
          "opensearch sink: PUT "
              + docUri
              + " failed: "
              + response.statusCode()
              + " "
              + response.body());
    }
  }

  private static String basicAuthHeader(Map<String, String> options) {
    String user = SecretRefs.resolve(options, "user");
    String password = SecretRefs.resolve(options, "password");
    if (user == null || password == null) {
      return null;
    }
    String credentials = user + ":" + password;
    return "Basic "
        + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  private static String requiredOption(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "SparkSinks: '" + key + "' is required in the sink's own options map for this connector");
    }
    return value;
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
