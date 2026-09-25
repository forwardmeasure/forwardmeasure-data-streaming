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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link SparkIngestionRunner} - {@code ExecutionPlan}'s (single-source,
 * no correlation) Spark entrypoint, renamed 2026-09-13 from {@code SparkSourceIngestionRunner} once
 * this class stopped also carrying correlation handling (moved to {@link SparkCorrelationRunner},
 * proven by {@code SparkCorrelationRunnerIntegrationTest}) - drives {@link
 * SparkCorrelationEngine#readAndMapSingleSource} end to end from a compiled plan and hands the
 * mapped result off to a real Kafka topic (retargeted 2026-09-21 from a direct {@code file} sink
 * write - see {@link SparkSinks}' own javadoc for why Spark no longer writes to a real business
 * destination directly).
 */
@WithKafkaContainer
class SparkIngestionRunnerIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-ingestion-runner-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void runProcessesASingleSourceIngestionSpecWithoutMergingRowsThatShareAFieldValue(
      @TempDir Path tempDir, KafkaTestContainer kafka) throws Exception {
    Path sourceCsv = writeFile(tempDir, "source.csv", SINGLE_SOURCE_CSV);
    String brokers = kafka.bootstrapServers();
    String handoffTopic = "fds-spark-ingestion-runner-test-" + UUID.randomUUID();

    ExecutionPlan plan =
        new ExecutionPlan(
            new ExecutionProfile(
                SourceCardinality.SINGLE, ExecutionMode.BOUNDED, DeliveryEngineKind.PEKKO_STREAMS),
            List.of(
                new SourcePlan(
                    "single",
                    new SourceSpec("file", sourceCsv.toString(), null, null),
                    new TransformSpec(
                        "party",
                        List.of(
                            new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                            new TransformSpec.FieldRule(
                                "name", "FULL_NAME", null, null, null, null, null),
                            new TransformSpec.FieldRule(
                                "category", "CATEGORY", null, null, null, null, null))),
                    1.0)),
            null,
            Optional.of(new SparkStagePlan(List.of("test-heavy-transform"), handoffTopic)),
            null,
            new SinkSpec("opensearch", "http://unused", "unused", null, null, Map.of()),
            new DeliverySemantics(true, null, null),
            null);

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, plan, brokers);

    assertEquals(3, result.recordsWritten());

    List<JsonNode> rows = consumeAll(brokers, handoffTopic, 3);
    // S1 and S2 deliberately share CATEGORY="person" - proving this path never groups/merges rows
    // that happen to share a field value the way SparkCorrelationEngine#correlate's own groupBy
    // would (that groupBy is for matching the same entity *across different sources*, not a safe
    // no-op for a single source - see SparkCorrelationEngine#readAndMapSingleSource's own javadoc).
    assertEquals(
        3,
        rows.size(),
        "S1 and S2 share category='person' - a correlation-style groupBy would wrongly merge them"
            + " into one record; this single-source path must not");

    JsonNode s1 = findByUid(rows, "S1");
    assertEquals("Alice Anderson", s1.path("name").asText());
    assertEquals("person", s1.path("category").asText());
    JsonNode s2 = findByUid(rows, "S2");
    assertEquals("Bob Baker", s2.path("name").asText());
    assertEquals("person", s2.path("category").asText());
    JsonNode s3 = findByUid(rows, "S3");
    assertEquals("Carol Carter", s3.path("name").asText());
    assertEquals("org", s3.path("category").asText());
  }

  /**
   * The Spark sibling of {@code PekkoIngestionRunnerIntegrationTest}'s own {@code
   * processesMultiThousandRowCsvWithRealConcurrency} - added 2026-09-14 once a direct side-by-side
   * comparison surfaced that this class had no equivalent proof at all. Not a literal port: {@code
   * ExecutionSpec.concurrency} is never read anywhere in the Spark path (confirmed by grep) -
   * Spark's own parallelism comes from RDD partitioning, not that spec field - so this proves the
   * thing that's actually real on this engine: a real multi-thousand-row CSV, explicitly
   * repartitioned, is genuinely processed by more than one executor thread under {@code local[2]},
   * not silently serialized onto one. Doesn't touch {@link SparkSinks} at all, so it is unaffected
   * by that class's 2026-09-21 sink retirement.
   */
  @Test
  void processesMultiThousandRowCsvWithRealConcurrency(@TempDir Path tempDir) throws Exception {
    int rowCount = 5_000;
    Path sourceCsv = tempDir.resolve("input.csv");
    writeCsv(sourceCsv, rowCount);

    SourceSpec source = new SourceSpec("file", sourceCsv.toString(), null, null);
    TransformSpec mapper =
        new TransformSpec(
            "schema://test/target/1.0",
            List.of(
                new TransformSpec.FieldRule("id", "id", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null)));

    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
                spark, source, mapper, Map.of(), IngestionPipeline.MalformedRecordPolicy.SKIP)
            .repartition(4);

    CollectionAccumulator<String> threadNames =
        spark.sparkContext().collectionAccumulator("threadNames");
    long processed =
        mapped
            .mapPartitions(
                (FlatMapFunction<Iterator<Map<String, Object>>, Map<String, Object>>)
                    rows -> {
                      threadNames.add(Thread.currentThread().getName());
                      return rows;
                    })
            .count();

    assertEquals(rowCount, processed);
    long distinctThreads = threadNames.value().stream().distinct().count();
    assertTrue(
        distinctThreads > 1,
        "expected multiple executor threads to process partitions under local[2], saw only: "
            + threadNames.value());
  }

  private static void writeCsv(Path file, int rowCount) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("id,name");
      writer.newLine();
      for (int i = 1; i <= rowCount; i++) {
        writer.write(i + ",Row " + i);
        writer.newLine();
      }
    }
  }

  private static JsonNode findByUid(List<JsonNode> rows, String uid) {
    return rows.stream()
        .filter(row -> uid.equals(row.path("uid").asText()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no merged record for uid " + uid));
  }

  private static List<JsonNode> consumeAll(String brokers, String topic, int expectedCount)
      throws IOException {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-ingestion-runner-test-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<JsonNode> rows = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
      while (rows.size() < expectedCount && System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        for (ConsumerRecord<String, String> record : records) {
          rows.add(MAPPER.readTree(record.value()));
        }
      }
    }
    assertTrue(
        rows.size() >= expectedCount,
        "expected at least " + expectedCount + " records on topic '" + topic + "', got " + rows);
    return rows;
  }

  private static Path writeFile(Path dir, String name, String content) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }

  private static final String SINGLE_SOURCE_CSV =
      """
      ID,FULL_NAME,CATEGORY
      S1,Alice Anderson,person
      S2,Bob Baker,person
      S3,Carol Carter,org
      """;
}
