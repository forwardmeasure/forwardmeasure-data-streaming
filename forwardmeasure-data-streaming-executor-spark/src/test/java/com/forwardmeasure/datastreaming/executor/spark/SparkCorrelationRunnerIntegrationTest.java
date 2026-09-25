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
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link SparkCorrelationRunner} - split out 2026-09-13 from what used to
 * be {@code SparkIngestionRunner}'s own second {@code run()} overload, once that arrangement was
 * flagged as inconsistent with the Pekko side's clean one-class-per-spec-shape split - actually
 * drives {@link SparkCorrelationEngine} end to end from a compiled {@link ExecutionPlan}: the exact
 * same two-source, three-subject scenario {@code CorrelatedSourceIngestionWorkerIntegrationTest}
 * (fei), {@code SparkCorrelationEngineIntegrationTest}, and {@code
 * PekkoCorrelationRunnerIntegrationTest} (this repo) already prove, this time driven purely by a
 * compiled plan, with the merged output handed off to a real Kafka topic (retargeted 2026-09-21
 * from a direct {@code file} sink write - see {@link SparkSinks}' own javadoc for why).
 */
@WithKafkaContainer
class SparkCorrelationRunnerIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-correlation-runner-integration-test")
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
  void runCorrelatesTwoSourcesAndWritesTheMergedResult(
      @TempDir Path tempDir, KafkaTestContainer kafka) throws Exception {
    Path sourceACsv = writeFile(tempDir, "source-a.csv", SOURCE_A_CSV);
    Path sourceBCsv = writeFile(tempDir, "source-b.csv", SOURCE_B_CSV);
    String brokers = kafka.bootstrapServers();
    String handoffTopic = "fds-spark-correlation-runner-test-" + UUID.randomUUID();

    ExecutionPlan plan =
        new ExecutionPlan(
            new ExecutionProfile(
                SourceCardinality.CORRELATED,
                ExecutionMode.BOUNDED,
                DeliveryEngineKind.PEKKO_STREAMS),
            List.of(
                new SourcePlan(
                    "core",
                    new SourceSpec("file", sourceACsv.toString(), null, null),
                    mappingA(),
                    1.0),
                new SourcePlan(
                    "enrichment",
                    new SourceSpec("file", sourceBCsv.toString(), null, null),
                    mappingB(),
                    0.4)),
            "uid",
            Optional.of(new SparkStagePlan(List.of("test-heavy-transform"), handoffTopic)),
            null,
            new SinkSpec("opensearch", "http://unused", "unused", null, null, Map.of()),
            new DeliverySemantics(true, null, null),
            null);

    SparkCorrelationRunner.CorrelationResult result =
        SparkCorrelationRunner.run(spark, plan, brokers);

    assertEquals(2, result.sourceCount());
    assertEquals(3, result.groupCount(), "expected 3 correlated groups: S1 (merged), S2, S3");

    List<JsonNode> rows = consumeAll(brokers, handoffTopic, 3);
    assertEquals(3, rows.size());

    JsonNode s1 = findByUid(rows, "S1");
    assertEquals("Alice Anderson", s1.path("name").asText());
    assertEquals("1985-03-12", s1.path("date_of_birth").asText(), "higher-trust source A must win");
    assertEquals("GB", s1.path("nationality_code").asText(), "must be filled from lower-trust B");

    JsonNode s2 = findByUid(rows, "S2");
    assertEquals("Bob Baker", s2.path("name").asText());
    assertTrue(s2.path("nationality_code").isMissingNode(), "S2 was never given a nationality");

    JsonNode s3 = findByUid(rows, "S3");
    assertEquals("US", s3.path("nationality_code").asText());
    assertTrue(s3.path("name").isMissingNode(), "S3 was never given a name");
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-correlation-runner-test-" + UUID.randomUUID());
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

  private static TransformSpec mappingA() {
    return new TransformSpec(
        "party",
        List.of(
            new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
            new TransformSpec.FieldRule("name", "FULL_NAME", null, null, null, null, null),
            new TransformSpec.FieldRule("date_of_birth", "DOB", null, null, null, true, null)));
  }

  private static TransformSpec mappingB() {
    return new TransformSpec(
        "party",
        List.of(
            new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
            new TransformSpec.FieldRule(
                "nationality_code", "NATIONALITY", null, null, null, true, null),
            new TransformSpec.FieldRule(
                "date_of_birth", "DOB_GUESS", null, null, null, true, null)));
  }

  private static Path writeFile(Path dir, String name, String content) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }

  private static final String SOURCE_A_CSV =
      """
      ID,FULL_NAME,DOB
      S1,Alice Anderson,1985-03-12
      S2,Bob Baker,1990-07-04
      """;

  private static final String SOURCE_B_CSV =
      """
      ID,NATIONALITY,DOB_GUESS
      S1,GB,1899-01-01
      S3,US,1975-05-20
      """;
}
