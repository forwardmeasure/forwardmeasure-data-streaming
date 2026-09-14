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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Real, no-mocks proof of Spark's own {@code kafka} source+sink, both added 2026-09-13: the sink
 * writes via Spark's native {@code kafka} format ({@link SparkSinks#write}'s own single-{@code
 * value}-column DataFrame, since Spark's kafka writer needs exactly that shape - confirmed by
 * Spark's own documented contract, not guessed); the source reads back via Spark's native
 * batch-mode {@code kafka} format ({@link SparkCorrelationEngine#readAndMapSingleSource}), whose
 * raw {@code value} column (the Kafka envelope, not business fields) needed a real fix in {@code
 * SparkCorrelationEngine} - see {@code sourceRowFor} - to parse as JSON before {@link
 * com.forwardmeasure.datastreaming.mappers.FieldMappingEngine} ever sees it.
 */
@Testcontainers
class SparkKafkaConnectorIntegrationTest {

  @Container private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-kafka-connector-integration-test")
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
  void sinkPublishesEachMappedRowAndSourceReadsThemBackBounded(@TempDir Path tempDir)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);
    String topic = "fds-spark-kafka-connector-test-" + UUID.randomUUID();
    String brokers = KAFKA.getBootstrapServers();

    IngestionSpec sinkSpec =
        new IngestionSpec(
            new SourceSpec("file", sourceCsv.toString(), null, null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "kafka", brokers, topic, null, null, Map.of("kafka.bootstrap.servers", brokers)),
            new ExecutionSpec("spark", null, null, null));

    SparkIngestionRunner.IngestionResult sinkResult = SparkIngestionRunner.run(spark, sinkSpec);
    assertEquals(2, sinkResult.recordsWritten());

    SourceSpec kafkaSource =
        new SourceSpec(
            "kafka",
            "n/a",
            null,
            null,
            null,
            Map.of(
                "kafka.bootstrap.servers",
                brokers,
                "subscribe",
                topic,
                "startingOffsets",
                "earliest",
                "endingOffsets",
                "latest"));

    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
            spark,
            kafkaSource,
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null),
                    new TransformSpec.FieldRule("name", "name", null, null, null, null, null))),
            Map.of(),
            IngestionPipeline.MalformedRecordPolicy.SKIP);
    List<Map<String, Object>> rows = mapped.collect();

    assertEquals(2, rows.size());
    List<String> ids = rows.stream().map(row -> String.valueOf(row.get("uid"))).toList();
    assertTrue(ids.contains("S1"), "expected S1 among: " + ids);
    assertTrue(ids.contains("S2"), "expected S2 among: " + ids);
  }
}
