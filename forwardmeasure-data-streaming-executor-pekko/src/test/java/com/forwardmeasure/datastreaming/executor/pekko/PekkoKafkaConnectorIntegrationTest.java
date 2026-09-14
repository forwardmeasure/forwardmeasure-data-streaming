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
package com.forwardmeasure.datastreaming.executor.pekko;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Real, no-mocks proof of the {@code kafka} connector both directions add 2026-09-13: the sink
 * needs zero new code beyond the {@code camel-kafka} dependency (it already goes through {@link
 * PekkoIngestionRunner#camelSink}'s existing generic path - confirmed here, not just claimed);
 * {@link PekkoIngestionRunner#rowSource}'s own new {@code kafkaRowSource} branch reads them back
 * with an explicit {@code maxMessages} bound, since (unlike {@code file}/{@code jdbc}) Camel's
 * kafka consumer has no built-in "read a snapshot and stop" semantics.
 */
@Testcontainers
class PekkoKafkaConnectorIntegrationTest {

  @Container private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Test
  void sinkPublishesEachMappedRowAndSourceReadsThemBackBounded(@TempDir Path tempDir)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);
    String topic = "fds-kafka-connector-test-" + UUID.randomUUID();
    String brokers = KAFKA.getBootstrapServers();

    IngestionSpec sinkSpec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                null,
                null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec("kafka", "kafka:" + topic + "?brokers=" + brokers, "n/a", null, null),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(2, 4), null, null));

    ActorSystem sinkSystem = ActorSystem.create("kafka-sink-integration-test");
    try {
      new PekkoIngestionRunner().run(sinkSpec, sinkSystem);
    } finally {
      sinkSystem.terminate();
    }

    SourceSpec kafkaSource =
        new SourceSpec(
            "kafka",
            "kafka:"
                + topic
                + "?brokers="
                + brokers
                + "&autoOffsetReset=earliest&groupId=fds-kafka-connector-test",
            null,
            null,
            null,
            Map.of("maxMessages", "2"));

    ActorSystem sourceSystem = ActorSystem.create("kafka-source-integration-test");
    List<SourceRow> rows;
    try (CamelBridge bridge = new CamelBridge()) {
      rows =
          PekkoIngestionRunner.rowSource(bridge, kafkaSource)
              .runWith(Sink.seq(), sourceSystem)
              .toCompletableFuture()
              .get(30, TimeUnit.SECONDS);
    } finally {
      sourceSystem.terminate();
    }

    assertEquals(
        2, rows.size(), "maxMessages=2 must bound the read to exactly the 2 published messages");
    List<String> ids = rows.stream().map(row -> row.get("uid")).toList();
    assertTrue(ids.contains("S1"), "expected S1 among: " + ids);
    assertTrue(ids.contains("S2"), "expected S2 among: " + ids);
  }
}
