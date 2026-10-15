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
package com.forwardmeasure.datastreaming.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.LoaderHandoffSpecs;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The loader against a real broker: every non-Kafka source's rows, in order, unmapped. */
@WithKafkaContainer
class SourceLoaderIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void writesEachLoadedSourceToItsOwnTopicAndLeavesKafkaSourcesAlone(
      KafkaTestContainer kafka, @TempDir Path directory) throws Exception {
    Path people = directory.resolve("people.csv");
    Files.writeString(people, "uid|name|name\np-1|Ada|later\np-2|Grace|\n", StandardCharsets.UTF_8);
    Path places = directory.resolve("places.csv");
    Files.writeString(places, "uid,city\np-1,London\n", StandardCharsets.UTF_8);
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null)));
    IngestionSpec spec =
        new IngestionSpec(
            List.of(
                new SourcePlan("people", csv(people, "|"), mapper, 1.0),
                new SourcePlan(
                    "stream",
                    new SourceSpec("kafka", "kafka:already-there?brokers=x:9092", null, null),
                    mapper,
                    0.5),
                new SourcePlan("places", csv(places, ","), mapper, 0.8)),
            "uid",
            null,
            new SinkSpec("opensearch", "http://search:9200", "party", null, null),
            ExecutionMode.BOUNDED,
            null,
            null);
    String loadTopic = "fds-load-" + UUID.randomUUID();

    Map<String, Long> written = SourceLoader.load(spec, kafka.bootstrapServers(), loadTopic);

    assertEquals(Map.of("people", 2L, "places", 1L), written);
    assertEquals(
        List.of(Map.of("uid", "p-1", "name", "Ada"), Map.of("uid", "p-2", "name", "Grace")),
        read(kafka, LoaderHandoffSpecs.sourceTopic(loadTopic, 0), 2));
    assertEquals(
        List.of(Map.of("uid", "p-1", "city", "London")),
        read(kafka, LoaderHandoffSpecs.sourceTopic(loadTopic, 2), 1));
  }

  @Test
  void loadsCallerOpenedContent(KafkaTestContainer kafka) throws Exception {
    SourceSpec source =
        new SourceSpec(
            "file",
            "unused",
            new SourceSpec.FormatSpec("csv"),
            null,
            null,
            Map.of("delimiter", ","));
    String topic = "fds-load-" + UUID.randomUUID() + "-source-0";
    long count =
        SourceLoader.load(
            () ->
                new java.io.ByteArrayInputStream(
                    "uid,name\nz-1,Zed\n".getBytes(StandardCharsets.UTF_8)),
            source,
            kafka.bootstrapServers(),
            topic);
    assertEquals(1, count);
    assertEquals(List.of(Map.of("uid", "z-1", "name", "Zed")), read(kafka, topic, 1));
  }

  /**
   * A retried or repeated load starts from an empty topic: the topic holds exactly the last
   * attempt's rows, never an earlier attempt's prefix again.
   */
  @Test
  void aRepeatedLoadReplacesTheEarlierAttemptsRows(KafkaTestContainer kafka) throws Exception {
    SourceSpec source =
        new SourceSpec(
            "file",
            "unused",
            new SourceSpec.FormatSpec("csv"),
            null,
            null,
            Map.of("delimiter", ","));
    String topic = "fds-load-" + UUID.randomUUID() + "-source-0";
    String earlier = "uid,name\na-1,Ann\nb-2,Bob\nc-3,Cy\n";
    String later = "uid,name\nz-1,Zed\nz-2,Zoe\n";
    for (String content : List.of(earlier, later)) {
      SourceLoader.load(
          () -> new java.io.ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
          source,
          kafka.bootstrapServers(),
          topic);
    }
    try (var admin =
        org.apache.kafka.clients.admin.Admin.create(
            Map.<String, Object>of("bootstrap.servers", kafka.bootstrapServers()))) {
      var partition = new org.apache.kafka.common.TopicPartition(topic, 0);
      long start =
          admin
              .listOffsets(Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.earliest()))
              .partitionResult(partition)
              .get()
              .offset();
      long end =
          admin
              .listOffsets(Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.latest()))
              .partitionResult(partition)
              .get()
              .offset();
      assertEquals(2, end - start, "only the later attempt's rows remain");
    }
    assertEquals(
        List.of(Map.of("uid", "z-1", "name", "Zed"), Map.of("uid", "z-2", "name", "Zoe")),
        read(kafka, topic, 2));
  }

  private static SourceSpec csv(Path file, String delimiter) {
    return new SourceSpec(
        "file",
        "file:"
            + file.getParent()
            + "?fileName="
            + file.getFileName()
            + "&noop=true&initialDelay=0&delay=100",
        new SourceSpec.FormatSpec("csv"),
        null,
        null,
        Map.of("delimiter", delimiter));
  }

  private static List<Map<String, Object>> read(KafkaTestContainer kafka, String topic, int count)
      throws Exception {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "loader-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    List<Map<String, Object>> rows = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (rows.size() < count && System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(500))) {
          rows.add(JSON.readValue(record.value(), new TypeReference<Map<String, Object>>() {}));
        }
      }
    }
    return rows;
  }
}
