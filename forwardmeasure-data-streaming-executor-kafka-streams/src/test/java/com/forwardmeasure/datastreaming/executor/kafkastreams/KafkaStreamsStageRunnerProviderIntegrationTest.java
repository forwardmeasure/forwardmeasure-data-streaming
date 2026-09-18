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
package com.forwardmeasure.datastreaming.executor.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.StreamingStageSpec;
import com.forwardmeasure.datastreaming.api.StreamingStageSpec.FilterSpec;
import com.forwardmeasure.datastreaming.api.StreamingStageSpec.StreamingExecutionSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageHandle;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Real, no-mocks end-to-end proof of the Phase 0 spike this repo's continuous-streaming
 * gap-bridging plan calls for: a real record produced to a source topic flows through one real
 * {@link KafkaStreamsStageRunnerProvider}-started stage (real {@code TransformSpec} mapping, real
 * {@code FilterSpec} predicate) and lands, correctly transformed, on the sink topic - against a
 * real Kafka broker, not a mocked topology test driver. Uses the exact same plain {@code
 * org.testcontainers.junit.jupiter} {@code @Testcontainers}/{@code @Container} pattern already
 * proven in this repo's own {@code PekkoKafkaConnectorIntegrationTest}/{@code
 * SparkKafkaConnectorIntegrationTest} - no org-specific Kafka testcontainer wrapper module exists
 * or is needed (see this module's own pom for why).
 */
@Testcontainers
class KafkaStreamsStageRunnerProviderIntegrationTest {

  @Container private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  @Timeout(60)
  void oneRecordFlowsThroughARealMappingAndFilterToTheSinkTopic() throws Exception {
    String inputTopic = "fds-stage-spike-in-" + UUID.randomUUID();
    String outputTopic = "fds-stage-spike-out-" + UUID.randomUUID();

    StreamingStageSpec spec =
        new StreamingStageSpec(
            "even-odd-spike",
            inputTopic,
            new TransformSpec(
                "party",
                List.of(
                    new FieldRule("uid", "id", null, null, null, null, null),
                    new FieldRule("parity", "parity", null, null, null, null, null))),
            new FilterSpec("parity", "even"),
            outputTopic,
            new StreamingExecutionSpec("kafka-streams"));

    // Kafka Streams needs the source topic's metadata to exist before it can assign partitions -
    // without this, the app hits INCOMPLETE_SOURCE_TOPIC_METADATA and shuts itself down
    // (SHUTDOWN_CLIENT) rather than reaching RUNNING at all (confirmed via the real captured
    // stream-thread logs, not assumed). Auto-topic-creation on produce is not fast/reliable enough
    // here since streams.start() races the very first produce() call below.
    createTopics(inputTopic, outputTopic);

    Map<String, Object> streamsConfig =
        Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest");
    KafkaStreamsStageRunnerProvider provider = new KafkaStreamsStageRunnerProvider(streamsConfig);

    try (StreamingStageHandle handle = provider.start(spec)) {
      awaitRunning(handle);

      produce(inputTopic, Map.of("id", "row-1", "parity", "even"));
      produce(inputTopic, Map.of("id", "row-2", "parity", "odd"));

      Map<String, Object> mapped = awaitOneRecord(outputTopic);
      assertEquals("row-1", mapped.get("uid"));
      assertEquals("even", mapped.get("parity"));
    }
  }

  private void createTopics(String... topics) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (Admin admin = Admin.create(props)) {
      List<NewTopic> newTopics =
          List.of(topics).stream().map(t -> new NewTopic(t, 1, (short) 1)).toList();
      admin.createTopics(newTopics).all().get();
    }
  }

  private void awaitRunning(StreamingStageHandle handle) throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(30);
    while (!handle.health().ready() && Instant.now().isBefore(deadline)) {
      Thread.sleep(200);
    }
    assertTrue(handle.health().ready(), "stage did not reach RUNNING before the deadline");
  }

  private void produce(String topic, Map<String, String> record) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer
          .send(new ProducerRecord<>(topic, record.get("id"), JSON.writeValueAsString(record)))
          .get();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> awaitOneRecord(String topic) throws Exception {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "spike-test-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      Instant deadline = Instant.now().plusSeconds(30);
      while (Instant.now().isBefore(deadline)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : records) {
          return JSON.readValue(record.value(), Map.class);
        }
      }
      throw new AssertionError("No record arrived on " + topic + " before the deadline");
    }
  }
}
