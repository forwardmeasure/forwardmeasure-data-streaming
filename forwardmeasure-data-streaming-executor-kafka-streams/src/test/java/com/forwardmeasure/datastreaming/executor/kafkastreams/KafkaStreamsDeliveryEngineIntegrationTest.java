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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, no-mocks proof that {@link KafkaStreamsDeliveryEngine} genuinely writes to a real sink now
 * (neither mode did before Phase C of the repo's own gap-bridging plan - the old provider only ever
 * did topic-in/topic-out) - a real Kafka broker, a real OpenSearch node, real HTTP PUTs landing
 * real documents.
 */
@WithKafkaContainer
@WithOpenSearchContainer
class KafkaStreamsDeliveryEngineIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaStreamsDeliveryEngineIntegrationTest.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  @Test
  @Timeout(60)
  void boundedModeProcessesExactlyTheFrontierAndWritesRealOpenSearchDocuments(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-kstreams-bounded-" + UUID.randomUUID();
    String index = "fds-kstreams-bounded-index";
    createTopic(kafka, topic);
    produce(kafka, topic, "S1", Map.of("uid", "S1", "name", "Alice Anderson"));
    produce(kafka, topic, "S2", Map.of("uid", "S2", "name", "Bob Baker"));

    ExecutionPlan plan = boundedPlan(kafka, topic, opensearch, index);
    KafkaStreamsDeliveryEngine engine = new KafkaStreamsDeliveryEngine(Map.of());

    ExecutionHandle handle = engine.execute(plan, ExecutionMode.BOUNDED);

    assertFalse(
        handle.isRunning(),
        "a BOUNDED run must have already completed when execute()" + " returns");

    JsonNode s1 = fetchDocument(opensearch, index, "S1");
    assertEquals("Alice Anderson", s1.path("_source").path("name").asText());
    JsonNode s2 = fetchDocument(opensearch, index, "S2");
    assertEquals("Bob Baker", s2.path("_source").path("name").asText());

    // A record produced AFTER the frontier was captured must NOT be processed by this run - real
    // proof of the InputFrontier contract, not just "the two rows I fed it happened to land."
    produce(kafka, topic, "S3", Map.of("uid", "S3", "name", "Carol Carter"));
    Thread.sleep(1000);
    HttpResponse<String> response = getDocument(opensearch, index, "S3");
    assertEquals(404, response.statusCode(), "S3 arrived after the frontier - must not be indexed");
  }

  @Test
  @Timeout(60)
  void continuousModeWritesRealMappedRowsToOpenSearchAsTheyArrive(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-kstreams-continuous-" + UUID.randomUUID();
    String index = "fds-kstreams-continuous-index";
    createTopic(kafka, topic);

    ExecutionPlan plan = continuousPlan(kafka, topic, opensearch, index);
    KafkaStreamsDeliveryEngine engine = new KafkaStreamsDeliveryEngine(Map.of());
    ExecutionHandle handle = engine.execute(plan, ExecutionMode.CONTINUOUS);
    try {
      assertTrue(handle.isRunning());

      produce(kafka, topic, "S1", Map.of("uid", "S1", "name", "Dana Diaz"));

      JsonNode s1 = awaitDocument(opensearch, index, "S1", Duration.ofSeconds(30));
      assertEquals("Dana Diaz", s1.path("_source").path("name").asText());
    } finally {
      handle.stop();
    }
  }

  private static ExecutionPlan boundedPlan(
      KafkaTestContainer kafka, String topic, OpenSearchTestContainer opensearch, String index) {
    IngestionSpec spec =
        new IngestionSpec(
            List.of(kafkaSource(kafka, topic)),
            null,
            null,
            openSearchSink(opensearch, index),
            ExecutionMode.BOUNDED,
            new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
            null);
    return ExecutionPlanCompiler.compile(spec);
  }

  private static ExecutionPlan continuousPlan(
      KafkaTestContainer kafka, String topic, OpenSearchTestContainer opensearch, String index) {
    IngestionSpec spec =
        new IngestionSpec(
            List.of(kafkaSource(kafka, topic)),
            null,
            null,
            openSearchSink(opensearch, index),
            ExecutionMode.CONTINUOUS,
            new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
            null);
    return ExecutionPlanCompiler.compile(spec);
  }

  private static SourcePlan kafkaSource(KafkaTestContainer kafka, String topic) {
    return new SourcePlan(
        "single",
        new SourceSpec(
            "kafka", "kafka:" + topic + "?brokers=" + kafka.bootstrapServers(), null, null),
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null))),
        1.0);
  }

  private static SinkSpec openSearchSink(OpenSearchTestContainer opensearch, String index) {
    return new SinkSpec(
        "opensearch",
        opensearch.hostEndpoint().toString(),
        index,
        null,
        null,
        Map.of("idField", "uid"));
  }

  private static void createTopic(KafkaTestContainer kafka, String topic) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    try (Admin admin = Admin.create(props)) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
    }
  }

  private static void produce(
      KafkaTestContainer kafka, String topic, String key, Map<String, String> record)
      throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, key, JSON.writeValueAsString(record))).get();
    }
  }

  private static JsonNode fetchDocument(OpenSearchTestContainer opensearch, String index, String id)
      throws Exception {
    HttpResponse<String> response = getDocument(opensearch, index, id);
    assertEquals(
        200, response.statusCode(), "expected document '" + id + "' to exist: " + response.body());
    return JSON.readTree(response.body());
  }

  private static JsonNode awaitDocument(
      OpenSearchTestContainer opensearch, String index, String id, Duration timeout)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      HttpResponse<String> response = getDocument(opensearch, index, id);
      if (response.statusCode() == 200) {
        return JSON.readTree(response.body());
      }
      Thread.sleep(200);
    }
    throw new AssertionError("document '" + id + "' never appeared before the deadline");
  }

  private static HttpResponse<String> getDocument(
      OpenSearchTestContainer opensearch, String index, String id) throws Exception {
    URI uri = URI.create(opensearch.hostEndpoint() + "/" + index + "/_doc/" + id);
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    LOGGER.info("GET {} -> {}", uri, response.statusCode());
    return response;
  }
}
