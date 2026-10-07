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
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, no-mocks proof that {@link PekkoStreamsDeliveryEngine} genuinely writes to a real sink in
 * both modes - {@code BOUNDED} via the existing, proven {@link PekkoIngestionRunner}, {@code
 * CONTINUOUS} via the new committable-source + {@link PekkoIngestionRunner#sendOneRow} +
 * commit-after-write path (real, not the old provider's topic-in/topic-out shape - see this
 * engine's own javadoc).
 */
@WithKafkaContainer
@WithOpenSearchContainer
class PekkoStreamsDeliveryEngineIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PekkoStreamsDeliveryEngineIntegrationTest.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static ActorSystem system;

  @BeforeAll
  static void startActorSystem() {
    system = ActorSystem.create("pekko-streams-delivery-engine-test");
  }

  @AfterAll
  static void stopActorSystem() {
    system.terminate();
  }

  @Test
  @Timeout(60)
  void boundedModeWritesRealMappedRowsToOpenSearch(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-pekko-bounded-" + UUID.randomUUID();
    String index = "fds-pekko-bounded-index";
    createTopic(kafka, topic);
    produce(kafka, topic, "S1", Map.of("uid", "S1", "name", "Alice Anderson"));
    produce(kafka, topic, "S2", Map.of("uid", "S2", "name", "Bob Baker"));

    ExecutionPlan plan =
        plan(kafka, topic, opensearch, index, ExecutionMode.BOUNDED, Map.of("maxMessages", "2"));
    PekkoStreamsDeliveryEngine engine = new PekkoStreamsDeliveryEngine(system);

    ExecutionHandle handle = engine.execute(plan, ExecutionMode.BOUNDED);

    assertFalse(
        handle.isRunning(),
        "a BOUNDED run must have already completed when execute()" + " returns");
    JsonNode s1 = fetchDocument(opensearch, index, "S1");
    assertEquals("Alice Anderson", s1.path("_source").path("name").asText());
    JsonNode s2 = fetchDocument(opensearch, index, "S2");
    assertEquals("Bob Baker", s2.path("_source").path("name").asText());
  }

  @Test
  @Timeout(60)
  void continuousModeWritesRealMappedRowsToOpenSearchAsTheyArrive(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-pekko-continuous-" + UUID.randomUUID();
    String index = "fds-pekko-continuous-index";
    createTopic(kafka, topic);

    ExecutionPlan plan = plan(kafka, topic, opensearch, index, ExecutionMode.CONTINUOUS, Map.of());
    PekkoStreamsDeliveryEngine engine = new PekkoStreamsDeliveryEngine(system);
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

  /**
   * Real, live proof of the continuous correlation gap closed 2026-09-25 (see {@code
   * PekkoStreamsDeliveryEngine}'s own javadoc, {@code ContinuousPekkoStreamsCorrelationRunner}'s
   * own mechanism javadoc) - mirrors {@code KafkaStreamsDeliveryEngineIntegrationTest}'s own
   * identical 3-source test exactly, for real, direct engine-parity proof: three sources ({@code
   * core} trust 1.0/{@code name}, {@code detail} trust 0.5/{@code position}, {@code extra} trust
   * 0.25/{@code department}), {@code K1} gets all three (real N=3 merge), {@code K2} deliberately
   * skips the *middle* source ({@code detail}).
   */
  @Test
  @Timeout(60)
  void continuousModeCorrelatesThreeSourcesAndMergesByTrustWeight(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String coreTopic = "fds-pekko-cont-correlation-core-" + UUID.randomUUID();
    String detailTopic = "fds-pekko-cont-correlation-detail-" + UUID.randomUUID();
    String extraTopic = "fds-pekko-cont-correlation-extra-" + UUID.randomUUID();
    String index = "fds-pekko-cont-correlation-index";
    createTopic(kafka, coreTopic);
    createTopic(kafka, detailTopic);
    createTopic(kafka, extraTopic);

    ExecutionPlan plan =
        new ExecutionPlan(
            new com.forwardmeasure.datastreaming.api.ExecutionProfile(
                com.forwardmeasure.datastreaming.api.SourceCardinality.CORRELATED,
                ExecutionMode.CONTINUOUS,
                com.forwardmeasure.datastreaming.api.DeliveryEngineKind.PEKKO_STREAMS),
            List.of(
                correlatedSource("core", kafka, coreTopic, "name", "name", 1.0),
                correlatedSource("detail", kafka, detailTopic, "position", "role", 0.5),
                correlatedSource("extra", kafka, extraTopic, "department", "dept", 0.25)),
            "uid",
            java.util.Optional.empty(),
            null,
            openSearchSink(opensearch, index),
            new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
            null);

    PekkoStreamsDeliveryEngine engine = new PekkoStreamsDeliveryEngine(system);
    ExecutionHandle handle = engine.execute(plan, ExecutionMode.CONTINUOUS);
    try {
      assertTrue(handle.isRunning());

      produce(kafka, coreTopic, "K1", Map.of("uid", "K1", "name", "Iris Ito"));
      produce(kafka, detailTopic, "K1", Map.of("uid", "K1", "role", "Director"));
      produce(kafka, extraTopic, "K1", Map.of("uid", "K1", "dept", "Engineering"));

      produce(kafka, coreTopic, "K2", Map.of("uid", "K2", "name", "Jae Kim"));
      produce(kafka, extraTopic, "K2", Map.of("uid", "K2", "dept", "Sales"));

      JsonNode k1 =
          awaitDocumentWithFields(
              opensearch, index, "K1", Duration.ofSeconds(30), "name", "position", "department");
      assertEquals("Iris Ito", k1.path("_source").path("name").asText(), () -> "full doc: " + k1);
      assertEquals("Director", k1.path("_source").path("position").asText());
      assertEquals("Engineering", k1.path("_source").path("department").asText());

      JsonNode k2 =
          awaitDocumentWithFields(
              opensearch, index, "K2", Duration.ofSeconds(30), "name", "department");
      assertEquals("Jae Kim", k2.path("_source").path("name").asText(), () -> "full doc: " + k2);
      assertEquals("Sales", k2.path("_source").path("department").asText());
      assertTrue(
          k2.path("_source").path("position").isMissingNode(),
          "K2 has no detail-source counterpart - must not have a position field at all: " + k2);
    } finally {
      handle.stop();
    }
  }

  @Test
  void continuousCorrelationSurfacesFatalMappingAndRecoversAfterSkippableMalformedInput(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    for (String policy : List.of("skip", "fail")) {
      String core = "contract-core-" + UUID.randomUUID();
      String detail = "contract-detail-" + UUID.randomUUID();
      createTopic(kafka, core);
      createTopic(kafka, detail);
      String index = "correlation-errors-" + policy;
      var plan =
          new ExecutionPlan(
              new com.forwardmeasure.datastreaming.api.ExecutionProfile(
                  com.forwardmeasure.datastreaming.api.SourceCardinality.CORRELATED,
                  ExecutionMode.CONTINUOUS,
                  com.forwardmeasure.datastreaming.api.DeliveryEngineKind.PEKKO_STREAMS),
              List.of(
                  correlatedSource("core", kafka, core, "name", "name", 1.0),
                  correlatedSource("detail", kafka, detail, "position", "role", 0.5)),
              "uid",
              java.util.Optional.empty(),
              null,
              openSearchSink(opensearch, index),
              new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
              new com.forwardmeasure.datastreaming.api.ErrorPolicy(policy, "fail"));
      var handle = new PekkoStreamsDeliveryEngine(system).execute(plan, ExecutionMode.CONTINUOUS);
      try {
        assertTrue(handle.failure().isEmpty());
        assertTrue(handle.id().startsWith("fds-pekko-correlation-"));
        produceRaw(kafka, core, "bad", "{not-json");
        if (policy.equals("fail")) {
          Instant deadline = Instant.now().plusSeconds(15);
          while (handle.isRunning() && Instant.now().isBefore(deadline)) Thread.sleep(50);
          assertFalse(handle.isRunning(), "Fatal mapping must terminate the logical execution");
          assertTrue(handle.failure().isPresent(), "Failure cause must be observable for recovery");
        } else {
          produce(kafka, core, "missing", Map.of("name", "No identity"));
          produce(kafka, core, "good", Map.of("uid", "good", "name", "Retained Name"));
          produce(kafka, detail, "good", Map.of("uid", "good", "role", "Enriched Role"));
          var document =
              awaitDocumentWithFields(
                  opensearch, index, "good", Duration.ofSeconds(30), "name", "position");
          assertEquals("Retained Name", document.path("_source").path("name").asText());
          assertEquals("Enriched Role", document.path("_source").path("position").asText());
          assertTrue(handle.isRunning());
          assertTrue(handle.failure().isEmpty());
        }
      } finally {
        handle.stop();
        handle.stop();
        assertFalse(handle.isRunning());
      }
    }
  }

  @Test
  @org.junit.jupiter.api.Timeout(60)
  void continuousCorrelationFailsMissingAndBlankKeysWithoutCommittingThem(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    for (Map<String, String> row :
        List.of(Map.of("name", "Missing"), Map.of("uid", "   ", "name", "Blank"))) {
      String core = "invalid-key-core-" + UUID.randomUUID();
      String detail = core + "-detail";
      createTopic(kafka, core);
      createTopic(kafka, detail);
      var plan =
          new ExecutionPlan(
              new com.forwardmeasure.datastreaming.api.ExecutionProfile(
                  com.forwardmeasure.datastreaming.api.SourceCardinality.CORRELATED,
                  ExecutionMode.CONTINUOUS,
                  com.forwardmeasure.datastreaming.api.DeliveryEngineKind.PEKKO_STREAMS),
              List.of(
                  correlatedSource("core", kafka, core, "name", "name", 1.0),
                  correlatedSource("detail", kafka, detail, "position", "role", 0.5)),
              "uid",
              java.util.Optional.empty(),
              null,
              openSearchSink(opensearch, core),
              new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
              new com.forwardmeasure.datastreaming.api.ErrorPolicy("fail", "fail"));
      var handle = new PekkoStreamsDeliveryEngine(system).execute(plan, ExecutionMode.CONTINUOUS);
      try {
        produce(kafka, core, "poison", row);
        Instant deadline = Instant.now().plusSeconds(15);
        while (handle.isRunning() && Instant.now().isBefore(deadline)) Thread.sleep(50);
        assertFalse(handle.isRunning(), "Invalid correlation identity must fail execution");
        assertTrue(handle.failure().isPresent());
        try (var admin =
            org.apache.kafka.clients.admin.Admin.create(
                Map.of("bootstrap.servers", kafka.bootstrapServers()))) {
          var offsets =
              admin
                  .listConsumerGroupOffsets(handle.id() + "-core")
                  .partitionsToOffsetAndMetadata()
                  .get();
          var offset = offsets.get(new org.apache.kafka.common.TopicPartition(core, 0));
          assertTrue(
              offset == null || offset.offset() == 0,
              "Failed correlation input must remain unacknowledged");
        }
      } finally {
        handle.stop();
      }
    }
  }

  @Test
  void continuousCorrelationRejectsMixedKafkaClustersBeforeCreatingState() {
    var mapper =
        new TransformSpec(
            "party",
            List.of(new TransformSpec.FieldRule("uid", "uid", null, null, null, false, false)));
    var first =
        new SourcePlan(
            "one",
            new SourceSpec("kafka", "kafka:first?brokers=127.0.0.1:1", null, null),
            mapper,
            1.0);
    var second =
        new SourcePlan(
            "two",
            new SourceSpec("kafka", "kafka:second?brokers=127.0.0.1:2", null, null),
            mapper,
            0.5);
    var sink = new SinkSpec("opensearch", "http://unused.invalid", "unused", null, null);
    for (var sources : List.of(List.of(first), List.of(first, second))) {
      var plan =
          new ExecutionPlan(
              new com.forwardmeasure.datastreaming.api.ExecutionProfile(
                  sources.size() == 1
                      ? com.forwardmeasure.datastreaming.api.SourceCardinality.SINGLE
                      : com.forwardmeasure.datastreaming.api.SourceCardinality.CORRELATED,
                  ExecutionMode.CONTINUOUS,
                  com.forwardmeasure.datastreaming.api.DeliveryEngineKind.PEKKO_STREAMS),
              sources,
              "uid",
              java.util.Optional.empty(),
              null,
              sink,
              null,
              null);
      org.junit.jupiter.api.Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> ContinuousPekkoStreamsCorrelationRunner.run(plan, system));
    }
  }

  private static SourcePlan correlatedSource(
      String sourceKey,
      KafkaTestContainer kafka,
      String topic,
      String targetField,
      String rawField,
      double trustWeight) {
    return new SourcePlan(
        sourceKey,
        new SourceSpec(
            "kafka",
            "kafka:"
                + topic
                + "?brokers="
                + kafka.bootstrapServers()
                + "&autoOffsetReset=earliest&groupId=fds-pekko-cont-correlation-test-"
                + UUID.randomUUID(),
            null,
            null),
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null),
                new TransformSpec.FieldRule(targetField, rawField, null, null, null, null, null))),
        trustWeight);
  }

  private static JsonNode awaitDocumentWithFields(
      OpenSearchTestContainer opensearch,
      String index,
      String id,
      Duration timeout,
      String... fields)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      HttpResponse<String> response = getDocument(opensearch, index, id);
      if (response.statusCode() == 200) {
        JsonNode node = JSON.readTree(response.body());
        boolean allPresent = true;
        for (String field : fields) {
          if (node.path("_source").path(field).isMissingNode()) {
            allPresent = false;
            break;
          }
        }
        if (allPresent) {
          return node;
        }
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "document '" + id + "' never reported all of " + java.util.Arrays.toString(fields));
  }

  private static ExecutionPlan plan(
      KafkaTestContainer kafka,
      String topic,
      OpenSearchTestContainer opensearch,
      String index,
      ExecutionMode mode,
      Map<String, String> sourceOptions) {
    IngestionSpec spec =
        new IngestionSpec(
            List.of(kafkaSource(kafka, topic, sourceOptions)),
            null,
            null,
            openSearchSink(opensearch, index),
            mode,
            new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
            null);
    return ExecutionPlanCompiler.compile(spec);
  }

  private static SourcePlan kafkaSource(
      KafkaTestContainer kafka, String topic, Map<String, String> options) {
    return new SourcePlan(
        "single",
        new SourceSpec(
            "kafka",
            "kafka:"
                + topic
                + "?brokers="
                + kafka.bootstrapServers()
                + "&autoOffsetReset=earliest&groupId=fds-pekko-engine-test-"
                + UUID.randomUUID(),
            null,
            null,
            null,
            options),
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
    produceRaw(kafka, topic, key, JSON.writeValueAsString(record));
  }

  private static void produceRaw(KafkaTestContainer kafka, String topic, String key, String record)
      throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, key, record)).get();
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
