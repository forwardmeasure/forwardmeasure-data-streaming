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
package com.forwardmeasure.datastreaming.testfixtures;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Input;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

/**
 * Runs a {@link StatefulGraphFixtures} case end to end on a delivery engine: its inputs on real
 * Kafka topics (one per source, one partition, in arrival order), a spec with the case's graph and
 * an OpenSearch sink, and the documents the engine wrote, read back for comparison. Each engine's
 * test supplies only the engine call, so both engines meet exactly the same spec.
 */
public final class StatefulGraphParity {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  /** One run of a case: its own topics and index, so runs never share data. */
  public record Run(Case fixture, Map<String, String> topics, String index) {}

  private StatefulGraphParity() {}

  /** Creates the case's topics and writes every input to its source's topic, in order. */
  public static Run prepare(Case fixture, String brokers) throws Exception {
    Run run = topics(fixture, brokers);
    produce(run, brokers, 0, fixture.inputs().size());
    return run;
  }

  /** Creates the case's topics, empty. */
  public static Run topics(Case fixture, String brokers) throws Exception {
    String run =
        fixture.name().replace('_', '-') + "-" + UUID.randomUUID().toString().substring(0, 8);
    Map<String, String> topics = new LinkedHashMap<>();
    for (String source : fixture.sources()) topics.put(source, "fds-parity-" + run + "-" + source);
    Properties admin = new Properties();
    admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (Admin client = Admin.create(admin)) {
      client
          .createTopics(
              topics.values().stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList())
          .all()
          .get();
    }
    return new Run(fixture, topics, "fds-parity-" + run);
  }

  /** Writes inputs {@code from} (inclusive) to {@code to} (exclusive) to their sources' topics. */
  public static void produce(Run run, String brokers, int from, int to) throws Exception {
    Properties producer = new Properties();
    producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> kafka = new KafkaProducer<>(producer)) {
      for (int sequence = from; sequence < to; sequence++) {
        Input input = run.fixture().inputs().get(sequence);
        kafka
            .send(
                new ProducerRecord<>(
                    run.topics().get(input.sourceKey()),
                    "r" + sequence,
                    JSON.writeValueAsString(input.record())))
            .get();
      }
    }
  }

  /** The spec every engine runs for {@code run}: the case's graph, ending in an id-writing step. */
  public static IngestionSpec spec(Run run, ExecutionMode mode, String brokers, URI openSearch) {
    Case fixture = run.fixture();
    List<SourcePlan> sources = new ArrayList<>();
    double trust = 1.0;
    for (String source : fixture.sources()) {
      long count =
          fixture.inputs().stream().filter(input -> input.sourceKey().equals(source)).count();
      Map<String, String> options =
          mode == ExecutionMode.BOUNDED ? Map.of("maxMessages", Long.toString(count)) : Map.of();
      sources.add(
          new SourcePlan(
              source,
              new SourceSpec(
                  "kafka",
                  "kafka:"
                      + run.topics().get(source)
                      + "?brokers="
                      + brokers
                      + "&autoOffsetReset=earliest&groupId=fds-parity-"
                      + UUID.randomUUID(),
                  null,
                  null,
                  null,
                  options),
              StatefulGraphFixtures.passThrough(source, fixture.inputFields()),
              trust));
      trust /= 2;
    }
    return new IngestionSpec(
        sources,
        fixture.blockingField(),
        StatefulGraphFixtures.withDocumentId(fixture),
        new SinkSpec(
            "opensearch",
            openSearch.toString(),
            run.index(),
            null,
            null,
            Map.of("idField", StatefulGraphFixtures.DOCUMENT_ID)),
        mode,
        new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
        new ErrorPolicy("fail", "fail"));
  }

  /** Every document in {@code run}'s index, once it is searchable. */
  public static List<Map<String, Object>> documents(Run run, URI openSearch) throws Exception {
    send(
        HttpRequest.newBuilder(URI.create(openSearch + "/" + run.index() + "/_refresh"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build());
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(
                    URI.create(openSearch + "/" + run.index() + "/_search?size=1000"))
                .GET()
                .build());
    if (response.statusCode() == 404) return List.of();
    if (response.statusCode() != 200)
      throw new IllegalStateException(
          "Search failed: " + response.statusCode() + " " + response.body());
    List<Map<String, Object>> documents = new ArrayList<>();
    for (JsonNode hit : JSON.readTree(response.body()).path("hits").path("hits"))
      documents.add(
          JSON.convertValue(hit.path("_source"), new TypeReference<Map<String, Object>>() {}));
    return documents;
  }

  /**
   * Waits until {@code run}'s index holds {@code expected} documents, then a further quiet period
   * to catch any extra output, and returns them.
   */
  public static List<Map<String, Object>> awaitDocuments(
      Run run, URI openSearch, int expected, Duration timeout) throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    List<Map<String, Object>> documents = documents(run, openSearch);
    while (documents.size() < expected && Instant.now().isBefore(deadline)) {
      Thread.sleep(250);
      documents = documents(run, openSearch);
    }
    Thread.sleep(3000);
    return documents(run, openSearch);
  }

  private static HttpResponse<String> send(HttpRequest request) throws Exception {
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
