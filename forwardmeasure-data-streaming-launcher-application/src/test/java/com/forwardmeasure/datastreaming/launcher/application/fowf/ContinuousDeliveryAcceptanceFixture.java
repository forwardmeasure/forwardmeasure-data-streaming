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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Input-only producer and read-only output oracle for the actual workflow-deployed streaming
 * worker.
 */
public final class ContinuousDeliveryAcceptanceFixture implements AutoCloseable {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final RealFowfWorkflowFixture runtime;
  private final OpenSearchTestContainer search;
  private final String topic = "fds-continuous-acceptance-" + UUID.randomUUID();
  private final String image;
  private final String command;

  public ContinuousDeliveryAcceptanceFixture(RealFowfWorkflowFixture runtime, String engine) {
    this.runtime = Objects.requireNonNull(runtime);
    this.search = new OpenSearchTestContainer().start();
    try {
      image =
          runtime
              .kubernetes()
              .loadImageAndPinDigest(
                  Objects.requireNonNull(
                      System.getProperty("fds.acceptance." + engine + ".image"),
                      "A current local " + engine + " executor image is required"));
      configureDisposableDns(runtime);
      try (var admin =
          AdminClient.create(Map.of("bootstrap.servers", runtime.kafka().bootstrapServers()))) {
        admin
            .createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
            .all()
            .get(30, TimeUnit.SECONDS);
      }
      var bounded =
          WorldCheckFixtures.boundedKafkaSpec(
              "kafka:" + topic + "?brokers=" + runtime.kafka().hostDockerInternalBootstrapServers(),
              "http://host.docker.internal:" + search.hostEndpoint().getPort(),
              Path.of(""));
      var continuous =
          new IngestionSpec(
              bounded.sources(),
              bounded.blockingField(),
              bounded.transforms(),
              bounded.sink(),
              ExecutionMode.CONTINUOUS,
              bounded.delivery(),
              bounded.errors(),
              bounded.mergePolicyUri());
      String encoded =
          Base64.getEncoder()
              .encodeToString(new ObjectMapper(new YAMLFactory()).writeValueAsBytes(continuous));
      // Only synthetic spec bytes are shell-transported; output is produced by the real executor.
      command =
          JSON.writeValueAsString(
              List.of(
                  "sh",
                  "-c",
                  "printf %s "
                      + encoded
                      + " | base64 -d > /tmp/ingestion-spec.yaml && exec java -Xmx512m -jar"
                      + " /deployments/application.jar /tmp/ingestion-spec.yaml"));
    } catch (Exception failure) {
      search.close();
      throw new IllegalStateException("Cannot provision real continuous delivery fixture", failure);
    }
  }

  public String image() {
    return image;
  }

  public String commandJson() {
    return command;
  }

  /** Called only after REST-launched workflow completion and worker Deployment readiness. */
  public void verifyOngoingDelivery() throws Exception {
    Map<String, String> first = sourceRow(1);
    Map<String, String> second = sourceRow(2);
    produce(first);
    var initial = awaitValue("wc-1", "position", "Businessman");
    assertEquals("person", initial.path("entity_kind").asText());
    assertTrue(initial.path("names").toString().contains("Smith"), initial.toString());
    assertEquals("1975-03-15", initial.path("date_of_birth").asText());
    // A second wave after confirmed persistence proves the worker did not stop at an initial
    // frontier.
    produce(second);
    awaitValue("wc-2", "entity_kind", "person");
    first.put("POSITION", "Updated After First Acknowledgement");
    produce(first);
    awaitValue("wc-1", "position", "Updated After First Acknowledgement");
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      var count = get("/worldcheck-screening-records/_count");
      if (count.path("count").asInt(-1) == 2) return;
      Thread.sleep(500);
    }
    throw new AssertionError("Expected exactly two stable document IDs after the update");
  }

  private void produce(Map<String, String> row) throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers",
                runtime.kafka().bootstrapServers(),
                "key.serializer",
                StringSerializer.class.getName(),
                "value.serializer",
                StringSerializer.class.getName(),
                "acks",
                "all"))) {
      producer
          .send(new ProducerRecord<>(topic, row.get("UID"), JSON.writeValueAsString(row)))
          .get(30, TimeUnit.SECONDS);
    }
  }

  private static Map<String, String> sourceRow(int line) {
    String[] lines = WorldCheckFixtures.SAMPLE_TSV.split("\n");
    String[] columns = lines[0].split("\t", -1), values = lines[line].split("\t", -1);
    assertEquals(columns.length, values.length);
    var row = new LinkedHashMap<String, String>();
    for (int i = 0; i < columns.length; i++) row.put(columns[i], values[i]);
    return row;
  }

  private JsonNode awaitValue(String id, String field, String expected) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    JsonNode last = JSON.missingNode();
    while (System.nanoTime() < deadline) {
      last = get("/worldcheck-screening-records/_doc/" + id).path("_source");
      if (expected.equals(last.path(field).asText())) return last;
      Thread.sleep(500);
    }
    throw new AssertionError(
        "Continuous delivery did not persist " + id + "/" + field + ": " + last);
  }

  private JsonNode get(String path) throws Exception {
    var response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(search.hostEndpoint() + path))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertTrue(response.statusCode() == 200 || response.statusCode() == 404, response.body());
    return JSON.readTree(response.body());
  }

  private static void configureDisposableDns(RealFowfWorkflowFixture runtime) throws Exception {
    var process =
        new ProcessBuilder(
                "docker",
                "network",
                "inspect",
                "bridge",
                "--format",
                "{{(index .IPAM.Config 0).Gateway}}")
            .start();
    String gateway =
        new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
    assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Docker bridge inspection timed out");
    assertEquals(0, process.exitValue());
    assertTrue(
        gateway.matches("[0-9.]+"), "Expected the disposable cluster's host gateway address");
    // This client belongs only to the Testcontainers K3s instance, never the user's kube context.
    try (var client = runtime.kubernetes().createClient()) {
      var map = client.configMaps().inNamespace("kube-system").withName("coredns").get();
      Objects.requireNonNull(map, "Disposable K3s CoreDNS configuration");
      String hosts = Objects.requireNonNull(map.getData().get("NodeHosts"), "K3s NodeHosts");
      map.getData().put("NodeHosts", hosts + "\n" + gateway + " host.docker.internal\n");
      client.configMaps().inNamespace("kube-system").resource(map).update();
    }
  }

  @Override
  public void close() {
    search.close();
  }
}
