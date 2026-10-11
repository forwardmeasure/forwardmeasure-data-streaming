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
      last = get("/worldcheck-screening-records/_doc/" + id);
      JsonNode source = last.path("_source");
      if (expected.equals(source.path(field).asText())) return source;
      Thread.sleep(500);
    }
    throw new AssertionError(
        "Continuous delivery did not persist "
            + id
            + "/"
            + field
            + ": "
            + last
            + "\n"
            + workerDiagnostics());
  }

  /** The deployed worker's pod state and logs - the only evidence of why it failed. */
  public String workerDiagnostics() {
    var out =
        new StringBuilder("=== continuous worker pods (image ").append(image).append(") ===\n");
    try (var client = runtime.kubernetes().createClient()) {
      for (var pod : client.pods().inAnyNamespace().list().getItems()) {
        boolean worker =
            pod.getSpec().getContainers().stream()
                .anyMatch(container -> image.equals(container.getImage()));
        if (!worker) continue;
        String namespace = pod.getMetadata().getNamespace();
        String name = pod.getMetadata().getName();
        out.append("--- ")
            .append(namespace)
            .append('/')
            .append(name)
            .append(" phase=")
            .append(pod.getStatus().getPhase())
            .append(" containers=")
            .append(pod.getStatus().getContainerStatuses())
            .append('\n');
        try {
          out.append(client.pods().inNamespace(namespace).withName(name).tailingLines(400).getLog())
              .append('\n');
        } catch (RuntimeException logFailure) {
          out.append("(log unavailable: ").append(logFailure.getMessage()).append(")\n");
        }
        try {
          // A crash-looping worker's real error is in the previous container's log.
          out.append("--- previous container ---\n")
              .append(
                  client
                      .pods()
                      .inNamespace(namespace)
                      .withName(name)
                      .terminated()
                      .tailingLines(400)
                      .getLog())
              .append('\n');
        } catch (RuntimeException noPrevious) {
          out.append("(no previous container log)\n");
        }
      }
    } catch (RuntimeException failure) {
      out.append("(diagnostics failed: ").append(failure).append(")\n");
    }
    return out.toString();
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
    JsonNode body = JSON.readTree(response.body());
    // Auto-creation may expose the index before its primary shard has finished recovery.
    // Keep polling only this specific readiness state, within awaitValue's existing deadline.
    boolean recoveringShard =
        response.statusCode() == 503
            && "no_shard_available_action_exception"
                .equals(body.path("error").path("type").asText())
            && "illegal_index_shard_state_exception"
                .equals(body.path("error").path("caused_by").path("type").asText());
    assertTrue(
        response.statusCode() == 200 || response.statusCode() == 404 || recoveringShard,
        response.body());
    return body;
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
    // K3s manages the coredns ConfigMap ("DO NOT EDIT"); coredns-custom is its supported import.
    try (var client = runtime.kubernetes().createClient()) {
      var configMaps = client.configMaps().inNamespace("kube-system");
      var custom = configMaps.withName("coredns-custom").get();
      if (custom == null) {
        custom =
            new io.fabric8.kubernetes.api.model.ConfigMapBuilder()
                .withNewMetadata()
                .withName("coredns-custom")
                .withNamespace("kube-system")
                .endMetadata()
                .build();
      }
      if (custom.getData() == null) custom.setData(new LinkedHashMap<>());
      custom
          .getData()
          .put(
              "host-docker-internal.server",
              "host.docker.internal:53 {\n    hosts {\n        "
                  + gateway
                  + " host.docker.internal\n    }\n}\n");
      configMaps
          .resource(custom)
          .createOr(io.fabric8.kubernetes.client.dsl.NonDeletingOperation::update);
      // A running CoreDNS sees the change only after the kubelet syncs the mounted ConfigMap:
      // 30-150 s on an idle cluster, over 3 minutes on a loaded test cluster (observed). Its
      // Deployment recreates the deleted pods, which mount the current ConfigMap at once.
      client.pods().inNamespace("kube-system").withLabel("k8s-app", "kube-dns").delete();
      awaitInClusterResolution(client, gateway);
    }
  }

  /**
   * CoreDNS sees a ConfigMap change only after the kubelet syncs the volume and CoreDNS reloads
   * (measured up to ~2 minutes). A worker started before then cannot resolve the host and
   * crash-loops past the delivery window, so do not return until a pod really resolves it.
   */
  private static void awaitInClusterResolution(
      io.fabric8.kubernetes.client.KubernetesClient client, String gateway) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
    String last = "";
    for (int attempt = 0; System.nanoTime() < deadline; attempt++) {
      String name = "fds-dns-probe-" + attempt;
      var pods = client.pods().inNamespace("kube-system");
      pods.resource(
              new io.fabric8.kubernetes.api.model.PodBuilder()
                  .withNewMetadata()
                  .withName(name)
                  .endMetadata()
                  .withNewSpec()
                  .withRestartPolicy("Never")
                  .addNewContainer()
                  .withName("nslookup")
                  .withImage("busybox:1.36")
                  .withCommand("nslookup", "host.docker.internal")
                  .endContainer()
                  .endSpec()
                  .build())
          .create();
      try {
        String phase = null;
        long podDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < podDeadline) {
          var pod = pods.withName(name).get();
          phase = pod == null || pod.getStatus() == null ? null : pod.getStatus().getPhase();
          if ("Succeeded".equals(phase) || "Failed".equals(phase)) break;
          Thread.sleep(500);
        }
        last = phase + ": " + pods.withName(name).getLog();
        if (last.contains("Address: " + gateway)) return;
      } finally {
        pods.withName(name).withGracePeriod(0).delete();
      }
      Thread.sleep(3_000);
    }
    throw new AssertionError(
        "host.docker.internal never resolved inside the disposable cluster; last lookup: " + last);
  }

  @Override
  public void close() {
    search.close();
  }
}
