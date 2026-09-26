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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * The Pekko sibling of {@code KafkaStreamsDeliveryEngineSigtermIntegrationTest} - real, separate
 * JVM subprocess running {@link PekkoStreamsDeliveryEngine#main}, sent a real {@code SIGTERM}, and
 * verified to exit *on its own*, only after its own shutdown hook (identical structure to the Kafka
 * Streams sibling's - a {@code CountDownLatch} released by {@code handle.stop()}) genuinely ran to
 * completion. See that class's own javadoc for why exit code {@code 143} (128+SIGTERM) is the JVM's
 * own real post-shutdown-hook exit code, not a sign of being force-killed, and why the subprocess's
 * own captured completion log line is the real proof, not the exit code alone.
 */
@WithKafkaContainer
@WithOpenSearchContainer
class PekkoStreamsDeliveryEngineSigtermIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PekkoStreamsDeliveryEngineSigtermIntegrationTest.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  @Test
  @Timeout(150)
  void continuousModeSubprocessExitsCleanlyOnSigterm(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-pekko-sigterm-" + UUID.randomUUID();
    String index = "fds-pekko-sigterm-index";
    createTopic(kafka, topic);

    IngestionSpec spec =
        new IngestionSpec(
            List.of(kafkaSource(kafka, topic)),
            null,
            null,
            openSearchSink(opensearch, index),
            ExecutionMode.CONTINUOUS,
            new DeliverySemantics(true, new ConcurrencySpec(1, null), null),
            null);
    Path specFile = Files.createTempFile("pekko-streams-sigterm-spec", ".yaml");
    Files.writeString(specFile, spec.toYaml());
    Path subprocessLog = Files.createTempFile("pekko-streams-sigterm-subprocess", ".log");

    Process process =
        new ProcessBuilder(
                javaExecutable(),
                "-cp",
                System.getProperty("java.class.path"),
                PekkoStreamsDeliveryEngine.class.getName(),
                specFile.toString())
            .redirectOutput(subprocessLog.toFile())
            .redirectErrorStream(true)
            .start();

    try {
      produce(kafka, topic, "S1", Map.of("uid", "S1", "name", "Henry Hill"));
      JsonNode s1 = awaitDocument(opensearch, index, "S1", Duration.ofSeconds(30));
      assertEquals(
          "Henry Hill",
          s1.path("_source").path("name").asText(),
          "the subprocess must genuinely be running the continuous stream before SIGTERM proves"
              + " anything about shutdown");

      LOGGER.info("Subprocess pid={} is live - sending SIGTERM (Process#destroy)", process.pid());
      long sigtermSentAt = System.nanoTime();
      process.destroy();

      // PekkoStreamsExecutionHandle#stop() has its own internal 30s timeout on
      // drainAndShutdown() alone - a real budget bigger than that (60s) is needed to tell "hangs
      // forever" apart from "just slower than the Kafka Streams sibling," which this run exists
      // to find out, not assume either way.
      boolean exitedOnItsOwn = process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS);
      double elapsedSeconds = (System.nanoTime() - sigtermSentAt) / 1_000_000_000.0;
      LOGGER.info(
          "Subprocess exited on its own = {}, elapsed since SIGTERM = {}s",
          exitedOnItsOwn,
          elapsedSeconds);
      String subprocessOutput = Files.readString(subprocessLog);
      assertTrue(
          exitedOnItsOwn,
          "the subprocess never exited on its own within 30s of SIGTERM - the shutdown hook did"
              + " not run, or hung. Subprocess output:\n"
              + subprocessOutput);
      assertEquals(
          143,
          process.exitValue(),
          "expected the JVM's own real post-shutdown-hook SIGTERM exit code. Subprocess output:\n"
              + subprocessOutput);
      assertTrue(
          subprocessOutput.contains("PekkoStreamsDeliveryEngine: run completed"),
          "expected the shutdown hook's own completion log line, printed only after"
              + " handle.stop() returned. Subprocess output:\n"
              + subprocessOutput);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  private static String javaExecutable() {
    return Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static SourcePlan kafkaSource(KafkaTestContainer kafka, String topic) {
    return new SourcePlan(
        "single",
        new SourceSpec(
            "kafka",
            "kafka:"
                + topic
                + "?brokers="
                + kafka.bootstrapServers()
                + "&autoOffsetReset=earliest&groupId=fds-pekko-sigterm-test-"
                + UUID.randomUUID(),
            null,
            null),
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

  private static JsonNode awaitDocument(
      OpenSearchTestContainer opensearch, String index, String id, Duration timeout)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      URI uri = URI.create(opensearch.hostEndpoint() + "/" + index + "/_doc/" + id);
      HttpResponse<String> response =
          HTTP_CLIENT.send(
              HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        return JSON.readTree(response.body());
      }
      Thread.sleep(200);
    }
    throw new AssertionError("document '" + id + "' never appeared before the deadline");
  }
}
