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
 * The real proof this repo's own continuous-mode shutdown path never had: a real, separate JVM
 * subprocess running {@link KafkaStreamsDeliveryEngine#main}, sent a real {@code SIGTERM} (what
 * Kubernetes actually sends a {@code Deployment} pod on eviction/rollout, and what {@link
 * Process#destroy()} sends on a Unix JVM - confirmed by the JDK's own {@code ProcessImpl}/{@code
 * UNIXProcess} javadoc, not assumed), and verified to exit *on its own*, cleanly, within a bounded
 * time - not force-killed. Before this test, {@code awaitShutdown}'s shutdown-hook mechanism was
 * reasoned through by analogy to a standard JVM idiom but never actually exercised end to end; this
 * is that proof.
 *
 * <p>Real end-to-end liveness check first (produce a row, confirm it lands in OpenSearch) before
 * sending {@code SIGTERM} - proves the subprocess was genuinely running the continuous stream, not
 * just that a JVM process existed. {@code Process#waitFor(long, TimeUnit)} returning {@code true}
 * (not a timeout) is the real assertion: {@link KafkaStreamsDeliveryEngine#awaitShutdown}'s
 * shutdown hook calling {@code handle.stop()} (closing the real {@link
 * org.apache.kafka.streams.KafkaStreams} instance) is the only thing that lets this process exit
 * gracefully instead of being force-killed by the JVM's own default {@code SIGTERM} handling once
 * the timeout in this test's own fallback {@code destroyForcibly()} would otherwise kick in.
 */
@WithKafkaContainer
@WithOpenSearchContainer
class KafkaStreamsDeliveryEngineSigtermIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaStreamsDeliveryEngineSigtermIntegrationTest.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  @Test
  @Timeout(90)
  void continuousModeSubprocessExitsCleanlyOnSigterm(
      KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    String topic = "fds-kstreams-sigterm-" + UUID.randomUUID();
    String index = "fds-kstreams-sigterm-index";
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
    Path specFile = Files.createTempFile("kafka-streams-sigterm-spec", ".yaml");
    Files.writeString(specFile, spec.toYaml());

    Path subprocessLog = Files.createTempFile("kafka-streams-sigterm-subprocess", ".log");
    var command = new java.util.ArrayList<String>();
    command.add(javaExecutable());
    java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("jacoco"))
        .forEach(command::add);
    command.addAll(
        List.of(
            "-cp",
            System.getProperty("java.class.path"),
            KafkaStreamsDeliveryEngine.class.getName(),
            specFile.toString()));
    Process process =
        new ProcessBuilder(command)
            .redirectOutput(subprocessLog.toFile())
            .redirectErrorStream(true)
            .start();

    try {
      produce(kafka, topic, "S1", Map.of("uid", "S1", "name", "Gina Green"));
      JsonNode s1 = awaitDocument(opensearch, index, "S1", Duration.ofSeconds(30));
      assertEquals(
          "Gina Green",
          s1.path("_source").path("name").asText(),
          "the subprocess must genuinely be running the continuous stream before SIGTERM proves"
              + " anything about shutdown");

      LOGGER.info("Subprocess pid={} is live - sending SIGTERM (Process#destroy)", process.pid());
      process.destroy();

      boolean exitedOnItsOwn = process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS);
      String subprocessOutput = Files.readString(subprocessLog);
      assertFalse(
          subprocessOutput.contains("KafkaStreamsDeliveryEngine: run failed"), subprocessOutput);
      assertTrue(
          exitedOnItsOwn,
          "the subprocess never exited on its own within 30s of SIGTERM - the shutdown hook did"
              + " not run, or hung. Subprocess output:\n"
              + subprocessOutput);
      // 143 (128+SIGTERM) is the JVM's own real exit code after running its shutdown hooks to
      // completion, not a sign of being force-killed - Process#destroy() sends SIGTERM only, and
      // the JVM's default signal handler runs every registered shutdown hook before exiting this
      // way. The exit code alone can't distinguish "ran the hook" from "got killed before it could
      // run" though (both would report 143), so the real proof is this log line, only ever printed
      // *after* awaitShutdown()'s handle.stop() (a real, blocking KafkaStreams#close() call)
      // returns.
      assertEquals(
          143,
          process.exitValue(),
          "expected the JVM's own real post-shutdown-hook SIGTERM exit code. Subprocess output:\n"
              + subprocessOutput);
      assertTrue(
          subprocessOutput.contains("KafkaStreamsDeliveryEngine: shutdown completed"),
          "expected the shutdown hook's own completion log line, printed only after"
              + " handle.stop() (a real, blocking KafkaStreams#close()) returned - its absence"
              + " would mean the process was killed before the hook finished. Subprocess output:\n"
              + subprocessOutput);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
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
