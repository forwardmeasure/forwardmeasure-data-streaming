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

import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.correlatedSource;
import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.createTopic;
import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.getDocument;
import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.kafkaSource;
import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.openSearchSink;
import static com.forwardmeasure.datastreaming.executor.kafkastreams.KafkaStreamsDeliveryEngineIntegrationTest.produce;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@WithKafkaContainer
@WithOpenSearchContainer
class KafkaEntrypointContractTest {
  @Test
  void boundedLaunchesDeliverSourceDerivedSingleAndCorrelatedDocuments(
      KafkaTestContainer kafka, OpenSearchTestContainer os, @TempDir Path dir) throws Exception {
    for (boolean correlated : List.of(false, true)) {
      String topic = "launch-" + UUID.randomUUID();
      String extra = topic + "-extra";
      createTopic(kafka, topic);
      createTopic(kafka, extra);
      produce(kafka, topic, "x", Map.of("uid", "x", "name", "Primary"));
      produce(kafka, extra, "x", Map.of("uid", "x", "country", "GB"));
      var sources =
          correlated
              ? List.of(
                  kafkaSource(kafka, topic),
                  correlatedSource("extra", kafka, extra, "country", "country", 0.5))
              : List.of(kafkaSource(kafka, topic));
      var spec =
          new IngestionSpec(
              sources,
              correlated ? "uid" : null,
              null,
              openSearchSink(os, topic),
              ExecutionMode.BOUNDED,
              null,
              null);
      Path yaml = dir.resolve(topic + ".yaml");
      Files.writeString(yaml, spec.toYaml());
      Path log = dir.resolve(topic + ".log");
      var process =
          start(
              correlated ? List.of(yaml.toString()) : List.of(),
              Map.of("INGESTION_SPEC_PATH", "  " + yaml + "  "),
              log);
      try {
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), Files.readString(log));
        assertEquals(0, process.exitValue(), Files.readString(log));
        var row =
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(getDocument(os, topic, "x").body())
                .path("_source");
        assertEquals("Primary", row.path("name").asText());
        if (correlated) assertEquals("GB", row.path("country").asText());
        assertTrue(Files.readString(log).contains("run completed"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void missingMountedSpecAndFatalContinuousInputExitNonzero(
      KafkaTestContainer kafka, OpenSearchTestContainer os, @TempDir Path dir) throws Exception {
    for (var env : List.of(Map.<String, String>of(), Map.of("INGESTION_SPEC_PATH", " "))) {
      Path log = dir.resolve("missing.log");
      var process = start(List.of(), env, log);
      try {
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        assertEquals(1, process.exitValue());
        assertTrue(Files.readString(log).contains("required environment variable"));
      } finally {
        stop(process);
      }
    }
    String topic = "fatal-main-" + UUID.randomUUID();
    createTopic(kafka, topic);
    var spec =
        new IngestionSpec(
            List.of(kafkaSource(kafka, topic)),
            null,
            null,
            openSearchSink(os, topic),
            ExecutionMode.CONTINUOUS,
            null,
            new ErrorPolicy("fail", "fail"));
    Path yaml = dir.resolve("fatal.yaml");
    Files.writeString(yaml, spec.toYaml());
    Path log = dir.resolve("fatal.log");
    try (var producer =
        new org.apache.kafka.clients.producer.KafkaProducer<String, String>(
            Map.of("bootstrap.servers", kafka.bootstrapServers()),
            new org.apache.kafka.common.serialization.StringSerializer(),
            new org.apache.kafka.common.serialization.StringSerializer())) {
      producer
          .send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "bad", "{broken"))
          .get();
    }
    var process = start(List.of(yaml.toString()), Map.of(), log);
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), Files.readString(log));
      assertEquals(1, process.exitValue(), Files.readString(log));
      assertTrue(Files.readString(log).contains("Continuous ingestion stopped unexpectedly"));
      assertFalse(Files.readString(log).contains("run completed"));
    } finally {
      stop(process);
    }
  }

  @Test
  void dispatchRejectsInvalidCardinalitySparkStagesAndMixedClustersBeforeConnecting(
      KafkaTestContainer kafka, OpenSearchTestContainer os) {
    var source = kafkaSource(kafka, "unread");
    var spec =
        new IngestionSpec(
            List.of(source),
            null,
            null,
            openSearchSink(os, "unwritten"),
            ExecutionMode.BOUNDED,
            null,
            null);
    var plan = ExecutionPlanCompiler.compile(spec);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ContinuousKafkaStreamsCorrelationRunner(Map.of()).run(plan));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BoundedKafkaStreamsCorrelationRunner.run(
                List.of(source),
                "uid",
                plan.destination(),
                null,
                new FieldMappingEngine(),
                plan.mergePolicy()));
    var staged =
        new ExecutionPlan(
            plan.profile(),
            plan.sources(),
            null,
            Optional.of(
                new SparkStagePlan(List.of("screen_against_worldcheck_reference"), "unread-stage")),
            null,
            plan.destination(),
            null,
            null);
    for (var mode : ExecutionMode.values())
      assertThrows(
          UnsupportedOperationException.class,
          () -> new KafkaStreamsDeliveryEngine(Map.of()).execute(staged, mode));
    var other =
        new SourcePlan(
            "other",
            new SourceSpec("kafka", "kafka:other?brokers=127.0.0.1:1", null, null),
            source.mapper(),
            0.5);
    var mixed =
        ExecutionPlanCompiler.compile(
            new IngestionSpec(
                List.of(source, other),
                "uid",
                null,
                plan.destination(),
                ExecutionMode.CONTINUOUS,
                null,
                null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KafkaStreamsDeliveryEngine(Map.of()).execute(mixed, ExecutionMode.CONTINUOUS));
  }

  private static Process start(List<String> args, Map<String, String> environment, Path log)
      throws Exception {
    var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(a -> a.startsWith("-javaagent:") && a.contains("jacoco"))
        .forEach(command::add);
    command.addAll(
        List.of(
            "-cp",
            System.getProperty("java.class.path"),
            KafkaStreamsDeliveryEngine.class.getName()));
    command.addAll(args);
    var builder =
        new ProcessBuilder(command).redirectOutput(log.toFile()).redirectErrorStream(true);
    builder.environment().remove("INGESTION_SPEC_PATH");
    builder.environment().putAll(environment);
    return builder.start();
  }

  private static void stop(Process p) throws Exception {
    if (p.isAlive()) {
      p.destroyForcibly();
      p.waitFor(10, TimeUnit.SECONDS);
    }
  }
}
