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
package com.forwardmeasure.datastreaming.executor.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@WithKafkaContainer
class SparkEntrypointContractTest {
  @Test
  void actualEntrypointsScreenAndHandOffSingleAndCorrelatedRows(
      KafkaTestContainer kafka, @TempDir Path directory) throws Exception {
    for (Class<?> main :
        List.of(SparkStageRunner.class, SparkIngestionRunner.class, SparkCorrelationRunner.class)) {
      for (boolean correlated : List.of(false, true)) {
        if (main == SparkCorrelationRunner.class && !correlated) continue;
        Path run = Files.createDirectory(directory.resolve(main.getSimpleName() + correlated));
        var spec = spec(run, correlated, true);
        Path yaml = run.resolve("spec.yaml");
        Files.writeString(yaml, spec.toYaml());
        String topic = "entrypoint-" + UUID.randomUUID();
        Path log = run.resolve("worker.log");
        var env = new HashMap<String, String>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "  " + kafka.bootstrapServers() + "  ");
        env.put("SPARK_MASTER", "local[2]");
        env.put("SPARK_HANDOFF_TOPIC", "  " + topic + "  ");
        env.put(
            main == SparkCorrelationRunner.class ? "CORRELATION_SPEC_PATH" : "INGESTION_SPEC_PATH",
            "  " + yaml + "  ");
        // Both mounted environment and explicit CLI launch are real deployment contracts.
        var process = start(main, correlated ? List.of(yaml.toString()) : List.of(), env, log);
        try {
          assertTrue(process.waitFor(60, TimeUnit.SECONDS), Files.readString(log));
          assertEquals(0, process.exitValue(), Files.readString(log));
          try (var reader =
              new KafkaConsumer<String, String>(
                  Map.of(
                      "bootstrap.servers", kafka.bootstrapServers(), "enable.auto.commit", false),
                  new StringDeserializer(),
                  new StringDeserializer())) {
            var partition = new TopicPartition(topic, 0);
            reader.assign(List.of(partition));
            reader.seekToBeginning(List.of(partition));
            var values = new ArrayList<String>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (values.isEmpty() && System.nanoTime() < deadline)
              reader.poll(Duration.ofMillis(200)).forEach(record -> values.add(record.value()));
            assertEquals(1, values.size(), Files.readString(log));
            var row = new ObjectMapper().readTree(values.getFirst());
            assertEquals("1", row.path("uid").asText());
            assertEquals("Acme Holdings", row.path("name").asText());
            assertFalse(
                row.path("matches").isEmpty(),
                "The actual screening transform must run before handoff");
            if (correlated) assertEquals("GB", row.path("country").asText());
          }
        } finally {
          stop(process);
        }
      }
    }
  }

  @Test
  void missingConfigurationAndPlansWithoutSparkStageExitNonzero(@TempDir Path directory)
      throws Exception {
    for (Class<?> main :
        List.of(SparkStageRunner.class, SparkIngestionRunner.class, SparkCorrelationRunner.class)) {
      Path run = Files.createDirectory(directory.resolve(main.getSimpleName()));
      for (var env :
          List.of(
              Map.<String, String>of(),
              Map.of("INGESTION_SPEC_PATH", " ", "CORRELATION_SPEC_PATH", " "))) {
        Path log = run.resolve("missing.log");
        var process = start(main, List.of(), env, log);
        try {
          assertTrue(process.waitFor(10, TimeUnit.SECONDS));
          assertEquals(1, process.exitValue());
          assertTrue(Files.readString(log).contains("required environment variable"));
        } finally {
          stop(process);
        }
      }
      var spec = spec(run, main == SparkCorrelationRunner.class, false);
      Path yaml = run.resolve("spec.yaml");
      Files.writeString(yaml, spec.toYaml());
      // Empty bootstrap fails before Spark; a no-stage plan fails before opening any source/sink.
      for (String brokers : List.of(" ", "127.0.0.1:1")) {
        Path log = run.resolve("invalid.log");
        var process =
            start(
                main,
                List.of(yaml.toString()),
                Map.of(
                    "SPARK_MASTER",
                    "local[1]",
                    "KAFKA_BOOTSTRAP_SERVERS",
                    brokers,
                    "SPARK_HANDOFF_TOPIC",
                    "ignored-for-no-stage"),
                log);
        try {
          assertTrue(process.waitFor(30, TimeUnit.SECONDS), Files.readString(log));
          assertEquals(1, process.exitValue(), Files.readString(log));
          assertTrue(
              Files.readString(log)
                  .contains(
                      brokers.isBlank() ? "KAFKA_BOOTSTRAP_SERVERS" : "plan has no sparkStage"),
              Files.readString(log));
        } finally {
          stop(process);
        }
      }
    }
  }

  @Test
  void wrongRunnerCardinalityFailsBeforeReadingSource(@TempDir Path directory) throws Exception {
    var single = ExecutionPlanCompiler.compile(spec(directory, false, true));
    var correlated = ExecutionPlanCompiler.compile(spec(directory, true, true));
    assertThrows(
        IllegalArgumentException.class, () -> SparkCorrelationRunner.run(null, single, "unused"));
    assertThrows(
        IllegalArgumentException.class, () -> SparkIngestionRunner.run(null, correlated, "unused"));
  }

  private static IngestionSpec spec(Path directory, boolean correlated, boolean heavy)
      throws Exception {
    Files.writeString(directory.resolve("one.csv"), "id,name\n1,Acme Holdings\n");
    Files.writeString(directory.resolve("two.csv"), "id,country\n1,GB\n");
    var fields = new ArrayList<TransformSpec.FieldRule>();
    fields.add(new TransformSpec.FieldRule("uid", "id", null, null, null, false, false));
    fields.add(new TransformSpec.FieldRule("name", "name", null, null, null, false, false));
    if (heavy)
      fields.add(
          new TransformSpec.FieldRule(
              "matches",
              null,
              null,
              Map.of("full_name", "name"),
              "screen_against_worldcheck_reference",
              false,
              false));
    var one =
        new SourcePlan(
            "one",
            new SourceSpec("file", directory.resolve("one.csv").toString(), null, null),
            new TransformSpec("party", fields),
            1.0);
    var two =
        new SourcePlan(
            "two",
            new SourceSpec("file", directory.resolve("two.csv").toString(), null, null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "id", null, null, null, false, false),
                    new TransformSpec.FieldRule(
                        "country", "country", null, null, null, false, false))),
            0.5);
    return new IngestionSpec(
        correlated ? List.of(one, two) : List.of(one),
        correlated ? "uid" : null,
        null,
        new SinkSpec("kafka", "kafka:business-destination?brokers=127.0.0.1:1", null, null, null),
        ExecutionMode.BOUNDED,
        null,
        null);
  }

  private static Process start(
      Class<?> main, List<String> args, Map<String, String> environment, Path log)
      throws Exception {
    var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(
            arg ->
                arg.startsWith("--add-opens")
                    || arg.startsWith("--add-exports")
                    || arg.startsWith("--enable-native-access")
                    || (arg.startsWith("-javaagent:") && arg.contains("jacoco")))
        .forEach(command::add);
    command.addAll(List.of("-cp", System.getProperty("java.class.path"), main.getName()));
    command.addAll(args);
    var builder =
        new ProcessBuilder(command).redirectOutput(log.toFile()).redirectErrorStream(true);
    for (String key :
        List.of(
            "INGESTION_SPEC_PATH",
            "CORRELATION_SPEC_PATH",
            "SPARK_HANDOFF_TOPIC",
            "KAFKA_BOOTSTRAP_SERVERS",
            "SPARK_MASTER",
            "FDS_EXECUTION_ID")) builder.environment().remove(key);
    builder.environment().putAll(environment);
    return builder.start();
  }

  private static void stop(Process process) throws Exception {
    if (process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(10, TimeUnit.SECONDS);
    }
  }
}
