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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PekkoEntrypointContractTest {
  @Test
  void boundedEntrypointsReadMountedSpecsAndProduceSourceDerivedDocuments(@TempDir Path directory)
      throws Exception {
    for (Class<?> main :
        List.of(
            PekkoIngestionRunner.class,
            PekkoCorrelationRunner.class,
            PekkoStreamsDeliveryEngine.class)) {
      Path run = Files.createDirectory(directory.resolve(main.getSimpleName()));
      boolean correlated = main != PekkoIngestionRunner.class;
      var spec = spec(run, correlated, false);
      Path specification = run.resolve("spec.yaml");
      Files.writeString(specification, spec.toYaml());
      Path log = run.resolve("process.log");
      // Exercise both the CLI argument and mounted-spec environment paths.
      boolean argument = main == PekkoIngestionRunner.class;
      var process =
          start(
              main,
              argument ? List.of(specification.toString()) : List.of(),
              argument
                  ? Map.of()
                  : Map.of(
                      main == PekkoCorrelationRunner.class
                          ? "CORRELATION_SPEC_PATH"
                          : "INGESTION_SPEC_PATH",
                      "  " + specification + "  "),
              log);
      try {
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), Files.readString(log));
        assertEquals(0, process.exitValue(), Files.readString(log));
        var rows = Files.readAllLines(run.resolve("output.jsonl"));
        assertEquals(1, rows.size());
        var result = new ObjectMapper().readTree(rows.getFirst());
        assertEquals("1", result.path("uid").asText());
        assertEquals("Primary", result.path("name").asText());
        if (correlated) assertEquals("GB", result.path("country").asText());
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void absentConfigurationAndRejectedMappingExitNonzero(@TempDir Path directory) throws Exception {
    for (Class<?> main :
        List.of(
            PekkoIngestionRunner.class,
            PekkoCorrelationRunner.class,
            PekkoStreamsDeliveryEngine.class)) {
      Path run = Files.createDirectory(directory.resolve(main.getSimpleName()));
      for (var environment :
          List.of(
              Map.<String, String>of(),
              Map.of("INGESTION_SPEC_PATH", " ", "CORRELATION_SPEC_PATH", " "))) {
        Path log = run.resolve("absent.log");
        var process = start(main, List.of(), environment, log);
        try {
          assertTrue(process.waitFor(10, TimeUnit.SECONDS));
          assertEquals(1, process.exitValue(), Files.readString(log));
          assertTrue(Files.readString(log).contains("required environment variable"));
        } finally {
          stop(process);
        }
      }
      var spec = spec(run, main == PekkoCorrelationRunner.class, true);
      Path specification = run.resolve("invalid.yaml");
      Files.writeString(specification, spec.toYaml());
      Path log = run.resolve("failure.log");
      var process = start(main, List.of(specification.toString()), Map.of(), log);
      try {
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), Files.readString(log));
        assertEquals(1, process.exitValue(), Files.readString(log));
        assertFalse(
            Files.exists(run.resolve("output.jsonl")), "Rejected mappings must not reach the sink");
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void dispatcherRejectsUndeliveredSparkStagesAndWrongCardinalityBeforeAnySourceRead(
      @TempDir Path directory) throws Exception {
    var spec = spec(directory, false, false);
    var original = ExecutionPlanCompiler.compile(spec);
    var system = ActorSystem.create("dispatch-contract");
    try {
      var engine = new PekkoStreamsDeliveryEngine(system);
      var spark =
          new ExecutionPlan(
              original.profile(),
              original.sources(),
              null,
              Optional.of(
                  new SparkStagePlan(List.of("screen_against_worldcheck_reference"), "handoff")),
              null,
              original.destination(),
              null,
              null);
      for (var mode : ExecutionMode.values()) {
        assertThrows(UnsupportedOperationException.class, () -> engine.execute(spark, mode));
      }
      assertThrows(IllegalArgumentException.class, () -> PekkoCorrelationRunner.run(spec, system));
      var correlated = spec(directory, true, false);
      assertThrows(
          IllegalArgumentException.class, () -> new PekkoIngestionRunner().run(correlated, system));
    } finally {
      system.terminate();
    }
  }

  private static IngestionSpec spec(Path directory, boolean correlated, boolean invalid)
      throws Exception {
    Files.writeString(directory.resolve("one.csv"), "id,name\n1,Primary\n");
    Files.writeString(directory.resolve("two.csv"), "id,country\n1,GB\n");
    var one =
        new SourcePlan(
            "one",
            source(directory, "one.csv"),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule(
                        "uid",
                        "id",
                        null,
                        null,
                        invalid ? "unknown_transform" : null,
                        false,
                        false),
                    new TransformSpec.FieldRule("name", "name", null, null, null, false, false))),
            1.0);
    var two =
        new SourcePlan(
            "two",
            source(directory, "two.csv"),
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
        new SinkSpec(
            "file",
            "file:" + directory + "?fileName=output.jsonl&fileExist=Append",
            "output",
            null,
            null),
        ExecutionMode.BOUNDED,
        null,
        new com.forwardmeasure.datastreaming.api.ErrorPolicy("fail", "fail"));
  }

  private static SourceSpec source(Path directory, String file) {
    return new SourceSpec(
        "file",
        "file:" + directory + "?fileName=" + file + "&noop=true&initialDelay=0&delay=50",
        null,
        null);
  }

  private static Process start(
      Class<?> main, List<String> args, Map<String, String> environment, Path log)
      throws Exception {
    var command = new java.util.ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("jacoco"))
        .forEach(command::add);
    command.addAll(List.of("-cp", System.getProperty("java.class.path"), main.getName()));
    command.addAll(args);
    var builder =
        new ProcessBuilder(command).redirectOutput(log.toFile()).redirectErrorStream(true);
    builder.environment().remove("INGESTION_SPEC_PATH");
    builder.environment().remove("CORRELATION_SPEC_PATH");
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
