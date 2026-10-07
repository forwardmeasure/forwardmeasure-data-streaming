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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PekkoSourceContractTest {
  @Test
  void textSourcesPreserveStructuredJsonAndExplicitCsvDelimiters(@TempDir Path directory)
      throws Exception {
    var system = ActorSystem.create("source-contract");
    try (var bridge = new CamelBridge()) {
      for (String format : List.of("json", "jsonl", "ndjson")) {
        var source =
            file(
                directory,
                format + ".txt",
                "  {\"uid\":\"é\",\"raw\":{\"number\":0,\"flag\":false}}\n\n{\"uid\":\"two\"}\n",
                format,
                Map.of());
        var rows =
            PekkoIngestionRunner.rowSource(bridge, source)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertEquals(2, rows.size());
        assertEquals(Map.of("number", 0, "flag", false), rows.getFirst().getRaw("raw"));
        assertEquals("é", rows.getFirst().get("uid"));
      }
      for (String delimiter : List.of("", "|")) {
        String separator = delimiter.isEmpty() ? "," : delimiter;
        var source =
            file(
                directory,
                "delimited" + delimiter.length() + ".csv",
                "uid" + separator + "name\n1" + separator + "Jane\n",
                "csv",
                Map.of("delimiter", delimiter));
        var rows =
            PekkoIngestionRunner.rowSource(bridge, source)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertEquals(List.of("Jane"), rows.stream().map(row -> row.get("name")).toList());
      }
    } finally {
      system.terminate();
    }
  }

  @Test
  void malformedAndUnsupportedFormatsFailTheSourceInsteadOfBecomingEmptySuccessfulLoads(
      @TempDir Path directory) throws Exception {
    var system = ActorSystem.create("invalid-source-contract");
    try (var bridge = new CamelBridge()) {
      for (var fixture :
          List.of(
              List.of("json", "{broken"),
              List.of("csv", "id,name\n1,\"unclosed"),
              List.of("xml", "<record/>"))) {
        var source =
            file(
                directory,
                fixture.getFirst() + ".txt",
                fixture.get(1),
                fixture.getFirst(),
                Map.of());
        var failure =
            assertThrows(
                ExecutionException.class,
                () ->
                    PekkoIngestionRunner.rowSource(bridge, source)
                        .runWith(Sink.seq(), system)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS));
        if (fixture.getFirst().equals("xml"))
          assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        else assertInstanceOf(java.io.UncheckedIOException.class, failure.getCause());
      }
      for (var options : List.of(Map.<String, String>of(), Map.of("maxMessages", " "))) {
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PekkoIngestionRunner.rowSource(
                    bridge,
                    new SourceSpec(
                        "kafka", "kafka:never?brokers=127.0.0.1:1", null, null, null, options)));
      }
      for (String query : new String[] {null, " "}) {
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PekkoIngestionRunner.rowSource(
                    bridge,
                    new SourceSpec(
                        "sql",
                        "jdbc:postgresql://127.0.0.1:1/unused",
                        null,
                        null,
                        query,
                        Map.of())));
      }
      for (String uri : new String[] {null, " "})
        assertThrows(
            IllegalArgumentException.class, () -> SqlDataSources.register(bridge, uri, Map.of()));
    } finally {
      system.terminate();
    }
  }

  @Test
  void correlationHonorsMalformedPolicyAcrossMappingFailuresAndMissingIdentity(
      @TempDir Path directory) throws Exception {
    var system = ActorSystem.create("correlation-errors-contract");
    var mapper =
        new TransformSpec(
            "party", List.of(new FieldRule("uid", "id", null, null, "validate_id", false, false)));
    Map<String, NamedTransform> transforms =
        Map.of(
            "validate_id",
            inputs -> {
              String value = inputs.get("value");
              if ("bad".equals(value)) throw new IllegalArgumentException("Rejected ID");
              return value;
            });
    try (var bridge = new CamelBridge()) {
      for (var policy : MalformedRecordPolicy.values()) {
        var source =
            file(
                directory,
                policy.name() + ".json",
                "[{\"id\":\"good\"},{\"id\":\"bad\"},{},{\"id\":\" \"}]",
                "json",
                Map.of());
        var result =
            PekkoCorrelationEngine.readAndMap(
                    bridge, system, source, mapper, "uid", "source", 1.0, transforms, policy)
                .toCompletableFuture();
        if (policy == MalformedRecordPolicy.FAIL) {
          var error =
              assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
          assertEquals("Rejected ID", error.getCause().getMessage());
        } else {
          var records = result.get(10, TimeUnit.SECONDS);
          assertEquals(1, records.size());
          assertEquals("GOOD", records.getFirst().blockingKey());
          assertEquals("good", records.getFirst().mappedFields().get("uid"));
        }
      }
      assertThrows(
          IllegalArgumentException.class, () -> PekkoCorrelationEngine.correlate(List.of()));
    } finally {
      system.terminate();
    }
  }

  @Test
  void missingAndBlankCorrelationKeysObeyFailPolicy(@TempDir Path directory) throws Exception {
    var system = ActorSystem.create("correlation-key-contract");
    var mapper =
        new TransformSpec(
            "party", List.of(new FieldRule("uid", "id", null, null, null, false, false)));
    try (var bridge = new CamelBridge()) {
      for (String row : List.of("{}", "{\"id\":\"   \"}")) {
        var source =
            file(
                directory,
                "invalid-key-" + java.util.UUID.randomUUID() + ".json",
                "[" + row + "]",
                "json",
                Map.of());
        var result =
            PekkoCorrelationEngine.readAndMap(
                    bridge,
                    system,
                    source,
                    mapper,
                    "uid",
                    "source",
                    1.0,
                    Map.of(),
                    MalformedRecordPolicy.FAIL)
                .toCompletableFuture();
        var error = assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
        org.junit.jupiter.api.Assertions.assertTrue(
            error.getCause() instanceof IllegalArgumentException);
        org.junit.jupiter.api.Assertions.assertTrue(
            error.getCause().getMessage().contains("correlation key"));
      }
    } finally {
      system.terminate();
    }
  }

  @Test
  void callerOwnedSinksPropagateCheckedAndRuntimeFailuresWithoutReportingSuccess(
      @TempDir Path directory) throws Exception {
    var source = file(directory, "one.csv", "id,name\n1,One\n", "csv", Map.of());
    var spec =
        new com.forwardmeasure.datastreaming.api.IngestionSpec(
            List.of(
                new com.forwardmeasure.datastreaming.api.SourcePlan(
                    "one",
                    source,
                    new TransformSpec(
                        "party",
                        List.of(new FieldRule("uid", "id", null, null, null, false, false))),
                    1.0)),
            null,
            null,
            new com.forwardmeasure.datastreaming.api.SinkSpec(
                "file", "unused", "unused", null, null),
            com.forwardmeasure.datastreaming.api.ExecutionMode.BOUNDED,
            null,
            new com.forwardmeasure.datastreaming.api.ErrorPolicy("fail", "fail"));
    var system = ActorSystem.create("sink-failure-propagation");
    try {
      for (Exception expected :
          List.of(
              new java.io.IOException("io-rejection"),
              new IllegalArgumentException("invalid"),
              new Exception("checked-rejection"))) {
        Exception actual =
            assertThrows(
                Exception.class,
                () ->
                    new PekkoIngestionRunner()
                        .run(
                            spec,
                            system,
                            row -> row.get("id"),
                            Sink.foreach(
                                value -> {
                                  throw expected;
                                })));
        if (expected.getClass() == Exception.class) {
          assertInstanceOf(IllegalStateException.class, actual);
          org.junit.jupiter.api.Assertions.assertSame(expected, actual.getCause());
        } else org.junit.jupiter.api.Assertions.assertSame(expected, actual);
      }
    } finally {
      system.terminate();
    }
  }

  private static SourceSpec file(
      Path directory, String name, String content, String format, Map<String, String> options)
      throws Exception {
    Files.writeString(directory.resolve(name), content);
    return new SourceSpec(
        "file",
        "file:" + directory + "?fileName=" + name + "&noop=true&initialDelay=0&delay=50",
        new SourceSpec.FormatSpec(format),
        null,
        null,
        options);
  }
}
