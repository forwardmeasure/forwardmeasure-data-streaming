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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.FlowControlSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import com.forwardmeasure.datastreaming.sourcereaders.TextContent;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Step 4's own definition of done, verified end to end: a real multi-thousand-row CSV file flows
 * through {@link PekkoIngestionRunner} - Camel file source, real row splitting, the real {@link
 * FieldMappingEngine} - with real concurrency, not "completes correctly but still sequential."
 * Concurrency is proven by recording which threads actually executed the transform step (a fast
 * per-row transform plus a tiny artificial delay, injected only by this test's own wrapper, widens
 * the window enough to make cross-thread scheduling observable) rather than by a wall-clock scaling
 * assertion, which would be more sensitive to the machine running the test.
 */
class PekkoIngestionRunnerIntegrationTest {

  private static final int ROW_COUNT = 5_000;

  @Test
  void processesMultiThousandRowCsvWithRealConcurrency(@TempDir Path tempDir) throws Exception {
    writeCsv(tempDir.resolve("input.csv"), ROW_COUNT);

    IngestionSpec spec =
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "single",
                    new SourceSpec(
                        "file",
                        "file:"
                            + tempDir.toAbsolutePath()
                            + "?fileName=input.csv&noop=true&initialDelay=0&delay=100",
                        new SourceSpec.FormatSpec("csv"),
                        new SourceSpec.SchemaRef("schema://test/source/1.0")),
                    new TransformSpec(
                        "schema://test/target/1.0",
                        List.of(
                            new TransformSpec.FieldRule("id", "id", null, null, null, null, null),
                            new TransformSpec.FieldRule(
                                "name", "name", null, null, null, null, null))),
                    1.0)),
            null,
            null,
            new SinkSpec("log", "n/a", new SourceSpec.SchemaRef("schema://test/target/1.0"), null),
            ExecutionMode.BOUNDED,
            new DeliverySemantics(true, new ConcurrencySpec(16, 32), new FlowControlSpec(5000)),
            new ErrorPolicy("dead-letter", "retry"));

    Set<String> threadNames = ConcurrentHashMap.newKeySet();
    FieldMappingEngine engine = new FieldMappingEngine();
    Function<SourceRow, Map<String, Object>> instrumentedTransform =
        row -> {
          threadNames.add(Thread.currentThread().getName());
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return engine.map(row, spec.sources().get(0).mapper());
        };
    Sink<Map<String, Object>, CompletionStage<Long>> countingSink =
        Sink.fold(0L, (count, ignored) -> count + 1);

    ActorSystem system = ActorSystem.create("ingestion-pipeline-runner-test");
    long processed;
    try {
      processed = new PekkoIngestionRunner().run(spec, system, instrumentedTransform, countingSink);
    } finally {
      system.terminate();
    }

    assertEquals(ROW_COUNT, processed);
    assertTrue(
        threadNames.size() > 1,
        "expected multiple worker threads to process rows, saw only: " + threadNames);
  }

  /**
   * The caller-opened source reads the same rows the file endpoint would, in one pass, honoring the
   * delimiter and charset options, and closes the stream it opened.
   */
  @Test
  void readsCallerOpenedContentWithTheSourceDialectAndClosesIt() throws Exception {
    StringBuilder text = new StringBuilder("id;name\n");
    for (int i = 1; i <= ROW_COUNT; i++) text.append(i).append(";Rów ").append(i).append('\n');
    byte[] latin1 = text.toString().getBytes(StandardCharsets.ISO_8859_1);
    java.util.concurrent.atomic.AtomicInteger opened =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.concurrent.atomic.AtomicBoolean closed =
        new java.util.concurrent.atomic.AtomicBoolean();
    TextContent content =
        () -> {
          opened.incrementAndGet();
          return new java.io.FilterInputStream(new java.io.ByteArrayInputStream(latin1)) {
            @Override
            public void close() throws IOException {
              closed.set(true);
              super.close();
            }
          };
        };
    Set<String> names = ConcurrentHashMap.newKeySet();
    ActorSystem system = ActorSystem.create("caller-opened-content");
    long processed;
    try {
      processed =
          new PekkoIngestionRunner()
              .run(
                  streamSpec(Map.of("delimiter", ";", "charset", "ISO-8859-1")),
                  content,
                  system,
                  row -> {
                    names.add(String.valueOf(row.get("name")));
                    return row;
                  },
                  Sink.<Long, SourceRow>fold(0L, (count, ignored) -> count + 1));
    } finally {
      system.terminate();
    }
    assertEquals(ROW_COUNT, processed);
    assertTrue(names.contains("Rów 1") && names.contains("Rów " + ROW_COUNT), names.toString());
    assertEquals(1, opened.get());
    assertTrue(closed.get(), "the runner closes the stream it read");
  }

  /** Bytes that are not text in the declared charset fail the run rather than being replaced. */
  @Test
  void callerOpenedContentWithInvalidBytesFailsTheRun() {
    byte[] invalidUtf8 = {'i', 'd', '\n', '1', (byte) 0xC3, '\n'};
    ActorSystem system = ActorSystem.create("caller-opened-invalid");
    try {
      assertThrows(
          RuntimeException.class,
          () ->
              new PekkoIngestionRunner()
                  .run(
                      streamSpec(Map.of()),
                      () -> new java.io.ByteArrayInputStream(invalidUtf8),
                      system,
                      row -> row,
                      Sink.<Long, SourceRow>fold(0L, (count, ignored) -> count + 1)));
    } finally {
      system.terminate();
    }
  }

  @Test
  void callerOpenedContentIsOnlyForTextConnectors() {
    IngestionSpec sql =
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "single",
                    new SourceSpec("sql", "sql:select 1", new SourceSpec.FormatSpec("csv"), null),
                    new TransformSpec("schema://test/target/1.0", List.of()),
                    1.0)),
            null,
            null,
            new SinkSpec("log", "n/a", new SourceSpec.SchemaRef("schema://test/target/1.0"), null),
            ExecutionMode.BOUNDED,
            new DeliverySemantics(true, new ConcurrencySpec(1, 1), new FlowControlSpec(10)),
            new ErrorPolicy("dead-letter", "retry"));
    ActorSystem system = ActorSystem.create("caller-opened-sql");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new PekkoIngestionRunner()
                  .run(
                      sql,
                      () -> new java.io.ByteArrayInputStream(new byte[0]),
                      system,
                      row -> row,
                      Sink.<Long, SourceRow>fold(0L, (count, ignored) -> count + 1)));
    } finally {
      system.terminate();
    }
  }

  private static IngestionSpec streamSpec(Map<String, String> options) {
    return new IngestionSpec(
        List.of(
            new SourcePlan(
                "single",
                new SourceSpec(
                    "object-storage",
                    "gs://bucket/feed.csv#1",
                    new SourceSpec.FormatSpec("csv"),
                    null,
                    null,
                    options),
                new TransformSpec("schema://test/target/1.0", List.of()),
                1.0)),
        null,
        null,
        new SinkSpec("log", "n/a", new SourceSpec.SchemaRef("schema://test/target/1.0"), null),
        ExecutionMode.BOUNDED,
        new DeliverySemantics(true, new ConcurrencySpec(16, 32), new FlowControlSpec(5000)),
        new ErrorPolicy("dead-letter", "retry"));
  }

  private static void writeCsv(Path file, int rowCount) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("id,name");
      writer.newLine();
      for (int i = 1; i <= rowCount; i++) {
        writer.write(i + ",Row " + i);
        writer.newLine();
      }
    }
  }
}
