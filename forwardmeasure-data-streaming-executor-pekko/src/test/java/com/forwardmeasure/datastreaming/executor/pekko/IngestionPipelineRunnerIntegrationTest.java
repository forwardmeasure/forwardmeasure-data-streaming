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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
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
 * through {@link IngestionPipelineRunner} - Camel file source, real row splitting, the real {@link
 * FieldMappingEngine} - with real concurrency, not "completes correctly but still sequential."
 * Concurrency is proven by recording which threads actually executed the transform step (a fast
 * per-row transform plus a tiny artificial delay, injected only by this test's own wrapper, widens
 * the window enough to make cross-thread scheduling observable) rather than by a wall-clock scaling
 * assertion, which would be more sensitive to the machine running the test.
 */
class IngestionPipelineRunnerIntegrationTest {

  private static final int ROW_COUNT = 5_000;

  @Test
  void processesMultiThousandRowCsvWithRealConcurrency(@TempDir Path tempDir) throws Exception {
    writeCsv(tempDir.resolve("input.csv"), ROW_COUNT);

    IngestionSpec spec =
        new IngestionSpec(
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
                    new TransformSpec.FieldRule("name", "name", null, null, null, null, null))),
            new SinkSpec("log", "n/a", new SourceSpec.SchemaRef("schema://test/target/1.0"), null),
            new ExecutionSpec(
                "pekko",
                new ExecutionSpec.ConcurrencySpec(16, 32),
                new ExecutionSpec.FlowControlSpec(5000),
                new ExecutionSpec.FailureSpec("dead-letter", "retry")));

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
          return engine.map(row, spec.mapper());
        };
    Sink<Map<String, Object>, CompletionStage<Long>> countingSink =
        Sink.fold(0L, (count, ignored) -> count + 1);

    ActorSystem system = ActorSystem.create("ingestion-pipeline-runner-test");
    long processed;
    try {
      processed =
          new IngestionPipelineRunner().run(spec, system, instrumentedTransform, countingSink);
    } finally {
      system.terminate();
    }

    assertEquals(ROW_COUNT, processed);
    assertTrue(
        threadNames.size() > 1,
        "expected multiple worker threads to process rows, saw only: " + threadNames);
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
