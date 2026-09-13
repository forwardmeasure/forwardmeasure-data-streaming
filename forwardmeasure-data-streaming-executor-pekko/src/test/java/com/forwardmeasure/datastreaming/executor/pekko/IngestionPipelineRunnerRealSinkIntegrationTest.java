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
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link IngestionPipelineRunner#run(IngestionSpec, ActorSystem)} writes
 * to an actual Camel sink endpoint, not a placeholder - and specifically that it does so
 * completely: an earlier version of this same test (backed by {@code CamelBridge}'s own now-removed
 * {@code sink()} helper) caught a real, confirmed bug where the last one or two rows were silently
 * dropped, since that helper's underlying {@code onNext()} dispatch was fire-and-forget relative to
 * the actual Camel send (see {@code CamelBridge}'s own javadoc). Uses {@code camel-file} for both
 * source and sink - the same component already proven on the source side, in producer mode this
 * time: every mapped row is written as one NDJSON line to a real output file, then read back and
 * verified - order-independent, since {@code mapAsyncUnordered} genuinely processes rows
 * concurrently and the sink's own {@code parallelism > 1} means row order in the output file is not
 * guaranteed to match input order.
 */
class IngestionPipelineRunnerRealSinkIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void runWritesEachMappedRowToARealCamelSink(@TempDir Path tempDir) throws Exception {
    writeCsv(tempDir.resolve("input.csv"));
    Path outputFile = tempDir.resolve("output.jsonl");

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
            new SinkSpec(
                "file",
                "file:" + tempDir.toAbsolutePath() + "?fileName=output.jsonl&fileExist=Append",
                "n/a",
                new SourceSpec.SchemaRef("schema://test/target/1.0"),
                null),
            new ExecutionSpec(
                "pekko",
                new ExecutionSpec.ConcurrencySpec(4, 8),
                new ExecutionSpec.FlowControlSpec(100),
                new ExecutionSpec.FailureSpec("dead-letter", "retry")));

    ActorSystem system = ActorSystem.create("ingestion-pipeline-runner-real-sink-test");
    IngestionPipelineRunner.IngestionResult result;
    try {
      result = new IngestionPipelineRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    assertEquals(3, result.recordsProcessed());

    // run() already blocked on the pipeline's own completion signal before returning, so this is
    // deliberately a real diagnostic, not a "just wait longer" workaround: recordsProcessed() == 3
    // above proves all 3 rows reached the sink stage's own map() step, so if the file still doesn't
    // show 3 lines even after a generous extra wait, that's real evidence of an actual dropped
    // write, not merely a test reading the file a moment too soon.
    List<String> lines = awaitLineCount(outputFile, 3, Duration.ofSeconds(15));
    assertEquals(3, lines.size(), "expected one NDJSON line per mapped row, got: " + lines);

    List<JsonNode> rows =
        lines.stream().map(IngestionPipelineRunnerRealSinkIntegrationTest::readTree).toList();
    assertRowPresent(rows, "1", "Alice");
    assertRowPresent(rows, "2", "Bob");
    assertRowPresent(rows, "3", "Charlie");
  }

  private static void assertRowPresent(List<JsonNode> rows, String id, String name) {
    assertTrue(
        rows.stream()
            .anyMatch(
                row ->
                    id.equals(row.path("id").asText()) && name.equals(row.path("name").asText())),
        "expected a row with id=" + id + " name=" + name + ", got: " + rows);
  }

  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeCsv(Path file) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("id,name");
      writer.newLine();
      writer.write("1,Alice");
      writer.newLine();
      writer.write("2,Bob");
      writer.newLine();
      writer.write("3,Charlie");
      writer.newLine();
    }
  }

  private static List<String> awaitLineCount(Path file, int expectedCount, Duration timeout)
      throws InterruptedException, IOException {
    Instant deadline = Instant.now().plus(timeout);
    List<String> lines = List.of();
    while (Instant.now().isBefore(deadline)) {
      if (Files.exists(file)) {
        lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.size() >= expectedCount) {
          return lines;
        }
      }
      Thread.sleep(50);
    }
    return lines;
  }
}
