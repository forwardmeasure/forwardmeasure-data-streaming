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
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
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
 * Real, no-mocks proof that {@link PekkoCorrelationRunner} - the {@code engine: pekko} counterpart
 * to {@code SparkCorrelationRunner}'s own {@code CorrelationSpec} handling, added 2026-09-13 -
 * drives {@link PekkoCorrelationEngine} end to end and produces the *identical* merge result
 * Spark's own {@code SparkCorrelationRunnerIntegrationTest} proves for the exact same two-source,
 * three-subject scenario: same source CSVs, same trust weights, same expected merged output. That
 * parity is the real point of this test - a correlation run must not depend on which engine ran it.
 */
class PekkoCorrelationRunnerIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void runCorrelatesTwoSourcesAndWritesTheMergedResult(@TempDir Path tempDir) throws Exception {
    writeFile(tempDir.resolve("source-a.csv"), SOURCE_A_CSV);
    writeFile(tempDir.resolve("source-b.csv"), SOURCE_B_CSV);
    Path outputFile = tempDir.resolve("output.jsonl");
    String dir = tempDir.toAbsolutePath().toString();

    CorrelationSpec spec =
        new CorrelationSpec(
            List.of(
                new CorrelationSpec.SourceEntry(
                    "core",
                    new SourceSpec(
                        "file",
                        "file:" + dir + "?fileName=source-a.csv&noop=true&initialDelay=0&delay=100",
                        null,
                        null),
                    mappingA(),
                    1.0),
                new CorrelationSpec.SourceEntry(
                    "enrichment",
                    new SourceSpec(
                        "file",
                        "file:" + dir + "?fileName=source-b.csv&noop=true&initialDelay=0&delay=100",
                        null,
                        null),
                    mappingB(),
                    0.4)),
            "uid",
            new SinkSpec(
                "file",
                "file:" + dir + "?fileName=output.jsonl&fileExist=Append",
                "n/a",
                null,
                null),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(4, 8), null, null));

    ActorSystem system = ActorSystem.create("pekko-correlation-runner-integration-test");
    PekkoCorrelationRunner.CorrelationResult result;
    try {
      result = PekkoCorrelationRunner.run(spec, system);
    } finally {
      system.terminate();
    }

    assertEquals(2, result.sourceCount());
    assertEquals(3, result.groupCount(), "expected 3 correlated groups: S1 (merged), S2, S3");

    List<JsonNode> rows = awaitRowCount(outputFile, 3, Duration.ofSeconds(15));
    assertEquals(3, rows.size());

    JsonNode s1 = findByUid(rows, "S1");
    assertEquals("Alice Anderson", s1.path("name").asText());
    assertEquals("1985-03-12", s1.path("date_of_birth").asText(), "higher-trust source A must win");
    assertEquals("GB", s1.path("nationality_code").asText(), "must be filled from lower-trust B");

    JsonNode s2 = findByUid(rows, "S2");
    assertEquals("Bob Baker", s2.path("name").asText());
    assertTrue(s2.path("nationality_code").isMissingNode(), "S2 was never given a nationality");

    JsonNode s3 = findByUid(rows, "S3");
    assertEquals("US", s3.path("nationality_code").asText());
    assertTrue(s3.path("name").isMissingNode(), "S3 was never given a name");
  }

  private static TransformSpec mappingA() {
    return new TransformSpec(
        "party",
        List.of(
            new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
            new TransformSpec.FieldRule("name", "FULL_NAME", null, null, null, null, null),
            new TransformSpec.FieldRule("date_of_birth", "DOB", null, null, null, true, null)));
  }

  private static TransformSpec mappingB() {
    return new TransformSpec(
        "party",
        List.of(
            new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
            new TransformSpec.FieldRule(
                "nationality_code", "NATIONALITY", null, null, null, true, null),
            new TransformSpec.FieldRule(
                "date_of_birth", "DOB_GUESS", null, null, null, true, null)));
  }

  private static JsonNode findByUid(List<JsonNode> rows, String uid) {
    return rows.stream()
        .filter(row -> uid.equals(row.path("uid").asText()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no merged record for uid " + uid + " in " + rows));
  }

  private static List<JsonNode> awaitRowCount(Path file, int expectedCount, Duration timeout)
      throws InterruptedException, IOException {
    Instant deadline = Instant.now().plus(timeout);
    List<String> lines = List.of();
    while (Instant.now().isBefore(deadline)) {
      if (Files.exists(file)) {
        lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.size() >= expectedCount) {
          break;
        }
      }
      Thread.sleep(50);
    }
    return lines.stream()
        .filter(line -> !line.isBlank())
        .map(PekkoCorrelationRunnerIntegrationTest::readTree)
        .toList();
  }

  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeFile(Path file, String content) throws IOException {
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }

  private static final String SOURCE_A_CSV =
      """
      ID,FULL_NAME,DOB
      S1,Alice Anderson,1985-03-12
      S2,Bob Baker,1990-07-04
      """;

  private static final String SOURCE_B_CSV =
      """
      ID,NATIONALITY,DOB_GUESS
      S1,GB,1899-01-01
      S3,US,1975-05-20
      """;
}
