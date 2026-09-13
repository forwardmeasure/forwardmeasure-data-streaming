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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link SparkIngestionRunner} - the standalone entrypoint this module
 * never had before - actually drives {@link SparkCorrelationEngine} end to end from a {@link
 * CorrelationSpec} and writes the merged result somewhere real: the exact same two-source,
 * three-subject scenario {@code CorrelatedSourceIngestionWorkerIntegrationTest} (fei) and {@code
 * SparkCorrelationEngineIntegrationTest} (this module) already prove, this time driven purely by a
 * loadable spec document rather than hand-constructed Java objects, with the merged output read
 * back from a real file {@code saveAsTextFile} wrote.
 */
class SparkIngestionRunnerIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-ingestion-runner-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void runCorrelatesTwoSourcesAndWritesTheMergedResult(@TempDir Path tempDir) throws Exception {
    Path sourceACsv = writeFile(tempDir, "source-a.csv", SOURCE_A_CSV);
    Path sourceBCsv = writeFile(tempDir, "source-b.csv", SOURCE_B_CSV);
    Path outputDir = tempDir.resolve("output");

    CorrelationSpec spec =
        new CorrelationSpec(
            List.of(
                new CorrelationSpec.SourceEntry(
                    "core",
                    new SourceSpec("file", sourceACsv.toString(), null, null),
                    mappingA(),
                    1.0),
                new CorrelationSpec.SourceEntry(
                    "enrichment",
                    new SourceSpec("file", sourceBCsv.toString(), null, null),
                    mappingB(),
                    0.4)),
            "uid",
            new SinkSpec("file", outputDir.toString(), "n/a", null, null),
            new ExecutionSpec("spark", null, null, null));

    SparkIngestionRunner.CorrelationResult result = SparkIngestionRunner.run(spark, spec);

    assertEquals(2, result.sourceCount());
    assertEquals(3, result.groupCount(), "expected 3 correlated groups: S1 (merged), S2, S3");

    List<JsonNode> rows = readNdjson(outputDir);
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

  @Test
  void runRejectsANonFileSinkWithoutTouchingSpark(@TempDir Path tempDir) throws Exception {
    Path sourceACsv = writeFile(tempDir, "source-a.csv", SOURCE_A_CSV);
    CorrelationSpec spec =
        new CorrelationSpec(
            List.of(
                new CorrelationSpec.SourceEntry(
                    "core",
                    new SourceSpec("file", sourceACsv.toString(), null, null),
                    mappingA(),
                    1.0)),
            "uid",
            new SinkSpec("opensearch", "opensearch://localhost:9200", "party", null, null),
            new ExecutionSpec("spark", null, null, null));

    assertThrows(UnsupportedOperationException.class, () -> SparkIngestionRunner.run(spark, spec));
  }

  private static JsonNode findByUid(List<JsonNode> rows, String uid) {
    return rows.stream()
        .filter(row -> uid.equals(row.path("uid").asText()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no merged record for uid " + uid));
  }

  private static List<JsonNode> readNdjson(Path outputDir) throws IOException {
    try (Stream<Path> files = Files.list(outputDir)) {
      List<Path> partFiles =
          files.filter(p -> p.getFileName().toString().startsWith("part-")).toList();
      List<JsonNode> rows = new java.util.ArrayList<>();
      for (Path partFile : partFiles) {
        for (String line : Files.readAllLines(partFile, StandardCharsets.UTF_8)) {
          if (!line.isBlank()) {
            rows.add(MAPPER.readTree(line));
          }
        }
      }
      return rows;
    }
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

  private static Path writeFile(Path dir, String name, String content) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
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
