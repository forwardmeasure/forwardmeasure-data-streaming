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

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Pekko sibling of {@code SparkCorrelationEngineIntegrationTest} - added 2026-09-14 once a
 * direct side-by-side comparison surfaced a real gap: {@link PekkoCorrelationEngine} exists as its
 * own class exactly the way {@code SparkCorrelationEngine} does, but had no test exercising its own
 * {@link PekkoCorrelationEngine#readAndMap}/{@link PekkoCorrelationEngine#correlate} directly -
 * only indirectly, via {@code PekkoCorrelationRunnerIntegrationTest}'s own full spec-to-sink run.
 * Same real fixtures, same three subjects, same expected merged values as the Spark sibling (and as
 * {@code PekkoCorrelationRunnerIntegrationTest}'s own scenario) - proving this engine's own merge
 * logic in isolation, not a parallel reimplementation of the runner-level proof.
 *
 * <p>The second test closes a real, separate gap found at the same time: zero tests anywhere in
 * this module proved a {@code json}/{@code ndjson} formatted file source actually works, despite
 * {@link PekkoIngestionRunner#parseTextRows} dispatching on {@code format.type()} for it since
 * 2026-09-13 - confirmed by grep before writing this, not assumed.
 */
class PekkoCorrelationEngineIntegrationTest {

  @Test
  void correlatesTwoSourcesAndMergesByTrustWeight(@TempDir Path tempDir) throws Exception {
    writeFile(tempDir.resolve("source-a.csv"), SOURCE_A_CSV);
    writeFile(tempDir.resolve("source-b.csv"), SOURCE_B_CSV);
    String dir = tempDir.toAbsolutePath().toString();

    SourceSpec sourceA =
        new SourceSpec(
            "file",
            "file:" + dir + "?fileName=source-a.csv&noop=true&initialDelay=0&delay=100",
            null,
            null);
    SourceSpec sourceB =
        new SourceSpec(
            "file",
            "file:" + dir + "?fileName=source-b.csv&noop=true&initialDelay=0&delay=100",
            null,
            null);

    ActorSystem system = ActorSystem.create("pekko-correlation-engine-integration-test");
    List<PekkoCorrelationEngine.PekkoCorrelationRecord> mappedA;
    List<PekkoCorrelationEngine.PekkoCorrelationRecord> mappedB;
    try (CamelBridge bridge = new CamelBridge()) {
      mappedA =
          PekkoCorrelationEngine.readAndMap(
                  bridge,
                  system,
                  sourceA,
                  mappingA(),
                  "uid",
                  "core",
                  1.0,
                  Map.of(),
                  IngestionPipeline.MalformedRecordPolicy.SKIP)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
      mappedB =
          PekkoCorrelationEngine.readAndMap(
                  bridge,
                  system,
                  sourceB,
                  mappingB(),
                  "uid",
                  "enrichment",
                  0.4,
                  Map.of(),
                  IngestionPipeline.MalformedRecordPolicy.SKIP)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    List<Map<String, Object>> merged = PekkoCorrelationEngine.correlate(List.of(mappedA, mappedB));

    assertEquals(3, merged.size(), "expected 3 correlated groups: S1 (merged), S2, S3");

    Map<String, Object> s1 = findByUid(merged, "S1");
    assertEquals("Alice Anderson", s1.get("name"));
    assertEquals("1985-03-12", s1.get("date_of_birth"), "higher-trust source A must win");
    assertEquals("GB", s1.get("nationality_code"), "must be filled from lower-trust B");

    Map<String, Object> s2 = findByUid(merged, "S2");
    assertEquals("Bob Baker", s2.get("name"));
    assertEquals("1990-07-04", s2.get("date_of_birth"));
    assertFalse(s2.containsKey("nationality_code"), "S2 was never given a nationality");

    Map<String, Object> s3 = findByUid(merged, "S3");
    assertEquals("US", s3.get("nationality_code"));
    assertEquals("1975-05-20", s3.get("date_of_birth"));
    assertFalse(s3.containsKey("name"), "S3 was never given a name");
  }

  /**
   * Proves {@link PekkoIngestionRunner#rowSource}'s {@code format.type()} dispatch genuinely
   * supports {@code json} (a JSON array of objects here; newline-delimited is the other real shape
   * {@link PekkoIngestionRunner#parseJsonRows} accepts) - no test anywhere in this module exercised
   * either shape before this one.
   */
  @Test
  void readsAJsonFileSourceWithNoNewJavaJustAFormatMetadataChange(@TempDir Path tempDir)
      throws Exception {
    writeFile(tempDir.resolve("source.json"), SOURCE_JSON);
    String dir = tempDir.toAbsolutePath().toString();

    SourceSpec source =
        new SourceSpec(
            "file",
            "file:" + dir + "?fileName=source.json&noop=true&initialDelay=0&delay=100",
            new SourceSpec.FormatSpec("json"),
            null);

    ActorSystem system = ActorSystem.create("pekko-json-format-integration-test");
    List<PekkoCorrelationEngine.PekkoCorrelationRecord> mapped;
    try (CamelBridge bridge = new CamelBridge()) {
      mapped =
          PekkoCorrelationEngine.readAndMap(
                  bridge,
                  system,
                  source,
                  mappingA(),
                  "uid",
                  "core",
                  1.0,
                  Map.of(),
                  IngestionPipeline.MalformedRecordPolicy.SKIP)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    assertEquals(2, mapped.size());
    Map<String, Object> s1 =
        mapped.stream()
            .filter(r -> "S1".equals(r.mappedFields().get("uid")))
            .findFirst()
            .orElseThrow()
            .mappedFields();
    assertEquals("Alice Anderson", s1.get("name"));
    Map<String, Object> s2 =
        mapped.stream()
            .filter(r -> "S2".equals(r.mappedFields().get("uid")))
            .findFirst()
            .orElseThrow()
            .mappedFields();
    assertEquals("Bob Baker", s2.get("name"));
  }

  private static Map<String, Object> findByUid(List<Map<String, Object>> merged, String uid) {
    return merged.stream()
        .filter(record -> uid.equals(record.get("uid")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no merged record for uid " + uid));
  }

  private static TransformSpec mappingA() {
    return new TransformSpec(
        "party",
        List.of(
            new FieldRule("uid", "ID", null, null, null, null, null),
            new FieldRule("name", "FULL_NAME", null, null, null, null, null),
            new FieldRule("date_of_birth", "DOB", null, null, null, true, null)));
  }

  private static TransformSpec mappingB() {
    return new TransformSpec(
        "party",
        List.of(
            new FieldRule("uid", "ID", null, null, null, null, null),
            new FieldRule("nationality_code", "NATIONALITY", null, null, null, true, null),
            new FieldRule("date_of_birth", "DOB_GUESS", null, null, null, true, null)));
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

  private static final String SOURCE_JSON =
      """
      [
        {"ID":"S1","FULL_NAME":"Alice Anderson","DOB":"1985-03-12"},
        {"ID":"S2","FULL_NAME":"Bob Baker","DOB":"1990-07-04"}
      ]
      """;
}
