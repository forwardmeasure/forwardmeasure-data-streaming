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

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, no-mocks proof that {@link SparkCorrelationEngine} generalizes fei's own {@code
 * CorrelatedSourceIngestionWorker} correctly: mirrors that class's real integration test scenario
 * (two hand-authored source CSVs, correlated on a shared {@code uid} blocking key, merged by source
 * trust weight) exactly - same fixtures, same three subjects, same expected merged values - run in
 * local-mode Spark ({@code local[2]}). Stops at the merged {@code Map<String,Object>} (this
 * module's own documented boundary - see {@link SparkCorrelationEngine}) rather than also proving a
 * sink write, since that's the caller's concern, not this engine's.
 */
class SparkCorrelationEngineIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SparkCorrelationEngineIntegrationTest.class);

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    LOGGER.info("Starting local-mode SparkSession");
    spark =
        SparkSession.builder()
            .appName("spark-correlation-engine-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      LOGGER.info("Stopping SparkSession");
      spark.stop();
    }
  }

  @Test
  void correlatesTwoSourcesAndMergesByTrustWeight(@TempDir Path tempDir) throws IOException {
    LOGGER.info("Writing source fixtures under {}", tempDir);
    Path sourceACsv = writeFile(tempDir, "source-a.csv", SOURCE_A_CSV);
    Path sourceBCsv = writeFile(tempDir, "source-b.csv", SOURCE_B_CSV);

    SparkSourceConfig sourceA =
        new SparkSourceConfig(
            "core", new SourceSpec("file", sourceACsv.toString(), null, null), mappingA(), 1.0);
    SparkSourceConfig sourceB =
        new SparkSourceConfig(
            "enrichment",
            new SourceSpec("file", sourceBCsv.toString(), null, null),
            mappingB(),
            0.4);

    LOGGER.info("Running correlation: 2 source(s), blocking on 'uid'");
    JavaRDD<SparkCorrelationRecord> mappedA =
        SparkCorrelationEngine.readAndMap(spark, sourceA, "uid", Map.of());
    JavaRDD<SparkCorrelationRecord> mappedB =
        SparkCorrelationEngine.readAndMap(spark, sourceB, "uid", Map.of());
    List<Map<String, Object>> merged =
        SparkCorrelationEngine.correlate(List.of(mappedA, mappedB)).collect();
    LOGGER.info("Correlation finished: groupCount={}", merged.size());

    assertEquals(3, merged.size(), "expected 3 correlated groups: S1 (merged), S2, S3");

    Map<String, Object> s1 = findByUid(merged, "S1");
    LOGGER.info("S1 merged record: {}", s1);
    assertEquals("Alice Anderson", s1.get("name"));
    assertEquals("1985-03-12", s1.get("date_of_birth"), "higher-trust source A must win");
    assertEquals("GB", s1.get("nationality_code"), "must be filled from lower-trust B");

    Map<String, Object> s2 = findByUid(merged, "S2");
    LOGGER.info("S2 merged record: {}", s2);
    assertEquals("Bob Baker", s2.get("name"));
    assertEquals("1990-07-04", s2.get("date_of_birth"));
    assertFalse(s2.containsKey("nationality_code"), "S2 was never given a nationality");

    Map<String, Object> s3 = findByUid(merged, "S3");
    LOGGER.info("S3 merged record: {}", s3);
    assertEquals("US", s3.get("nationality_code"));
    assertEquals("1975-05-20", s3.get("date_of_birth"));
    assertFalse(s3.containsKey("name"), "S3 was never given a name");
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
