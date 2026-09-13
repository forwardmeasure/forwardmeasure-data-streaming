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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Step 5 of the design plan's §5 build order: proves the new path against a real WorldCheck-shaped
 * input, not a fabricated toy CSV - the same synthetic-row generator and the same real mapping
 * definition ({@code mapping/worldcheck-simple-ingestion.yaml}) {@code
 * SimpleSourceIngestionWorkerIntegrationTest} uses in fei today, run through {@link
 * IngestionPipelineRunner} instead of {@code SimpleSourceIngestionWorker}.
 *
 * <p>This doesn't invoke fei's {@code GenericRecordMapper} in-process (that would mean this repo
 * depending on fei, backwards from the intended direction). Instead it asserts against fei's own
 * integration test's already-proven expected values for specific rows (wc-0, wc-1) - a real,
 * independently-verified oracle, not values invented for this test - plus a full-population
 * consistency check fei's own spot-check didn't do, since diffing "the new path's output" against
 * "the old path's proven-correct output" is exactly what this step calls for.
 */
class WorldCheckParityIntegrationTest {

  private static final int ROW_COUNT = 10_000;

  @Test
  void producesTheSameMappedValuesAsGenericRecordMapperForRealWorldCheckData(@TempDir Path tempDir)
      throws Exception {
    writeWorldCheckCsv(tempDir.resolve("worldcheck.csv"), ROW_COUNT);

    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=worldcheck.csv&noop=true&initialDelay=0&delay=100",
                new SourceSpec.FormatSpec("csv"),
                new SourceSpec.SchemaRef("schema://worldcheck/simple-ingestion/1.0")),
            worldCheckMapping(),
            new SinkSpec("log", "n/a", new SourceSpec.SchemaRef("schema://party/1.0"), null),
            new ExecutionSpec(
                "pekko",
                new ExecutionSpec.ConcurrencySpec(16, 32),
                new ExecutionSpec.FlowControlSpec(5000),
                new ExecutionSpec.FailureSpec("dead-letter", "retry")));

    Map<String, NamedTransform> worldCheckTransforms =
        Map.of(
            "map_worldcheck_entity_kind",
            WorldCheckTestTransforms::map_worldcheck_entity_kind,
            "parse_worldcheck_date",
            inputs -> WorldCheckTestTransforms.parse_worldcheck_date(inputs.get("value")));

    FieldMappingEngine engine = new FieldMappingEngine();
    Function<SourceRow, Map<String, Object>> transform =
        row -> engine.map(row, spec.mapper(), worldCheckTransforms);

    Sink<Map<String, Object>, CompletionStage<Map<String, Map<String, Object>>>> collectingSink =
        Sink.fold(
            new HashMap<>(),
            (acc, mapped) -> {
              acc.put(String.valueOf(mapped.get("uid")), mapped);
              return acc;
            });

    ActorSystem system = ActorSystem.create("worldcheck-parity-test");
    Map<String, Map<String, Object>> byUid;
    try {
      byUid = new IngestionPipelineRunner().run(spec, system, transform, collectingSink);
    } finally {
      system.terminate();
    }

    assertEquals(ROW_COUNT, byUid.size());

    // fei's own SimpleSourceIngestionWorkerIntegrationTest already asserts these exact values for
    // these exact rows against the real GenericRecordMapper/NamedTransformRegistry path - this is
    // the real oracle, not a value invented for this test.
    Map<String, Object> wc0 = byUid.get("wc-0");
    assertEquals("Smith0, Jane0", wc0.get("name"));
    assertEquals("person", wc0.get("entity_kind"));
    assertEquals("US", wc0.get("nationality_code"));
    assertEquals("1980-01-01", wc0.get("date_of_birth"));

    Map<String, Object> wc1 = byUid.get("wc-1");
    assertEquals("organization", wc1.get("entity_kind"));

    // Full-population consistency check - beyond fei's own 2-row spot check, since the new engine
    // now processes all 10,000 rows concurrently rather than sequentially.
    for (int i = 0; i < ROW_COUNT; i++) {
      Map<String, Object> mapped = byUid.get("wc-" + i);
      boolean person = i % 2 == 0;
      assertEquals(person ? "person" : "organization", mapped.get("entity_kind"), "row " + i);
      assertEquals("US", mapped.get("nationality_code"), "row " + i);
      String expectedDay = String.format("%02d", (i % 28) + 1);
      assertEquals("1980-01-" + expectedDay, mapped.get("date_of_birth"), "row " + i);
    }
  }

  private static TransformSpec worldCheckMapping() {
    return new TransformSpec(
        "schema://worldcheck/party/1.0",
        List.of(
            new TransformSpec.FieldRule("uid", "UID", null, null, null, null, null),
            new TransformSpec.FieldRule(
                "name", null, "{LAST_NAME}, {FIRST_NAME}", null, null, null, null),
            new TransformSpec.FieldRule(
                "entity_kind",
                null,
                null,
                Map.of("value", "CATEGORY", "entity_indicator", "ENTITY_INDICATOR"),
                "map_worldcheck_entity_kind",
                null,
                null),
            new TransformSpec.FieldRule(
                "date_of_birth", "DATE_OF_BIRTH", null, null, "parse_worldcheck_date", true, null),
            new TransformSpec.FieldRule(
                "nationality_code",
                "CITIZENSHIP",
                null,
                null,
                "resolve_iso2_country",
                true,
                null)));
  }

  /**
   * Ported verbatim from fei's own
   * SimpleSourceIngestionWorkerIntegrationTest#generateWorldCheckCsv.
   */
  private static void writeWorldCheckCsv(Path file, int rowCount) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("UID,LAST_NAME,FIRST_NAME,CATEGORY,ENTITY_INDICATOR,DATE_OF_BIRTH,CITIZENSHIP");
      writer.newLine();
      for (int i = 0; i < rowCount; i++) {
        boolean person = i % 2 == 0;
        String category = person ? "INDIVIDUAL" : "ORGANIZATION";
        String indicator = person ? "M" : "E";
        StringBuilder row = new StringBuilder();
        row.append("wc-").append(i).append(',');
        row.append(person ? "Smith" + i : "Acme" + i + " Corp").append(',');
        row.append(person ? "Jane" + i : "").append(',');
        row.append(category).append(',');
        row.append(indicator).append(',');
        row.append("1980/01/").append(String.format("%02d", (i % 28) + 1)).append(',');
        row.append("UNITED STATES");
        writer.write(row.toString());
        writer.newLine();
      }
    }
  }
}
