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
package com.forwardmeasure.datastreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IngestionSpecTest {

  private static final String YAML =
      """
      source:
        connector: file
        uri: /data/worldcheck/reference-population.csv
        format:
          type: csv
        schema:
          ref: schema://worldcheck/reference-population/1.0

      mapper:
        target: schema://party/2.0
        fields:
          - target: fullName
            template: "{LAST NAME}, {FIRST NAME}"
          - target: dateOfBirth
            source: dateOfBirth
            transform: format_individual_dob_yyyy_mm_dd_clamp_1900
          - target: entityKind
            inputs:
              value: entityType
              entity_indicator: entityIndicator
            transform: map_worldcheck_entity_kind
          - target: locations
            source: locations
            transform: parse_worldcheck_locations
            repeated: true
            optional: true

      sink:
        connector: opensearch
        index: party-2-0
        schema:
          ref: schema://party/2.0
        batching:
          maxRecords: 1000
          maxWait: PT2S

      execution:
        engine: pekko
        concurrency:
          preferred: 16
          maximum: 32
        flowControl:
          maxInFlightRecords: 5000
        failure:
          malformedRecord: dead-letter
          sinkFailure: retry
      """;

  @Test
  void parsesEveryFieldFromRealisticYaml() {
    IngestionSpec spec = IngestionSpec.parseYaml(YAML);

    assertEquals("file", spec.source().connector());
    assertEquals("/data/worldcheck/reference-population.csv", spec.source().uri());
    assertEquals("csv", spec.source().format().type());
    assertEquals("schema://worldcheck/reference-population/1.0", spec.source().schema().ref());

    assertEquals("schema://party/2.0", spec.mapper().target());
    assertEquals(4, spec.mapper().fields().size());

    TransformSpec.FieldRule fullName = spec.mapper().fields().get(0);
    assertEquals("fullName", fullName.target());
    assertEquals("{LAST NAME}, {FIRST NAME}", fullName.template());

    TransformSpec.FieldRule dob = spec.mapper().fields().get(1);
    assertEquals("format_individual_dob_yyyy_mm_dd_clamp_1900", dob.transform());
    assertEquals(Map.of("value", "dateOfBirth"), dob.effectiveInputs());

    TransformSpec.FieldRule entityKind = spec.mapper().fields().get(2);
    assertEquals("map_worldcheck_entity_kind", entityKind.transform());
    assertEquals(
        Map.of("value", "entityType", "entity_indicator", "entityIndicator"),
        entityKind.effectiveInputs());

    TransformSpec.FieldRule locations = spec.mapper().fields().get(3);
    assertTrue(locations.isRepeated());
    assertTrue(locations.isOptional());

    assertEquals("opensearch", spec.sink().connector());
    assertEquals("party-2-0", spec.sink().index());
    assertEquals(1000, spec.sink().batching().maxRecords());
    assertEquals(Duration.ofSeconds(2), spec.sink().batching().maxWait());

    assertEquals("pekko", spec.execution().engine());
    assertEquals(16, spec.execution().concurrency().preferred());
    assertEquals(32, spec.execution().concurrency().maximum());
    assertEquals(5000, spec.execution().flowControl().maxInFlightRecords());
    assertEquals("dead-letter", spec.execution().failure().malformedRecord());
    assertEquals("retry", spec.execution().failure().sinkFailure());
  }

  @Test
  void roundTripsThroughYamlWithoutLoss() throws IOException {
    IngestionSpec original = IngestionSpec.parseYaml(YAML);

    String serialized = original.toYaml();
    IngestionSpec reparsed = IngestionSpec.parseYaml(serialized);

    assertEquals(original, reparsed);
  }

  @Test
  void loadsFromPath(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir)
      throws IOException {
    java.nio.file.Path specFile = tempDir.resolve("ingestion-spec.yaml");
    java.nio.file.Files.writeString(specFile, YAML);

    IngestionSpec spec = IngestionSpec.load(specFile);

    assertEquals("file", spec.source().connector());
    assertEquals(4, spec.mapper().fields().size());
  }
}
