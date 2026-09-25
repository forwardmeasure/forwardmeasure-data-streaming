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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers both real cases the unified {@link IngestionSpec} now serves - single-source (one element
 * in {@code sources}, no {@code blockingField}) and correlated (more than one, merged on a real
 * {@code blockingField}) - since {@code CorrelationSpec} no longer exists as a separate type (see
 * the repo's own gap-bridging plan, "a single IngestionSpec, not four types").
 */
class IngestionSpecTest {

  private static final String SINGLE_SOURCE_YAML =
      """
      sources:
        - sourceKey: reference
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
                transform: classify_party_kind
              - target: locations
                source: locations
                transform: parse_worldcheck_locations
                repeated: true
                optional: true
          trustWeight: 1.0

      sink:
        connector: opensearch
        index: party-2-0
        schema:
          ref: schema://party/2.0
        batching:
          maxRecords: 1000
          maxWait: PT2S

      executionMode: BOUNDED

      delivery:
        exactlyOnce: false
        concurrency:
          preferred: 16
          maximum: 32
        flowControl:
          maxInFlightRecords: 5000

      errors:
        malformedRecord: dead-letter
        sinkFailure: retry
      """;

  private static final String CORRELATED_YAML =
      """
      sources:
        - sourceKey: core
          source:
            connector: file
            uri: /data/worldcheck/core.csv
            format:
              type: csv
          mapper:
            target: schema://party/2.0
            fields:
              - target: uid
                source: ID
              - target: name
                source: FULL_NAME
          trustWeight: 1.0
        - sourceKey: enrichment
          source:
            connector: file
            uri: /data/worldcheck/enrichment.csv
            format:
              type: csv
          mapper:
            target: schema://party/2.0
            fields:
              - target: uid
                source: ID
              - target: nationality_code
                source: NATIONALITY
                optional: true
          trustWeight: 0.4

      blockingField: uid

      sink:
        connector: opensearch
        uri: opensearch://localhost:9200
        index: party-2-0

      executionMode: BOUNDED

      delivery:
        exactlyOnce: false
        concurrency:
          preferred: 8
      """;

  @Test
  void parsesEveryFieldFromRealisticSingleSourceYaml() {
    IngestionSpec spec = IngestionSpec.parseYaml(SINGLE_SOURCE_YAML);

    assertEquals(SourceCardinality.SINGLE, spec.sourceCardinality());
    assertEquals(1, spec.sources().size());

    SourcePlan reference = spec.sources().get(0);
    assertEquals("reference", reference.sourceKey());
    assertEquals("file", reference.source().connector());
    assertEquals("/data/worldcheck/reference-population.csv", reference.source().uri());
    assertEquals("csv", reference.source().format().type());
    assertEquals("schema://worldcheck/reference-population/1.0", reference.source().schema().ref());
    assertEquals("schema://party/2.0", reference.mapper().target());
    assertEquals(4, reference.mapper().fields().size());
    assertEquals(1.0, reference.trustWeight());

    TransformSpec.FieldRule fullName = reference.mapper().fields().get(0);
    assertEquals("fullName", fullName.target());
    assertEquals("{LAST NAME}, {FIRST NAME}", fullName.template());

    TransformSpec.FieldRule dob = reference.mapper().fields().get(1);
    assertEquals("format_individual_dob_yyyy_mm_dd_clamp_1900", dob.transform());
    assertEquals(Map.of("value", "dateOfBirth"), dob.effectiveInputs());

    TransformSpec.FieldRule entityKind = reference.mapper().fields().get(2);
    assertEquals("classify_party_kind", entityKind.transform());
    assertEquals(
        Map.of("value", "entityType", "entity_indicator", "entityIndicator"),
        entityKind.effectiveInputs());

    TransformSpec.FieldRule locations = reference.mapper().fields().get(3);
    assertTrue(locations.isRepeated());
    assertTrue(locations.isOptional());

    assertEquals("opensearch", spec.sink().connector());
    assertEquals("party-2-0", spec.sink().index());
    assertEquals(1000, spec.sink().batching().maxRecords());
    assertEquals(Duration.ofSeconds(2), spec.sink().batching().maxWait());

    assertEquals(ExecutionMode.BOUNDED, spec.executionMode());
    assertEquals(16, spec.delivery().concurrency().preferred());
    assertEquals(32, spec.delivery().concurrency().maximum());
    assertEquals(5000, spec.delivery().flowControl().maxInFlightRecords());
    assertEquals("dead-letter", spec.errors().malformedRecord());
    assertEquals("retry", spec.errors().sinkFailure());
  }

  @Test
  void parsesEveryFieldFromRealisticCorrelatedYaml() {
    IngestionSpec spec = IngestionSpec.parseYaml(CORRELATED_YAML);

    assertEquals(SourceCardinality.CORRELATED, spec.sourceCardinality());
    assertEquals(2, spec.sources().size());

    SourcePlan core = spec.sources().get(0);
    assertEquals("core", core.sourceKey());
    assertEquals("file", core.source().connector());
    assertEquals("/data/worldcheck/core.csv", core.source().uri());
    assertEquals(2, core.mapper().fields().size());
    assertEquals(1.0, core.trustWeight());

    SourcePlan enrichment = spec.sources().get(1);
    assertEquals("enrichment", enrichment.sourceKey());
    assertEquals(0.4, enrichment.trustWeight());
    assertEquals("nationality_code", enrichment.mapper().fields().get(1).target());

    assertEquals("uid", spec.blockingField());
    assertEquals("opensearch", spec.sink().connector());
    assertEquals("opensearch://localhost:9200", spec.sink().uri());
    assertEquals(8, spec.delivery().concurrency().preferred());
  }

  @Test
  void roundTripsThroughYamlWithoutLoss() throws IOException {
    IngestionSpec original = IngestionSpec.parseYaml(SINGLE_SOURCE_YAML);

    String serialized = original.toYaml();
    IngestionSpec reparsed = IngestionSpec.parseYaml(serialized);

    assertEquals(original, reparsed);
  }

  @Test
  void loadsFromPath(@TempDir Path tempDir) throws IOException {
    Path specFile = tempDir.resolve("ingestion-spec.yaml");
    Files.writeString(specFile, CORRELATED_YAML);

    IngestionSpec spec = IngestionSpec.load(specFile);

    assertEquals(2, spec.sources().size());
    assertEquals("uid", spec.blockingField());
  }
}
