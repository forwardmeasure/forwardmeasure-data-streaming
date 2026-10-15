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
package com.forwardmeasure.datastreaming.testfixtures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Real, cheap verification that both Customer Master spec variants actually parse into a valid
 * {@link IngestionSpec} - mirrors {@link WorldCheckFixturesTest} exactly. Field-by-field mapping
 * correctness (does the real data actually resolve to the right values) is {@link
 * TestCustomerMasterMappingTest}'s own job, not this class's.
 */
class TestCustomerMasterFixturesTest {

  @Test
  void boundedFileSpecParsesWithTheRealFieldShape() {
    IngestionSpec spec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp/test-customer-master.csv",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    assertEquals(1, spec.sources().size());
    assertEquals("file", spec.sources().get(0).source().connector());
    assertEquals(ExecutionMode.BOUNDED, spec.executionMode());
    assertEquals("test-customer-master-screening-records", spec.sink().index());
    assertTrue(
        spec.sources().get(0).mapper().fields().stream()
            .anyMatch(f -> "entity_kind".equals(f.target())),
        "expected the real entity_kind field rule to survive parsing");
  }

  @Test
  void boundedKafkaSpecParsesWithTheIdenticalFieldShape() {
    IngestionSpec fileSpec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp/test-customer-master.csv",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    IngestionSpec kafkaSpec =
        TestCustomerMasterFixtures.boundedKafkaSpec(
            "kafka:test-customer-master-rows?brokers=kafka:9092",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    assertEquals("kafka", kafkaSpec.sources().get(0).source().connector());
    assertEquals(ExecutionMode.BOUNDED, kafkaSpec.executionMode());
    assertEquals(
        fileSpec.sources().get(0).mapper().fields().size(),
        kafkaSpec.sources().get(0).mapper().fields().size(),
        "the kafka-sourced variant must map the identical fields as the file-sourced one");
  }

  @Test
  void sampleCsvHasTheRealFiveRowShape() {
    String[] lines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
    assertEquals(5, lines.length, "header + 4 real rows");
    assertTrue(lines[0].startsWith("ENTITYS_UNIQUE_ID|BUSINESS_ENTITY_RECORD_ID|KYC_ID"));
  }

  @Test
  void indexSettingsJsonIsRealAndNonEmpty() {
    String json = TestCustomerMasterFixtures.indexSettingsJson();
    assertTrue(json.contains("phonetic"), "expected the real phonetic analyzer settings");
  }

  @Test
  void sampleCsvWithScreeningMatchAddsOneRowWithTheKnownMatchingShape() {
    String[] baseLines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
    String[] extendedLines = TestCustomerMasterFixtures.sampleCsvWithScreeningMatch().split("\n");
    assertEquals(baseLines.length + 1, extendedLines.length, "base rows plus one matching row");
    assertTrue(extendedLines[extendedLines.length - 1].startsWith("KYC-SCREEN-1|"));
    assertTrue(extendedLines[extendedLines.length - 1].contains("Jose Smith"));
  }

  @Test
  void screenedIndexSettingsJsonDeclaresTheScreeningHitsMapping() {
    String json = TestCustomerMasterFixtures.screenedIndexSettingsJson();
    assertTrue(json.contains("screening_hits"), "expected a real screening_hits nested mapping");
    assertTrue(json.contains("phonetic"), "expected the same real phonetic analyzer settings too");
  }
}
