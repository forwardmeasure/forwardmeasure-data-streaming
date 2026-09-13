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
package com.forwardmeasure.datastreaming.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldMappingEngineTest {

  private final FieldMappingEngine engine = new FieldMappingEngine();

  private static SourceRow rowOf(Map<String, String> columns) {
    return columns::get;
  }

  @Test
  void resolvesSingleInputSourceShorthand() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(new FieldRule("dateOfBirth", "dob", null, null, "parse_yyyymmdd", null, null)));

    Map<String, Object> mapped = engine.map(rowOf(Map.of("dob", "20230115")), spec);

    assertEquals("2023-01-15", mapped.get("dateOfBirth"));
  }

  @Test
  void resolvesMultiColumnTemplateBeforeAnyTransform() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(new FieldRule("fullName", null, "{LAST}, {FIRST}", null, null, null, null)));

    Map<String, Object> mapped = engine.map(rowOf(Map.of("LAST", "Doe", "FIRST", "Jane")), spec);

    assertEquals("Doe, Jane", mapped.get("fullName"));
  }

  @Test
  void resolvesMultiInputTransform() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(
                new FieldRule(
                    "identifierNorm",
                    null,
                    null,
                    Map.of("scheme", "scheme", "value", "rawId"),
                    "normalise_identifier",
                    null,
                    null)));

    Map<String, Object> mapped =
        engine.map(rowOf(Map.of("scheme", "EIN", "rawId", "12-3456789")), spec);

    assertEquals("123456789", mapped.get("identifierNorm"));
  }

  @Test
  void repeatedRulesAccumulateIntoOneListPerTarget() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(
                new FieldRule("aliases", "alias1", null, null, null, null, true),
                new FieldRule("aliases", "alias2", null, null, null, null, true)));

    Map<String, Object> mapped =
        engine.map(rowOf(Map.of("alias1", "Jane Doe", "alias2", "J. Doe")), spec);

    assertEquals(List.of("Jane Doe", "J. Doe"), mapped.get("aliases"));
  }

  @Test
  void missingOptionalFieldIsSkippedSilently() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(new FieldRule("middleName", "middle", null, null, null, true, null)));

    Map<String, Object> mapped = engine.map(rowOf(Map.of()), spec);

    assertFalse(mapped.containsKey("middleName"));
  }

  @Test
  void supplementalTransformIsCheckedBeforeTheSharedRegistry() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(
                new FieldRule(
                    "entityKind", "category", null, null, "domain_specific", null, null)));
    NamedTransform domainSpecific = inputs -> "person";

    Map<String, Object> mapped =
        engine.map(
            rowOf(Map.of("category", "INDIVIDUAL")),
            spec,
            Map.of("domain_specific", domainSpecific));

    assertEquals("person", mapped.get("entityKind"));
  }

  @Test
  void missingRequiredFieldIsSkippedNotThrown() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(new FieldRule("dateOfBirth", "dob", null, null, null, null, null)));

    Map<String, Object> mapped = engine.map(rowOf(Map.of()), spec);

    assertTrue(mapped.isEmpty());
  }
}
