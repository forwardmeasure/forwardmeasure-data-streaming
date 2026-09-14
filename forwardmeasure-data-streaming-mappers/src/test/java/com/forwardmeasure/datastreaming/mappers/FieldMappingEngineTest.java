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

  /**
   * Real-world shape this closes a gap for (2026-09-14): {@code
   * entity-intelligence-specifications}' own real WorldCheck mapping's {@code names} field needs
   * each repeated element to carry its own {@code name_type} tag alongside the resolved value -
   * impossible to express before {@link FieldRule#metadata()} existed, since a bare accumulated
   * value has nowhere to put it.
   */
  @Test
  void repeatedRuleWithMetadataWrapsEachElementWithItsOwnTagAlongsideTheValue() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(
                new FieldRule(
                    "names",
                    null,
                    "{LAST}, {FIRST}",
                    null,
                    null,
                    false,
                    true,
                    Map.of("name_type", "PRIMARY")),
                new FieldRule(
                    "names", "alias", null, null, null, true, true, Map.of("name_type", "ALIAS"))));

    Map<String, Object> mapped =
        engine.map(rowOf(Map.of("LAST", "Doe", "FIRST", "Jane", "alias", "J. Doe")), spec);

    assertEquals(
        List.of(
            Map.of("value", "Doe, Jane", "name_type", "PRIMARY"),
            Map.of("value", "J. Doe", "name_type", "ALIAS")),
        mapped.get("names"));
  }

  /**
   * A real compounding case found building the real WorldCheck example (2026-09-14): {@code
   * ALIASES} resolves via {@code parse_semicolon_list}, itself already a {@code List<String>} from
   * one rule. Each alias must become its own tagged {@code {value, name_type}} entry - not one
   * entry whose own {@code value} is the whole list, which would bury every alias inside a single
   * opaque array instead of matching how a single-valued rule's own entries look.
   */
  @Test
  void repeatedRuleWithMetadataFlattensAListValuedTransformIntoOneTaggedEntryPerElement() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(
                new FieldRule(
                    "names",
                    "aliases",
                    null,
                    null,
                    "parse_semicolon_list",
                    true,
                    true,
                    Map.of("name_type", "ALIAS"))));

    Map<String, Object> mapped =
        engine.map(rowOf(Map.of("aliases", "Johnny Smith;J. Smith")), spec);

    assertEquals(
        List.of(
            Map.of("value", "Johnny Smith", "name_type", "ALIAS"),
            Map.of("value", "J. Smith", "name_type", "ALIAS")),
        mapped.get("names"));
  }

  /**
   * {@code metadata} absent must leave the original bare-value behavior byte-for-byte unchanged.
   */
  @Test
  void repeatedRuleWithoutMetadataStaysABareValueList() {
    TransformSpec spec =
        new TransformSpec(
            "schema://party/2.0",
            List.of(new FieldRule("aliases", "alias1", null, null, null, null, true, Map.of())));

    Map<String, Object> mapped = engine.map(rowOf(Map.of("alias1", "Jane Doe")), spec);

    assertEquals(List.of("Jane Doe"), mapped.get("aliases"));
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
