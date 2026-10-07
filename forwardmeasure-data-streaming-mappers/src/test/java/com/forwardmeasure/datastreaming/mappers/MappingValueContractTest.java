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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MappingValueContractTest {
  @Test
  void rawVendorValuesKeepFalseZeroAndObjectsButOmitEmptyCollections() {
    var source =
        Map.<String, Object>of(
            "zero",
            0,
            "false",
            false,
            "object",
            Map.of("vendor", "x"),
            "empty",
            List.of(),
            "blank",
            " ");
    var fields =
        source.keySet().stream()
            .map(key -> new FieldRule(key, key, null, null, null, false, false, Map.of(), true))
            .toList();
    var result =
        new FieldMappingEngine()
            .map(new MapSourceRow(source), new TransformSpec("canonical", fields));
    assertEquals(Map.of("zero", 0, "false", false, "object", Map.of("vendor", "x")), result);
    SourceRow legacy = Map.of("id", "123")::get;
    assertEquals("123", legacy.getRaw("id"));
    assertThrows(
        UnsupportedOperationException.class,
        legacy::rawFields,
        "Source validation must not silently accept incomplete adapters");
  }

  @Test
  void missingTemplateInputsDoNotInventPartialNamesOrCrashTransforms() {
    var fields =
        List.of(
            new FieldRule("name", null, "{first} {last}", null, null, true, false),
            new FieldRule(
                "normalized", null, "{first} {last}", null, "parse_semicolon_list", true, false),
            new FieldRule("blank", null, "  ", null, null, true, false));
    var engine = new FieldMappingEngine();
    var spec = new TransformSpec("canonical", fields);
    assertEquals(Map.of(), engine.map(Map.of("first", "Jane")::get, spec));
    assertEquals(Map.of(), engine.map(Map.of("first", "Jane", "last", " ")::get, spec));
    assertEquals(
        Map.of("name", "Jane Doe", "normalized", List.of("Jane Doe")),
        engine.map(Map.of("first", "Jane", "last", "Doe")::get, spec));
    assertTrue(
        engine.map(Map.of("first", "Jane")::get, new TransformSpec("canonical", null)).isEmpty());
  }

  @Test
  void templateValuesContainingPlaceholderSyntaxAreNotSubstitutedAgain() {
    var spec =
        new TransformSpec(
            "canonical",
            List.of(new FieldRule("name", null, "{first} {last}", null, null, false, false)));
    assertEquals(
        Map.of("name", "{last} Doe"),
        new FieldMappingEngine().map(Map.of("first", "{last}", "last", "Doe")::get, spec));
  }
}
