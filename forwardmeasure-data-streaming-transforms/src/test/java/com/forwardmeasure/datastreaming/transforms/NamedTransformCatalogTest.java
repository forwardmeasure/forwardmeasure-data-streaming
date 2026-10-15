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
package com.forwardmeasure.datastreaming.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.Cardinality;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.ExecutionCost;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.StateRequirement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class NamedTransformCatalogTest {

  private static final Set<String> GENERIC_BUILT_INS =
      Set.of(
          "resolve_iso2_country",
          "normalise_iso2_country",
          "parse_yyyymmdd",
          "parse_mmddyyyy",
          "parse_identifier_pairs",
          "normalise_identifier",
          "parse_semicolon_list",
          "parse_tilde_list",
          "parse_semicolon_iso2_list",
          "parse_url_list",
          "extract_url_domains",
          "parse_partial_date_ymd",
          "join_labeled_fields");

  @Test
  void builtInsAreExactlyTheGenericDomainFreeTransforms() {
    assertEquals(GENERIC_BUILT_INS, NamedTransformCatalog.builtIns().names());
    for (String name : NamedTransformCatalog.builtIns().names()) {
      assertFalse(name.startsWith("test_"), name);
      assertSame(
          TransformCharacteristics.PURE_ROW_FUNCTION,
          NamedTransformCatalog.builtIns().characteristics(name),
          name);
    }
  }

  @Test
  void withNoProviderOnTheClasspathInstalledIsTheBuiltIns() {
    assertEquals(GENERIC_BUILT_INS, NamedTransformCatalog.installed().names());
  }

  @Test
  void aCallersTransformsAreAddedWithTheirOwnCharacteristics() {
    TransformCharacteristics heavy =
        new TransformCharacteristics(
            Cardinality.ONE, StateRequirement.STATELESS, ExecutionCost.HEAVY, false, true);
    NamedTransformCatalog catalog =
        NamedTransformCatalog.builtIns()
            .with(
                Map.of(
                    "product_upper",
                    inputs -> inputs.get("value").toUpperCase(),
                    "product_slow",
                    NamedTransform.of(heavy, inputs -> "slow")));

    assertEquals("ABC", catalog.get("product_upper").apply(Map.of("value", "abc")));
    assertSame(
        TransformCharacteristics.PURE_ROW_FUNCTION, catalog.characteristics("product_upper"));
    assertEquals(heavy, catalog.characteristics("product_slow"));
    assertEquals("slow", catalog.get("product_slow").apply(Map.of()));
    assertFalse(NamedTransformCatalog.builtIns().contains("product_upper"), "immutable");
  }

  @Test
  void aNameCanBeRegisteredOnlyOnce() {
    IllegalArgumentException builtInClash =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                NamedTransformCatalog.builtIns()
                    .with(Map.of("parse_yyyymmdd", inputs -> "replaced")));
    assertTrue(builtInClash.getMessage().contains("parse_yyyymmdd"));

    NamedTransformCatalog once =
        NamedTransformCatalog.builtIns().with(Map.of("product_x", inputs -> "x"));
    assertThrows(
        IllegalArgumentException.class, () -> once.with(Map.of("product_x", inputs -> "y")));
  }

  @Test
  void unknownNamesFailWithTheName() {
    IllegalArgumentException unknown =
        assertThrows(
            IllegalArgumentException.class,
            () -> NamedTransformCatalog.builtIns().characteristics("nope"));
    assertTrue(unknown.getMessage().contains("nope"));
  }

  @Test
  void aCatalogSurvivesJavaSerializationForSparkClosures() throws Exception {
    NamedTransformCatalog catalog =
        NamedTransformCatalog.builtIns().with(Map.of("product_upper", inputs -> "UP"));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(catalog);
    }
    NamedTransformCatalog copy;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (NamedTransformCatalog) in.readObject();
    }
    assertEquals(catalog.names(), copy.names());
    assertEquals("UP", copy.get("product_upper").apply(Map.of()));
    assertEquals("2024-02-29", copy.get("parse_yyyymmdd").apply(Map.of("value", "20240229")));
  }
}
