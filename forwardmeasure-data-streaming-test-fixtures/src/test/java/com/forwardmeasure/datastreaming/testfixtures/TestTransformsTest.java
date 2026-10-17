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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.ExecutionCost;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestTransformsTest {

  @Test
  void everyTestTransformIsPrefixedAndRegisteredThroughTheProviderNotTheBuiltIns() {
    NamedTransformCatalog installed = NamedTransformCatalog.installed();
    for (String name : TestTransforms.all().keySet()) {
      assertTrue(name.startsWith("test_"), name);
      assertTrue(installed.contains(name), name + " must be discovered through the provider");
      assertTrue(!NamedTransformCatalog.builtIns().contains(name), name + " must not be built in");
    }
  }

  @Test
  void onlyTheScreeningTransformIsHeavy() {
    NamedTransformCatalog installed = NamedTransformCatalog.installed();
    assertEquals(
        ExecutionCost.HEAVY, installed.characteristics("test_screen_against_reference").cost());
    for (String name : TestTransforms.all().keySet()) {
      if (!name.equals("test_screen_against_reference")) {
        assertSame(
            TransformCharacteristics.PURE_ROW_FUNCTION, installed.characteristics(name), name);
      }
    }
  }

  @Test
  void missingOptionalInputsNeverInventLocationsIdentifiersOrMatches() {
    NamedTransformCatalog installed = NamedTransformCatalog.installed();
    for (String name :
        List.of(
            "test_classify_party_categories",
            "test_parse_tilde_delimited_locations",
            "test_build_customer_master_locations",
            "test_build_customer_master_identifiers",
            "test_screen_against_reference")) {
      assertEquals(List.of(), installed.get(name).apply(Map.of()), name);
    }
    for (String name :
        List.of(
            "test_classify_party_category",
            "test_classify_party_kind",
            "test_classify_customer_master_party_kind")) {
      assertEquals("unknown", installed.get(name).apply(Map.of()), name);
    }
  }

  @Test
  void classifyPartyCategory() {
    assertEquals("person", TestTransforms.classifyPartyCategory("INDIVIDUAL"));
    assertEquals("person", TestTransforms.classifyPartyCategory("political individual"));
    assertEquals("organization", TestTransforms.classifyPartyCategory("BANK"));
    assertEquals("organization", TestTransforms.classifyPartyCategory("shell bank or company"));
    assertEquals("physical_asset", TestTransforms.classifyPartyCategory("VESSEL"));
    assertEquals("unknown", TestTransforms.classifyPartyCategory("SOMETHING ELSE"));
    assertEquals("unknown", TestTransforms.classifyPartyCategory(null));
  }

  @Test
  void classifyPartyKind() {
    // A person-indicating discriminator wins outright, regardless of category.
    assertEquals(
        "person",
        TestTransforms.classifyPartyKind(Map.of("value", "ORGANIZATION", "entity_indicator", "M")));
    assertEquals(
        "person",
        TestTransforms.classifyPartyKind(Map.of("value", "VESSEL", "entity_indicator", "I")));

    // "E" is authoritatively non-person; category only refines physical_asset vs organization.
    assertEquals(
        "physical_asset",
        TestTransforms.classifyPartyKind(Map.of("value", "VESSEL", "entity_indicator", "E")));
    assertEquals(
        "organization",
        TestTransforms.classifyPartyKind(Map.of("value", "BANK", "entity_indicator", "E")));

    // No (or unrecognised) discriminator falls back to category alone.
    assertEquals("person", TestTransforms.classifyPartyKind(Map.of("value", "INDIVIDUAL")));
    assertEquals(
        "organization",
        TestTransforms.classifyPartyKind(Map.of("value", "ORGANIZATION", "entity_indicator", "X")));
  }

  @Test
  void classifyPartyCategories() {
    assertEquals(
        List.of("sanctions", "pep"),
        TestTransforms.classifyPartyCategories("Sanctions Related; PEP"));
    // Aliases fold to the same canonical value; duplicates are dropped.
    assertEquals(
        List.of("sanctions"),
        TestTransforms.classifyPartyCategories("Terror Related; Explicit Sanctions"));
    assertEquals(
        List.of("adverse_media"),
        TestTransforms.classifyPartyCategories("Adverse Media - Financial Crime"));
    // An unrecognised entry is skipped, not fatal to the rest of the list.
    assertEquals(
        List.of("enforcement"),
        TestTransforms.classifyPartyCategories("Nonsense Category; Enforcement"));
    assertEquals(List.of(), TestTransforms.classifyPartyCategories(null));
    assertEquals(List.of(), TestTransforms.classifyPartyCategories(""));
  }

  @Test
  void parseTildeDelimitedLocations() {
    List<Map<String, Object>> locations =
        TestTransforms.parseTildeDelimitedLocations("~ Moscow, Moscow Oblast ~ RUSSIA; ~ ~ SYRIA");
    assertEquals(2, locations.size());

    Map<String, Object> first = locations.get(0);
    assertEquals("Moscow", first.get("city"));
    assertEquals("Moscow Oblast", first.get("state_or_province"));
    assertEquals("RUSSIA", first.get("country_name"));
    assertEquals("RU", first.get("country_code"));
    assertEquals("REGISTERED", first.get("location_type"));

    Map<String, Object> second = locations.get(1);
    assertNull(second.get("city"));
    assertEquals("SYRIA", second.get("country_name"));
    assertEquals("SY", second.get("country_code"));

    // No city and an unresolvable/UNKNOWN country is dropped entirely, not emitted as a blank
    // location - matches the origin's own real-data noise-filtering rule.
    assertEquals(List.of(), TestTransforms.parseTildeDelimitedLocations("~ ~ UNKNOWN"));
    assertEquals(List.of(), TestTransforms.parseTildeDelimitedLocations(null));
    assertEquals(List.of(), TestTransforms.parseTildeDelimitedLocations(""));
  }

  @Test
  void classifyPartyKindTestCustomerMaster() {
    assertEquals(
        "person", TestTransforms.classifyCustomerMasterPartyKind(Map.of("value", "Individual")));
    assertEquals(
        "person", TestTransforms.classifyCustomerMasterPartyKind(Map.of("value", "individual")));
    assertEquals(
        "organization",
        TestTransforms.classifyCustomerMasterPartyKind(Map.of("value", "Mutual Fund")));
    assertEquals(
        "organization",
        TestTransforms.classifyCustomerMasterPartyKind(Map.of("value", "Trust (Corp & Ind)")));
    assertEquals("unknown", TestTransforms.classifyCustomerMasterPartyKind(Map.of()));
  }

  @Test
  void buildTestCustomerMasterLocations() {
    Map<String, String> inputs = new java.util.HashMap<>();
    inputs.put("address_line_1_1", "440 West Nyack Road");
    inputs.put("city_1", "West Nyack Road");
    inputs.put("country_1", "United States (the)");
    inputs.put("address_line_1_2", "2 Church Street");
    inputs.put("address_line_2_2", "Clarendon House");
    inputs.put("city_2", "Hamilton");
    inputs.put("country_2", "Bermuda");
    // Location 3 fully blank - contributes nothing.

    List<Map<String, Object>> locations = TestTransforms.buildCustomerMasterLocations(inputs);
    assertEquals(2, locations.size());

    Map<String, Object> registered = locations.get(0);
    assertEquals("440 West Nyack Road", registered.get("address"));
    assertEquals("West Nyack Road", registered.get("city"));
    assertEquals("United States (the)", registered.get("country_name"));
    assertEquals("REGISTERED", registered.get("location_type"));

    Map<String, Object> mailing = locations.get(1);
    assertEquals("2 Church Street, Clarendon House", mailing.get("address"));
    assertEquals("Hamilton", mailing.get("city"));
    assertEquals("MAILING", mailing.get("location_type"));

    assertEquals(List.of(), TestTransforms.buildCustomerMasterLocations(Map.of()));
  }

  @Test
  void buildTestCustomerMasterIdentifiers() {
    List<Map<String, Object>> identifiers =
        TestTransforms.buildCustomerMasterIdentifiers(
            Map.of(
                "business_entity_record_id",
                "0134e358-9af0-4150-a210-90761b91143e",
                "raw_domain_unique_id",
                "202238556",
                "entity_public_identifier",
                "{LEI}549300Y4ZFSCU6XVGF08"));

    assertEquals(3, identifiers.size());
    assertEquals(
        "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", identifiers.get(0).get("scheme"));
    assertEquals("0134e358-9af0-4150-a210-90761b91143e", identifiers.get(0).get("value"));
    assertEquals("TEST_CUSTOMER_MASTER_GEMS_ID", identifiers.get(1).get("scheme"));
    assertEquals("202238556", identifiers.get(1).get("value"));
    assertEquals("LEI", identifiers.get(2).get("scheme"));
    assertEquals("549300Y4ZFSCU6XVGF08", identifiers.get(2).get("value"));

    assertEquals(List.of(), TestTransforms.buildCustomerMasterIdentifiers(Map.of()));
  }

  @Test
  void screenAgainstReferenceFindsARealMatch() {
    Map<String, String> inputs =
        Map.of(
            "full_name", "Jose Smith",
            "entity_type", "Individual",
            "country", "RUSSIA",
            "dob", "19750315");

    List<Map<String, Object>> hits = TestTransforms.screenAgainstReference(inputs);

    assertEquals(1, hits.size());
    assertEquals("wc-1", hits.get(0).get("reference_uid"));
    assertTrue((double) hits.get(0).get("composite_score") > 0.0);
    assertEquals("Jose Smith", hits.get(0).get("matched_subject_name"));
  }

  @Test
  void screenAgainstReferenceFindsNoMatchForAnUnrelatedName() {
    Map<String, String> inputs =
        Map.of(
            "full_name", "Completely Unrelated Person",
            "entity_type", "Individual",
            "country", "JAPAN");

    assertEquals(List.of(), TestTransforms.screenAgainstReference(inputs));
  }

  @Test
  void screenAgainstReferenceReturnsEmptyWithNoUsableName() {
    assertEquals(
        List.of(), TestTransforms.screenAgainstReference(Map.of("entity_type", "Individual")));
  }

  @Test
  void sparseAddressesRetainTheirPositionAndDoNotBecomeEmptyLocationRecords() {
    for (String field :
        List.of("address_line_1_", "address_line_2_", "city_", "state_or_province_", "country_")) {
      var result = TestTransforms.buildCustomerMasterLocations(Map.of(field + "2", "Example"));
      assertEquals(1, result.size());
      assertEquals("MAILING", result.getFirst().get("location_type"));
      assertNull(result.getFirst().get("country_code"));
    }
    assertEquals(
        List.of(), TestTransforms.parseTildeDelimitedLocations(" ;garbage;~ ~ ;~ ~ UNKNOWN"));
    var city = TestTransforms.parseTildeDelimitedLocations("~ Paris ~ ;~ ~ France");
    assertEquals(2, city.size());
    assertEquals("Paris", city.getFirst().get("city"));
    assertNull(city.getFirst().get("country_code"));
    assertEquals("FR", city.get(1).get("country_code"));
  }
}
