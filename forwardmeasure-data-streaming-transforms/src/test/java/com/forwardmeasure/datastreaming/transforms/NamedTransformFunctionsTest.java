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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.transforms.NamedTransformFunctions.ParsedIdentifier;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Parity tests: each assertion mirrors known, verified behavior of {@code
 * forwardmeasure-entity-intelligence}'s own {@code NamedTransformFunctions} - this is a port, not a
 * rewrite, so identical inputs must produce identical outputs (D3, corrected; see this module's own
 * package-info.java and pom.xml description).
 */
class NamedTransformFunctionsTest {

  @Test
  void resolveIso2Country() {
    assertEquals("RU", NamedTransformFunctions.resolve_iso2_country("Russia"));
    assertEquals("RU", NamedTransformFunctions.resolve_iso2_country("RUSSIAN FEDERATION"));
    assertEquals("US", NamedTransformFunctions.resolve_iso2_country("US"));
    assertNull(NamedTransformFunctions.resolve_iso2_country("Wakanda"));
    assertNull(NamedTransformFunctions.resolve_iso2_country(null));
    assertNull(NamedTransformFunctions.resolve_iso2_country("  "));
  }

  @Test
  void normaliseIso2Country() {
    assertEquals("US", NamedTransformFunctions.normalise_iso2_country("us"));
    assertEquals("RU", NamedTransformFunctions.normalise_iso2_country("Russia"));
    assertNull(NamedTransformFunctions.normalise_iso2_country(null));
    assertNull(NamedTransformFunctions.normalise_iso2_country(""));
  }

  @Test
  void parseYyyymmdd() {
    assertEquals("2023-01-15", NamedTransformFunctions.parse_yyyymmdd("20230115"));
    assertNull(NamedTransformFunctions.parse_yyyymmdd("not-a-date"));
    assertNull(NamedTransformFunctions.parse_yyyymmdd(null));
  }

  @Test
  void parseMmddyyyy() {
    assertEquals("2023-01-15", NamedTransformFunctions.parse_mmddyyyy("01/15/2023"));
    assertNull(NamedTransformFunctions.parse_mmddyyyy("15/01/2023"));
    assertNull(NamedTransformFunctions.parse_mmddyyyy(null));
  }

  @Test
  void parseIdentifierPairs() {
    List<ParsedIdentifier> parsed =
        NamedTransformFunctions.parse_identifier_pairs("{EIN}12-3456789;{SSN}123-45-6789");

    assertEquals(2, parsed.size());
    assertEquals(new ParsedIdentifier("US_EIN", "12-3456789", "123456789"), parsed.get(0));
    assertEquals(new ParsedIdentifier("US_SSN", "123-45-6789", "123456789"), parsed.get(1));
    assertTrue(NamedTransformFunctions.parse_identifier_pairs(null).isEmpty());
    assertTrue(NamedTransformFunctions.parse_identifier_pairs("").isEmpty());
  }

  @Test
  void normaliseIdentifier() {
    assertEquals("123456789", NamedTransformFunctions.normalise_identifier("EIN", "12-3456789"));
    assertEquals(
        "GB29NWBK60161331926819",
        NamedTransformFunctions.normalise_identifier("IBAN", "GB29 NWBK 6016 1331 9268 19"));
    assertEquals("abc", NamedTransformFunctions.normalise_identifier("FOO", " abc "));
    assertEquals("abc", NamedTransformFunctions.normalise_identifier(null, " abc "));
    assertNull(NamedTransformFunctions.normalise_identifier("EIN", null));
  }

  @Test
  void parseSemicolonList() {
    assertEquals(List.of("a", "b", "c"), NamedTransformFunctions.parse_semicolon_list("a; b ;;c"));
    assertEquals(List.of(), NamedTransformFunctions.parse_semicolon_list(null));
  }

  @Test
  void parseTildeList() {
    assertEquals(List.of("a", "b", "c"), NamedTransformFunctions.parse_tilde_list("a~ b ~~c"));
    assertEquals(List.of(), NamedTransformFunctions.parse_tilde_list(""));
  }

  @Test
  void parseSemicolonIso2List() {
    assertEquals(
        List.of("RU", "US"),
        NamedTransformFunctions.parse_semicolon_iso2_list("Russia;US;Wakanda;Russia"));
  }

  @Test
  void parseUrlList() {
    assertEquals(
        List.of("https://example.com/page"),
        NamedTransformFunctions.parse_url_list("Visit https://example.com/page for info"));
    assertEquals(List.of(), NamedTransformFunctions.parse_url_list("no urls here"));
  }

  @Test
  void extractUrlDomains() {
    assertEquals(
        List.of("example.com"),
        NamedTransformFunctions.extract_url_domains("Visit https://example.com/page for info"));
    assertEquals(
        List.of("example.com", "sub.example.org"),
        NamedTransformFunctions.extract_url_domains("example.com; sub.example.org"));
  }

  @Test
  void classifyPartyCategory() {
    assertEquals("person", NamedTransformFunctions.classify_party_category("INDIVIDUAL"));
    assertEquals("person", NamedTransformFunctions.classify_party_category("political individual"));
    assertEquals("organization", NamedTransformFunctions.classify_party_category("BANK"));
    assertEquals(
        "organization", NamedTransformFunctions.classify_party_category("shell bank or company"));
    assertEquals("physical_asset", NamedTransformFunctions.classify_party_category("VESSEL"));
    assertEquals("unknown", NamedTransformFunctions.classify_party_category("SOMETHING ELSE"));
    assertEquals("unknown", NamedTransformFunctions.classify_party_category(null));
  }

  @Test
  void classifyPartyKind() {
    // A person-indicating discriminator wins outright, regardless of category.
    assertEquals(
        "person",
        NamedTransformFunctions.classify_party_kind(
            Map.of("value", "ORGANIZATION", "entity_indicator", "M")));
    assertEquals(
        "person",
        NamedTransformFunctions.classify_party_kind(
            Map.of("value", "VESSEL", "entity_indicator", "I")));

    // "E" is authoritatively non-person; category only refines physical_asset vs organization.
    assertEquals(
        "physical_asset",
        NamedTransformFunctions.classify_party_kind(
            Map.of("value", "VESSEL", "entity_indicator", "E")));
    assertEquals(
        "organization",
        NamedTransformFunctions.classify_party_kind(
            Map.of("value", "BANK", "entity_indicator", "E")));

    // No (or unrecognised) discriminator falls back to category alone.
    assertEquals(
        "person", NamedTransformFunctions.classify_party_kind(Map.of("value", "INDIVIDUAL")));
    assertEquals(
        "organization",
        NamedTransformFunctions.classify_party_kind(
            Map.of("value", "ORGANIZATION", "entity_indicator", "X")));
  }

  @Test
  void parsePartialDateYmd() {
    assertEquals("1980-01-15", NamedTransformFunctions.parse_partial_date_ymd("1980/01/15"));
    // A 0 placeholder in month or day means "unknown" - clamped to 1, not rejected.
    assertEquals("1980-01-01", NamedTransformFunctions.parse_partial_date_ymd("1980/0/0"));
    assertNull(NamedTransformFunctions.parse_partial_date_ymd("not/a/date"));
    assertNull(NamedTransformFunctions.parse_partial_date_ymd("1980/01"));
    assertNull(NamedTransformFunctions.parse_partial_date_ymd(null));
    assertNull(NamedTransformFunctions.parse_partial_date_ymd(""));
  }

  @Test
  void classifyPartyCategories() {
    assertEquals(
        List.of("sanctions", "pep"),
        NamedTransformFunctions.classify_party_categories("Sanctions Related; PEP"));
    // Aliases fold to the same canonical value; duplicates are dropped.
    assertEquals(
        List.of("sanctions"),
        NamedTransformFunctions.classify_party_categories("Terror Related; Explicit Sanctions"));
    assertEquals(
        List.of("adverse_media"),
        NamedTransformFunctions.classify_party_categories("Adverse Media - Financial Crime"));
    // An unrecognised entry is skipped, not fatal to the rest of the list.
    assertEquals(
        List.of("enforcement"),
        NamedTransformFunctions.classify_party_categories("Nonsense Category; Enforcement"));
    assertEquals(List.of(), NamedTransformFunctions.classify_party_categories(null));
    assertEquals(List.of(), NamedTransformFunctions.classify_party_categories(""));
  }

  @Test
  void parseTildeDelimitedLocations() {
    List<Map<String, Object>> locations =
        NamedTransformFunctions.parse_tilde_delimited_locations(
            "~ Moscow, Moscow Oblast ~ RUSSIA; ~ ~ SYRIA");
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
    assertEquals(List.of(), NamedTransformFunctions.parse_tilde_delimited_locations("~ ~ UNKNOWN"));
    assertEquals(List.of(), NamedTransformFunctions.parse_tilde_delimited_locations(null));
    assertEquals(List.of(), NamedTransformFunctions.parse_tilde_delimited_locations(""));
  }
}
