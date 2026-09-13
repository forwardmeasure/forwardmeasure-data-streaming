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
}
