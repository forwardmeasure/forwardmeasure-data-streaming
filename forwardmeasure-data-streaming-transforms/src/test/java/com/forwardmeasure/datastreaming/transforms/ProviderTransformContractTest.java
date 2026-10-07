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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProviderTransformContractTest {
  @Test
  void missingOptionalInputsNeverInventIdentifiersDatesLocationsOrMatches() {
    for (String name :
        List.of(
            "resolve_iso2_country",
            "normalise_iso2_country",
            "parse_yyyymmdd",
            "parse_mmddyyyy",
            "normalise_identifier",
            "parse_partial_date_ymd",
            "join_labeled_fields")) {
      assertNull(NamedTransformRegistry.get(name).apply(Map.of()), name);
    }
    for (String name :
        List.of(
            "parse_identifier_pairs",
            "parse_semicolon_list",
            "parse_tilde_list",
            "parse_semicolon_iso2_list",
            "parse_url_list",
            "extract_url_domains",
            "classify_party_categories",
            "parse_tilde_delimited_locations",
            "build_test_customer_master_locations",
            "build_test_customer_master_identifiers",
            "screen_against_worldcheck_reference")) {
      assertEquals(List.of(), NamedTransformRegistry.get(name).apply(Map.of()), name);
    }
    for (String name :
        List.of(
            "classify_party_category",
            "classify_party_kind",
            "classify_party_kind_test_customer_master")) {
      assertEquals("unknown", NamedTransformRegistry.get(name).apply(Map.of()), name);
    }
    assertTrue(NamedTransformRegistry.names().contains("normalise_identifier"));
    assertTrue(NamedTransformRegistry.contains("parse_yyyymmdd"));
    assertThrows(
        IllegalArgumentException.class, () -> NamedTransformRegistry.get("not-a-transform"));
  }

  @Test
  void impossibleCalendarDatesAreNotSilentlyRewrittenIntoDifferentBirthdays() {
    for (String value : List.of("20250229", "20260231", "20260431"))
      assertNull(NamedTransformFunctions.parse_yyyymmdd(value), value);
    for (String value : List.of("02/29/2025", "02/31/2026", "04/31/2026"))
      assertNull(NamedTransformFunctions.parse_mmddyyyy(value), value);
    assertEquals(
        "2024-02-29",
        NamedTransformRegistry.get("parse_yyyymmdd").apply(Map.of("value", "20240229")));
    assertEquals(
        "2024-02-29",
        NamedTransformRegistry.get("parse_mmddyyyy").apply(Map.of("value", "02/29/2024")));
    assertNull(NamedTransformFunctions.parse_yyyymmdd(" "));
    assertNull(NamedTransformFunctions.parse_mmddyyyy(" "));
  }

  @Test
  void identifierSchemesPreserveProviderMeaningAndApplySchemeSpecificNormalization() {
    var schemes =
        Map.of(
            "EIN", "US_EIN", "SSN", "US_SSN", "ITIN", "US_ITIN", "CRN", "UK_CRN", "NINO", "UK_NINO",
            "UTR", "UK_UTR", "VAT", "UK_VAT", "SIN", "CA_SIN", "BN", "CA_BN");
    schemes.forEach(
        (raw, expected) ->
            assertEquals(
                expected,
                NamedTransformFunctions.parse_identifier_pairs("{" + raw + "}123")
                    .getFirst()
                    .scheme()));
    assertTrue(NamedTransformFunctions.parse_identifier_pairs("{ }123;{LEI}   ").isEmpty());
    for (String scheme : List.of("EIN", "US_EIN", "SSN", "US_SSN", "ITIN", "US_ITIN", "TIN"))
      assertEquals("1234", NamedTransformFunctions.normalise_identifier(scheme, "12-34"));
    for (String scheme :
        List.of(
            "CRN",
            "UK_CRN",
            "CA_BN",
            "IN_CIN",
            "IN_DIN",
            "NL_KVK",
            "SE_ORGNR",
            "CH_UID",
            "DE_HRB",
            "FR_SIREN",
            "FR_SIRET",
            "COMPANY_REGISTRATION",
            "LEI",
            "ISIN",
            "IN_PAN",
            "SWIFT_BIC"))
      assertEquals("AB-123", NamedTransformFunctions.normalise_identifier(scheme, " ab-123 "));
    for (String scheme :
        List.of(
            "EU_VAT",
            "UK_VAT",
            "DE_VAT",
            "FR_VAT",
            "IT_VAT",
            "ES_VAT",
            "NL_VAT",
            "IE_VAT",
            "SE_VAT",
            "CH_VAT",
            "CA_GST_HST",
            "CA_QST"))
      assertEquals("AB123", NamedTransformFunctions.normalise_identifier(scheme, "ab- 123"));
    assertEquals("AB123", NamedTransformFunctions.normalise_identifier("IBAN", "ab 123"));
    assertEquals("AB123", NamedTransformFunctions.normalise_identifier("UK_NINO", "ab 123"));
    assertEquals("ab123", NamedTransformFunctions.normalise_identifier("IN_AADHAAR", "ab- 123"));
    assertEquals("ab123", NamedTransformFunctions.normalise_identifier("IN_GSTIN", "ab- 123"));
    assertEquals("ab 123", NamedTransformFunctions.normalise_identifier(null, " ab 123 "));
  }

  @Test
  void noisyProviderLinksAndCountryListsAreDeduplicatedWithoutInventingHosts() {
    assertEquals(
        List.of("example.com"),
        NamedTransformFunctions.extract_url_domains(
            "https://user@example.com:443/a?q=1#b https://example.com/again"));
    assertEquals(
        List.of("example.com", "other.org"),
        NamedTransformFunctions.extract_url_domains(
            "www.www.Example.com?x=1;other.org#part;localhost;.;example.com:80"));
    for (String suffix : List.of(".", ",", ";", ")", "]", "}"))
      assertEquals(
          List.of("https://example.com"),
          NamedTransformFunctions.parse_url_list("https://example.com" + suffix));
    assertTrue(NamedTransformFunctions.extract_url_domains("https://localhost").isEmpty());
    assertTrue(NamedTransformFunctions.extract_url_domains(" ").isEmpty());
    assertTrue(NamedTransformFunctions.parse_url_list(" ").isEmpty());
    assertEquals(
        List.of("https://example.com"),
        NamedTransformFunctions.parse_url_list("https://example.com https://example.com"));
    assertEquals(
        List.of("US"),
        NamedTransformFunctions.parse_semicolon_iso2_list(";United States;US;Atlantis; "));
    assertTrue(NamedTransformFunctions.parse_semicolon_iso2_list(" ").isEmpty());
    assertTrue(NamedTransformFunctions.parse_semicolon_list(" ").isEmpty());
    assertTrue(NamedTransformFunctions.parse_tilde_list(" ").isEmpty());
    assertNull(NamedTransformFunctions.resolve_iso2_country("1!"));
  }

  @Test
  void sparseAddressesRetainTheirPositionAndDoNotBecomeEmptyLocationRecords() {
    for (String field :
        List.of("address_line_1_", "address_line_2_", "city_", "state_or_province_", "country_")) {
      var result =
          NamedTransformFunctions.build_test_customer_master_locations(
              Map.of(field + "2", "Example"));
      assertEquals(1, result.size());
      assertEquals("MAILING", result.getFirst().get("location_type"));
      assertNull(result.getFirst().get("country_code"));
    }
    assertEquals(
        List.of(),
        NamedTransformFunctions.parse_tilde_delimited_locations(" ;garbage;~ ~ ;~ ~ UNKNOWN"));
    var city = NamedTransformFunctions.parse_tilde_delimited_locations("~ Paris ~ ;~ ~ France");
    assertEquals(2, city.size());
    assertEquals("Paris", city.getFirst().get("city"));
    assertNull(city.getFirst().get("country_code"));
    assertEquals("FR", city.get(1).get("country_code"));
  }
}
