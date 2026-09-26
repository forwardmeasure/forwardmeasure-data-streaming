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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Real field-by-field correctness proof, at the {@link FieldMappingEngine} layer directly (no
 * container, no network) - does {@code test-customer-master-to-opensearch.yaml}'s own real field
 * rules, run against {@link TestCustomerMasterFixtures#SAMPLE_CSV}'s own real rows, actually
 * resolve to the right values, not just "does it parse" ({@link TestCustomerMasterFixturesTest}'s
 * own narrower job) or "does it compile." This is the fast, deterministic proof that the mapping is
 * correct before investing in a real container-based end-to-end matrix cell.
 */
class TestCustomerMasterMappingTest {

  private static final FieldMappingEngine ENGINE = new FieldMappingEngine();

  @Test
  void individualRowWithSsnDobLocationMapsCorrectly() {
    Map<String, Object> mapped = mapRow(1);

    assertEquals("KYC85310AML", mapped.get("uid"));
    assertEquals("KYC85310AML", mapped.get("source_record_id"));
    assertEquals("person", mapped.get("entity_kind"));
    assertEquals("1953-12-31", mapped.get("date_of_birth"));
    assertEquals("Senior Councel", mapped.get("position"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> names = (List<Map<String, Object>>) mapped.get("names");
    assertEquals(1, names.size());
    assertEquals("Steven DOSHAY", names.get(0).get("value"));
    assertEquals("PRIMARY", names.get(0).get("name_type"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> identifiers = (List<Map<String, Object>>) mapped.get("identifiers");
    assertEquals(2, identifiers.size());
    assertEquals(
        "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", identifiers.get(0).get("scheme"));
    assertEquals("f1321672-c82e-4595-93be-be072caf2481", identifiers.get(0).get("value"));
    assertEquals("US_SSN", identifiers.get(1).get("scheme"));
    assertEquals("XXX-XX-9875", identifiers.get(1).get("value"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> locations = (List<Map<String, Object>>) mapped.get("locations");
    assertEquals(1, locations.size(), "only location 1 is populated on this real row");
    assertEquals("3521 Malaga Ct", locations.get(0).get("address"));
    assertEquals("CA", locations.get(0).get("city"));
    assertEquals("US", locations.get(0).get("country_code"));
    assertEquals("REGISTERED", locations.get(0).get("location_type"));
  }

  @Test
  void individualRowWithMostFieldsBlankStillMapsTheRequiredCore() {
    Map<String, Object> mapped = mapRow(2);

    assertEquals("KYC1955299c-71f6-40ad-9a2c-9e949fb4f8cdAML", mapped.get("uid"));
    assertEquals("person", mapped.get("entity_kind"));
    assertEquals("Director", mapped.get("position"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> names = (List<Map<String, Object>>) mapped.get("names");
    assertEquals(1, names.size());
    assertEquals("Chui-Lung Chang", names.get(0).get("value"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> identifiers = (List<Map<String, Object>>) mapped.get("identifiers");
    assertEquals(1, identifiers.size(), "only BUSINESS_ENTITY_RECORD_ID is populated on this row");
    assertEquals(
        "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", identifiers.get(0).get("scheme"));

    assertNull(mapped.get("locations"), "no location source is populated on this real row");
  }

  @Test
  void organisationRowWithAllThreeLocationsMapsCorrectly() {
    Map<String, Object> mapped = mapRow(3);

    assertEquals("KYC22438AML", mapped.get("uid"));
    assertEquals("organization", mapped.get("entity_kind"));
    assertEquals("2008-02-15", mapped.get("date_of_birth"), "incorporation date");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> identifiers = (List<Map<String, Object>>) mapped.get("identifiers");
    assertEquals(3, identifiers.size());
    assertEquals(
        "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", identifiers.get(0).get("scheme"));
    assertEquals("0134e358-9af0-4150-a210-90761b91143e", identifiers.get(0).get("value"));
    assertEquals("TEST_CUSTOMER_MASTER_KYC_ID", identifiers.get(1).get("scheme"));
    assertEquals("202238556", identifiers.get(1).get("value"));
    assertEquals("LEI", identifiers.get(2).get("scheme"));
    assertEquals("549300Y4ZFSCU6XVGF08", identifiers.get(2).get("value"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> locations = (List<Map<String, Object>>) mapped.get("locations");
    assertEquals(3, locations.size(), "all 3 real locations are populated on this real row");
    assertEquals("REGISTERED", locations.get(0).get("location_type"));
    assertEquals("Dublin 2", locations.get(0).get("city"));
    assertEquals("MAILING", locations.get(1).get("location_type"));
    assertEquals("Dublin 4", locations.get(1).get("city"));
    assertEquals("OTHER", locations.get(2).get("location_type"));
    assertEquals("Dublin", locations.get(2).get("city"));
    assertEquals(
        "International Financial Services Centre, Dublin 1", locations.get(2).get("address"));

    String furtherInformation = (String) mapped.get("further_information");
    assertTrue(
        furtherInformation.contains("Classification: Customer (Contracting Party)"),
        "unexpected further_information: " + furtherInformation);
    assertTrue(
        furtherInformation.contains("Primary BU: IS EMEA:Ireland"),
        "unexpected further_information: " + furtherInformation);
    assertTrue(
        !furtherInformation.contains("Occupation:"),
        "OCCUPATION is blank on this real row and must not appear");

    @SuppressWarnings("unchecked")
    List<String> countries = (List<String>) mapped.get("countries");
    // Each of the 3 location-country rules accumulates independently - no dedup, matching
    // FieldMappingEngine's own real repeated-field accumulation semantics.
    assertEquals(List.of("IE", "IE", "IE"), countries);
  }

  @Test
  void trustRowWithAliasNameMapsCorrectly() {
    Map<String, Object> mapped = mapRow(4);

    assertEquals("organization", mapped.get("entity_kind"), "Trust (Corp & Ind) is not a person");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> names = (List<Map<String, Object>>) mapped.get("names");
    assertEquals(2, names.size());
    assertEquals("PRIMARY", names.get(0).get("name_type"));
    assertEquals("DOROTHY C. BECKMANN CHARITABLE REMAINDER UNITRUST", names.get(0).get("value"));
    assertEquals("ALIAS", names.get(1).get("name_type"));
    assertEquals("BECKMANN DOROTHY 7% PL I(TSA-E)", names.get(1).get("value"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> identifiers = (List<Map<String, Object>>) mapped.get("identifiers");
    assertEquals(3, identifiers.size());
    assertEquals(
        "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", identifiers.get(0).get("scheme"));
    assertEquals("TEST_CUSTOMER_MASTER_KYC_ID", identifiers.get(1).get("scheme"));
    assertEquals("333614640", identifiers.get(1).get("value"));
    assertEquals("TIN", identifiers.get(2).get("scheme"));
    assertEquals("13-7046993", identifiers.get(2).get("value"));
  }

  /**
   * Row 1-4, 1-indexed to match {@link TestCustomerMasterFixtures#SAMPLE_CSV}'s own header + 4
   * rows.
   */
  private static Map<String, Object> mapRow(int rowNumber) {
    String[] lines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
    String[] header = lines[0].split("\\|", -1);
    String[] values = lines[rowNumber].split("\\|", -1);

    Map<String, String> row = new LinkedHashMap<>();
    for (int i = 0; i < header.length && i < values.length; i++) {
      row.put(header[i], values[i]);
    }
    SourceRow sourceRow = row::get;

    IngestionSpec spec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp/test-customer-master.csv",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    return ENGINE.map(sourceRow, spec.sources().get(0).mapper());
  }
}
