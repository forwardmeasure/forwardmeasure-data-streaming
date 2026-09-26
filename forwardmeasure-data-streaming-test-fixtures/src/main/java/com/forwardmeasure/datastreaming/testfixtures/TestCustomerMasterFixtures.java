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

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

/**
 * Phase H's own sibling of {@link WorldCheckFixtures} - a real customer-master shape (pipe-
 * delimited, UTF-8, 45-column layout - 42 real columns + 3 trailing positional-duplicate columns
 * the source system itself appends, never referenced by any mapping), ported field-by-field from
 * {@code entity-intelligence-specifications}' own real customer-master mapping/schema documents
 * (client name withheld here by design - see this module's own commit history for provenance), not
 * an invented shape. {@link #SAMPLE_CSV}'s 4 rows are real, unmodified rows sliced directly out of
 * a real customer-master POV dataset (~175k + ~45k rows total, path likewise withheld) - two {@code
 * Individual} rows (one with SSN/DOB/location populated, one with almost every optional field
 * blank), one well-populated organisation row (all 3 structured locations, GEMS_ID, a single LEI
 * identifier pair, incorporation date, every business-relationship field populated), and one {@code
 * Trust (Corp & Ind)} row with a populated {@code TRADE_AS_NAME} alias - chosen to exercise every
 * field rule at least once, the same coverage reasoning {@link WorldCheckFixtures#SAMPLE_TSV}
 * already documents for its own 4 rows.
 *
 * <p>Two real {@code IngestionSpec} variants, both {@code BOUNDED}, mirroring {@link
 * WorldCheckFixtures} exactly: a {@code file}-sourced one (the default - every Customer Master
 * transform is {@code STATELESS}/{@code LIGHT}, so per {@code ExecutionPlanCompiler}'s own real
 * engine-selection table a file-sourced bounded spec always resolves to {@code PEKKO_STREAMS}) and
 * a {@code kafka}-sourced one (the only legitimate way to reach the {@code KAFKA_STREAMS} cells for
 * this data).
 */
public final class TestCustomerMasterFixtures {

  private TestCustomerMasterFixtures() {}

  private static final String HEADER =
      "ENTITYS_UNIQUE_ID|BUSINESS_ENTITY_RECORD_ID|GEMS_ID|ENTITYS_FULL_NAME|ENTITY_PREFIX|"
          + "ENTITY_FIRST_NAME|ENTITY_MIDDLE_NAME|ENTITY_LAST_NAME|ENTITY_SUFFIX|TRADE_AS_NAME|"
          + "FORMER_NAME|ENTITY_LOCATION_1_ADDRESS_LINE_1|ENTITY_LOCATION_1_ADDRESS_LINE_2|"
          + "ENTITY_LOCATION_1_CITY|ENTITY_LOCATION_1_STATE_PROVINCE|ENTITY_LOCATION_1_COUNTRY|"
          + "ENTITY_LOCATION_2_ADDRESS_LINE_1|ENTITY_LOCATION_2_ADDRESS_LINE_2|"
          + "ENTITY_LOCATION_2_CITY|ENTITY_LOCATION_2_STATE_PROVINCE|ENTITY_LOCATION_2_COUNTRY|"
          + "ENTITY_LOCATION_3_ADDRESS_LINE_1|ENTITY_LOCATION_3_ADDRESS_LINE_2|"
          + "ENTITY_LOCATION_3_CITY|ENTITY_LOCATION_3_STATE_PROVINCE|ENTITY_LOCATION_3_COUNTRY|"
          + "COUNTRY_OF_CITIZENSHIP_INCORPORATION|COUNTRY_OF_NATIONALITY|"
          + "DATE_OF_BIRTH_DATE_OF_INCORPORATION|OCCUPATION|JOB_TITLE|LAST_UPDATED_DATE|"
          + "ENTITY_TYPE_CODE|ENTITYS_TYPE|ENTITY_PUBLIC_IDENTIFIER|PRIMARY_BUSINESS_UNIT|"
          + "CONTRACTING_ENTITIES_AND_LOCATIONS|SERVICING_ENTITIES_AND_LOCATIONS|"
          + "BOOKING_ENTITIES_AND_LOCATIONS|ADDITIONAL_BUSINESS_UNITS_AND_LOCATIONS|"
          + "ENTITY_CLASSIFICATION|PLACE_OF_BIRTH|ENTITYS_FULL_NAME|ENTITY_LOCATION_1_COUNTRY|"
          + "ENTITY_LOCATION_1_CITY|ENTITY_LOCATION_1_STATE_PROVINCE";

  /** Individual, ss-1: SSN, DOB, location1 (address/city/state/country), job title populated. */
  private static final String ROW_INDIVIDUAL_WELL_POPULATED =
      "KYC85310AML|f1321672-c82e-4595-93be-be072caf2481||Steven DOSHAY||Steven||DOSHAY||||3521"
          + " Malaga Ct||CA|California|United States (the)|||||||||||||19531231||Senior"
          + " Councel|2026-03-27|I                                                                 "
          + "                                  |Individual|{SSN}XXX-XX-9875||Customer Master Bank"
          + " and Trust Company:United States (the);||||Associated Third Party||Steven"
          + " DOSHAY|United States (the)|CA|California";

  /** Individual, ss-2: almost every optional field blank - tests optional-field robustness. */
  private static final String ROW_INDIVIDUAL_MINIMAL =
      "KYC1955299c-71f6-40ad-9a2c-9e949fb4f8cdAML|1955299c-71f6-40ad-9a2c-9e949fb4f8cd||"
          + "Chui-Lung Chang||Chui-Lung||Chang|||||||||||||||||||||18000101||Director|2025-12-18|"
          + "I                                                                                    "
          + "               |"
          + "Individual||Sales & Trading:Taiwan (Province of China)|"
          + "Customer Master Bank and Trust Company - Taipei Branch:Taiwan (Province of China); "
          + "Customer Master Global Advisors Asia Limited:Hong Kong; "
          + "Customer Master Global Markets International Limited:United Kingdom (the); "
          + "Customer Master Global Markets, LLC:United States (the);||||Associated Third Party||"
          + "Chui-Lung Chang|||";

  /** Organisation (Mutual Fund), ss-3: all 3 locations, GEMS_ID, a single LEI identifier pair. */
  private static final String ROW_ORGANIZATION_ALL_LOCATIONS =
      "KYC22438AML|0134e358-9af0-4150-a210-90761b91143e|202238556|iShares V Public Limited Company"
          + " - iShares J.P. Morgan $ EM Corp Bond UCITS ETF||||||||J.P. Morgan, 200 Capital"
          + " Dock|79 Sir John Rogersons Quay|Dublin 2|Dublin|Ireland|BlackRock Asset Management"
          + " Ireland Limited|2 Ballsbridge Park, Ballsbridge, 1st Floor|Dublin"
          + " 4|Dublin|Ireland|International Financial Services Centre|Dublin"
          + " 1|Dublin||Ireland|Ireland||20080215|||2024-08-13|C                                   "
          + "                                                                |Mutual"
          + " Fund|{LEI}549300Y4ZFSCU6XVGF08|IS EMEA:Ireland|Customer Master Custodial Services"
          + " (Ireland) Limited:Ireland; Customer Master Fund Services (Ireland) Limited:Ireland;FX"
          + " Connect, LLC:United States (the); Customer Master Bank International GmbH (Frankfurt"
          + " am Main, Hessen, DE, Branch):Germany; Customer Master Bank International GmbH"
          + " (London, GB, Branch):United Kingdom (the); Customer Master Bank International GmbH,"
          + " Munich, Zurich Branch:Switzerland; Customer Master Bank and Trust Company (Central"
          + " and Western District, Hong Kong Island, HK, Branch):Hong Kong; Customer Master Bank"
          + " and Trust Company (Jung-gu, Seoul, KR, Branch):Korea (the Republic of); Customer"
          + " Master Bank and Trust Company (London, GB, Branch):United Kingdom (the); Customer"
          + " Master Bank and Trust Company (Singapore, SG, Branch):Singapore; Customer Master Bank"
          + " and Trust Company - Seoul Branch:Korea (the Republic of); Customer Master Bank and"
          + " Trust Company:United States (the); Customer Master Custodial Services (Ireland)"
          + " Limited:Ireland; Customer Master Europe Limited:United Kingdom (the); Customer Master"
          + " Fund Services (Ireland) Limited:Ireland; Customer Master Global Advisors Europe"
          + " Limited:Ireland; Customer Master Global Advisors Limited:United Kingdom (the);"
          + " Customer Master Global Markets International Limited:United Kingdom (the); Customer"
          + " Master GlobalLink Asia Pacific Limited:Hong Kong|Customer Master Custodial Services"
          + " (Ireland) Limited:Ireland; Customer Master Fund Services (Ireland)"
          + " Limited:Ireland||IS EMEA:Ireland; Portfolio Solutions:United Kingdom (the); Asset"
          + " Managers:United States (the); Alternatives:Ireland; Alternatives:United States (the);"
          + " Asset Managers:United States (the); GlobalLink:Hong Kong; GlobalLink:United Kingdom"
          + " (the); GlobalLink:United States (the); IS EMEA:Ireland; IS EMEA:Switzerland;"
          + " Portfolio Solutions:United Kingdom (the); SSGA:Ireland; SSGA:United Kingdom (the);"
          + " Sales & Trading:Hong Kong; Sales & Trading:Korea (the Republic of); Sales &"
          + " Trading:Singapore; Sales & Trading:United Kingdom (the); IS EMEA:Ireland|Customer"
          + " (Contracting Party)||iShares V Public Limited Company - iShares J.P. Morgan $ EM Corp"
          + " Bond UCITS ETF|Ireland|Dublin 2|Dublin";

  /** Trust, ss-4: TRADE_AS_NAME alias populated, a TIN identifier, partial location1. */
  private static final String ROW_TRUST_WITH_ALIAS =
      "KYC232217AML|40b65e7d-e3f9-4075-9d4c-1da1e4c3647c|333614640|"
          + "DOROTHY C. BECKMANN CHARITABLE REMAINDER UNITRUST||||||"
          + "BECKMANN DOROTHY 7% PL I(TSA-E)||440 West Nyack Road||West Nyack Road|New York|"
          + "United States (the)|||||||||||United States (the)||19940812|||2025-11-10|"
          + "I                                                                                    "
          + "               |"
          + "Trust (Corp & Ind)|{TIN}13-7046993|SSGA:United States (the)|"
          + "Customer Master Global Advisors Trust Company:United States (the);||||"
          + "Customer (Contracting Party)||"
          + "DOROTHY C. BECKMANN CHARITABLE REMAINDER UNITRUST|United States (the)|"
          + "West Nyack Road|New York";

  /**
   * Real Customer Master customer-master shape: header + 4 real, unmodified rows sliced from the
   * real POV files (see this class's own javadoc for which rows and why). ss-1/ss-2: individuals.
   * ss-3: organisation (Mutual Fund) with all 3 structured locations. ss-4: trust with an alias
   * name.
   *
   * <p>The real source file's own trailing pipe (an empty, undocumented, never-mapped 46th column)
   * is deliberately dropped here, unlike the other 3 trailing positional-duplicate columns - found
   * live: Apache Commons CSV's {@code CSVParser} (what {@code PekkoIngestionRunner#parseCsvRows}
   * actually uses) rejects a blank header name outright ({@code IllegalArgumentException: A header
   * name is missing}), so keeping it byte-faithful would make this fixture unusable through the
   * real CSV connector for no real informational gain (the column is genuinely never populated).
   *
   * <p>Deliberately built via a method call, not a literal concatenation, despite every piece being
   * a compile-time-constant string - a {@code public static final String} initialized directly by a
   * constant expression (JLS §15.28) gets its *value* inlined into every referencing class's own
   * bytecode at compile time, not read from this field at runtime. A fixture whose content can
   * change (this one already has, once) must not have that shape: fixing this class and
   * reinstalling its jar would silently not propagate to an already-compiled consumer class until
   * that consumer itself gets rebuilt for an unrelated reason - a real, live-hit trap, not a
   * hypothetical one.
   */
  public static final String SAMPLE_CSV = buildSampleCsv();

  private static String buildSampleCsv() {
    return HEADER
        + "\n"
        + ROW_INDIVIDUAL_WELL_POPULATED
        + "\n"
        + ROW_INDIVIDUAL_MINIMAL
        + "\n"
        + ROW_ORGANIZATION_ALL_LOCATIONS
        + "\n"
        + ROW_TRUST_WITH_ALIAS
        + "\n";
  }

  /**
   * {@link #SAMPLE_CSV} plus one additional, real-shaped row engineered to score a genuine match
   * against {@code screen_against_worldcheck_reference}'s own bundled reference population (see
   * that transform's javadoc, in {@code forwardmeasure-data-streaming-transforms}) - name, country,
   * and date of birth all agree with reference entity {@code wc-1}. None of {@link #SAMPLE_CSV}'s
   * own 4 real rows are engineered to match anything (real customers overwhelmingly don't), so a
   * test proving the screening path actually produces a non-empty {@code screening_hits} result
   * needs a row that deliberately does.
   *
   * <p>Built via {@link #buildRow}, not a hand-typed pipe-delimited literal - this fixture's real
   * column count (46, including the source system's own trailing positional-duplicate columns) is
   * exactly the kind of thing a manually-counted literal silently misaligns without erroring.
   */
  public static String sampleCsvWithScreeningMatch() {
    return SAMPLE_CSV
        + buildRow(
            Map.of(
                "ENTITYS_UNIQUE_ID", "KYC-SCREEN-1",
                "ENTITYS_FULL_NAME", "Jose Smith",
                "ENTITY_FIRST_NAME", "Jose",
                "ENTITY_LAST_NAME", "Smith",
                "ENTITY_LOCATION_1_COUNTRY", "RUSSIA",
                "COUNTRY_OF_CITIZENSHIP_INCORPORATION", "Russia",
                "COUNTRY_OF_NATIONALITY", "Russia",
                "DATE_OF_BIRTH_DATE_OF_INCORPORATION", "19750315",
                "ENTITY_TYPE_CODE", "I",
                "ENTITYS_TYPE", "Individual"))
        + "\n";
  }

  /**
   * One pipe-delimited data row in {@link #HEADER}'s own real column order, {@code overrides}'
   * values by column name, blank for every column not named in {@code overrides} - the same "derive
   * order from the canonical header, not a manually-counted literal" safety {@link
   * #sampleCsvWithScreeningMatch} exists to get right.
   */
  private static String buildRow(Map<String, String> overrides) {
    StringBuilder row = new StringBuilder();
    String[] columns = HEADER.split("\\|");
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        row.append('|');
      }
      row.append(overrides.getOrDefault(columns[i], ""));
    }
    return row.toString();
  }

  /**
   * Real index settings - the same generic {@code ScreeningRecord} mapping {@link
   * WorldCheckFixtures#indexSettingsJson()} uses, under its own resource copy.
   */
  public static String indexSettingsJson() {
    return resource("fixtures/test-customer-master-opensearch-index-settings.json");
  }

  /**
   * Renders and parses the real, file-sourced {@code IngestionSpec} - {@code sourceUri} must be a
   * real Camel {@code file:} endpoint URI, {@code openSearchUrl} the real sink base URL, {@code
   * indexSettingsFilePath} a real local file path holding {@link #indexSettingsJson()}'s own
   * content.
   */
  public static IngestionSpec boundedFileSpec(
      String sourceUri, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/test-customer-master-to-opensearch.yaml")
            .replace("{{SOURCE_URI}}", sourceUri)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  /**
   * Real index settings for {@link #boundedFileSpecWithScreening} - {@link #indexSettingsJson()}
   * plus one added {@code screening_hits} nested mapping (a separate resource, not an edit to the
   * shared one - see that spec's own YAML comment for why).
   */
  public static String screenedIndexSettingsJson() {
    return resource("fixtures/test-customer-master-opensearch-index-settings-screened.json");
  }

  /**
   * The one real {@code HEAVY}-transform spec in this fixture set: {@link #boundedFileSpec}'s
   * identical mapping plus one added {@code screening_hits} field rule ({@code
   * screen_against_worldcheck_reference}) - the only spec in this whole test-fixtures module whose
   * compiled {@link com.forwardmeasure.datastreaming.api.ExecutionPlan} carries a real {@code
   * sparkStage}, since every other registered transform is {@code LIGHT}. Deliberately a separate
   * spec/index/index-settings from {@link #boundedFileSpec} - see that YAML's own comment for why
   * editing the base spec in place would have been a regression, not a refinement.
   */
  public static IngestionSpec boundedFileSpecWithScreening(
      String sourceUri, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/test-customer-master-to-opensearch-screened.yaml")
            .replace("{{SOURCE_URI}}", sourceUri)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  /**
   * Renders and parses the real, {@code kafka}-sourced variant of the identical mapping - the only
   * legitimate way to exercise a {@code KAFKA_STREAMS}-resolved cell for this dataset. {@code
   * kafkaUri} must be the real {@code kafka:<topic>?brokers=<brokers>} form {@code
   * KafkaConnectorUri} parses - the topic must already be seeded with real Customer Master rows
   * encoded the same way {@link #SAMPLE_CSV}'s own rows are (one JSON object per row, field names
   * matching the header's own column names) before a test dispatches this spec.
   */
  public static IngestionSpec boundedKafkaSpec(
      String kafkaUri, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/test-customer-master-to-opensearch-kafka.yaml")
            .replace("{{KAFKA_URI}}", kafkaUri)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  /**
   * The {@code kafka}-sourced sibling of {@link #boundedFileSpecWithScreening} - sidesteps having
   * to inject a real CSV file into a dispatched Spark pod's own filesystem entirely, using {@code
   * SparkIngestionRunner}'s real Kafka source support instead. Unlike {@link #boundedKafkaSpec} (a
   * single {@code kafkaUri} string, the {@code BoundedKafkaStreamsConsumerRunner}/{@code
   * KafkaConnectorUri} convention), this spec is read by Spark, whose own native kafka source reads
   * {@code kafka.bootstrap.servers}/{@code subscribe} off {@code source.options()} directly (see
   * this fixture's own YAML comment) - {@code kafkaBrokers}/{@code kafkaTopic} are therefore
   * separate parameters, not one pre-joined URI. Seeding rows onto {@code kafkaTopic} must follow
   * {@link #boundedKafkaSpec}'s own seeding contract identically (one flat JSON object per row,
   * keys matching the real column names).
   */
  public static IngestionSpec boundedKafkaSpecWithScreening(
      String kafkaBrokers, String kafkaTopic, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/test-customer-master-to-opensearch-kafka-screened.yaml")
            .replace("{{KAFKA_BROKERS}}", kafkaBrokers)
            .replace("{{KAFKA_TOPIC}}", kafkaTopic)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  private static String resource(String classpathPath) {
    try (InputStream in =
        TestCustomerMasterFixtures.class.getClassLoader().getResourceAsStream(classpathPath)) {
      if (in == null) {
        throw new IllegalStateException(classpathPath + " not found on classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
