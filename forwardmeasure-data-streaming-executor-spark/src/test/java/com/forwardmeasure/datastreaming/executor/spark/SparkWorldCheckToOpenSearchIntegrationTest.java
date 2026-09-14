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
package com.forwardmeasure.datastreaming.executor.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchContainerConfiguration;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 * The Spark sibling of {@code executor-pekko}'s own {@code
 * PekkoWorldCheckToOpenSearchIntegrationTest} - same real, checked-in spec shape (this module's own
 * copy of {@code worldcheck-to-opensearch.yaml}, {@code execution.engine: spark} instead of {@code
 * pekko}, otherwise identical source/mapper/sink), same real WorldCheck fixture rows
 * (tab-delimited, ISO-8859-1, real column names), same real {@code
 * screening-records-worldcheck-opensearch-indexing-spec.json} applied via {@link
 * com.forwardmeasure.datastreaming.api.OpenSearchIndexInitializer} - run through {@link
 * SparkIngestionRunner#run(SparkSession, IngestionSpec)} instead of {@code
 * PekkoIngestionRunner.run}. Rewritten 2026-09-14 alongside the Pekko sibling once this class's own
 * first version was found to be an invented, oversimplified, comma-delimited toy that didn't
 * reflect what fei's own {@code SimpleSourceIngestionWorker} actually ingests in production - see
 * the checked-in spec's own comments for the full reasoning.
 *
 * <p>Does not use {@code @WithOpenSearchContainer} (added 2026-09-14, same as the Pekko sibling):
 * the real indexing spec's own {@code name_double_metaphone} filter needs OpenSearch's {@code
 * analysis-phonetic} plugin, which the plain default image doesn't have. Builds a phonetic-plugin
 * image via {@link ImageFromDockerfile} instead, the exact same real, already-proven pattern {@code
 * forwardmeasure-entity-intelligence}'s own {@code ScreeningMatchServiceIntegrationTest} uses for
 * the identical real requirement - not invented here. Built from {@code opensearch:3.8.0}, matching
 * {@code forwardmeasure-testcontainers-opensearch}'s own {@code
 * OpenSearchContainerConfiguration#DEFAULT_IMAGE} and that fei test's own base - {@code 3.8.0} is
 * the real, currently-deployed version per {@code helm-charts}' own {@code
 * opensearch-cluster-helm-chart} values.
 */
class SparkWorldCheckToOpenSearchIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
  private static SparkSession spark;
  private static OpenSearchTestContainer opensearch;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-worldcheck-to-opensearch-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @BeforeAll
  static void startOpenSearchWithPhoneticPlugin() throws Exception {
    ImageFromDockerfile image =
        new ImageFromDockerfile("opensearchproject/opensearch:3.8.0-fds-worldcheck-test", false)
            .withDockerfileFromBuilder(
                builder ->
                    builder
                        .from("opensearchproject/opensearch:3.8.0")
                        .run(
                            "/usr/share/opensearch/bin/opensearch-plugin install --batch"
                                + " analysis-phonetic")
                        .build());
    opensearch =
        new OpenSearchTestContainer(
                new OpenSearchContainerConfiguration(
                    DockerImageName.parse(image.get()), false, Optional.empty(), List.of(), 0L, 0L))
            .start();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @AfterAll
  static void stopOpenSearch() {
    if (opensearch != null) {
      opensearch.close();
    }
  }

  @Test
  void ingestsRealWorldCheckRowsIntoOpenSearchViaTheCheckedInSpec(@TempDir Path tempDir)
      throws Exception {
    Path sourceCsv = tempDir.resolve("worldcheck.csv");
    Files.write(sourceCsv, WORLDCHECK_TSV.getBytes(ISO_8859_1));

    Path indexSettingsFile = copyIndexSettingsFile(tempDir);

    String template = readSpecTemplate();
    String rendered =
        template
            .replace("{{SOURCE_PATH}}", sourceCsv.toString())
            .replace("{{OPENSEARCH_URL}}", opensearch.hostEndpoint().toString())
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFile.toString());
    Path specPath = tempDir.resolve("worldcheck-to-opensearch.yaml");
    Files.writeString(specPath, rendered, StandardCharsets.UTF_8);

    IngestionSpec spec = IngestionSpec.load(specPath);

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, spec);
    assertEquals(4, result.recordsWritten());

    // wc-1: entity_indicator "M" wins outright as a person; FIRST NAME is non-ASCII (José),
    // proving ISO-8859-1 was genuinely honored (Spark's own native "encoding" CSV option), not
    // just declared. names: PRIMARY entry plus one ALIAS entry per alias - see
    // FieldMappingEngineTest's own proof of why ALIASES (parse_semicolon_list, already a List)
    // flattens into one tagged entry per element rather than one entry holding the whole list.
    JsonNode wc1 = fetchDocument(opensearch, "wc-1");
    assertEquals("person", wc1.path("_source").path("entity_kind").asText());
    assertEquals("1975-03-15", wc1.path("_source").path("date_of_birth").asText());
    assertEquals(List.of("US", "RU"), toStringList(wc1.path("_source").path("nationalities")));
    assertEquals("Businessman", wc1.path("_source").path("position").asText());
    assertEquals("Former PEP", wc1.path("_source").path("pep_status").asText());
    assertEquals(
        List.of("Senator", "Governor"), toStringList(wc1.path("_source").path("pep_roles")));
    assertEquals(List.of("Fraud", "Bribery"), toStringList(wc1.path("_source").path("keywords")));
    assertEquals(
        List.of("https://example.com/report1"),
        toStringList(wc1.path("_source").path("external_sources")));
    assertEquals(
        List.of("example.com"), toStringList(wc1.path("_source").path("external_source_domains")));
    JsonNode wc1Names = wc1.path("_source").path("names");
    assertEquals(3, wc1Names.size());
    assertEquals("Smith, José", wc1Names.get(0).path("value").asText());
    assertEquals("PRIMARY", wc1Names.get(0).path("name_type").asText());
    assertEquals("Johnny Smith", wc1Names.get(1).path("value").asText());
    assertEquals("ALIAS", wc1Names.get(1).path("name_type").asText());
    assertEquals("J. Smith", wc1Names.get(2).path("value").asText());
    assertEquals("ALIAS", wc1Names.get(2).path("name_type").asText());
    JsonNode wc1Ids = wc1.path("_source").path("identifiers");
    assertEquals(1, wc1Ids.size());
    assertEquals("123-45-6789", wc1Ids.get(0).path("value").asText());
    assertEquals("SSN", wc1Ids.get(0).path("scheme").asText());
    // categories/locations (added 2026-09-14): classify_party_categories/
    // parse_tilde_delimited_locations, now in NamedTransformRegistry - see the checked-in spec's
    // own comment for why these two moved from "deliberately not ported" to "populated".
    assertEquals(List.of("sanctions", "pep"), toStringList(wc1.path("_source").path("categories")));
    JsonNode wc1Locations = wc1.path("_source").path("locations");
    assertEquals(1, wc1Locations.size());
    assertEquals("Moscow", wc1Locations.get(0).path("city").asText());
    assertEquals("Moscow Oblast", wc1Locations.get(0).path("state_or_province").asText());
    assertEquals("RUSSIA", wc1Locations.get(0).path("country_name").asText());
    assertEquals("RU", wc1Locations.get(0).path("country_code").asText());
    assertEquals("REGISTERED", wc1Locations.get(0).path("location_type").asText());

    // wc-2: legacy "I" indicator, also a person outright; no ALIASES, so exactly one PRIMARY name
    // - including categories/locations, proving the "no source value, no field at all" rule still
    // holds for the two newly-populated fields too.
    JsonNode wc2 = fetchDocument(opensearch, "wc-2");
    assertEquals("person", wc2.path("_source").path("entity_kind").asText());
    assertEquals("1982-07-22", wc2.path("_source").path("date_of_birth").asText());
    assertEquals(List.of("RU"), toStringList(wc2.path("_source").path("nationalities")));
    JsonNode wc2Names = wc2.path("_source").path("names");
    assertEquals(1, wc2Names.size());
    assertEquals("Ivanov, Petr", wc2Names.get(0).path("value").asText());
    assertEquals("PRIMARY", wc2Names.get(0).path("name_type").asText());
    assertTrue(
        wc2.path("_source").path("categories").isMissingNode(),
        "a row with no SPECIAL INTEREST CATEGORIES value must have no 'categories' at all");
    assertTrue(
        wc2.path("_source").path("locations").isMissingNode(),
        "a row with no LOCATIONS value must have no 'locations' at all");

    // wc-3: entity_indicator "E" (authoritatively non-person) + CATEGORY=ORGANIZATION ->
    // organization. Blank FIRST NAME means the primary template rule resolves to nothing (the
    // mapping engine's own all-or-nothing blank-column rule for `template`) and there are no
    // ALIASES either, so 'names' is absent entirely - a real, documented limitation of this port,
    // not a bug (see the checked-in spec's own comment).
    JsonNode wc3 = fetchDocument(opensearch, "wc-3");
    assertEquals("organization", wc3.path("_source").path("entity_kind").asText());
    assertEquals(List.of("GB"), toStringList(wc3.path("_source").path("nationalities")));
    assertTrue(
        wc3.path("_source").path("names").isMissingNode(),
        "an org row with a blank FIRST_NAME and no ALIASES must have no 'names' at all");
    assertTrue(
        wc3.path("_source").path("date_of_birth").isMissingNode(),
        "org row has no DOB - the optional field must be omitted, not present as null");

    // wc-4: entity_indicator "E" + CATEGORY=VESSEL -> physical_asset, not organization.
    JsonNode wc4 = fetchDocument(opensearch, "wc-4");
    assertEquals("physical_asset", wc4.path("_source").path("entity_kind").asText());
    assertEquals(List.of("IR"), toStringList(wc4.path("_source").path("nationalities")));
  }

  private static List<String> toStringList(JsonNode arrayNode) {
    List<String> values = new ArrayList<>();
    arrayNode.forEach(node -> values.add(node.asText()));
    return values;
  }

  private static Path copyIndexSettingsFile(Path tempDir) throws Exception {
    Path target = tempDir.resolve("worldcheck-opensearch-index-settings.json");
    try (InputStream in =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("specs/worldcheck-opensearch-index-settings.json")) {
      if (in == null) {
        throw new IllegalStateException(
            "specs/worldcheck-opensearch-index-settings.json not found on classpath");
      }
      Files.write(target, in.readAllBytes());
    }
    return target;
  }

  private static String readSpecTemplate() throws Exception {
    try (InputStream in =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("specs/worldcheck-to-opensearch.yaml")) {
      if (in == null) {
        throw new IllegalStateException(
            "specs/worldcheck-to-opensearch.yaml not found on classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static JsonNode fetchDocument(OpenSearchTestContainer opensearch, String id)
      throws Exception {
    URI uri = URI.create(opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/" + id);
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200, response.statusCode(), "expected document '" + id + "' to exist: " + response.body());
    return MAPPER.readTree(response.body());
  }

  // Real WorldCheck reference-population shape: tab-delimited, real column names (spaces/slashes),
  // written to disk as ISO-8859-1 bytes above - matching entity-intelligence-specifications' own
  // checked-in schema-worldcheck-reference-population.yaml, byte-identical to executor-pekko's own
  // copy of this same fixture.
  private static final String WORLDCHECK_TSV =
      "UID\tLAST NAME\tFIRST NAME\tCATEGORY\tE/I\tDOB\tCITIZENSHIP\tCOUNTRIES\tALIASES\tSSN\t"
          + "POSITION\tPEP ROLES\tPEP STATUS\tKEYWORDS\tEXTERNAL SOURCES\tFURTHER INFORMATION\t"
          + "SPECIAL INTEREST CATEGORIES\tLOCATIONS\n"
          + "wc-1\tSmith\tJosé\tINDIVIDUAL\tM\t1975/03/15\tUNITED STATES;RUSSIA\t\t"
          + "Johnny Smith;J. Smith\t123-45-6789\tBusinessman\tSenator~Governor\tFormer PEP\t"
          + "Fraud~Bribery\thttps://example.com/report1\tSubject of an investigation.\t"
          + "Sanctions Related;PEP\t~ Moscow, Moscow Oblast ~ RUSSIA\n"
          + "wc-2\tIvanov\tPetr\tINDIVIDUAL\tI\t1982/07/22\tRUSSIA\t\t\t\t\t\t\t\t\t\t\t\n"
          + "wc-3\tAcme Holdings\t\tORGANIZATION\tE\t\tUNITED KINGDOM\t\t\t\t\t\t\t\t\t\t\t\n"
          + "wc-4\tMV Example Star\t\tVESSEL\tE\t\tIRAN\t\t\t\t\t\t\t\t\t\t\t\n";
}
