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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * The real WorldCheck reference-population fixture - byte-for-byte the same 4-row TSV, real index
 * mapping, and real field-by-field {@code IngestionSpec} shape already live-verified in {@code
 * forwardmeasure-data-streaming-executor-pekko}'s own {@code
 * PekkoWorldCheckToOpenSearchIntegrationTest} (tab-delimited, ISO-8859-1, real column names like
 * {@code "LAST NAME"}/{@code "E/I"} - not a simplified/fictional shape). Relocated here (not moved
 * - that test keeps its own copy; duplicated on purpose, since the two consumption contexts are
 * genuinely different: that test proves the Pekko delivery engine's own field-mapping fidelity in
 * isolation, this module exists so Phase G/H's 18/36-cell matrix can reach the identical fixture as
 * a real, ordinary {@code main}-scope dependency from three separate framework-binding test suites,
 * not copy-pasted a third time).
 *
 * <p>Two real {@code IngestionSpec} variants, both {@code BOUNDED}: a {@code file}-sourced one (the
 * default - every WorldCheck/customer-master transform is {@code STATELESS}/{@code LIGHT}, so per
 * {@code ExecutionPlanCompiler}'s own real engine-selection table a file-sourced bounded spec
 * always resolves to {@code PEKKO_STREAMS}) and a {@code kafka}-sourced one (the *only* legitimate
 * way to reach the {@code KAFKA_STREAMS} cells for this data, per the same compiler rule - source
 * connector {@code kafka} is the one real trigger for {@code KAFKA_STREAMS} in {@code BOUNDED}
 * mode). Both are real classpath YAML templates, rendered with real runtime values and parsed via
 * {@link IngestionSpec#parseYaml}, never a hand-built Java object - so what a real workflow author
 * would actually write is exactly what these tests exercise.
 */
public final class WorldCheckFixtures {

  private WorldCheckFixtures() {}

  public static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

  /**
   * Real WorldCheck reference-population shape: tab-delimited, real column names (spaces/slashes).
   * wc-1: person (M indicator) with aliases/categories/locations populated. wc-2: person (legacy I
   * indicator), every optional field blank. wc-3: organization (E + CATEGORY=ORGANIZATION). wc-4:
   * physical_asset (E + CATEGORY=VESSEL). Byte-for-byte the same fixture {@code
   * PekkoWorldCheckToOpenSearchIntegrationTest} already proves live - copied, not reinvented.
   *
   * <p>Deliberately built via a method call, not a literal concatenation - see {@link
   * TestCustomerMasterFixtures#SAMPLE_CSV}'s own javadoc for why a {@code public static final
   * String} fixture must never be a compile-time constant expression (JLS §15.28): its value would
   * get inlined into every referencing class's own bytecode at compile time, so a fix here could
   * silently fail to propagate to an already-compiled consumer.
   */
  public static final String SAMPLE_TSV = buildSampleTsv();

  private static String buildSampleTsv() {
    return "UID\tLAST NAME\tFIRST NAME\tCATEGORY\tE/I\tDOB\tCITIZENSHIP\tCOUNTRIES\tALIASES\tSSN\t"
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

  /**
   * Real index settings - byte-identical copy of entity-intelligence-specifications' own {@code
   * screening-records-worldcheck-opensearch-indexing-spec.json}, including the real custom
   * analyzers (ngram/phonetic name matching) and {@code "dynamic": "strict"} nested
   * names/identifiers mapping - every target field the specs below write must be one this mapping
   * actually declares.
   */
  public static String indexSettingsJson() {
    return resource("fixtures/worldcheck-opensearch-index-settings.json");
  }

  /**
   * Renders and parses the real, file-sourced {@code IngestionSpec} - {@code sourceUri} must be a
   * real Camel {@code file:} endpoint URI (see {@code PekkoWorldCheckToOpenSearchIntegrationTest}
   * for the exact real form, including {@code charset=ISO-8859-1}), {@code openSearchUrl} the real
   * sink base URL, {@code indexSettingsFilePath} a real local file path holding {@link
   * #indexSettingsJson()}'s own content (the sink's {@code indexSettingsFile} option reads from
   * disk, not an inline value).
   */
  public static IngestionSpec boundedFileSpec(
      String sourceUri, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/worldcheck-to-opensearch.yaml")
            .replace("{{SOURCE_URI}}", sourceUri)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  /**
   * Renders and parses the real, {@code kafka}-sourced variant of the identical mapping - the only
   * legitimate way to exercise a {@code KAFKA_STREAMS}-resolved cell for this dataset (see this
   * class's own javadoc). {@code kafkaUri} must be the real {@code kafka:<topic>?brokers=<brokers>}
   * form {@code KafkaConnectorUri} parses (e.g. {@code kafka:worldcheck-rows?brokers=kafka:9092}) -
   * the topic must already be seeded with real WorldCheck rows encoded the same way {@link
   * #SAMPLE_TSV}'s own rows are (one JSON object per row, field names matching the TSV's own column
   * headers) before a test dispatches this spec.
   */
  public static IngestionSpec boundedKafkaSpec(
      String kafkaUri, String openSearchUrl, Path indexSettingsFilePath) {
    String rendered =
        resource("fixtures/worldcheck-to-opensearch-kafka.yaml")
            .replace("{{KAFKA_URI}}", kafkaUri)
            .replace("{{OPENSEARCH_URL}}", openSearchUrl)
            .replace("{{INDEX_SETTINGS_FILE}}", indexSettingsFilePath.toString());
    return IngestionSpec.parseYaml(rendered);
  }

  private static String resource(String classpathPath) {
    try (InputStream in =
        WorldCheckFixtures.class.getClassLoader().getResourceAsStream(classpathPath)) {
      if (in == null) {
        throw new IllegalStateException(classpathPath + " not found on classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
