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

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.Cardinality;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.ExecutionCost;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.StateRequirement;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import com.forwardmeasure.datastreaming.transforms.NamedTransformFunctions;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-only named transforms for FDS's party-, sanctions- and KYC-shaped fixtures. Every name
 * starts with {@code test_}. They reach FDS the same way a product's transforms do, through {@link
 * TestTransformProvider} (a {@code NamedTransformProvider}), so the tests exercise the plug-in
 * mechanism. This module is only ever a test-scope dependency; a build check enforces that.
 */
public final class TestTransforms {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestTransforms.class);

  /** Slow per-row work: the planner inserts a Spark stage for it. */
  static final TransformCharacteristics HEAVY_ROW_FUNCTION =
      new TransformCharacteristics(
          Cardinality.ONE, StateRequirement.STATELESS, ExecutionCost.HEAVY, false, true);

  private static final Pattern TILDE_LOCATION_PATTERN =
      Pattern.compile("~\\s*([^~,]*?)(?:,\\s*([^~]*))?\\s*~\\s*([^;~]*)");

  private TestTransforms() {}

  /** Every test transform by name. */
  public static Map<String, NamedTransform> all() {
    Map<String, NamedTransform> transforms = new LinkedHashMap<>();
    transforms.put(
        "test_classify_party_category", inputs -> classifyPartyCategory(inputs.get("value")));
    transforms.put("test_classify_party_kind", TestTransforms::classifyPartyKind);
    transforms.put(
        "test_classify_party_categories", inputs -> classifyPartyCategories(inputs.get("value")));
    transforms.put(
        "test_parse_tilde_delimited_locations",
        inputs -> parseTildeDelimitedLocations(inputs.get("value")));
    transforms.put(
        "test_classify_customer_master_party_kind",
        TestTransforms::classifyCustomerMasterPartyKind);
    transforms.put(
        "test_build_customer_master_locations", TestTransforms::buildCustomerMasterLocations);
    transforms.put(
        "test_build_customer_master_identifiers", TestTransforms::buildCustomerMasterIdentifiers);
    transforms.put(
        "test_screen_against_reference",
        NamedTransform.of(HEAVY_ROW_FUNCTION, TestTransforms::screenAgainstReference));
    return Map.copyOf(transforms);
  }

  public static String classifyPartyCategory(String category) {
    if (category == null) {
      return "unknown";
    }
    return switch (category.trim().toUpperCase(Locale.ROOT)) {
      case "INDIVIDUAL", "POLITICAL INDIVIDUAL", "DIPLOMAT", "MILITARY", "LEGAL" -> "person";
      case "ORGANIZATION",
          "ORGANISATION",
          "CORPORATE",
          "BANK",
          "SHELL BANK OR COMPANY",
          "POLITICAL PARTY",
          "WEBSITE",
          "PORT",
          "TRADE UNION",
          "COUNTRY",
          "SPECIAL JURISDICTION",
          "EMBARGO",
          "ADDRESS" ->
          "organization";
      case "VESSEL", "EMBARGO VESSEL", "AIRCRAFT", "EMBARGO AIRCRAFT" -> "physical_asset";
      default -> {
        LOGGER.debug(
            "TestTransforms.classifyPartyCategory: unmapped '{}' - defaulting to" + " unknown",
            category);
        yield "unknown";
      }
    };
  }

  public static String classifyPartyKind(Map<String, String> inputs) {
    String category = inputs.get("value");
    String entityIndicator = inputs.get("entity_indicator");
    if (entityIndicator != null) {
      String indicator = entityIndicator.trim().toUpperCase(Locale.ROOT);
      if (Set.of("M", "F", "U", "I").contains(indicator)) {
        return "person";
      }
      if ("E".equals(indicator)) {
        String categoryKind = classifyPartyCategory(category);
        return "physical_asset".equals(categoryKind) ? "physical_asset" : "organization";
      }
    }
    return classifyPartyCategory(category);
  }

  public static List<String> classifyPartyCategories(String raw) {
    List<String> result = new ArrayList<>();
    if (raw == null || raw.isBlank()) {
      return result;
    }
    for (String entry : raw.split(";")) {
      String trimmed = entry.strip();
      if (trimmed.isBlank()) {
        continue;
      }
      String category = classifySingleCategory(trimmed);
      if (category != null && !result.contains(category)) {
        result.add(category);
      }
    }
    return result;
  }

  private static String classifySingleCategory(String value) {
    return switch (value.toLowerCase(Locale.ROOT)) {
      case "sanctions related", "explicit sanctions", "sanctions" -> "sanctions";
      case "pep", "politically exposed person" -> "pep";
      case "adverse media",
          "adverse media - financial crime",
          "adverse media - violent crime",
          "adverse media - other" ->
          "adverse_media";
      case "enforcement", "regulatory enforcement", "law enforcement" -> "enforcement";
      case "terror related", "terrorism" -> "sanctions";
      default -> {
        LOGGER.debug(
            "TestTransforms.classifyPartyCategories: unmapped category '{}' - skipping", value);
        yield null;
      }
    };
  }

  public static List<Map<String, Object>> parseTildeDelimitedLocations(String raw) {
    List<Map<String, Object>> result = new ArrayList<>();
    if (raw == null || raw.isBlank()) {
      return result;
    }
    for (String entry : raw.split(";")) {
      String trimmed = entry.strip();
      if (trimmed.isBlank()) {
        continue;
      }
      Matcher m = TILDE_LOCATION_PATTERN.matcher(trimmed);
      if (!m.find()) {
        LOGGER.debug("TestTransforms.parseTildeDelimitedLocations: no match for '{}'", trimmed);
        continue;
      }
      String city = m.group(1) != null ? m.group(1).strip() : null;
      String region = m.group(2) != null ? m.group(2).strip() : null;
      String countryName = m.group(3) != null ? m.group(3).strip() : null;
      if ((city == null || city.isBlank()) && (countryName == null || countryName.isBlank())) {
        continue;
      }
      if ((city == null || city.isBlank()) && "UNKNOWN".equalsIgnoreCase(countryName)) {
        continue;
      }
      Map<String, Object> location = new LinkedHashMap<>();
      location.put("city", city != null && !city.isBlank() ? city : null);
      location.put("state_or_province", region != null && !region.isBlank() ? region : null);
      location.put(
          "country_name", countryName != null && !countryName.isBlank() ? countryName : null);
      location.put("country_code", NamedTransformFunctions.resolve_iso2_country(countryName));
      location.put("location_type", "REGISTERED");
      result.add(location);
    }
    return result;
  }

  public static String classifyCustomerMasterPartyKind(Map<String, String> inputs) {
    String entityType = inputs.get("value");
    if (isBlank(entityType)) {
      return "unknown";
    }
    return "individual".equalsIgnoreCase(entityType.trim()) ? "person" : "organization";
  }

  public static List<Map<String, Object>> buildCustomerMasterLocations(Map<String, String> inputs) {
    List<Map<String, Object>> result = new ArrayList<>();
    addTestCustomerMasterLocationIfPresent(result, inputs, 1, "REGISTERED");
    addTestCustomerMasterLocationIfPresent(result, inputs, 2, "MAILING");
    addTestCustomerMasterLocationIfPresent(result, inputs, 3, "OTHER");
    return result;
  }

  private static void addTestCustomerMasterLocationIfPresent(
      List<Map<String, Object>> result,
      Map<String, String> inputs,
      int position,
      String locationType) {
    String addressLine1 = inputs.get("address_line_1_" + position);
    String addressLine2 = inputs.get("address_line_2_" + position);
    String city = inputs.get("city_" + position);
    String stateOrProvince = inputs.get("state_or_province_" + position);
    String country = inputs.get("country_" + position);
    if (isBlank(addressLine1)
        && isBlank(addressLine2)
        && isBlank(city)
        && isBlank(stateOrProvince)
        && isBlank(country)) {
      return;
    }
    String address =
        java.util.stream.Stream.of(addressLine1, addressLine2)
            .filter(part -> !isBlank(part))
            .collect(java.util.stream.Collectors.joining(", "));
    Map<String, Object> location = new LinkedHashMap<>();
    location.put("address", address.isBlank() ? null : address);
    location.put("city", isBlank(city) ? null : city);
    location.put("state_or_province", isBlank(stateOrProvince) ? null : stateOrProvince);
    location.put("country_name", isBlank(country) ? null : country);
    location.put("country_code", NamedTransformFunctions.resolve_iso2_country(country));
    location.put("location_type", locationType);
    result.add(location);
  }

  public static List<Map<String, Object>> buildCustomerMasterIdentifiers(
      Map<String, String> inputs) {
    List<Map<String, Object>> result = new ArrayList<>();
    String businessEntityRecordId = inputs.get("business_entity_record_id");
    if (!isBlank(businessEntityRecordId)) {
      result.add(
          customerMasterIdentifier(
              "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", businessEntityRecordId, null));
    }
    String kycId = inputs.get("raw_domain_unique_id");
    if (!isBlank(kycId)) {
      result.add(customerMasterIdentifier("TEST_CUSTOMER_MASTER_GEMS_ID", kycId, null));
    }
    for (NamedTransformFunctions.ParsedIdentifier parsed :
        NamedTransformFunctions.parse_identifier_pairs(inputs.get("entity_public_identifier"))) {
      result.add(customerMasterIdentifier(parsed.scheme(), parsed.valueRaw(), parsed.valueNorm()));
    }
    return result;
  }

  private static Map<String, Object> customerMasterIdentifier(
      String scheme, String value, String valueNorm) {
    Map<String, Object> identifier = new LinkedHashMap<>();
    identifier.put("scheme", scheme);
    identifier.put("value", value);
    if (valueNorm != null) {
      identifier.put("value_norm", valueNorm);
    }
    return identifier;
  }

  /**
   * Screens a row's names against {@link #REFERENCE_POPULATION} with simple, self-contained
   * scoring: accent- and case-insensitive name-token overlap (Jaccard), compatible kinds, and equal
   * dates of birth when both sides have one. Declared {@code HEAVY} because the real shape - every
   * row compared with a whole reference population - is what sends a spec to Spark.
   */
  public static List<Map<String, Object>> screenAgainstReference(Map<String, String> inputs) {
    List<String> subjectNames = new ArrayList<>();
    addIfPresent(subjectNames, inputs.get("full_name"));
    String givenFamily =
        java.util.stream.Stream.of(inputs.get("first_name"), inputs.get("last_name"))
            .filter(part -> !isBlank(part))
            .collect(java.util.stream.Collectors.joining(" "));
    addIfPresent(subjectNames, givenFamily);
    addIfPresent(subjectNames, inputs.get("alias"));
    if (subjectNames.isEmpty()) {
      return List.of();
    }
    String subjectKind =
        classifyCustomerMasterPartyKind(Map.of("value", nullToEmpty(inputs.get("entity_type"))));
    String subjectDob = NamedTransformFunctions.parse_yyyymmdd(inputs.get("dob"));

    List<Map<String, Object>> hits = new ArrayList<>();
    for (ReferenceEntity reference : REFERENCE_POPULATION) {
      if (!kindsCompatible(subjectKind, reference.kind())
          || (subjectDob != null
              && reference.dob() != null
              && !subjectDob.equals(reference.dob()))) {
        continue;
      }
      String bestSubject = null;
      String bestReference = null;
      double best = 0;
      for (String subjectName : subjectNames) {
        for (String referenceName : reference.names()) {
          double score = jaccard(tokens(subjectName), tokens(referenceName));
          if (score > best) {
            best = score;
            bestSubject = subjectName;
            bestReference = referenceName;
          }
        }
      }
      if (best >= MATCH_THRESHOLD) {
        Map<String, Object> hit = new LinkedHashMap<>();
        hit.put("reference_uid", reference.uid());
        hit.put("matched_subject_name", bestSubject);
        hit.put("matched_reference_name", bestReference);
        hit.put("composite_score", best);
        hit.put("decision_rule", "test_name_token_overlap");
        hits.add(hit);
      }
    }
    return hits;
  }

  private static final double MATCH_THRESHOLD = 0.75;

  private record ReferenceEntity(String uid, String kind, List<String> names, String dob) {}

  /** The fictional identities {@code WorldCheckFixtures} uses. */
  private static final List<ReferenceEntity> REFERENCE_POPULATION =
      List.of(
          new ReferenceEntity(
              "wc-1", "person", List.of("José Smith", "Johnny Smith", "J. Smith"), "1975-03-15"),
          new ReferenceEntity("wc-2", "person", List.of("Petr Ivanov"), "1982-07-22"),
          new ReferenceEntity("wc-3", "organization", List.of("Acme Holdings"), null),
          new ReferenceEntity("wc-4", "unknown", List.of("MV Example Star"), null));

  private static boolean kindsCompatible(String subject, String reference) {
    return "unknown".equals(subject) || "unknown".equals(reference) || subject.equals(reference);
  }

  private static Set<String> tokens(String name) {
    String folded =
        Normalizer.normalize(name, Normalizer.Form.NFD)
            .replaceAll("\\p{M}", "")
            .toLowerCase(Locale.ROOT);
    Set<String> tokens = new LinkedHashSet<>();
    for (String token : folded.split("[^\\p{L}\\p{N}]+")) {
      if (!token.isBlank()) {
        tokens.add(token);
      }
    }
    return tokens;
  }

  private static double jaccard(Set<String> a, Set<String> b) {
    if (a.isEmpty() || b.isEmpty()) {
      return 0;
    }
    Set<String> union = new LinkedHashSet<>(a);
    union.addAll(b);
    long shared = a.stream().filter(b::contains).count();
    return (double) shared / union.size();
  }

  private static void addIfPresent(List<String> values, String value) {
    if (!isBlank(value)) {
      values.add(value.trim());
    }
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
