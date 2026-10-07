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

import com.forwardmeasure.entitymatching.EntityEvidence;
import com.forwardmeasure.entitymatching.EntityKind;
import com.forwardmeasure.entitymatching.EntityMatchResult;
import com.forwardmeasure.entitymatching.EntityMatcher;
import com.forwardmeasure.entitymatching.MatchRequest;
import com.ibm.icu.text.LocaleDisplayNames;
import com.ibm.icu.util.ULocale;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pure string-in/value-out functions referenced by name from a {@code TransformSpec}, ported
 * verbatim from {@code forwardmeasure-entity-intelligence}'s own {@code NamedTransformFunctions}
 * (D3, corrected) - same names, same logic, same behavior for every function below. This is a port,
 * not a rewrite: parity with the origin is the bar (see this module's own tests).
 *
 * <p>Mostly the GENERIC subset - date/string/number formatting and country-code resolution,
 * domain-agnostic across any source. Two of fei's own originally-WorldCheck-named functions were
 * incorporated here too, 2026-09-13, renamed to name the actual functional capability rather than
 * the one source they were first observed on ({@code classify_party_category}/{@code
 * classify_party_kind}, from {@code map_worldcheck_category}/{@code map_worldcheck_entity_kind};
 * {@code parse_partial_date_ymd}, from {@code parse_worldcheck_date}) - see each one's own javadoc
 * for why: a person/organization/physical-asset party-kind taxonomy and a gender/individual-vs-
 * entity discriminator convention are common across sanctions/watchlist data providers generally,
 * not one vendor's private invention, the same reasoning that already justified {@link
 * #resolve_iso2_country}'s own country-alias table being generic. A consumer whose own provider
 * uses different exact label strings overrides the default via {@code FieldMappingEngine}'s own
 * {@code supplementalTransforms} map (checked before this registry, per its own javadoc) rather
 * than being locked into these defaults.
 *
 * <p><b>Updated 2026-09-14</b>: the remaining two, {@link #classify_party_categories} (from fei's
 * own {@code map_worldcheck_categories}) and {@link #parse_tilde_delimited_locations} (from fei's
 * own {@code parse_worldcheck_locations}), are incorporated here too, reversing this class's
 * original D3/D7 "genuinely WorldCheck-specific, belongs downstream" call for both - user-directed,
 * once the fully spec-driven path (no custom Java at all, see {@code
 * forwardmeasure-data-streaming-executor-{pekko,spark}}'s own checked-in {@code
 * worldcheck-to-opensearch.yaml}) became the preferred way to run this exact pipeline: a generic
 * runner has no {@code supplementalTransforms} caller to fall back on, so a transform this pipeline
 * genuinely needs has to live here or the pipeline can't be spec-only. Both are
 * special-interest-category and free-text-location vocabulary shapes common across sanctions/
 * watchlist providers generally, the same "not one vendor's private invention" reasoning already
 * applied to the other three above - not a new exception to it.
 */
public final class NamedTransformFunctions {

  private static final Logger LOGGER = LoggerFactory.getLogger(NamedTransformFunctions.class);

  private static final DateTimeFormatter YYYYMMDD =
      DateTimeFormatter.ofPattern("uuuuMMdd")
          .withResolverStyle(java.time.format.ResolverStyle.STRICT);
  private static final DateTimeFormatter MMDDYYYY =
      DateTimeFormatter.ofPattern("MM/dd/uuuu")
          .withResolverStyle(java.time.format.ResolverStyle.STRICT);

  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\{([A-Z0-9_-]+)}([^;{]+)");

  private static final Pattern URL_TOKEN_PATTERN = Pattern.compile("(?i)\\bhttps?://[^\\s;]+");

  private static final Pattern TILDE_LOCATION_PATTERN =
      Pattern.compile("~\\s*([^~,]*?)(?:,\\s*([^~]*))?\\s*~\\s*([^;~]*)");

  private static final LocaleDisplayNames ICU_DISPLAY_NAMES =
      LocaleDisplayNames.getInstance(ULocale.ENGLISH);

  private static final Map<String, String> COUNTRY_ALIASES =
      Map.ofEntries(
          Map.entry("RUSSIA", "RU"),
          Map.entry("RUSSIAN FEDERATION", "RU"),
          Map.entry("UNITED STATES", "US"),
          Map.entry("UNITED STATES OF AMERICA", "US"),
          Map.entry("USA", "US"),
          Map.entry("U.S.A.", "US"),
          Map.entry("U.S.", "US"),
          Map.entry("UK", "GB"),
          Map.entry("U.K.", "GB"),
          Map.entry("UNITED KINGDOM", "GB"),
          Map.entry("GREAT BRITAIN", "GB"),
          Map.entry("BRITAIN", "GB"),
          Map.entry("HONG KONG", "HK"),
          Map.entry("MACAU", "MO"),
          Map.entry("MACAO", "MO"),
          Map.entry("IRAN", "IR"),
          Map.entry("SYRIA", "SY"),
          Map.entry("VIETNAM", "VN"),
          Map.entry("VIET NAM", "VN"),
          Map.entry("NORTH KOREA", "KP"),
          Map.entry("SOUTH KOREA", "KR"),
          Map.entry("VENEZUELA", "VE"),
          Map.entry("BOLIVIA", "BO"),
          Map.entry("TANZANIA", "TZ"),
          Map.entry("MOLDOVA", "MD"),
          Map.entry("LAOS", "LA"),
          Map.entry("PALESTINE", "PS"),
          Map.entry("CZECH REPUBLIC", "CZ"),
          Map.entry("CZECHIA", "CZ"),
          Map.entry("MACEDONIA", "MK"),
          Map.entry("NORTH MACEDONIA", "MK"),
          Map.entry("IVORY COAST", "CI"),
          Map.entry("CAPE VERDE", "CV"),
          Map.entry("CABO VERDE", "CV"),
          Map.entry("BURMA", "MM"),
          Map.entry("MYANMAR", "MM"),
          Map.entry("SWAZILAND", "SZ"),
          Map.entry("ESWATINI", "SZ"),
          Map.entry("TURKEY", "TR"),
          Map.entry("TURKIYE", "TR"),
          Map.entry("KOSOVO", "XK"));

  /**
   * Country lookup is built once per JVM/classloader - same performance rationale as the origin (a
   * hot path for CITIZENSHIP/COUNTRIES/parsed-location-country resolution).
   */
  private static final Map<String, String> COUNTRY_LOOKUP = buildCountryLookup();

  private NamedTransformFunctions() {}

  private static Map<String, String> buildCountryLookup() {
    Map<String, String> lookup = new java.util.HashMap<>();
    for (Map.Entry<String, String> alias : COUNTRY_ALIASES.entrySet()) {
      lookup.put(normaliseCountryLookupKey(alias.getKey()), alias.getValue());
    }
    for (ULocale candidate : ULocale.getAvailableLocales()) {
      String region = candidate.getCountry();
      if (region == null || region.length() != 2) {
        continue;
      }
      String displayName = ICU_DISPLAY_NAMES.regionDisplayName(region);
      if (displayName != null && !displayName.isBlank()) {
        lookup.putIfAbsent(normaliseCountryLookupKey(displayName), region);
      }
      lookup.putIfAbsent(region.toUpperCase(Locale.ROOT), region);
    }
    return Map.copyOf(lookup);
  }

  private static String normaliseCountryLookupKey(String raw) {
    if (raw == null) {
      return "";
    }
    return raw.replace(' ', ' ')
        .replaceAll("\\s*\\(the\\)\\s*$", "")
        .trim()
        .replaceAll("\\s+", " ")
        .toUpperCase(Locale.ROOT);
  }

  /**
   * Resolves a source country value to ISO-3166-1 alpha-2. Does not canonicalise display text -
   * callers that preserve a country-name field separately should use this only to derive a
   * country-code field.
   */
  public static String resolve_iso2_country(String countryName) {
    if (countryName == null || countryName.isBlank()) {
      return null;
    }
    String normalised = normaliseCountryLookupKey(countryName);
    if (normalised.length() == 2 && normalised.matches("[A-Z]{2}")) {
      return normalised;
    }
    String resolved = COUNTRY_LOOKUP.get(normalised);
    if (resolved != null) {
      return resolved;
    }
    LOGGER.debug(
        "NamedTransformFunctions.resolve_iso2_country: could not resolve '{}'", countryName);
    return null;
  }

  /** Normalises either an ISO-2 country code or a country name to ISO-2. */
  public static String normalise_iso2_country(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String value = raw.trim().toUpperCase(Locale.ROOT);
    if (value.matches("[A-Z]{2}")) {
      return value;
    }
    return resolve_iso2_country(raw);
  }

  public static String parse_yyyymmdd(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return LocalDate.parse(raw.trim(), YYYYMMDD).toString();
    } catch (DateTimeParseException e) {
      LOGGER.debug("NamedTransformFunctions.parse_yyyymmdd: cannot parse '{}'", raw);
      return null;
    }
  }

  public static String parse_mmddyyyy(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return LocalDate.parse(raw.trim(), MMDDYYYY).toString();
    } catch (DateTimeParseException e) {
      LOGGER.debug("NamedTransformFunctions.parse_mmddyyyy: cannot parse '{}'", raw);
      return null;
    }
  }

  public record ParsedIdentifier(String scheme, String valueRaw, String valueNorm) {}

  public static List<ParsedIdentifier> parse_identifier_pairs(String raw) {
    List<ParsedIdentifier> result = new ArrayList<>();
    if (raw == null || raw.isBlank()) {
      return result;
    }
    Matcher matcher = IDENTIFIER_PATTERN.matcher(raw);
    while (matcher.find()) {
      String scheme = normaliseScheme(matcher.group(1).trim());
      String value = matcher.group(2).trim();
      if (scheme != null && !scheme.isBlank() && !value.isBlank()) {
        result.add(new ParsedIdentifier(scheme, value, normalise_identifier(scheme, value)));
      }
    }
    return result;
  }

  /**
   * Normalises a raw scheme string from source data to a canonical identifier-scheme value. Source
   * systems often use bare abbreviated scheme strings (EIN, SSN, CRN) rather than fully qualified
   * values (US_EIN, US_SSN, UK_CRN) - this promotes them at parse time. Unrecognised scheme strings
   * are returned uppercased and unchanged.
   */
  private static String normaliseScheme(String rawScheme) {
    if (rawScheme == null) {
      return null;
    }
    return switch (rawScheme.toUpperCase(Locale.ROOT)) {
      case "EIN" -> "US_EIN";
      case "SSN" -> "US_SSN";
      case "ITIN" -> "US_ITIN";
      case "CRN" -> "UK_CRN";
      case "NINO" -> "UK_NINO";
      case "UTR" -> "UK_UTR";
      case "VAT" -> "UK_VAT";
      case "SIN" -> "CA_SIN";
      case "BN" -> "CA_BN";
      default -> rawScheme.toUpperCase(Locale.ROOT);
    };
  }

  public static List<String> parse_semicolon_list(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    return java.util.Arrays.stream(raw.split(";"))
        .map(String::strip)
        .filter(s -> !s.isBlank())
        .toList();
  }

  public static List<String> parse_tilde_list(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    return java.util.Arrays.stream(raw.split("~"))
        .map(String::strip)
        .filter(s -> !s.isBlank())
        .toList();
  }

  public static List<String> parse_semicolon_iso2_list(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    List<String> result = new ArrayList<>();
    for (String part : raw.split(";")) {
      String value = part.strip();
      if (value.isBlank()) {
        continue;
      }
      String iso2 = resolve_iso2_country(value);
      if (iso2 != null && !iso2.isBlank() && !result.contains(iso2)) {
        result.add(iso2);
      }
    }
    return result;
  }

  /** Extracts URL tokens; tolerant of whitespace/semicolon/comma-separated provider fields. */
  public static List<String> parse_url_list(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    List<String> urls = new ArrayList<>();
    Matcher matcher = URL_TOKEN_PATTERN.matcher(raw);
    while (matcher.find()) {
      String url = trimUrlToken(matcher.group());
      if (url != null && !urls.contains(url)) {
        urls.add(url);
      }
    }
    return urls;
  }

  /** Extracts lowercase host/domain names from a URL/domain field. */
  public static List<String> extract_url_domains(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    List<String> domains = new ArrayList<>();
    List<String> urls = parse_url_list(raw);
    if (!urls.isEmpty()) {
      for (String url : urls) {
        String domain = extractDomainFast(url);
        if (domain != null && !domains.contains(domain)) {
          domains.add(domain);
        }
      }
      return domains;
    }
    for (String part : raw.split("[;\\s,]+")) {
      String domain = extractDomainFast(part);
      if (domain != null && !domains.contains(domain)) {
        domains.add(domain);
      }
    }
    return domains;
  }

  private static String trimUrlToken(String rawUrl) {
    if (rawUrl == null || rawUrl.isBlank()) {
      return null;
    }
    String value = rawUrl.strip();
    while (!value.isBlank()
        && (value.endsWith(".")
            || value.endsWith(",")
            || value.endsWith(";")
            || value.endsWith(")")
            || value.endsWith("]")
            || value.endsWith("}"))) {
      value = value.substring(0, value.length() - 1).strip();
    }
    return value.isBlank() ? null : value;
  }

  private static String extractDomainFast(String rawUrl) {
    if (rawUrl == null || rawUrl.isBlank()) {
      return null;
    }
    String value = trimUrlToken(rawUrl);
    if (value == null || value.isBlank()) {
      return null;
    }
    value = value.toLowerCase(Locale.ROOT);
    int schemeIdx = value.indexOf("://");
    if (schemeIdx >= 0) {
      value = value.substring(schemeIdx + 3);
    }
    int atIdx = value.lastIndexOf('@');
    if (atIdx >= 0) {
      value = value.substring(atIdx + 1);
    }
    int slashIdx = value.indexOf('/');
    if (slashIdx >= 0) {
      value = value.substring(0, slashIdx);
    }
    int questionIdx = value.indexOf('?');
    if (questionIdx >= 0) {
      value = value.substring(0, questionIdx);
    }
    int hashIdx = value.indexOf('#');
    if (hashIdx >= 0) {
      value = value.substring(0, hashIdx);
    }
    int colonIdx = value.indexOf(':');
    if (colonIdx >= 0) {
      value = value.substring(0, colonIdx);
    }
    while (value.startsWith("www.")) {
      value = value.substring(4);
    }
    value = value.strip();
    if (value.isBlank() || !value.contains(".")) {
      return null;
    }
    return value;
  }

  public static String normalise_identifier(String scheme, String valueRaw) {
    if (valueRaw == null) {
      return null;
    }
    if (scheme == null) {
      return valueRaw.trim();
    }
    return switch (scheme.toUpperCase(Locale.ROOT)) {
      case "EIN", "US_EIN", "SSN", "US_SSN", "ITIN", "US_ITIN", "TIN" ->
          valueRaw.replaceAll("[^0-9]", "");
      case "LEI" -> valueRaw.trim().toUpperCase(Locale.ROOT);
      case "CRN",
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
          "COMPANY_REGISTRATION" ->
          valueRaw.trim().toUpperCase(Locale.ROOT);
      case "EU_VAT",
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
          "CA_QST" ->
          valueRaw.replaceAll("[\\s\\-]", "").toUpperCase(Locale.ROOT);
      case "IBAN" -> valueRaw.replaceAll("\\s", "").toUpperCase(Locale.ROOT);
      case "SWIFT_BIC" -> valueRaw.trim().toUpperCase(Locale.ROOT);
      case "ISIN" -> valueRaw.trim().toUpperCase(Locale.ROOT);
      case "IN_PAN" -> valueRaw.trim().toUpperCase(Locale.ROOT);
      case "IN_AADHAAR", "IN_GSTIN" -> valueRaw.replaceAll("[\\s\\-]", "");
      case "UK_NINO" -> valueRaw.replaceAll("\\s", "").toUpperCase(Locale.ROOT);
      default -> valueRaw.trim();
    };
  }

  /**
   * Classifies a sanctions/watchlist party-category label into {@code person}/{@code
   * organization}/{@code physical_asset}/{@code unknown} - renamed and incorporated 2026-09-13 from
   * fei's own {@code map_worldcheck_category} (ported verbatim, same lookup table, same fallback):
   * this specific label set (INDIVIDUAL/ORGANIZATION/VESSEL/AIRCRAFT/BANK/POLITICAL PARTY/...) is
   * common vocabulary across sanctions-list providers generally (WorldCheck, OFAC, EU/UN
   * consolidated lists all describe the same regulatory domain with heavily overlapping category
   * taxonomies), not one vendor's private invention - the same reasoning that already justified
   * {@link #resolve_iso2_country}'s own country-alias table being generic rather than
   * WorldCheck-specific. A consumer whose own provider spells these differently supplies its own
   * override via {@code FieldMappingEngine}'s {@code supplementalTransforms} (checked before this
   * registry) rather than being locked into this exact table.
   */
  public static String classify_party_category(String category) {
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
            "NamedTransformFunctions.classify_party_category: unmapped '{}' - defaulting to"
                + " unknown",
            category);
        yield "unknown";
      }
    };
  }

  /**
   * Derives a party's canonical kind using an overloaded gender/individual-vs-entity discriminator
   * ({@code entity_indicator}: {@code M}/{@code F}/{@code U}/legacy {@code I} are people regardless
   * of category; {@code E} is authoritatively non-person, {@code category} only refines it into
   * {@code physical_asset} or {@code organization}; an absent/unrecognised discriminator falls back
   * to {@link #classify_party_category} alone) - renamed and incorporated 2026-09-13 from fei's own
   * {@code map_worldcheck_entity_kind} (ported verbatim). This discriminator convention - a gender
   * code doubling as a person/entity flag - is common across sanctions-list providers, not
   * WorldCheck-specific; see {@link #classify_party_category}'s own javadoc for the same reasoning
   * applied to the category table this falls back to, including how a consumer overrides either.
   */
  public static String classify_party_kind(Map<String, String> inputs) {
    String category = inputs.get("value");
    String entityIndicator = inputs.get("entity_indicator");
    if (entityIndicator != null) {
      String indicator = entityIndicator.trim().toUpperCase(Locale.ROOT);
      if (Set.of("M", "F", "U", "I").contains(indicator)) {
        return "person";
      }
      if ("E".equals(indicator)) {
        String categoryKind = classify_party_category(category);
        return "physical_asset".equals(categoryKind) ? "physical_asset" : "organization";
      }
    }
    return classify_party_category(category);
  }

  /**
   * Parses a slash-separated {@code year/month/day} date, tolerant of a {@code 0} placeholder in
   * the month or day position (treated as "unknown", clamped to {@code 1}) - renamed and
   * incorporated 2026-09-13 from fei's own {@code parse_worldcheck_date} (ported verbatim): unlike
   * {@link #parse_yyyymmdd} (strict, no separator) or {@link #parse_mmddyyyy} (strict, US
   * month-first convention), this is a genuinely distinct, reusable capability - a lenient,
   * year-first partial-date parser - not a WorldCheck-only format.
   */
  public static String parse_partial_date_ymd(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String[] parts = raw.trim().split("/");
    if (parts.length != 3) {
      LOGGER.debug("NamedTransformFunctions.parse_partial_date_ymd: unexpected format '{}'", raw);
      return null;
    }
    try {
      int year = Integer.parseInt(parts[0]);
      int month = Integer.parseInt(parts[1]);
      int day = Integer.parseInt(parts[2]);
      if (year <= 0) {
        return null;
      }
      month = month <= 0 ? 1 : month;
      day = day <= 0 ? 1 : day;
      return LocalDate.of(year, month, day).toString();
    } catch (RuntimeException e) {
      LOGGER.debug("NamedTransformFunctions.parse_partial_date_ymd: cannot parse '{}'", raw);
      return null;
    }
  }

  /**
   * Classifies a semicolon-separated list of special-interest-category labels into the platform's
   * canonical {@code sanctions}/{@code pep}/{@code adverse_media}/{@code enforcement} vocabulary,
   * dropping unrecognised entries and de-duplicating - incorporated 2026-09-14 from fei's own
   * {@code map_worldcheck_categories} (ported verbatim, same lookup table). See this class's own
   * javadoc for why this is now here rather than staying downstream.
   */
  public static List<String> classify_party_categories(String raw) {
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
            "NamedTransformFunctions.classify_party_categories: unmapped category '{}' - skipping",
            value);
        yield null;
      }
    };
  }

  /**
   * Parses a semicolon-separated list of {@code "~ City, Region ~ Country"}-shaped free-text
   * location entries into structured location objects - incorporated 2026-09-14 from fei's own
   * {@code parse_worldcheck_locations} (ported logic verbatim: same regex, same city/country
   * presence rules, same {@code UNKNOWN}-country skip). <b>One deliberate deviation from the
   * origin</b>: the origin's own output keys are camelCase ({@code stateOrProvince}, {@code
   * countryName}, {@code countryCode}, {@code locationType}) because its caller coerces the result
   * into a typed Java model whose own {@code @JsonProperty} annotations translate camelCase Java
   * fields to the real index's snake_case wire names before anything is ever written to OpenSearch.
   * This module's own generic runner has no such coercion step - it writes a rule's resolved value
   * as-is - so against a real, {@code "dynamic": "strict"} WorldCheck index (confirmed from {@code
   * entity-intelligence-specifications}' own checked-in {@code
   * screening-records-worldcheck-opensearch-indexing-spec.json}, whose own {@code
   * locations.properties} are {@code country_code}/{@code country_name}/{@code
   * state_or_province}/{@code location_type} - confirmed as the org's real, consistent convention
   * against {@code resolved-entity-opensearch-indexing-spec.json} and {@code
   * mapping-test-customer-master-customer-master.yaml} too, not this one spec's own quirk), a
   * camelCase key would be rejected outright rather than silently dropped. This function's output
   * keys are snake_case for exactly that reason - the parsing/matching logic itself is unchanged.
   */
  public static List<Map<String, Object>> parse_tilde_delimited_locations(String raw) {
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
        LOGGER.debug(
            "NamedTransformFunctions.parse_tilde_delimited_locations: no match for '{}'", trimmed);
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
      location.put("country_code", resolve_iso2_country(countryName));
      location.put("location_type", "REGISTERED");
      result.add(location);
    }
    return result;
  }

  /**
   * Classifies a Customer Master customer-master row's own {@code ENTITYS_TYPE} value into the
   * shared person/organization party-kind taxonomy (see {@link #classify_party_kind}'s own javadoc
   * for that taxonomy's cross-provider reasoning). Genuinely Customer-Master-specific, unlike
   * {@code classify_party_kind}/{@code classify_party_category}: {@code ENTITYS_TYPE}'s own literal
   * values ({@code "Mutual Fund"}, {@code "Hedge Fund & Private Equity"}, {@code "Trust (Corp &
   * Ind)"}, ...) are this source's own KYC entity-type vocabulary, not a shared cross-provider
   * convention - the real distinct values (confirmed against the real POV data, not the schema
   * doc's own incomplete comment) are exactly one person-shaped value ({@code "Individual"}) plus a
   * fixed set of organization-shaped ones. No Customer Master customer-master row is a {@code
   * physical_asset}.
   */
  public static String classify_party_kind_test_customer_master(Map<String, String> inputs) {
    String entityType = inputs.get("value");
    if (isBlank(entityType)) {
      return "unknown";
    }
    return "individual".equalsIgnoreCase(entityType.trim()) ? "person" : "organization";
  }

  /**
   * Combines a Customer Master customer-master row's own 3 structured location blocks ({@code
   * address_line_1}/{@code _2}, {@code city}, {@code state_or_province}, {@code country} per
   * position, keyed {@code <field>_1}/{@code _2}/{@code _3}) into the real target {@code locations}
   * nested shape ({@code address}/{@code city}/{@code state_or_province}/{@code country_code}/
   * {@code country_name}/{@code location_type}) in one call. Unlike {@link
   * #parse_tilde_delimited_locations}, Customer Master's own source already has structured location
   * columns rather than one free-text field to parse, so this transform's own job is assembly, not
   * parsing. Location 1/2/3 map to {@code REGISTERED}/{@code MAILING}/{@code OTHER} respectively,
   * mirroring {@code entity-intelligence-specifications}' own {@code
   * mapping-test-customer-master-customer-master.yaml}. A position whose own 5 source columns are
   * all blank contributes no element (not an all-null placeholder object).
   */
  public static List<Map<String, Object>> build_test_customer_master_locations(
      Map<String, String> inputs) {
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
    location.put("country_code", resolve_iso2_country(country));
    location.put("location_type", locationType);
    result.add(location);
  }

  /**
   * Combines Customer Master's own 3 identifier sources - {@code business_entity_record_id} (fixed
   * scheme {@code TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID}), {@code gems_id} (fixed scheme
   * {@code TEST_CUSTOMER_MASTER_KYC_ID}), and {@code entity_public_identifier} (semicolon-separated
   * {@code {SCHEME}value} pairs, parsed via {@link #parse_identifier_pairs}) - into one target
   * {@code identifiers} list in a single call. Needed because {@code FieldMappingEngine}'s own
   * {@code repeated}+{@code metadata} accumulation (see its own javadoc) only composes several
   * single-valued rules that all share one fixed per-rule tag; it has no way to also merge in a
   * rule whose own transform already returns a fully-formed, variably-tagged list (each {@link
   * #parse_identifier_pairs} result carries its own scheme, not a fixed one) onto the same target
   * without either silently overwriting or double-wrapping it - so this one call produces the whole
   * list directly instead, the same "one rule returns the whole target list" shape {@link
   * #parse_tilde_delimited_locations}/{@link #build_test_customer_master_locations} already use.
   */
  public static List<Map<String, Object>> build_test_customer_master_identifiers(
      Map<String, String> inputs) {
    List<Map<String, Object>> result = new ArrayList<>();
    String businessEntityRecordId = inputs.get("business_entity_record_id");
    if (!isBlank(businessEntityRecordId)) {
      result.add(
          customerMasterIdentifier(
              "TEST_CUSTOMER_MASTER_BUSINESS_ENTITY_RECORD_ID", businessEntityRecordId, null));
    }
    String gemsId = inputs.get("gems_id");
    if (!isBlank(gemsId)) {
      result.add(customerMasterIdentifier("TEST_CUSTOMER_MASTER_KYC_ID", gemsId, null));
    }
    for (ParsedIdentifier parsed : parse_identifier_pairs(inputs.get("entity_public_identifier"))) {
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
   * Joins several optional, labeled source values into one free-text string, skipping any that are
   * blank, in the order {@code inputs} itself declares (preserving {@code
   * TransformSpec.FieldRule}'s own YAML {@code inputs:} key order) - e.g. {@code "Occupation:
   * Banker | Classification: Customer"}. Generic multi-labeled-field concatenation, not tied to any
   * one source; each {@code inputs} key is used verbatim as the label. Distinct from {@code
   * template} (also available on {@code FieldRule}) because {@code template} resolves to null if
   * ANY one placeholder is blank - the right behavior for a single combined value like a name,
   * wrong for a "keep whichever of several optional fields happen to be populated" free-text
   * summary, which this exists for - needed by Customer Master's own {@code further_information} (7
   * optional business-relationship fields, legacy-mapped with a template that assumes partial
   * fill), not itself Customer-Master-specific.
   */
  public static String join_labeled_fields(Map<String, String> inputs) {
    StringBuilder joined = new StringBuilder();
    for (Map.Entry<String, String> entry : inputs.entrySet()) {
      if (isBlank(entry.getValue())) {
        continue;
      }
      if (!joined.isEmpty()) {
        joined.append(" | ");
      }
      joined.append(entry.getKey()).append(": ").append(entry.getValue());
    }
    return joined.isEmpty() ? null : joined.toString();
  }

  private static final EntityMatcher WORLDCHECK_ENTITY_MATCHER = new EntityMatcher();

  /**
   * The real sanctions/PEP screening step every onboarded Customer Master customer row goes through
   * in this org's actual business use case: score the row's own name(s) against a WorldCheck-shaped
   * reference population using {@code forwardmeasure-entity-matching-core}'s real,
   * production-validated {@link EntityMatcher} - the same scorer {@code
   * entity-intelligence-resolution} uses for real recall/matching, not a hand-rolled comparison
   * built for this transform alone.
   *
   * <p>Genuinely {@code HEAVY}, not just labeled that way: every candidate row must be compared
   * against every reference entity (a real broadcast-join shape - the reference population is the
   * "small" side, the ingested dataset the "large" side), and each comparison itself runs {@link
   * EntityMatcher}'s own real fuzzy name/date/geography/identifier scoring, not a cheap equality
   * check. At this fixture's own reference-population size that cost is trivial; at a real
   * WorldCheck population's real size (tens to hundreds of thousands of entities) it is exactly the
   * kind of bulk, CPU-bound, per-row-times-reference-population work {@code
   * TransformCharacteristics.ExecutionCost#HEAVY}'s own javadoc describes - the mechanism's shape
   * is what earns the classification, not this particular reference list's row count (matching how
   * {@code WorldCheckFixtures}/{@code TestCustomerMasterFixtures} throughout this codebase are
   * already small, deterministic stand-ins for a much larger real dataset).
   *
   * <p>{@code WORLDCHECK_REFERENCE_POPULATION} below is a real, structurally-faithful stand-in, not
   * a placeholder in the "TODO, fill in later" sense: fictional names/geographies in the exact
   * WorldCheck row shape, deliberately reusing this repo's own already-reviewed fictional fixture
   * identities (Jos&#233; Smith, Petr Ivanov, Acme Holdings, MV Example Star - see {@code
   * WorldCheckFixtures#SAMPLE_TSV} in {@code forwardmeasure-data-streaming-test-fixtures}) rather
   * than inventing new ones or naming any real sanctioned individual/entity in source code. A real
   * deployment replaces this static list with a real, bulk-loaded WorldCheck population read inside
   * the Spark stage this transform's own {@code HEAVY} classification triggers - that data-sourcing
   * question is deliberately out of scope here; this transform's job is proving the real scoring
   * mechanism and the real dispatch path it requires, not sourcing production reference data.
   *
   * @param inputs {@code full_name}/{@code first_name}/{@code last_name}/{@code alias} (subject
   *     name candidates - at least one non-blank value required), {@code entity_type} ({@code
   *     "Individual"} maps to {@link EntityKind#PERSON}, anything else non-blank to {@link
   *     EntityKind#ORGANIZATION}, blank to {@link EntityKind#UNKNOWN} - same rule as {@link
   *     #classify_party_kind_test_customer_master}), {@code country}/{@code nationality} (ISO
   *     country names, geography evidence), {@code dob} ({@code yyyyMMdd}, parsed via {@link
   *     #parse_yyyymmdd}).
   * @return one entry per reference entity {@link EntityMatcher} judged a real match ({@code
   *     reference_uid}/{@code matched_subject_name}/{@code matched_reference_name}/{@code
   *     composite_score}/{@code decision_rule}) - empty (never null) when no name candidate is
   *     usable or nothing matched.
   */
  public static List<Map<String, Object>> screen_against_worldcheck_reference(
      Map<String, String> inputs) {
    EntityEvidence subject = subjectEvidenceFor(inputs);
    if (subject.names().isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> hits = new ArrayList<>();
    for (ReferenceEntity reference : WORLDCHECK_REFERENCE_POPULATION) {
      EntityMatchResult result =
          WORLDCHECK_ENTITY_MATCHER.match(
              new MatchRequest(subject, reference.evidence(), null, null));
      if (result.matched()) {
        Map<String, Object> hit = new LinkedHashMap<>();
        hit.put("reference_uid", reference.uid());
        hit.put("matched_subject_name", result.matchedSubjectName());
        hit.put("matched_reference_name", result.matchedReferenceName());
        hit.put("composite_score", result.compositeScore());
        hit.put("decision_rule", result.decisionRule());
        hits.add(hit);
      }
    }
    return hits;
  }

  private static EntityEvidence subjectEvidenceFor(Map<String, String> inputs) {
    List<String> names = new ArrayList<>();
    addIfPresent(names, inputs.get("full_name"));
    String combinedGivenFamily =
        java.util.stream.Stream.of(inputs.get("first_name"), inputs.get("last_name"))
            .filter(part -> !isBlank(part))
            .collect(java.util.stream.Collectors.joining(" "));
    addIfPresent(names, combinedGivenFamily.isBlank() ? null : combinedGivenFamily);
    addIfPresent(names, inputs.get("alias"));

    String entityType = inputs.get("entity_type");
    EntityKind entityKind =
        isBlank(entityType)
            ? EntityKind.UNKNOWN
            : "individual".equalsIgnoreCase(entityType.trim())
                ? EntityKind.PERSON
                : EntityKind.ORGANIZATION;

    List<LocalDate> dates = new ArrayList<>();
    String isoDob = parse_yyyymmdd(inputs.get("dob"));
    if (isoDob != null) {
      dates.add(LocalDate.parse(isoDob));
    }

    List<String> countries = new ArrayList<>();
    addIfPresent(countries, inputs.get("country"));
    List<String> nationalities = new ArrayList<>();
    addIfPresent(nationalities, inputs.get("nationality"));

    return new EntityEvidence(
        entityKind, names, dates, nationalities, countries, List.of(), List.of());
  }

  private static void addIfPresent(List<String> values, String value) {
    if (!isBlank(value)) {
      values.add(value.trim());
    }
  }

  private record ReferenceEntity(String uid, EntityEvidence evidence) {}

  /**
   * A real, structurally-faithful WorldCheck-shaped reference population stand-in - see {@link
   * #screen_against_worldcheck_reference}'s own javadoc for why these specific fictional identities
   * were reused rather than invented fresh or drawn from any real sanctioned individual/entity.
   */
  private static final List<ReferenceEntity> WORLDCHECK_REFERENCE_POPULATION =
      List.of(
          new ReferenceEntity(
              "wc-1",
              new EntityEvidence(
                  EntityKind.PERSON,
                  List.of("José Smith", "Johnny Smith", "J. Smith"),
                  List.of(LocalDate.of(1975, 3, 15)),
                  List.of(),
                  List.of("UNITED STATES", "RUSSIA"),
                  List.of(),
                  List.of())),
          new ReferenceEntity(
              "wc-2",
              new EntityEvidence(
                  EntityKind.PERSON,
                  List.of("Petr Ivanov"),
                  List.of(LocalDate.of(1982, 7, 22)),
                  List.of(),
                  List.of("RUSSIA"),
                  List.of(),
                  List.of())),
          new ReferenceEntity(
              "wc-3",
              new EntityEvidence(
                  EntityKind.ORGANIZATION,
                  List.of("Acme Holdings"),
                  List.of(),
                  List.of(),
                  List.of("UNITED KINGDOM"),
                  List.of(),
                  List.of())),
          new ReferenceEntity(
              "wc-4",
              new EntityEvidence(
                  EntityKind.UNKNOWN,
                  List.of("MV Example Star"),
                  List.of(),
                  List.of(),
                  List.of("IRAN"),
                  List.of(),
                  List.of())));

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
