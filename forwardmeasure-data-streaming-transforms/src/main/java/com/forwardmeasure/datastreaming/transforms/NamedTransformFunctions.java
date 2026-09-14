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

  private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
  private static final DateTimeFormatter MMDDYYYY = DateTimeFormatter.ofPattern("MM/dd/yyyy");

  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\{([A-Z0-9_-]+)}([^;{]+)");

  private static final Pattern URL_TOKEN_PATTERN = Pattern.compile("(?i)\\bhttps?://[^\\s;]+");

  private static final Pattern TILDE_LOCATION_PATTERN =
      Pattern.compile("~\\s*([^~,]*?)(?:,\\s*([^~]*))?\\s*~\\s*([^;~]+)");

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
   * mapping-state-street-customer-master.yaml} too, not this one spec's own quirk), a camelCase key
   * would be rejected outright rather than silently dropped. This function's output keys are
   * snake_case for exactly that reason - the parsing/matching logic itself is unchanged.
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
}
