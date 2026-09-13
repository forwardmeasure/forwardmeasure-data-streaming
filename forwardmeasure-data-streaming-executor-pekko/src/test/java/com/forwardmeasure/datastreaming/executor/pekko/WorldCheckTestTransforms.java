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
package com.forwardmeasure.datastreaming.executor.pekko;

import java.time.LocalDate;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Test-only: the two WorldCheck-specific named transforms {@code
 * mapping/worldcheck-simple-ingestion.yaml} (fei's own real fixture, used by {@code
 * SimpleSourceIngestionWorkerIntegrationTest}) actually references - ported verbatim from fei's
 * {@code NamedTransformFunctions}. Deliberately not in the shared {@code
 * forwardmeasure-data-streaming-transforms} module (D3/D7: domain-specific transforms belong
 * downstream) - this class exists solely to exercise {@link FieldMappingEngine}'s supplemental-
 * registry extension point for {@link WorldCheckParityIntegrationTest}'s real parity proof.
 */
final class WorldCheckTestTransforms {

  private WorldCheckTestTransforms() {}

  static String map_worldcheck_category(String category) {
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
      default -> "unknown";
    };
  }

  static Object map_worldcheck_entity_kind(Map<String, String> inputs) {
    String category = inputs.get("value");
    String entityIndicator = inputs.get("entity_indicator");
    if (entityIndicator != null) {
      String indicator = entityIndicator.trim().toUpperCase(Locale.ROOT);
      if (Set.of("M", "F", "U", "I").contains(indicator)) {
        return "person";
      }
      if ("E".equals(indicator)) {
        String categoryKind = map_worldcheck_category(category);
        return "physical_asset".equals(categoryKind) ? "physical_asset" : "organization";
      }
    }
    return map_worldcheck_category(category);
  }

  static String parse_worldcheck_date(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String[] parts = raw.trim().split("/");
    if (parts.length != 3) {
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
    } catch (Exception e) {
      return null;
    }
  }
}
