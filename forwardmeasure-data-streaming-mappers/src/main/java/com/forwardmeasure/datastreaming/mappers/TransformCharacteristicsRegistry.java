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
package com.forwardmeasure.datastreaming.mappers;

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.Cardinality;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.ExecutionCost;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.StateRequirement;
import com.forwardmeasure.datastreaming.transforms.NamedTransformRegistry;
import java.util.Map;
import java.util.Set;

/**
 * One flat {@code name -> characteristics} registry, mirroring {@link NamedTransformRegistry}'s own
 * shape - the planner (see the repo's own gap-bridging plan, "the planner" section) reads this to
 * resolve {@code DeliveryEngineKind}/whether a {@code SparkStagePlan} is needed, without the author
 * ever naming an engine. Every entry here must have a matching {@link NamedTransformRegistry} entry
 * and vice versa - see this module's own {@code TransformCharacteristicsRegistryTest}, which
 * asserts the two can't silently drift apart as new transforms are added to one but not the other.
 *
 * <p>Every transform registered today is a pure, single-row function (string/date parsing, list
 * splitting, table lookups - confirmed by reading {@code NamedTransformFunctions} directly, not
 * assumed) - {@code ONE}-cardinality/{@code STATELESS}/{@code LIGHT} for all of them. None depends
 * on row order, and all are deterministic given the same input.
 */
public final class TransformCharacteristicsRegistry {

  private static final TransformCharacteristics PURE_ROW_FUNCTION =
      new TransformCharacteristics(
          Cardinality.ONE, StateRequirement.STATELESS, ExecutionCost.LIGHT, false, true);

  private static final Map<String, TransformCharacteristics> CHARACTERISTICS =
      Map.ofEntries(
          Map.entry("resolve_iso2_country", PURE_ROW_FUNCTION),
          Map.entry("normalise_iso2_country", PURE_ROW_FUNCTION),
          Map.entry("parse_yyyymmdd", PURE_ROW_FUNCTION),
          Map.entry("parse_mmddyyyy", PURE_ROW_FUNCTION),
          Map.entry("parse_identifier_pairs", PURE_ROW_FUNCTION),
          Map.entry("normalise_identifier", PURE_ROW_FUNCTION),
          Map.entry("parse_semicolon_list", PURE_ROW_FUNCTION),
          Map.entry("parse_tilde_list", PURE_ROW_FUNCTION),
          Map.entry("parse_semicolon_iso2_list", PURE_ROW_FUNCTION),
          Map.entry("parse_url_list", PURE_ROW_FUNCTION),
          Map.entry("extract_url_domains", PURE_ROW_FUNCTION),
          Map.entry("classify_party_category", PURE_ROW_FUNCTION),
          Map.entry("classify_party_kind", PURE_ROW_FUNCTION),
          Map.entry("parse_partial_date_ymd", PURE_ROW_FUNCTION),
          Map.entry("classify_party_categories", PURE_ROW_FUNCTION),
          Map.entry("parse_tilde_delimited_locations", PURE_ROW_FUNCTION),
          Map.entry("classify_party_kind_test_customer_master", PURE_ROW_FUNCTION),
          Map.entry("build_test_customer_master_locations", PURE_ROW_FUNCTION),
          Map.entry("build_test_customer_master_identifiers", PURE_ROW_FUNCTION),
          Map.entry("join_labeled_fields", PURE_ROW_FUNCTION),
          // The one genuinely HEAVY transform in this registry - see its own javadoc in
          // NamedTransformFunctions for why (a real per-row broadcast comparison against a
          // reference population using EntityMatcher's own fuzzy scoring, not a cheap lookup).
          // This is what lets ExecutionPlanCompiler.resolveSparkStage ever produce a real
          // SparkStagePlan - deliberately not marking anything else HEAVY just to exercise that
          // path (see the repo's own "ask before deferring" discipline: this is the real, business-
          // grounded transform the user asked for instead of a synthetic placeholder).
          Map.entry(
              "screen_against_worldcheck_reference",
              new TransformCharacteristics(
                  Cardinality.ONE, StateRequirement.STATELESS, ExecutionCost.HEAVY, false, true)));

  private TransformCharacteristicsRegistry() {}

  public static TransformCharacteristics get(String name) {
    TransformCharacteristics characteristics = CHARACTERISTICS.get(name);
    if (characteristics == null) {
      throw new IllegalArgumentException("unknown named transform: " + name);
    }
    return characteristics;
  }

  public static boolean contains(String name) {
    return CHARACTERISTICS.containsKey(name);
  }

  /**
   * Every registered name - public so this module's own drift-check test can compare against {@link
   * NamedTransformRegistry#names()}.
   */
  public static Set<String> names() {
    return CHARACTERISTICS.keySet();
  }
}
