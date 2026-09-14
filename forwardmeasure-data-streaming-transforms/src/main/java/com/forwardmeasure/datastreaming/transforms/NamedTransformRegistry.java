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

import java.util.Map;

/**
 * One flat {@code name -> transform} registry - adding a new named transform, single- or
 * multi-input, is always: write the pure function in {@link NamedTransformFunctions} + add one
 * entry below. No dispatcher branching by arity, ever - see {@link NamedTransform}'s own javadoc
 * for the problem this replaces. Ported from {@code forwardmeasure-entity-intelligence}'s own
 * {@code NamedTransformRegistry} (D3, corrected), restricted to the generic subset this module
 * actually contains - a consumer with domain-specific transforms builds its own registry entries
 * for those and looks up this registry as a fallback, rather than this module trying to be a
 * superset of every consumer's needs.
 *
 * <p>Every entry unpacks a {@code TransformSpec.FieldRule#effectiveInputs()}-shaped map into the
 * real function's parameters. Single-input transforms read the conventional {@code "value"} key.
 */
public final class NamedTransformRegistry {

  private static final Map<String, NamedTransform> TRANSFORMS =
      Map.ofEntries(
          Map.entry(
              "resolve_iso2_country",
              inputs -> NamedTransformFunctions.resolve_iso2_country(inputs.get("value"))),
          Map.entry(
              "normalise_iso2_country",
              inputs -> NamedTransformFunctions.normalise_iso2_country(inputs.get("value"))),
          Map.entry(
              "parse_yyyymmdd",
              inputs -> NamedTransformFunctions.parse_yyyymmdd(inputs.get("value"))),
          Map.entry(
              "parse_mmddyyyy",
              inputs -> NamedTransformFunctions.parse_mmddyyyy(inputs.get("value"))),
          Map.entry(
              "parse_identifier_pairs",
              inputs -> NamedTransformFunctions.parse_identifier_pairs(inputs.get("value"))),
          Map.entry(
              "normalise_identifier",
              inputs ->
                  NamedTransformFunctions.normalise_identifier(
                      inputs.get("scheme"), inputs.get("value"))),
          Map.entry(
              "parse_semicolon_list",
              inputs -> NamedTransformFunctions.parse_semicolon_list(inputs.get("value"))),
          Map.entry(
              "parse_tilde_list",
              inputs -> NamedTransformFunctions.parse_tilde_list(inputs.get("value"))),
          Map.entry(
              "parse_semicolon_iso2_list",
              inputs -> NamedTransformFunctions.parse_semicolon_iso2_list(inputs.get("value"))),
          Map.entry(
              "parse_url_list",
              inputs -> NamedTransformFunctions.parse_url_list(inputs.get("value"))),
          Map.entry(
              "extract_url_domains",
              inputs -> NamedTransformFunctions.extract_url_domains(inputs.get("value"))),
          Map.entry(
              "classify_party_category",
              inputs -> NamedTransformFunctions.classify_party_category(inputs.get("value"))),
          Map.entry("classify_party_kind", NamedTransformFunctions::classify_party_kind),
          Map.entry(
              "parse_partial_date_ymd",
              inputs -> NamedTransformFunctions.parse_partial_date_ymd(inputs.get("value"))),
          Map.entry(
              "classify_party_categories",
              inputs -> NamedTransformFunctions.classify_party_categories(inputs.get("value"))),
          Map.entry(
              "parse_tilde_delimited_locations",
              inputs ->
                  NamedTransformFunctions.parse_tilde_delimited_locations(inputs.get("value"))));

  private NamedTransformRegistry() {}

  public static NamedTransform get(String name) {
    NamedTransform transform = TRANSFORMS.get(name);
    if (transform == null) {
      throw new IllegalArgumentException("unknown named transform: " + name);
    }
    return transform;
  }

  public static boolean contains(String name) {
    return TRANSFORMS.containsKey(name);
  }
}
