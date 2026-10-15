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

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * The named transforms a mapping engine or planner can use: FDS's own generic built-ins plus
 * whatever a product registers. Immutable; {@link #with(Map)} returns a new catalog. A name can be
 * registered only once, so a product can never silently replace a built-in or another product's
 * transform.
 */
public final class NamedTransformCatalog implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final NamedTransformCatalog BUILT_INS =
      new NamedTransformCatalog(Map.of()).withAll(builtInTransforms(), "built-in");

  private final Map<String, NamedTransform> transforms;

  private NamedTransformCatalog(Map<String, NamedTransform> transforms) {
    this.transforms = Map.copyOf(transforms);
  }

  /** FDS's generic, domain-free transforms only. */
  public static NamedTransformCatalog builtIns() {
    return BUILT_INS;
  }

  /**
   * The built-ins plus every {@link NamedTransformProvider} on the classpath. This is the catalog
   * an FDS engine uses when its caller passes none. Discovered once per JVM.
   */
  public static NamedTransformCatalog installed() {
    return Installed.CATALOG;
  }

  private static final class Installed {
    private static final NamedTransformCatalog CATALOG = discover();
  }

  private static NamedTransformCatalog discover() {
    NamedTransformCatalog catalog = BUILT_INS;
    for (NamedTransformProvider provider :
        ServiceLoader.load(
            NamedTransformProvider.class, NamedTransformCatalog.class.getClassLoader())) {
      catalog = catalog.withAll(provider.transforms(), provider.getClass().getName());
    }
    return catalog;
  }

  /** This catalog plus {@code additional}; fails if any name is already registered. */
  public NamedTransformCatalog with(Map<String, NamedTransform> additional) {
    return withAll(additional, "caller");
  }

  private NamedTransformCatalog withAll(Map<String, NamedTransform> additional, String source) {
    if (additional.isEmpty()) {
      return this;
    }
    Map<String, NamedTransform> merged = new LinkedHashMap<>(transforms);
    for (Map.Entry<String, NamedTransform> entry : additional.entrySet()) {
      if (merged.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
        throw new IllegalArgumentException(
            "named transform '" + entry.getKey() + "' from " + source + " is already registered");
      }
    }
    return new NamedTransformCatalog(merged);
  }

  public NamedTransform get(String name) {
    NamedTransform transform = transforms.get(name);
    if (transform == null) {
      throw new IllegalArgumentException("unknown named transform: " + name);
    }
    return transform;
  }

  public TransformCharacteristics characteristics(String name) {
    return get(name).characteristics();
  }

  public boolean contains(String name) {
    return transforms.containsKey(name);
  }

  public Set<String> names() {
    return transforms.keySet();
  }

  private static Map<String, NamedTransform> builtInTransforms() {
    return Map.ofEntries(
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
            "parse_partial_date_ymd",
            inputs -> NamedTransformFunctions.parse_partial_date_ymd(inputs.get("value"))),
        Map.entry("join_labeled_fields", NamedTransformFunctions::join_labeled_fields));
  }
}
