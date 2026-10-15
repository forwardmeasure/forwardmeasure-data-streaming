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

import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies a {@link TransformSpec} to a {@link SourceRow}, producing a schema-agnostic {@code
 * Map<String, Object>} - one entry per resolved target field. Ported from {@code
 * forwardmeasure-entity-intelligence}'s own {@code GenericRecordMapper} (D3, corrected): the field
 * resolution and named-transform dispatch logic is unchanged, only the output type differs. The
 * origin coerces its resolved values into a Protobuf {@code DynamicMessage} against a runtime
 * {@code Descriptor}; that coercion step - and the typed-target problem it exists to solve - is
 * D1's separate, explicitly-deferred concern (see the design plan's §5). This engine stops one step
 * earlier, at the resolved-and-transformed value, deliberately: a caller that needs a typed target
 * (Protobuf today, an OpenAPI-generated model once D1 lands) coerces this map's values itself.
 *
 * <p>Field resolution order per {@link FieldRule}, mirroring the origin: {@code template}
 * (multi-column string combination) wins over {@code source}/{@code inputs} (single- or multi-input
 * transform). A rule whose resolved value is null/blank/empty is skipped silently if {@link
 * FieldRule#isOptional()}, logged at debug otherwise - matches the origin's own "missing required
 * field" logging, not a hard failure. When {@link FieldRule#isRepeated()} is true, every rule
 * sharing the same {@code target} contributes one element to a {@code List} at that target, per
 * {@link FieldRule}'s own documented semantics.
 *
 * <p>Transform names resolve against the {@link NamedTransformCatalog} this engine was built with:
 * {@link NamedTransformCatalog#installed()} by default, or a caller's own catalog carrying its
 * product's transforms.
 */
public final class FieldMappingEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(FieldMappingEngine.class);

  private static final Pattern TEMPLATE_PLACEHOLDER = Pattern.compile("\\{([^}]+)}");

  /** First-N-then-every-Mth sampling for {@code record.transformed} - see {@link #map} below. */
  private static final long SAMPLE_FIRST_N = 5;

  private static final long SAMPLE_EVERY_M = 1000;

  private final AtomicLong callCount = new AtomicLong();

  private final NamedTransformCatalog catalog;

  /** An engine using {@link NamedTransformCatalog#installed()}. */
  public FieldMappingEngine() {
    this(NamedTransformCatalog.installed());
  }

  public FieldMappingEngine(NamedTransformCatalog catalog) {
    this.catalog = java.util.Objects.requireNonNull(catalog, "catalog");
  }

  public Map<String, Object> map(SourceRow row, TransformSpec spec) {
    Map<String, Object> result = new LinkedHashMap<>();
    if (spec.fields() == null) {
      return result;
    }

    for (FieldRule rule : spec.fields()) {
      Object value = resolveValue(row, rule);
      if (isEffectivelyEmpty(value)) {
        if (!rule.isOptional()) {
          LOGGER.debug(
              "FieldMappingEngine: required field '{}' resolved to no value", rule.target());
        }
        continue;
      }

      if (rule.isRepeated()) {
        @SuppressWarnings("unchecked")
        List<Object> accumulated =
            (List<Object>) result.computeIfAbsent(rule.target(), key -> new ArrayList<>());
        if (rule.metadata().isEmpty()) {
          accumulated.add(value);
        } else if (value instanceof List<?> multiValue) {
          // A transform like parse_semicolon_list already returns a List (e.g. every alias) from
          // one rule - each element gets its own tagged entry, not one entry whose own "value" is
          // the whole list; that would bury every alias inside a single opaque array instead of
          // giving each one the same real {value, name_type}-shaped object a single-valued rule
          // gets.
          for (Object element : multiValue) {
            accumulated.add(withMetadata(rule, element));
          }
        } else {
          accumulated.add(withMetadata(rule, value));
        }
      } else {
        result.put(rule.target(), value);
      }
    }

    logSampledTransform(result);
    return result;
  }

  /** Reports sampled output counts without logging source or transformed personal data. */
  private void logSampledTransform(Map<String, Object> after) {
    if (!LOGGER.isInfoEnabled()) {
      return;
    }
    long n = callCount.incrementAndGet();
    if (n > SAMPLE_FIRST_N && n % SAMPLE_EVERY_M != 0) {
      return;
    }
    LOGGER.info("record.transformed n={} output_fields={}", n, after.size());
  }

  /**
   * {@code rule.metadata()}'s own entries plus {@code "value"} - see {@link FieldRule}'s own
   * javadoc for why a repeated field sometimes needs each element to carry a fixed per-rule tag
   * (e.g. {@code name_type}/{@code scheme}) alongside the resolved value, not just the bare value.
   */
  private static Map<String, Object> withMetadata(FieldRule rule, Object value) {
    Map<String, Object> element = new LinkedHashMap<>(rule.metadata());
    element.put("value", value);
    return element;
  }

  private Object resolveValue(SourceRow row, FieldRule rule) {
    if (rule.isRaw()) {
      if (rule.source() == null) {
        throw new IllegalArgumentException(
            "FieldMappingEngine: raw field rule for target '"
                + rule.target()
                + "' must set 'source' - raw mode has no template/inputs form");
      }
      return row.getRaw(rule.source());
    }
    if (rule.template() != null) {
      String resolved = resolveTemplate(row, rule.template());
      // Map.of rejects a null value outright; every ported transform already treats "" the same
      // as null (isBlank() guards), so this is a null-safety fix, not a behavior change.
      return rule.transform() != null
          ? applyTransform(rule.transform(), Map.of("value", resolved == null ? "" : resolved))
          : resolved;
    }

    Map<String, String> namedInputs = resolveNamedInputs(row, rule.effectiveInputs());
    if (rule.transform() != null) {
      return applyTransform(rule.transform(), namedInputs);
    }
    return namedInputs.get("value");
  }

  private Map<String, String> resolveNamedInputs(SourceRow row, Map<String, String> inputSpec) {
    Map<String, String> resolved = new LinkedHashMap<>();
    inputSpec.forEach(
        (logicalName, sourceField) -> resolved.put(logicalName, row.get(sourceField)));
    return resolved;
  }

  private Object applyTransform(String transformName, Map<String, String> namedInputs) {
    return catalog.get(transformName).apply(namedInputs);
  }

  private String resolveTemplate(SourceRow row, String template) {
    StringBuilder result = new StringBuilder();
    Matcher matcher = TEMPLATE_PLACEHOLDER.matcher(template);
    while (matcher.find()) {
      String columnName = matcher.group(1);
      String value = row.get(columnName);
      if (value == null || value.isBlank()) {
        return null;
      }
      matcher.appendReplacement(result, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(result);
    String resolved = result.toString().trim();
    return resolved.isBlank() ? null : resolved;
  }

  private boolean isEffectivelyEmpty(Object value) {
    if (value == null) {
      return true;
    }
    if (value instanceof String s) {
      return s.isBlank();
    }
    if (value instanceof java.util.Collection<?> c) {
      return c.isEmpty();
    }
    return false;
  }
}
