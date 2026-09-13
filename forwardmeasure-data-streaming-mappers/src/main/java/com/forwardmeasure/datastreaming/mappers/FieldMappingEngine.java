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
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import com.forwardmeasure.datastreaming.transforms.NamedTransformRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * <p>Domain-specific named transforms (e.g. WorldCheck's own {@code map_worldcheck_entity_kind})
 * deliberately aren't in the shared {@code NamedTransformRegistry} (D3/D7: that registry is the
 * generic, cross-source pool). A caller with domain-specific transforms supplies them via the
 * {@code supplementalTransforms} map on {@link #map(SourceRow, TransformSpec, Map)} - checked
 * first, falling back to the shared registry - rather than this module ever growing source-specific
 * logic of its own.
 */
public final class FieldMappingEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(FieldMappingEngine.class);

  private static final Pattern TEMPLATE_PLACEHOLDER = Pattern.compile("\\{([^}]+)}");

  public Map<String, Object> map(SourceRow row, TransformSpec spec) {
    return map(row, spec, Map.of());
  }

  public Map<String, Object> map(
      SourceRow row, TransformSpec spec, Map<String, NamedTransform> supplementalTransforms) {
    Map<String, Object> result = new LinkedHashMap<>();
    if (spec.fields() == null) {
      return result;
    }

    for (FieldRule rule : spec.fields()) {
      Object value = resolveValue(row, rule, supplementalTransforms);
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
        accumulated.add(value);
      } else {
        result.put(rule.target(), value);
      }
    }

    return result;
  }

  private Object resolveValue(
      SourceRow row, FieldRule rule, Map<String, NamedTransform> supplementalTransforms) {
    if (rule.template() != null) {
      String resolved = resolveTemplate(row, rule.template());
      // Map.of rejects a null value outright; every ported transform already treats "" the same
      // as null (isBlank() guards), so this is a null-safety fix, not a behavior change.
      return rule.transform() != null
          ? applyTransform(
              rule.transform(),
              Map.of("value", resolved == null ? "" : resolved),
              supplementalTransforms)
          : resolved;
    }

    Map<String, String> namedInputs = resolveNamedInputs(row, rule.effectiveInputs());
    if (rule.transform() != null) {
      return applyTransform(rule.transform(), namedInputs, supplementalTransforms);
    }
    return namedInputs.get("value");
  }

  private Map<String, String> resolveNamedInputs(SourceRow row, Map<String, String> inputSpec) {
    Map<String, String> resolved = new LinkedHashMap<>();
    inputSpec.forEach(
        (logicalName, sourceField) -> resolved.put(logicalName, row.get(sourceField)));
    return resolved;
  }

  private Object applyTransform(
      String transformName,
      Map<String, String> namedInputs,
      Map<String, NamedTransform> supplementalTransforms) {
    NamedTransform transform = supplementalTransforms.get(transformName);
    if (transform == null) {
      transform = NamedTransformRegistry.get(transformName);
    }
    return transform.apply(namedInputs);
  }

  private String resolveTemplate(SourceRow row, String template) {
    StringBuilder result = new StringBuilder(template);
    Matcher matcher = TEMPLATE_PLACEHOLDER.matcher(template);
    while (matcher.find()) {
      String placeholder = matcher.group(0);
      String columnName = matcher.group(1);
      String value = row.get(columnName);
      if (value == null || value.isBlank()) {
        return null;
      }
      int idx = result.indexOf(placeholder);
      if (idx >= 0) {
        result.replace(idx, idx + placeholder.length(), value);
      }
    }
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
