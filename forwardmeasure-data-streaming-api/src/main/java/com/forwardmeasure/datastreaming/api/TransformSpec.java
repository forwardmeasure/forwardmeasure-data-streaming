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
package com.forwardmeasure.datastreaming.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A runtime-interpreted field-mapping spec: {@code target field <- named input(s) [+ transform
 * name]}, one rule per target field. This is a direct port of {@code
 * forwardmeasure-entity-intelligence}'s own {@code MappingDefinition}/{@code FieldRule}
 * (ingestion-service-application's {@code mapping} package) - the shape is unchanged, not
 * redesigned, per D3 (corrected 2026-09-12) in the design plan: that engine already generalizes
 * data-fabric's {@code MappingConfig} into one target-schema-agnostic rule list, dispatched through
 * a flat {@code NamedTransformRegistry} with no arity-based branching. Reusing it here means a new
 * source is onboarded by writing a new YAML spec, never by generating or compiling new Java code -
 * the entire point of keeping this off MapStruct.
 *
 * <p>{@code target} at the top level names the destination schema this spec produces fields for
 * (e.g. {@code schema://party/2.0}) - informational today, not yet validated against a generated
 * model class (D1's typed-target coercion is explicitly out of scope for the build order this type
 * was introduced under; see the plan doc's §5).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TransformSpec(
    @JsonProperty("target") String target, @JsonProperty("fields") List<FieldRule> fields)
    implements Serializable {

  /**
   * One mapping rule. Exactly one of {@code source}, {@code template}, or {@code inputs} should be
   * set - mirrors {@code MappingDefinition.FieldRule} field-for-field:
   *
   * <ul>
   *   <li>{@code source} - the common single-column case; normalized internally to {@code inputs =
   *       {"value": source}}.
   *   <li>{@code template} - combine multiple columns into one string via {@code {ColumnName}}
   *       placeholders (e.g. {@code "{LAST NAME}, {FIRST NAME}"}), resolved before any transform
   *       runs.
   *   <li>{@code inputs} - the general multi-input case for a named transform that reads more than
   *       one source column (e.g. {@code classify_party_kind}'s {@code value}/{@code
   *       entity_indicator} pair).
   * </ul>
   *
   * <p>When {@code repeated} is true, every rule sharing the same {@code target} contributes one
   * element to an array at that target; otherwise a rule sets its target directly. When {@code
   * repeated} is true AND {@code metadata} is non-empty (added 2026-09-14, a real gap found trying
   * to port {@code entity-intelligence-specifications}' own real WorldCheck mapping - its {@code
   * names}/{@code identifiers} fields need each repeated element to carry its own fixed per-rule
   * tag, e.g. {@code name_type: PRIMARY} or {@code scheme: SSN}, alongside the resolved value; a
   * bare accumulated value had no way to express that), each contributed list element is a {@code
   * Map<String,Object>} - {@code metadata}'s own entries plus {@code "value"} set to the rule's
   * resolved value - instead of the bare resolved value. {@code metadata} absent/empty preserves
   * the original bare-value behavior exactly, so no existing spec's output shape changes.
   *
   * <p>{@code raw} (added 2026-09-25): when {@code true}, {@code source} is read via {@code
   * SourceRow#getRaw} (forwardmeasure-data-streaming-mappers - not linked here to avoid a circular
   * module dependency) instead of {@code SourceRow#get}, and used as the target value exactly as
   * returned - no {@code transform}, no string coercion. For a row whose field is a real structured
   * value (a {@code List<Map<String,Object>>}, e.g. an already-fully-mapped {@code names}/{@code
   * identifiers}/{@code locations} value read back off a Kafka topic a Spark stage wrote to),
   * {@code SourceRow#get}'s own {@code String.valueOf(...)} would mangle it - this is the escape
   * hatch for that one real case, not a general-purpose feature every mapping should reach for.
   * Requires {@code source} (not {@code template}/{@code inputs}) and ignores {@code transform} if
   * also set (raw mode's whole point is skipping transformation) - always paired with {@code
   * repeated: false} in practice, since a raw value is already in its own final, complete target
   * shape, not one element to accumulate into a list.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FieldRule(
      @JsonProperty("target") String target,
      @JsonProperty("source") String source,
      @JsonProperty("template") String template,
      @JsonProperty("inputs") Map<String, String> inputs,
      @JsonProperty("transform") String transform,
      @JsonProperty("optional") Boolean optional,
      @JsonProperty("repeated") Boolean repeated,
      @JsonProperty("metadata") Map<String, String> metadata,
      @JsonProperty("raw") Boolean raw)
      implements Serializable {

    public FieldRule {
      metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    /** Pre-{@code raw} shape, kept working unchanged for every existing caller. */
    public FieldRule(
        String target,
        String source,
        String template,
        Map<String, String> inputs,
        String transform,
        Boolean optional,
        Boolean repeated,
        Map<String, String> metadata) {
      this(target, source, template, inputs, transform, optional, repeated, metadata, null);
    }

    /** Pre-{@code metadata} shape, kept working unchanged for every existing caller. */
    public FieldRule(
        String target,
        String source,
        String template,
        Map<String, String> inputs,
        String transform,
        Boolean optional,
        Boolean repeated) {
      this(target, source, template, inputs, transform, optional, repeated, Map.of(), null);
    }

    public boolean isOptional() {
      return Boolean.TRUE.equals(optional);
    }

    public boolean isRepeated() {
      return Boolean.TRUE.equals(repeated);
    }

    public boolean isRaw() {
      return Boolean.TRUE.equals(raw);
    }

    /** Named inputs this rule actually reads, normalizing the {@code source} shorthand. */
    public Map<String, String> effectiveInputs() {
      if (inputs != null && !inputs.isEmpty()) {
        return inputs;
      }
      if (source != null) {
        return Map.of("value", source);
      }
      return Map.of();
    }
  }
}
