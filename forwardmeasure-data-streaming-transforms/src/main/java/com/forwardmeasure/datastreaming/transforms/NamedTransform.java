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

import java.io.Serializable;
import java.util.Map;

/**
 * One named transform - single- or multi-input, always the same shape. Ported verbatim from {@code
 * forwardmeasure-entity-intelligence}'s own {@code NamedTransform} (D3, corrected): every
 * transform, regardless of arity, is a function from its named inputs to a value; {@link
 * NamedTransformRegistry} is one flat map, no arity-based branching anywhere.
 *
 * <p>A single-input transform's one input is keyed {@code "value"} by convention (see {@code
 * com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule#effectiveInputs()}).
 *
 * <p>Extends {@link Serializable}: a caller's own domain-specific transforms (e.g. WorldCheck's
 * {@code map_worldcheck_entity_kind}) are supplied per-call as {@code supplementalTransforms} (see
 * {@code FieldMappingEngine#map(SourceRow, TransformSpec, Map)}), and on the Spark executor that
 * map crosses a real closure/shuffle boundary (D4) - a method reference or lambda only serializes
 * there if the functional interface it targets is itself declared {@code Serializable}.
 */
@FunctionalInterface
public interface NamedTransform extends Serializable {
  Object apply(Map<String, String> inputs);
}
