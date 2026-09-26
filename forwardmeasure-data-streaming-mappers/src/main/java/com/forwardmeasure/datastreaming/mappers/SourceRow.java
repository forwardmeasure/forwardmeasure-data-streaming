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

/**
 * One raw source record's field accessor, by column/field name. Ported verbatim from {@code
 * forwardmeasure-entity-intelligence}'s own {@code SourceRow} (D2: its shape was never the problem
 * - keep it as-is). {@code org.apache.commons.csv.CSVRecord} satisfies this directly; a future
 * Spark-backed correlation-path source would satisfy it too, without this engine ever depending on
 * Spark or Commons CSV.
 */
@FunctionalInterface
public interface SourceRow {

  /** The raw string value of {@code fieldName}, or {@code null} if absent/blank. */
  String get(String fieldName);

  /**
   * The field's real, unstringified value - {@code null}/absent if the field is missing, otherwise
   * whatever object is actually stored there. Added 2026-09-25 for a genuinely different real need
   * than {@link #get}: a source whose rows already carry a structured (non-scalar) value for some
   * field - a {@code List<Map<String,Object>>}, e.g. an already-mapped {@code names}/{@code
   * identifiers}/{@code locations} value read back off a Kafka topic a Spark stage wrote to (see
   * {@code TransformSpec.FieldRule#raw()}'s own javadoc) - has no way to get that value back out
   * through {@link #get} without lossy, incorrect {@code String.valueOf(...)} stringification.
   *
   * <p>Defaults to {@link #get} unchanged for every row implementation that never needs this (CSV
   * rows, a plain Spark {@code Row}'s own scalar columns) - only a source that genuinely can hold
   * structured values (today: {@link MapSourceRow}, JSON-shaped) overrides it. Not a replacement
   * for {@link #get}; a separate, narrower escape hatch a {@code raw: true} {@code FieldRule} opts
   * into explicitly.
   */
  default Object getRaw(String fieldName) {
    return get(fieldName);
  }
}
