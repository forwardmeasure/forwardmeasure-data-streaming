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

import java.util.Map;

/**
 * Adapts a plain {@code Map<String, ?>} to {@link SourceRow} - added 2026-09-13 alongside the
 * connector architecture work: unlike {@code CsvSourceRow} (Commons CSV-specific) or {@code
 * SparkSourceRow} (Spark {@code Row}-specific), several genuinely different sources already hand
 * back map-shaped rows with zero marshalling work of this library's own - a JSON object per line, a
 * {@code camel-sql} consumer's {@code List<Map<String,Object>>} result set, a Kafka message whose
 * value was already JSON-deserialized - so one adapter here, not one per caller, is the right
 * amount of code (see D5/the connector architecture plan's own "metadata over a bespoke class per
 * connector" principle - this is the one small piece of glue every map-shaped source reuses, not a
 * per-connector class).
 */
public final class MapSourceRow implements SourceRow {

  private final Map<String, ?> row;

  public MapSourceRow(Map<String, ?> row) {
    this.row = row;
  }

  @Override
  public String get(String fieldName) {
    if (fieldName == null || !row.containsKey(fieldName)) {
      return null;
    }
    Object value = row.get(fieldName);
    if (value == null) {
      return null;
    }
    String stringValue = String.valueOf(value);
    return stringValue.isBlank() ? null : stringValue.strip();
  }
}
