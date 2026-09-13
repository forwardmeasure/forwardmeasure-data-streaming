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
package com.forwardmeasure.datastreaming.executor.spark;

import com.forwardmeasure.datastreaming.mappers.SourceRow;
import org.apache.spark.sql.Row;

/**
 * Adapts a Spark {@link Row} to FDS's {@link SourceRow} - ported verbatim from fei's own {@code
 * SparkSourceRow} (D3, corrected): {@link
 * com.forwardmeasure.datastreaming.mappers.FieldMappingEngine} needs zero Spark-awareness of its
 * own, the same way it needs zero CSV-awareness on the Pekko path.
 *
 * <p>Deliberately not {@link java.io.Serializable}: an instance is created and consumed entirely
 * inside one partition's own {@code mapPartitions} loop, never crossing a shuffle boundary.
 */
public final class SparkSourceRow implements SourceRow {

  private final Row row;

  public SparkSourceRow(Row row) {
    this.row = row;
  }

  @Override
  public String get(String fieldName) {
    int index;
    try {
      index = row.fieldIndex(fieldName);
    } catch (IllegalArgumentException notPresent) {
      return null;
    }
    if (row.isNullAt(index)) {
      return null;
    }
    Object value = row.get(index);
    String stringValue = String.valueOf(value);
    return stringValue.isBlank() ? null : stringValue;
  }
}
