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
package com.forwardmeasure.datastreaming.sourcereaders;

import com.forwardmeasure.datastreaming.mappers.SourceRow;
import org.apache.commons.csv.CSVRecord;

/**
 * Adapts a Commons CSV {@link CSVRecord} to {@link SourceRow} - ported verbatim from {@code
 * forwardmeasure-entity-intelligence}'s own {@code CsvSourceRow} (D2: the mapping engine itself has
 * zero CSV dependency; this is the one, deliberately thin, glue class a CSV-backed executor needs).
 * Field names resolve through the file's {@link CsvHeader}, not the parser's own header map, which
 * would let a repeated name read its last column.
 */
public final class CsvSourceRow implements SourceRow {

  private final CsvHeader header;
  private final CSVRecord record;

  CsvSourceRow(CsvHeader header, CSVRecord record) {
    this.header = header;
    this.record = record;
  }

  @Override
  public java.util.Map<String, ?> rawFields() {
    if (record.size() != header.width())
      throw new IllegalArgumentException("CSV column count differs from header");
    java.util.Map<String, String> fields = new java.util.LinkedHashMap<>();
    header.columns().forEach((name, column) -> fields.put(name, record.get(column)));
    return fields;
  }

  @Override
  public String get(String fieldName) {
    Integer column = fieldName == null ? null : header.columns().get(fieldName);
    if (column == null || column >= record.size()) {
      return null;
    }
    String value = record.get(column);
    return value == null || value.isBlank() ? null : value.strip();
  }
}
