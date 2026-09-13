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
package com.forwardmeasure.datastreaming.executor.pekko;

import com.forwardmeasure.datastreaming.mappers.SourceRow;
import org.apache.commons.csv.CSVRecord;

/**
 * Adapts a Commons CSV {@link CSVRecord} to {@link SourceRow} - ported verbatim from {@code
 * forwardmeasure-entity-intelligence}'s own {@code CsvSourceRow} (D2: the mapping engine itself has
 * zero CSV dependency; this is the one, deliberately thin, glue class a CSV-backed executor needs).
 */
public final class CsvSourceRow implements SourceRow {

  private final CSVRecord record;

  public CsvSourceRow(CSVRecord record) {
    this.record = record;
  }

  @Override
  public String get(String fieldName) {
    if (fieldName == null || !record.isMapped(fieldName) || !record.isSet(fieldName)) {
      return null;
    }
    String value = record.get(fieldName);
    return value == null || value.isBlank() ? null : value.strip();
  }
}
