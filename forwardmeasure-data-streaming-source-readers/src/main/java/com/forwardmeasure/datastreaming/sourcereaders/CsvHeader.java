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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;

/**
 * A CSV file's header line: which physical column each field name reads. A name repeated later in
 * the header reads its first column; a column with no name is skipped. Provider exports append
 * copies of earlier columns and end lines with a delimiter, so neither may change what a name means
 * or stop the file from being read. The file itself remains the record of every column.
 */
final class CsvHeader {
  static final CsvHeader EMPTY = new CsvHeader(0, Map.of());

  private final int width;
  private final Map<String, Integer> columns;

  private CsvHeader(int width, Map<String, Integer> columns) {
    this.width = width;
    this.columns = columns;
  }

  static CsvHeader of(CSVRecord header) {
    Map<String, Integer> columns = new LinkedHashMap<>();
    for (int i = 0; i < header.size(); i++) {
      String name = header.get(i);
      if (name != null && !name.isBlank()) columns.putIfAbsent(name, i);
    }
    return new CsvHeader(header.size(), Collections.unmodifiableMap(columns));
  }

  /** Physical column count, unnamed and repeated columns included. */
  int width() {
    return width;
  }

  /** Each field name with the column it reads, in header order. */
  Map<String, Integer> columns() {
    return columns;
  }
}
