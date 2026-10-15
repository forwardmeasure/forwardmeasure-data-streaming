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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Gives a CSV read with a header the same field names the Pekko reader gives it: a name repeated
 * later in the header reads its first column, and a column with no name is dropped. Spark itself
 * renames every copy of a repeated name by appending its position ({@code NAME1}, {@code NAME4})
 * and calls an unnamed column {@code _c<position>}, so a mapping that reads {@code NAME} would find
 * nothing. The header line is read again without header handling to recover the names as written.
 */
final class CsvHeaderColumns {
  private CsvHeaderColumns() {}

  static Dataset<Row> firstOccurrence(
      SparkSession spark, Dataset<Row> rows, Map<String, String> options, String path) {
    Map<String, String> raw = new HashMap<>(options);
    raw.put("header", "false");
    raw.remove("inferSchema");
    List<Row> first = spark.read().format("csv").options(raw).load(path).limit(1).collectAsList();
    if (first.isEmpty()) return rows;
    Row header = first.getFirst();
    String[] columns = rows.columns();
    if (header.size() != columns.length)
      throw new IllegalStateException(
          "CSV header has "
              + header.size()
              + " columns but Spark read "
              + columns.length
              + " from "
              + path);
    Set<String> named = new HashSet<>();
    List<Column> kept = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      Object value = header.get(i);
      String name = value == null ? null : value.toString();
      if (name == null || name.isBlank() || !named.add(name)) continue;
      kept.add(rows.col("`" + columns[i].replace("`", "``") + "`").as(name));
    }
    return rows.select(kept.toArray(Column[]::new));
  }
}
