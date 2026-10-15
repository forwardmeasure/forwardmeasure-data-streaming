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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvHeaderColumnsIntegrationTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("csv-header-columns-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) spark.stop();
  }

  @Test
  void providerExportWithTrailingCopiesAndATrailingDelimiterReadsTheOriginalColumns(
      @TempDir Path directory) throws IOException {
    Path file = directory.resolve("customer-master.psv");
    Files.writeString(
        file,
        "UID|FULL_NAME|CITY|PLACE_OF_BIRTH|FULL_NAME|CITY|\n"
            + "1|Original Name|Boston|Paris|Later Name||\n"
            + "2|Second Name|Denver||||\n");

    Dataset<Row> rows = read(file, Map.of("delimiter", "|"));

    assertEquals(List.of("UID", "FULL_NAME", "CITY", "PLACE_OF_BIRTH"), List.of(rows.columns()));
    List<Row> collected = rows.orderBy("UID").collectAsList();
    assertEquals("Original Name", collected.get(0).getAs("FULL_NAME"));
    assertEquals("Boston", collected.get(0).getAs("CITY"));
    assertEquals("Second Name", collected.get(1).getAs("FULL_NAME"));
    assertEquals("Denver", collected.get(1).getAs("CITY"));
  }

  @Test
  void aHeaderWithoutRepeatsOrBlanksKeepsEveryColumnUnchanged(@TempDir Path directory)
      throws IOException {
    Path file = directory.resolve("plain.csv");
    Files.writeString(file, "id,name\n1,Alice\n");

    Dataset<Row> rows = read(file, Map.of());

    assertEquals(List.of("id", "name"), List.of(rows.columns()));
    assertEquals("Alice", rows.first().getAs("name"));
  }

  @Test
  void anEmptyFileIsLeftAsSparkReadIt(@TempDir Path directory) throws IOException {
    Path file = directory.resolve("empty.csv");
    Files.writeString(file, "");

    assertEquals(0, read(file, Map.of()).count());
  }

  private static Dataset<Row> read(Path file, Map<String, String> extra) {
    Map<String, String> options = new HashMap<>(extra);
    options.put("header", "true");
    Dataset<Row> rows = spark.read().format("csv").options(options).load(file.toString());
    return CsvHeaderColumns.firstOccurrence(spark, rows, options, file.toString());
  }
}
