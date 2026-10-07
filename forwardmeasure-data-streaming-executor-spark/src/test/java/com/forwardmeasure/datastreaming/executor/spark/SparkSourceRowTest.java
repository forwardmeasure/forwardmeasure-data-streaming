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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

class SparkSourceRowTest {
  @Test
  void rawFieldsRetainNativeTypesNullsAndStructuredValues() {
    var nestedSchema =
        DataTypes.createStructType(
            List.of(DataTypes.createStructField("score", DataTypes.IntegerType, true)));
    var schema =
        DataTypes.createStructType(
            List.of(
                DataTypes.createStructField("active", DataTypes.BooleanType, true),
                DataTypes.createStructField("missing", DataTypes.StringType, true),
                DataTypes.createStructField("nested", nestedSchema, true),
                DataTypes.createStructField(
                    "tags", DataTypes.createArrayType(DataTypes.StringType), true)));
    var source =
        new SparkSourceRow(
            new GenericRowWithSchema(
                new Object[] {
                  true,
                  null,
                  new GenericRowWithSchema(new Object[] {42}, nestedSchema),
                  new Object[] {"one", "two"}
                },
                schema));
    assertEquals(true, source.getRaw("active"));
    assertTrue(source.rawFields().containsKey("missing"));
    assertNull(source.rawFields().get("missing"));
    assertEquals(java.util.Map.of("score", 42), source.getRaw("nested"));
    assertEquals(List.of("one", "two"), source.getRaw("tags"));
    assertNull(source.getRaw("absent"));
  }

  @Test
  void providerCollectionsAndDatesRemainJsonValuesInsteadOfScalaObjectStrings() throws Exception {
    var vendor = new java.util.LinkedHashMap<String, Object>();
    vendor.put("zero", 0);
    vendor.put("disabled", false);
    vendor.put("empty", null);
    var scalaMap = scala.jdk.javaapi.CollectionConverters.asScala(vendor);
    var scalaSequence = scala.jdk.javaapi.CollectionConverters.asScala(List.of(scalaMap)).toSeq();
    var values = new java.util.LinkedHashMap<String, Object>();
    values.put("scala_map", scalaMap);
    values.put("scala_sequence", scalaSequence);
    values.put("java_map", vendor);
    values.put("java_list", List.of(vendor));
    values.put("sql_date", java.sql.Date.valueOf("2024-02-29"));
    values.put("date", java.time.LocalDate.of(2024, 2, 29));
    values.put(
        "timestamp", java.sql.Timestamp.from(java.time.Instant.parse("2024-02-29T12:34:56Z")));
    values.put("blank", "  ");
    values.put("null", null);
    var schema =
        DataTypes.createStructType(
            values.keySet().stream()
                .map(name -> DataTypes.createStructField(name, DataTypes.StringType, true))
                .toList());
    var row = new SparkSourceRow(new GenericRowWithSchema(values.values().toArray(), schema));
    var json = new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(row.rawFields());
    assertEquals(json.path("java_map"), json.path("scala_map"));
    assertEquals(json.path("java_list"), json.path("scala_sequence"));
    assertEquals(0, json.path("scala_sequence").get(0).path("zero").intValue());
    assertEquals(false, json.path("scala_map").path("disabled").booleanValue());
    assertTrue(json.path("scala_map").path("empty").isNull());
    assertEquals("2024-02-29", json.path("sql_date").asText());
    assertEquals("2024-02-29", json.path("date").asText());
    assertEquals("2024-02-29T12:34:56Z", json.path("timestamp").asText());
    assertNull(row.get("blank"));
    assertNull(row.get("null"));
    assertNull(row.get("absent"));
    assertEquals("2024-02-29", row.get("date"));
  }
}
