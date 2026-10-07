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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapSourceRowTest {

  @Test
  void completeRecordPreservesUnknownColumnsNullsAndNativeTypesForValidation() {
    Map<String, Object> record = new java.util.LinkedHashMap<>();
    record.put("unknown_vendor_field", java.util.List.of(12, 34));
    record.put("explicit_null", null);
    record.put("blank", "");
    assertEquals(record, new MapSourceRow(record).rawFields());
  }

  @Test
  void returnsTheStringifiedValueForAPresentField() {
    SourceRow row = new MapSourceRow(Map.of("ID", "S1", "AGE", 42));

    assertEquals("S1", row.get("ID"));
    assertEquals("42", row.get("AGE"));
  }

  @Test
  void returnsNullForAMissingField() {
    SourceRow row = new MapSourceRow(Map.of("ID", "S1"));

    assertNull(row.get("MISSING"));
  }

  @Test
  void returnsNullForANullFieldNameOrANullOrBlankValue() {
    Map<String, Object> underlying = new HashMap<>();
    underlying.put("ID", "S1");
    underlying.put("NAME", null);
    underlying.put("NOTES", "   ");
    SourceRow row = new MapSourceRow(underlying);

    assertNull(row.get(null));
    assertNull(row.get("NAME"));
    assertNull(row.get("NOTES"));
  }

  /**
   * {@code getRaw} exists precisely because {@code get}'s own {@code String.valueOf(...)} mangles a
   * structured value - contrasted directly here so the difference is unmistakable.
   */
  @Test
  void getRawReturnsAStructuredValueUnchangedWhileGetStringifiesIt() {
    java.util.List<Map<String, Object>> names =
        java.util.List.of(Map.of("value", "Steven DOSHAY", "name_type", "PRIMARY"));
    SourceRow row = new MapSourceRow(Map.of("names", names));

    assertEquals(names, row.getRaw("names"));
    assertEquals(names.toString(), row.get("names"));
  }

  @Test
  void getRawReturnsNullForAMissingOrNullFieldName() {
    Map<String, Object> underlying = new HashMap<>();
    underlying.put("ID", "S1");
    underlying.put("NAME", null);
    SourceRow row = new MapSourceRow(underlying);

    assertNull(row.getRaw(null));
    assertNull(row.getRaw("MISSING"));
    assertNull(row.getRaw("NAME"));
  }
}
