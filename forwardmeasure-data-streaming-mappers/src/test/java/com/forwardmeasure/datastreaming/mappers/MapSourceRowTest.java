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
}
