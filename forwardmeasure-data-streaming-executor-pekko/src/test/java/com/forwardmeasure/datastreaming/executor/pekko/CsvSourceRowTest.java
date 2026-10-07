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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.Test;

class CsvSourceRowTest {
  @Test
  void repeatedExportHeadersDoNotMakeValidRowsMalformed() throws IOException {
    try (var parser = parse("id|name|country|name|country\n42|Original|US|Current|GB\n")) {
      var row = new CsvSourceRow(parser.getRecords().getFirst());
      assertEquals(Map.of("id", "42", "name", "Current", "country", "GB"), row.rawFields());
      assertEquals("Current", row.get("name"));
      assertEquals("GB", row.get("country"));
    }
  }

  @Test
  void physicalWidthRejectsBothMissingAndExtraCellsEvenWithRepeatedHeaders() throws IOException {
    for (String data : new String[] {"42|Name|US", "42|Name|US|Name|US|extra"}) {
      try (var parser = parse("id|name|country|name|country\n" + data + "\n")) {
        var row = new CsvSourceRow(parser.getRecords().getFirst());
        assertThrows(IllegalArgumentException.class, row::rawFields);
      }
    }
  }

  @Test
  void emptyTrailingCellsRemainPresentAndOptionalLookupsRemainEmpty() throws IOException {
    try (var parser = parse("id|name|country\n42|  Name  |\n")) {
      var row = new CsvSourceRow(parser.getRecords().getFirst());
      assertEquals(Map.of("id", "42", "name", "  Name  ", "country", ""), row.rawFields());
      assertEquals("Name", row.get("name"));
      assertNull(row.get("country"));
      assertNull(row.get("missing"));
      assertNull(row.get(null));
    }
  }

  private static CSVParser parse(String text) throws IOException {
    return CSVParser.parse(
        text,
        CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setDelimiter('|').get());
  }
}
