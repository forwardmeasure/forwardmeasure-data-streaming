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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

class CsvSourceRowTest {
  @Test
  void repeatedExportHeadersReadTheirFirstColumn() throws IOException {
    var row = firstRow("id|name|country|name|country\n42|Original|US|Current|GB\n");
    assertEquals(Map.of("id", "42", "name", "Original", "country", "US"), row.rawFields());
    assertEquals("Original", row.get("name"));
    assertEquals("US", row.get("country"));
  }

  @Test
  void aBlankLaterCopyDoesNotHideTheOriginalValue() throws IOException {
    var row = firstRow("id|name|name\n42|Original|\n");
    assertEquals("Original", row.get("name"));
    assertEquals(Map.of("id", "42", "name", "Original"), row.rawFields());
  }

  @Test
  void unnamedColumnsAreSkippedButStillCountTowardTheRecordWidth() throws IOException {
    var row = firstRow("id|name||  |\n42|Name|x|y|\n");
    assertEquals(List.of("id", "name"), List.copyOf(row.rawFields().keySet()));
    assertNull(row.get(""));
    assertNull(row.get("  "));
    for (String data : new String[] {"42|Name|x|y", "42|Name|x|y||extra"}) {
      var malformed = firstRow("id|name||  |\n" + data + "\n");
      assertThrows(IllegalArgumentException.class, malformed::rawFields);
    }
  }

  @Test
  void physicalWidthRejectsBothMissingAndExtraCellsEvenWithRepeatedHeaders() throws IOException {
    for (String data : new String[] {"42|Name|US", "42|Name|US|Name|US|extra"}) {
      var row = firstRow("id|name|country|name|country\n" + data + "\n");
      assertThrows(IllegalArgumentException.class, row::rawFields);
    }
  }

  @Test
  void emptyTrailingCellsRemainPresentAndOptionalLookupsRemainEmpty() throws IOException {
    var row = firstRow("id|name|country\n42|  Name  |\n");
    assertEquals(Map.of("id", "42", "name", "  Name  ", "country", ""), row.rawFields());
    assertEquals("Name", row.get("name"));
    assertNull(row.get("country"));
    assertNull(row.get("missing"));
    assertNull(row.get(null));
  }

  @Test
  void aShortRecordReadsMissingTrailingFieldsAsAbsent() throws IOException {
    var row = firstRow("id|name|country\n42|Name\n");
    assertEquals("Name", row.get("name"));
    assertNull(row.get("country"));
  }

  /** Reads the header the way StreamingTextRows does: as the file's first record. */
  private static CsvSourceRow firstRow(String text) throws IOException {
    try (CSVParser parser =
        CSVParser.parse(text, CSVFormat.DEFAULT.builder().setDelimiter('|').get())) {
      List<CSVRecord> records = parser.getRecords();
      return new CsvSourceRow(CsvHeader.of(records.get(0)), records.get(1));
    }
  }
}
