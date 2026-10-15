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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Rows read with no stream runtime, by the same rules the Pekko runner applies. */
class BoundedRowReaderTest {

  private static SourceSpec csv(Path file, String delimiter) {
    return new SourceSpec(
        "file",
        "file:"
            + file.getParent()
            + "?fileName="
            + file.getFileName()
            + "&noop=true&initialDelay=0&delay=100",
        new SourceSpec.FormatSpec("csv"),
        null,
        null,
        Map.of("delimiter", delimiter));
  }

  @Test
  void readsADelimitedFileInOrderWithTheRepeatedHeaderRule(@TempDir Path directory)
      throws Exception {
    Path file = directory.resolve("source.csv");
    Files.writeString(file, "id|name|name\n1|First|later\n2|Second|\n", StandardCharsets.UTF_8);
    List<Map<String, ?>> rows = new ArrayList<>();
    try (CamelBridge bridge = new CamelBridge()) {
      long count =
          BoundedRowReader.forEach(bridge, csv(file, "|"), row -> rows.add(row.rawFields()));
      assertEquals(2, count);
    }
    assertEquals(
        List.of(Map.of("id", "1", "name", "First"), Map.of("id", "2", "name", "Second")), rows);
  }

  @Test
  void readsCallerOpenedContent() throws Exception {
    SourceSpec spec =
        new SourceSpec(
            "file",
            "unused",
            new SourceSpec.FormatSpec("csv"),
            null,
            null,
            Map.of("delimiter", ","));
    List<String> ids = new ArrayList<>();
    long count =
        BoundedRowReader.forEach(
            () ->
                new ByteArrayInputStream("id,v\na,1\nb,2\nc,3\n".getBytes(StandardCharsets.UTF_8)),
            spec,
            row -> ids.add(row.get("id")));
    assertEquals(3, count);
    assertEquals(List.of("a", "b", "c"), ids);
  }

  @Test
  void readsAJsonFile(@TempDir Path directory) throws Exception {
    Path file = directory.resolve("source.json");
    Files.writeString(
        file, "[{\"id\":\"x\",\"n\":{\"k\":1}},{\"id\":\"y\"}]", StandardCharsets.UTF_8);
    SourceSpec spec =
        new SourceSpec(
            "file",
            "file:" + directory + "?fileName=source.json&noop=true&initialDelay=0&delay=100",
            new SourceSpec.FormatSpec("json"),
            null,
            null,
            Map.of());
    List<String> ids = new ArrayList<>();
    try (CamelBridge bridge = new CamelBridge()) {
      assertEquals(2, BoundedRowReader.forEach(bridge, spec, row -> ids.add(row.get("id"))));
    }
    assertEquals(List.of("x", "y"), ids);
  }

  @Test
  void aConsumerFailureStopsTheRead(@TempDir Path directory) throws Exception {
    Path file = directory.resolve("source.csv");
    Files.writeString(file, "id\n1\n2\n3\n", StandardCharsets.UTF_8);
    List<String> seen = new ArrayList<>();
    try (CamelBridge bridge = new CamelBridge()) {
      assertThrows(
          IllegalStateException.class,
          () ->
              BoundedRowReader.forEach(
                  bridge,
                  csv(file, ","),
                  row -> {
                    seen.add(row.get("id"));
                    if (seen.size() == 2) {
                      throw new IllegalStateException("stop");
                    }
                  }));
    }
    assertEquals(List.of("1", "2"), seen);
  }

  @Test
  void kafkaIsNotABoundedReaderSource() {
    SourceSpec kafka = new SourceSpec("kafka", "kafka:t?brokers=b:9092", null, null);
    assertThrows(
        IllegalArgumentException.class,
        () -> BoundedRowReader.forEach(new CamelBridge(), kafka, row -> {}));
  }
}
