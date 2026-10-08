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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StreamingTextRowsTest {
  @TempDir Path directory;

  private SourceSpec source(Path file) {
    return new SourceSpec(
        "file",
        "file:"
            + directory
            + "?fileName="
            + file.getFileName()
            + "&noop=true&initialDelay=0&delay=100&charset=ISO-8859-1",
        new SourceSpec.FormatSpec("csv"),
        null,
        null,
        Map.of("delimiter", "\t"));
  }

  @Test
  void deliversEarlyRecordsBeforeReadingMalformedTailAndPreservesQuotedMultilineLatin1()
      throws Exception {
    Path file = directory.resolve("late-error.tsv");
    try (var writer = Files.newBufferedWriter(file, StandardCharsets.ISO_8859_1)) {
      writer.write("id\tnote\n1\t\"café\tfirst\nsecond \"\"quoted\"\"\"\n");
      for (int i = 2; i <= 20000; i++) writer.write(i + "\tvalid\n");
      writer.write("20001\t\"unterminated");
    }
    var system = ActorSystem.create("streaming-late-error");
    AtomicInteger delivered = new AtomicInteger();
    try (var bridge = new CamelBridge()) {
      var done =
          PekkoIngestionRunner.rowSource(bridge, source(file))
              .runWith(
                  Sink.foreach(
                      row -> {
                        if (delivered.getAndIncrement() == 0)
                          assertEquals("café\tfirst\nsecond \"quoted\"", row.get("note"));
                      }),
                  system)
              .toCompletableFuture();
      assertThrows(
          java.util.concurrent.ExecutionException.class, () -> done.get(30, TimeUnit.SECONDS));
      assertTrue(
          delivered.get() > 19000, "Valid rows must reach the sink before the late parser failure");
    } finally {
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void literalQuoteDialectPreservesProviderTextIncludingLeadingAndUnmatchedQuotes()
      throws Exception {
    Path file = directory.resolve("literal-quotes.tsv");
    String note = "\"Alias\", additional text; \"unmatched";
    Files.writeString(file, "id\tnote\n1\t" + note + "\n2\tnext\n");
    var normal = source(file);
    var literal =
        new SourceSpec(
            normal.connector(),
            normal.uri(),
            normal.format(),
            null,
            null,
            Map.of("delimiter", "\t", "quote", ""));
    var system = ActorSystem.create("literal-provider-quotes");
    try (var bridge = new CamelBridge()) {
      var rows =
          PekkoIngestionRunner.rowSource(bridge, literal)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
      assertEquals(2, rows.size());
      assertEquals(note, rows.getFirst().get("note"));
    } finally {
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void oversizedUnterminatedRecordFailsWithinTheConfiguredRecordBound() throws Exception {
    Path file = directory.resolve("oversized.tsv");
    Files.writeString(file, "id\tnote\n1\t\"" + "x".repeat(2 * 1024 * 1024));
    var system = ActorSystem.create("bounded-record");
    try (var bridge = new CamelBridge()) {
      var done =
          PekkoIngestionRunner.rowSource(bridge, source(file))
              .runWith(Sink.ignore(), system)
              .toCompletableFuture();
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class, () -> done.get(15, TimeUnit.SECONDS));
      assertTrue(failure.toString().contains("maxRecordCharacters"));
    } finally {
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void cancellationDoesNotScanTheRestOfALargeSource() throws Exception {
    Path file = directory.resolve("cancel.tsv");
    // A sparse tail larger than a Java String can hold. A one-row consumer must never read it.
    try (var fileOut = new java.io.RandomAccessFile(file.toFile(), "rw")) {
      fileOut.write("id\tnote\n1\tfirst\n".getBytes(StandardCharsets.ISO_8859_1));
      for (int i = 2; i < 1024; i++)
        fileOut.write((i + "\tpadding\n").getBytes(StandardCharsets.ISO_8859_1));
      fileOut.setLength(6L * 1024 * 1024 * 1024);
    }
    var system = ActorSystem.create("streaming-cancellation");
    try (var bridge = new CamelBridge()) {
      var first =
          PekkoIngestionRunner.rowSource(bridge, source(file))
              .take(1)
              .runWith(Sink.head(), system)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
      assertEquals("first", first.get("note"));
    } finally {
      system.terminate();
      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }
}
