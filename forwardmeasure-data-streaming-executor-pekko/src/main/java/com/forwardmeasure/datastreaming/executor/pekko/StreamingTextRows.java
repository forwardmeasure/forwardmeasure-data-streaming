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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Optional;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** A single file exchange, consumed incrementally and acknowledged only when its reader closes. */
final class StreamingTextRows implements AutoCloseable {
  private final ConsumerTemplate consumer;
  private Exchange exchange;
  private Reader reader;
  private boolean exhausted;
  private CSVParser csv;
  private Iterator<CSVRecord> csvRows;
  private MappingIterator<java.util.Map<String, Object>> jsonRows;

  private StreamingTextRows(ConsumerTemplate consumer) {
    this.consumer = consumer;
  }

  static StreamingTextRows open(CamelBridge bridge, SourceSpec source) throws Exception {
    StreamingTextRows rows = new StreamingTextRows(bridge.camelContext().createConsumerTemplate());
    try {
      long timeout =
          Long.parseLong(source.options().getOrDefault("acquisitionTimeoutMillis", "60000"));
      if (timeout < 1)
        throw new IllegalArgumentException("acquisitionTimeoutMillis must be positive");
      rows.exchange = rows.consumer.receive(source.uri(), timeout);
      if (rows.exchange == null) throw new IOException("Source acquisition timed out");
      if (rows.exchange.getException() != null) throw rows.exchange.getException();
      // Camel's GenericFile -> Reader conversion honors the endpoint charset without loading
      // the file into a String. Keep the exchange alive until the parser has finished reading it.
      rows.reader = rows.exchange.getMessage().getMandatoryBody(Reader.class);
      String format = source.format() == null ? "csv" : source.format().type();
      switch (format) {
        case "csv" -> {
          String delimiter = source.options().getOrDefault("delimiter", ",");
          if (delimiter.isEmpty()) delimiter = ",";
          if (delimiter.length() != 1)
            throw new IllegalArgumentException("CSV delimiter must be one character");
          String quoteOption = source.options().getOrDefault("quote", "\"");
          if (quoteOption.length() > 1)
            throw new IllegalArgumentException("CSV quote must be one character or empty");
          Character quote = quoteOption.isEmpty() ? null : quoteOption.charAt(0);
          long maxRecord =
              Long.parseLong(source.options().getOrDefault("maxRecordCharacters", "1048576"));
          if (maxRecord < 1)
            throw new IllegalArgumentException("maxRecordCharacters must be positive");
          rows.reader =
              new CsvRecordLimitReader(rows.reader, delimiter.charAt(0), quote, maxRecord);
          rows.csv =
              CSVFormat.DEFAULT
                  .builder()
                  .setHeader()
                  .setSkipHeaderRecord(true)
                  .setDelimiter(delimiter.charAt(0))
                  .setQuote(quote)
                  .get()
                  .parse(rows.reader);
          rows.csvRows = rows.csv.iterator();
        }
        case "json", "ndjson", "jsonl" ->
            rows.jsonRows =
                new ObjectMapper()
                    .readerFor(new TypeReference<java.util.Map<String, Object>>() {})
                    .readValues(rows.reader);
        default -> throw new IllegalArgumentException("Unsupported source format: " + format);
      }
      return rows;
    } catch (Exception failure) {
      if (rows.exchange != null) rows.exchange.setException(failure);
      try {
        rows.close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      if (failure instanceof IOException io) throw new java.io.UncheckedIOException(io);
      throw failure;
    }
  }

  Optional<SourceRow> next() throws Exception {
    try {
      if (csvRows != null && csvRows.hasNext())
        return Optional.of(new CsvSourceRow(csvRows.next()));
      if (jsonRows != null && jsonRows.hasNextValue())
        return Optional.of(new MapSourceRow(jsonRows.nextValue()));
      exhausted = true;
      return Optional.empty();
    } catch (Exception failure) {
      exchange.setException(failure);
      if (failure instanceof IOException io) throw new java.io.UncheckedIOException(io);
      throw failure;
    }
  }

  @Override
  public void close() throws Exception {
    if (exchange != null && !exhausted && exchange.getException() == null)
      exchange.setException(new IOException("Source consumption cancelled before EOF"));
    try {
      if (reader != null) reader.close();
    } finally {
      try {
        if (exchange != null) consumer.doneUoW(exchange);
      } finally {
        consumer.stop();
      }
    }
  }

  /** Bound an unterminated quoted record as well as valid records before CSVParser allocates it. */
  private static final class CsvRecordLimitReader extends java.io.FilterReader {
    private final char delimiter;
    private final Character quote;
    private final long maximum;
    private long count;
    private boolean quoted;
    private boolean afterQuote;
    private boolean fieldStart = true;

    CsvRecordLimitReader(Reader reader, char delimiter, Character quote, long maximum) {
      super(reader);
      this.delimiter = delimiter;
      this.quote = quote;
      this.maximum = maximum;
    }

    private void observe(int c) throws IOException {
      if (c < 0) return;
      if (++count > maximum)
        throw new IOException("CSV record exceeds maxRecordCharacters=" + maximum);
      if (quoted) {
        if (quote != null && c == quote) {
          quoted = false;
          afterQuote = true;
        }
      } else if (afterQuote && quote != null && c == quote) {
        quoted = true;
        afterQuote = false;
      } else {
        afterQuote = false;
        if (c == '\n' || c == '\r') {
          count = 0;
          fieldStart = true;
        } else if (c == delimiter) fieldStart = true;
        else {
          quoted = fieldStart && quote != null && c == quote;
          fieldStart = false;
        }
      }
    }

    @Override
    public int read() throws IOException {
      int c = in.read();
      observe(c);
      return c;
    }

    @Override
    public int read(char[] chars, int offset, int length) throws IOException {
      int n = in.read(chars, offset, length);
      for (int i = 0; i < n; i++) observe(chars[offset + i]);
      return n;
    }
  }
}
