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

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reads one bounded, non-Kafka source row by row on the calling thread, with no stream runtime:
 * delimited or JSON text through {@link StreamingTextRows} (from a Camel endpoint or caller-opened
 * {@link TextContent}), and {@code jdbc}/{@code sql} result sets through Camel's SQL component. The
 * same parsing rules as the Pekko runner, which wraps these readers in its own stream stages.
 */
public final class BoundedRowReader {

  /** Receives each row in source order; a thrown exception stops the read. */
  @FunctionalInterface
  public interface RowConsumer {
    void accept(SourceRow row) throws Exception;
  }

  private static final String JDBC_CONNECTOR = "jdbc";
  private static final String SQL_CONNECTOR = "sql";
  private static final String KAFKA_CONNECTOR = "kafka";

  private BoundedRowReader() {}

  public static boolean isSql(SourceSpec source) {
    return JDBC_CONNECTOR.equals(source.connector()) || SQL_CONNECTOR.equals(source.connector());
  }

  public static boolean isKafka(SourceSpec source) {
    return KAFKA_CONNECTOR.equals(source.connector());
  }

  /** Reads every row of {@code source}, returning how many were read. */
  public static long forEach(CamelBridge bridge, SourceSpec source, RowConsumer consumer)
      throws Exception {
    if (isKafka(source)) {
      throw new IllegalArgumentException(
          "BoundedRowReader reads file, object-storage, text and SQL sources; a kafka source is"
              + " already on Kafka");
    }
    if (isSql(source)) {
      return forEachSqlRow(bridge, source, consumer);
    }
    try (StreamingTextRows rows = StreamingTextRows.open(bridge, source)) {
      return drain(rows, consumer);
    }
  }

  /** Reads every row of caller-opened text {@code content} parsed as {@code source} declares. */
  public static long forEach(TextContent content, SourceSpec source, RowConsumer consumer)
      throws Exception {
    if (isKafka(source) || isSql(source)) {
      throw new IllegalArgumentException(
          "Caller-supplied content is text; connector " + source.connector() + " is not");
    }
    try (StreamingTextRows rows = StreamingTextRows.open(content, source)) {
      return drain(rows, consumer);
    }
  }

  private static long drain(StreamingTextRows rows, RowConsumer consumer) throws Exception {
    long count = 0;
    for (Optional<SourceRow> row = rows.next(); row.isPresent(); row = rows.next()) {
      consumer.accept(row.get());
      count++;
    }
    return count;
  }

  /**
   * The whole result set arrives as one exchange ({@code useIterator=false}), and an empty result
   * still produces one ({@code routeEmptyResultSet=true}) - the same endpoint options the Pekko
   * runner uses.
   */
  @SuppressWarnings("unchecked")
  private static long forEachSqlRow(CamelBridge bridge, SourceSpec source, RowConsumer consumer)
      throws Exception {
    String dataSourceName = SqlDataSources.register(bridge, source.uri(), source.options());
    String query = source.query();
    if (query == null || query.isBlank()) {
      throw new IllegalArgumentException(
          "a 'jdbc'/'sql' source needs a non-blank query() - the SQL text to run");
    }
    String sqlUri =
        "sql:"
            + query
            + "?dataSource=#"
            + dataSourceName
            + "&useIterator=false&routeEmptyResultSet=true";
    var template = bridge.camelContext().createConsumerTemplate();
    try {
      List<?> rows = template.receiveBody(sqlUri, List.class);
      long count = 0;
      for (Object row : rows == null ? List.of() : rows) {
        consumer.accept(new MapSourceRow((Map<String, Object>) row));
        count++;
      }
      return count;
    } finally {
      template.stop();
    }
  }
}
