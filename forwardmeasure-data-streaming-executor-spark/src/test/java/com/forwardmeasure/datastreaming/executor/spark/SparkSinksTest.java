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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Proves the real, load-bearing invariant behind {@link SparkSinks}' 2026-09-21 retirement of its
 * old {@code file}/{@code jdbc}/{@code opensearch} direct-write paths: {@link SparkSinks#write}
 * literally cannot construct a non-Kafka sink writer anymore, not just "nothing in this repo
 * happens to call it that way" (see the repo's own gap-bridging plan, "Spark: an optional
 * distributed compute stage, never a delivery engine").
 */
class SparkSinksTest {

  @Test
  void writeRejectsAnyConnectorOtherThanKafka() {
    SinkSpec openSearchSink =
        new SinkSpec("opensearch", "http://localhost:9200", "index", null, null, Map.of());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SparkSinks.write(null, null, openSearchSink, new ErrorPolicy(null, null)));
    assertTrue(
        exception.getMessage().contains("opensearch"),
        "expected the rejection to name the offending connector: " + exception.getMessage());
  }

  @Test
  void writeRejectsFileConnectorToo() {
    SinkSpec fileSink = new SinkSpec("file", "/tmp/unused", "n/a", null, null);

    assertThrows(
        IllegalArgumentException.class,
        () -> SparkSinks.write(null, null, fileSink, new ErrorPolicy(null, null)));
  }

  @Test
  void writeRejectsJdbcConnectorToo() {
    SinkSpec jdbcSink =
        new SinkSpec("jdbc", "jdbc:postgresql://localhost/db", "table", null, null, Map.of());

    assertThrows(
        IllegalArgumentException.class,
        () -> SparkSinks.write(null, null, jdbcSink, new ErrorPolicy(null, null)));
  }
}
