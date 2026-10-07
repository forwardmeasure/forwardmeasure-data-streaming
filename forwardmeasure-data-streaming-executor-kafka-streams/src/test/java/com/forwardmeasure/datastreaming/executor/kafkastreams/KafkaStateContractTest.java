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
package com.forwardmeasure.datastreaming.executor.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class KafkaStateContractTest {
  @Test
  void recoverySerializationPreservesStructuredEvidenceAndRejectsCorruptState() {
    try (var serde = new StageRecordSerde()) {
      var raw = new java.util.LinkedHashMap<String, Object>();
      raw.put("zero", 0);
      raw.put("disabled", false);
      raw.put("missing", null);
      Map<String, Object> record =
          Map.of(
              "uid",
              "source a",
              "source_data",
              raw,
              "names",
              List.of(Map.of("value", "José Smith")));
      byte[] serialized = serde.serializer().serialize("state", record);
      assertEquals(record, serde.deserializer().deserialize("state", serialized));
      assertNull(serde.serializer().serialize("state", null));
      assertNull(serde.deserializer().deserialize("state", null));
      assertThrows(
          SerializationException.class,
          () ->
              serde
                  .deserializer()
                  .deserialize(
                      "state", "{broken".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      assertThrows(
          SerializationException.class,
          () -> serde.serializer().serialize("state", Map.of("unsupported", new Object())));
    }
  }

  @Test
  void boundedFrontierIsImmutableExclusiveAndRequiresEveryPartition() {
    var first = new TopicPartition("input", 0);
    var second = new TopicPartition("input", 1);
    var offsets = new java.util.HashMap<TopicPartition, Long>();
    offsets.put(first, 2L);
    offsets.put(second, 4L);
    var frontier = new InputFrontier(offsets);
    offsets.put(first, 100L);
    assertFalse(frontier.reached(first, 1));
    assertTrue(frontier.reached(first, 2));
    assertFalse(frontier.allReached(Map.of(first, 2L)));
    assertFalse(frontier.allReached(Map.of(first, 2L, second, 3L)));
    assertTrue(frontier.allReached(Map.of(first, 2L, second, 4L)));
    assertTrue(frontier.reached(new TopicPartition("late", 0), 0));
    assertThrows(UnsupportedOperationException.class, () -> frontier.endOffsets().put(first, 100L));
    assertTrue(new InputFrontier(Map.of()).allReached(Map.of()));
  }
}
