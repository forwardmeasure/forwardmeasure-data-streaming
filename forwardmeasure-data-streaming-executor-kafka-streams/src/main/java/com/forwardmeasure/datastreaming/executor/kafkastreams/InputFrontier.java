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

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

/**
 * The precise termination contract {@link BoundedKafkaConsumerRunner} runs to - "terminate when
 * consumer lag reaches zero" is unsound (new records can arrive while checking), so this captures
 * {@code endOffsets} exactly once, at run start, via a real {@code consumer.endOffsets(partitions)}
 * call - the run processes every record up to (not including) each partition's captured offset and
 * stops there, regardless of what arrives on the topic afterward (see the repo's own gap-bridging
 * plan for why this precise contract is needed - Kafka Streams' own DSL runtime has no equivalent,
 * which is why this is a plain consumer/producer poll loop, not a {@code KafkaStreams} topology).
 */
record InputFrontier(Map<TopicPartition, Long> endOffsets) {

  InputFrontier {
    endOffsets = Map.copyOf(Objects.requireNonNull(endOffsets, "endOffsets"));
  }

  /** Whether {@code position} (the next offset that would be fetched) has reached this frontier. */
  boolean reached(TopicPartition partition, long position) {
    Long endOffset = endOffsets.get(partition);
    return endOffset == null || position >= endOffset;
  }

  boolean allReached(Map<TopicPartition, Long> positions) {
    for (TopicPartition partition : endOffsets.keySet()) {
      Long position = positions.get(partition);
      if (position == null || !reached(partition, position)) {
        return false;
      }
    }
    return true;
  }
}
