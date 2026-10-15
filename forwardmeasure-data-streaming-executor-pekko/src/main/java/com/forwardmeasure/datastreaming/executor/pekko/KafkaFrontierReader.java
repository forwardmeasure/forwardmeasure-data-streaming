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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * A bounded read of a whole topic: every committed record from the start of each partition up to
 * the end offsets captured when the reader opens - what a bounded run reads from a stage's handoff
 * topic, as Kafka Streams' bounded runner does. Records written after it opens are not read.
 */
final class KafkaFrontierReader implements AutoCloseable {

  private final KafkaConsumer<String, String> consumer;
  private final List<TopicPartition> partitions;
  private final Map<TopicPartition, Long> end;
  private final ArrayDeque<String> buffered = new ArrayDeque<>();

  KafkaFrontierReader(String bootstrapServers, String topic) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fds-pekko-bounded-" + UUID.randomUUID());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = new KafkaConsumer<>(properties);
    try {
      partitions =
          consumer.partitionsFor(topic).stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .toList();
      if (partitions.isEmpty())
        throw new IllegalStateException("Topic " + topic + " has no partitions");
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      end = new HashMap<>(consumer.endOffsets(partitions));
    } catch (RuntimeException failure) {
      consumer.close();
      throw failure;
    }
  }

  /** The next record's value, or {@code null} once every partition has reached its end offset. */
  String next() {
    while (buffered.isEmpty()) {
      if (partitions.stream().allMatch(p -> consumer.position(p) >= end.get(p))) return null;
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        if (record.offset() < end.get(partition) && record.value() != null)
          buffered.add(record.value());
      }
    }
    return buffered.poll();
  }

  @Override
  public void close() {
    consumer.close();
  }
}
