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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.SinkFailurePolicy;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.OpenSearchSinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one {@code BOUNDED} Kafka-sourced ingestion to completion against a real {@link
 * InputFrontier} - a plain {@link KafkaConsumer} poll loop, not the Kafka Streams DSL runtime
 * (which has no native "stop at these offsets" concept; see this repo's own gap-bridging plan).
 * Genuinely new capability - no bounded Kafka path existed anywhere in this repo before this.
 *
 * <p>Always reads from the beginning of the topic to the captured frontier, every run (a fresh,
 * random consumer group id per run - see {@link #run}) - this is a "process everything currently on
 * the topic, once" contract, not an incremental resume-from-last-commit one; a caller wanting
 * incremental processing runs this repeatedly against a topic that's been trimmed/compacted between
 * runs, or (more commonly) simply uses {@code CONTINUOUS} mode instead.
 *
 * <p>Process-then-commit, one record at a time, feeding the same {@link SinkRowWriter} the
 * continuous Kafka Streams path uses - real, not simulated exactly-once-in-practice: an OpenSearch
 * document PUT by id is genuinely idempotent, so an at-least-once redelivery (a crash between a
 * successful write and its offset commit) never produces a duplicate or corrupt result. No Kafka
 * transactions needed for that reason - see {@link OpenSearchSinkRowWriter}'s own javadoc.
 */
final class BoundedKafkaConsumerRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedKafkaConsumerRunner.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RECORD_TYPE = new TypeReference<>() {};

  private BoundedKafkaConsumerRunner() {}

  /** Summary of one bounded run - how many records were read, mapped, and written. */
  record Result(long recordsRead, long recordsWritten) {}

  static Result run(
      SourcePlan source, SinkSpec sink, ErrorPolicy errors, FieldMappingEngine mapper) {
    KafkaConnectorUri sourceUri = KafkaConnectorUri.parse(source.source().uri());
    MalformedRecordPolicy malformedRecordPolicy = MalformedRecordPolicy.from(errors);
    SinkFailurePolicy sinkFailurePolicy = SinkFailurePolicy.from(errors);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceUri.bootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fds-bounded-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    long recordsRead = 0;
    long recordsWritten = 0;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        SinkRowWriter sinkRowWriter = new OpenSearchSinkRowWriter(sink)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(sourceUri.topic()).stream()
              .map(PartitionInfo::partition)
              .map(partition -> new TopicPartition(sourceUri.topic(), partition))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);

      InputFrontier frontier = new InputFrontier(consumer.endOffsets(partitions));
      LOGGER.info(
          "BoundedKafkaConsumerRunner: topic={} partitions={} frontier={}",
          sourceUri.topic(),
          partitions.size(),
          frontier.endOffsets());

      Map<TopicPartition, Long> positions = new HashMap<>();
      for (TopicPartition partition : partitions) {
        positions.put(partition, consumer.position(partition));
      }

      while (!frontier.allReached(positions)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : records) {
          recordsRead++;
          TopicPartition partition = new TopicPartition(record.topic(), record.partition());
          if (!frontier.reached(partition, record.offset())) {
            Map<String, Object> mapped =
                mapOrHandle(record.value(), source.mapper(), mapper, malformedRecordPolicy);
            if (mapped != null) {
              IngestionPipeline.withSinkFailureHandlingBlocking(
                  sinkFailurePolicy, () -> sinkRowWriter.write(mapped));
              recordsWritten++;
            }
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
          }
          positions.put(partition, record.offset() + 1);
        }
      }
    }
    LOGGER.info(
        "BoundedKafkaConsumerRunner: completed recordsRead={} recordsWritten={}",
        recordsRead,
        recordsWritten);
    return new Result(recordsRead, recordsWritten);
  }

  private static Map<String, Object> mapOrHandle(
      String json,
      TransformSpec mapperSpec,
      FieldMappingEngine mapper,
      MalformedRecordPolicy policy) {
    try {
      Map<String, Object> raw = JSON.readValue(json, RECORD_TYPE);
      return mapper.map(toSourceRow(raw), mapperSpec);
    } catch (RuntimeException | JsonProcessingException failure) {
      return switch (policy) {
        case FAIL -> {
          if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
          }
          throw new IllegalArgumentException("Unable to parse or map record", failure);
        }
        case DEAD_LETTER -> {
          LOGGER.error("BoundedKafkaConsumerRunner: record dead-lettered: {}", json, failure);
          yield null;
        }
        case SKIP -> {
          LOGGER.warn("BoundedKafkaConsumerRunner: record skipped: {}", json, failure);
          yield null;
        }
      };
    }
  }

  private static SourceRow toSourceRow(Map<String, Object> record) {
    return field -> {
      Object value = record.get(field);
      return value == null ? null : value.toString();
    };
  }
}
