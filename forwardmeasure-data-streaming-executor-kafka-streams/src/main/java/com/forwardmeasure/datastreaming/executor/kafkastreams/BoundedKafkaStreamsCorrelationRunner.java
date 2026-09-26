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
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.OpenSearchSinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SinkRowWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, live-found capability gap closed (2026-09-25): {@code KafkaStreamsDeliveryEngine} used to
 * reject any correlated ({@code sources.size() > 1}) plan outright - not a missing niche feature, a
 * real, acknowledged gap the user explicitly confirmed was needed, just queued behind the Spark
 * two-Job build. This is the Kafka Streams counterpart to {@code PekkoCorrelationEngine}/{@code
 * SparkCorrelationEngine} - same real merge semantics (verified by reading both directly, not
 * assumed): reads and maps every source, groups by {@code blockingField}, merges each group by
 * trust weight (highest-trust record in a group wins every field it resolved; a lower-trust record
 * only fills what the winner left unset - {@code mergeGroup}'s real {@code putIfAbsent} logic,
 * copied to match, not shared code - the existing precedent between Pekko/Spark is one independent-
 * but-identical implementation per engine, not a shared library call).
 *
 * <p>Correlation genuinely needs every source's rows before it can group/merge by blocking key (see
 * {@code PekkoCorrelationEngine}'s own javadoc for why there's no way to bound memory below "every
 * mapped row across every source") - each source gets its own real {@link InputFrontier}, captured
 * and read to completion independently, before any merging starts; this is a real, two-phase
 * bounded run (read everything, then merge and write), not the single-pass interleaved read/write
 * {@link BoundedKafkaStreamsConsumerRunner} uses for its own single-source case (which can write
 * incrementally because it never needs to wait for a second source).
 */
final class BoundedKafkaStreamsCorrelationRunner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BoundedKafkaStreamsCorrelationRunner.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RECORD_TYPE = new TypeReference<>() {};

  private BoundedKafkaStreamsCorrelationRunner() {}

  /** Summary of one correlation run - how many sources were read and how many groups resulted. */
  record Result(long sourceCount, long groupCount) {}

  private record CorrelationRecord(
      String blockingKey, String sourceKey, double trustWeight, Map<String, Object> mappedFields) {}

  static Result run(
      List<SourcePlan> sources,
      String blockingField,
      SinkSpec sink,
      ErrorPolicy errors,
      FieldMappingEngine mapper) {
    if (sources.size() <= 1) {
      throw new IllegalArgumentException(
          "BoundedKafkaStreamsCorrelationRunner: expected more than one source, got "
              + sources.size()
              + " - use BoundedKafkaStreamsConsumerRunner for a single-source plan");
    }
    MalformedRecordPolicy malformedRecordPolicy = MalformedRecordPolicy.from(errors);

    List<CorrelationRecord> allRecords = new ArrayList<>();
    for (SourcePlan source : sources) {
      allRecords.addAll(readSource(source, blockingField, mapper, malformedRecordPolicy));
    }

    Map<String, List<CorrelationRecord>> grouped = new LinkedHashMap<>();
    for (CorrelationRecord record : allRecords) {
      grouped.computeIfAbsent(record.blockingKey(), key -> new ArrayList<>()).add(record);
    }

    long groupCount = 0;
    try (SinkRowWriter sinkRowWriter = new OpenSearchSinkRowWriter(sink)) {
      for (List<CorrelationRecord> group : grouped.values()) {
        sinkRowWriter.write(mergeGroup(group));
        groupCount++;
      }
    }
    LOGGER.info(
        "BoundedKafkaStreamsCorrelationRunner: completed sourceCount={} groupCount={}",
        sources.size(),
        groupCount);
    return new Result(sources.size(), groupCount);
  }

  private static List<CorrelationRecord> readSource(
      SourcePlan source,
      String blockingField,
      FieldMappingEngine mapper,
      MalformedRecordPolicy malformedRecordPolicy) {
    KafkaConnectorUri sourceUri = KafkaConnectorUri.parse(source.source().uri());
    List<CorrelationRecord> records = new ArrayList<>();

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceUri.bootstrapServers());
    consumerProps.put(
        ConsumerConfig.GROUP_ID_CONFIG, "fds-bounded-correlation-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(sourceUri.topic()).stream()
              .map(PartitionInfo::partition)
              .map(partition -> new TopicPartition(sourceUri.topic(), partition))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);

      InputFrontier frontier = new InputFrontier(consumer.endOffsets(partitions));
      LOGGER.info(
          "BoundedKafkaStreamsCorrelationRunner: source={} topic={} partitions={} frontier={}",
          source.sourceKey(),
          sourceUri.topic(),
          partitions.size(),
          frontier.endOffsets());

      Map<TopicPartition, Long> positions = new HashMap<>();
      for (TopicPartition partition : partitions) {
        positions.put(partition, consumer.position(partition));
      }

      while (!frontier.allReached(positions)) {
        ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : polled) {
          TopicPartition partition = new TopicPartition(record.topic(), record.partition());
          if (!frontier.reached(partition, record.offset())) {
            CorrelationRecord mapped =
                mapOrHandle(record.value(), source, blockingField, mapper, malformedRecordPolicy);
            if (mapped != null) {
              records.add(mapped);
            }
          }
          positions.put(partition, record.offset() + 1);
        }
      }
    }
    return records;
  }

  private static CorrelationRecord mapOrHandle(
      String json,
      SourcePlan source,
      String blockingField,
      FieldMappingEngine mapper,
      MalformedRecordPolicy policy) {
    Map<String, Object> mapped;
    try {
      Map<String, Object> raw = JSON.readValue(json, RECORD_TYPE);
      mapped = mapper.map(new MapSourceRow(raw), source.mapper());
    } catch (RuntimeException | JsonProcessingException failure) {
      switch (policy) {
        case FAIL -> {
          if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
          }
          throw new IllegalArgumentException("Unable to parse or map record", failure);
        }
        case DEAD_LETTER ->
            LOGGER.error(
                "BoundedKafkaStreamsCorrelationRunner: source '{}' record dead-lettered: {}",
                source.sourceKey(),
                json,
                failure);
        case SKIP ->
            LOGGER.warn(
                "BoundedKafkaStreamsCorrelationRunner: source '{}' record skipped: {}",
                source.sourceKey(),
                json,
                failure);
      }
      return null;
    }
    Object blockingValue = mapped.get(blockingField);
    if (blockingValue == null) {
      return null;
    }
    String blockingKey = normalizeBlockingKey(String.valueOf(blockingValue));
    if (blockingKey.isBlank()) {
      return null;
    }
    return new CorrelationRecord(blockingKey, source.sourceKey(), source.trustWeight(), mapped);
  }

  private static Map<String, Object> mergeGroup(List<CorrelationRecord> group) {
    List<CorrelationRecord> sorted = new ArrayList<>(group);
    sorted.sort(Comparator.comparingDouble(CorrelationRecord::trustWeight).reversed());

    Map<String, Object> merged = new LinkedHashMap<>();
    for (CorrelationRecord record : sorted) {
      for (Map.Entry<String, Object> field : record.mappedFields().entrySet()) {
        merged.putIfAbsent(field.getKey(), field.getValue());
      }
    }
    return merged;
  }

  private static String normalizeBlockingKey(String value) {
    return value == null ? "" : value.strip().toUpperCase(Locale.ROOT);
  }
}
