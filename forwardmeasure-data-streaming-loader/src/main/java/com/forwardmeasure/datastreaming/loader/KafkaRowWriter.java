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
package com.forwardmeasure.datastreaming.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Writes source rows, unmapped, as JSON objects to load topics with an idempotent, all-replica
 * producer. A load topic has one partition so the delivery stage reads rows in source order, its
 * records carry no key (the row's own fields are its whole content), and it keeps them for {@link
 * #RETENTION} - long enough to rerun a delivery, not forever.
 */
public final class KafkaRowWriter implements AutoCloseable {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** How long a load topic keeps its rows. */
  public static final java.time.Duration RETENTION = java.time.Duration.ofDays(7);

  private final String bootstrapServers;
  private final KafkaProducer<String, String> producer;

  public KafkaRowWriter(String bootstrapServers) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(properties);
  }

  /**
   * Makes {@code topic} an empty one-partition load topic: creates it, or - when an earlier attempt
   * of the same load already wrote to it (a retried Job, a repeated run) - deletes every record it
   * holds. Each attempt therefore starts from nothing, and a reader that starts at the beginning
   * sees exactly one attempt's records, never an earlier prefix twice.
   */
  public void emptyTopic(String topic) throws InterruptedException, ExecutionException {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (Admin admin = Admin.create(properties)) {
      try {
        admin
            .createTopics(
                List.of(
                    new NewTopic(topic, 1, (short) -1)
                        .configs(Map.of("retention.ms", String.valueOf(RETENTION.toMillis())))))
            .all()
            .get();
        return;
      } catch (ExecutionException failure) {
        if (!(failure.getCause() instanceof TopicExistsException)) {
          throw failure;
        }
      }
      List<TopicPartition> partitions =
          admin
              .describeTopics(List.of(topic))
              .allTopicNames()
              .get()
              .get(topic)
              .partitions()
              .stream()
              .map(partition -> new TopicPartition(topic, partition.partition()))
              .toList();
      Map<TopicPartition, OffsetSpec> latest = new java.util.HashMap<>();
      for (TopicPartition partition : partitions) latest.put(partition, OffsetSpec.latest());
      Map<TopicPartition, RecordsToDelete> earlier = new java.util.HashMap<>();
      admin
          .listOffsets(latest)
          .all()
          .get()
          .forEach(
              (partition, end) ->
                  earlier.put(partition, RecordsToDelete.beforeOffset(end.offset())));
      admin.deleteRecords(earlier).all().get();
    }
  }

  /**
   * Sends one row; a failed send surfaces at {@link #flush()} or {@link #close()} at the latest.
   */
  public void write(String topic, SourceRow row) {
    Map<String, ?> fields = row.rawFields();
    String value;
    try {
      value = JSON.writeValueAsString(fields);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Source row cannot be written as JSON", failure);
    }
    producer.send(
        new ProducerRecord<>(topic, value),
        (metadata, failure) -> {
          if (failure != null) {
            sendFailure = failure;
          }
        });
  }

  private volatile Exception sendFailure;

  /** Blocks until every sent row is acknowledged, failing if any send failed. */
  public void flush() {
    producer.flush();
    if (sendFailure != null) {
      throw new IllegalStateException("Writing a source row to Kafka failed", sendFailure);
    }
  }

  @Override
  public void close() {
    try {
      flush();
    } finally {
      producer.close();
    }
  }
}
