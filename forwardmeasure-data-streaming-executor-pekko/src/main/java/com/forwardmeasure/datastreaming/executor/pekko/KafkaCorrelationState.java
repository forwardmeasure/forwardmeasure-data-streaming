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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.core.CorrelationMerge;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Compacted, transactional per-source correlation state. Input offsets commit only after both this
 * state and the business sink succeed. A stable transactional id fences a superseded pod.
 * Correlated Pekko deployments run one active replica; scale independent pipelines separately.
 */
final class KafkaCorrelationState implements AutoCloseable {
  private static final ObjectMapper JSON =
      com.fasterxml.jackson.databind.json.JsonMapper.builder()
          .enable(com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .enable(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .build();
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final Map<String, Map<String, Contribution>> state = new HashMap<>();
  private final List<SourcePlan> sources;
  private final ExecutionPlan plan;

  record Contribution(
      String blockingKey,
      String sourceKey,
      String inputTopic,
      int partition,
      long offset,
      Map<String, Object> fields) {}

  static String identity(ExecutionPlan plan) {
    return com.forwardmeasure.datastreaming.api.ExecutionIdentity.of(
        plan, "fds-pekko-correlation-", System.getenv("FDS_EXECUTION_ID"));
  }

  KafkaCorrelationState(ExecutionPlan plan, String brokers, String identity) {
    this.plan = plan;
    sources =
        plan.sources().stream()
            .sorted(
                Comparator.comparingDouble(SourcePlan::trustWeight)
                    .reversed()
                    .thenComparing(SourcePlan::sourceKey))
            .toList();
    topic = identity + "-state-v1";
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (Admin admin = Admin.create(config)) {
      try {
        admin
            .createTopics(
                List.of(
                    new NewTopic(topic, java.util.Optional.of(1), java.util.Optional.empty())
                        .configs(Map.of("cleanup.policy", "compact"))))
            .all()
            .get(30, TimeUnit.SECONDS);
      } catch (ExecutionException failure) {
        if (!(failure.getCause() instanceof TopicExistsException)) throw failure;
      }
      var resource =
          new org.apache.kafka.common.config.ConfigResource(
              org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic);
      String cleanup =
          admin
              .describeConfigs(List.of(resource))
              .all()
              .get(30, TimeUnit.SECONDS)
              .get(resource)
              .get("cleanup.policy")
              .value();
      if (!"compact".equals(cleanup)) {
        throw new IllegalStateException(
            "Correlation changelog must retain compacted state: " + topic);
      }
    } catch (Exception failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("Cannot prepare correlation changelog " + topic, failure);
    }
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, identity);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(config);
    try {
      // Fence the previous writer before capturing the restoration frontier.
      producer.initTransactions();
      restore(brokers);
    } catch (RuntimeException failure) {
      producer.close(Duration.ofSeconds(5));
      throw failure;
    }
  }

  private void restore(String brokers) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(partition -> new TopicPartition(topic, partition.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> frontier = consumer.endOffsets(partitions);
      long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      while (partitions.stream()
          .anyMatch(partition -> consumer.position(partition) < frontier.get(partition))) {
        if (System.nanoTime() > deadline)
          throw new IllegalStateException("Timed out restoring " + topic);
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          if (record.value() == null) continue;
          Contribution contribution = JSON.readValue(record.value(), Contribution.class);
          remember(contribution);
        }
      }
    } catch (java.io.IOException failure) {
      throw new IllegalStateException("Invalid correlation changelog " + topic, failure);
    }
  }

  /** Serialized with the sink write so concurrent sources cannot publish an older merge last. */
  synchronized void updateAndWrite(
      Contribution contribution, Function<Map<String, Object>, CompletionStage<Void>> sink) {
    boolean transactionOpen = false;
    try {
      Contribution previous =
          state.getOrDefault(contribution.blockingKey(), Map.of()).get(contribution.sourceKey());
      if (previous == null
          || !previous.inputTopic().equals(contribution.inputTopic())
          || previous.partition() != contribution.partition()
          || previous.offset() < contribution.offset()) {
        producer.beginTransaction();
        transactionOpen = true;
        producer
            .send(
                new ProducerRecord<>(
                    topic,
                    JSON.writeValueAsString(
                        List.of(contribution.blockingKey(), contribution.sourceKey())),
                    JSON.writeValueAsString(contribution)))
            .get(30, TimeUnit.SECONDS);
        producer.commitTransaction();
        transactionOpen = false;
        remember(contribution);
      }
      List<Map<String, Object>> rows = new ArrayList<>();
      var perSource = state.get(contribution.blockingKey());
      for (SourcePlan source : sources) {
        Contribution current = perSource.get(source.sourceKey());
        if (current != null) rows.add(current.fields());
      }
      sink.apply(CorrelationMerge.merge(rows, plan.mergePolicy())).toCompletableFuture().join();
    } catch (Exception failure) {
      if (transactionOpen) {
        try {
          producer.abortTransaction();
        } catch (RuntimeException abortFailure) {
          failure.addSuppressed(abortFailure);
        }
      }
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("Unable to persist/deliver correlated record", failure);
    }
  }

  private void remember(Contribution contribution) {
    state
        .computeIfAbsent(contribution.blockingKey(), ignored -> new HashMap<>())
        .put(contribution.sourceKey(), contribution);
  }

  @Override
  public void close() {
    producer.close(Duration.ofSeconds(10));
  }
}
