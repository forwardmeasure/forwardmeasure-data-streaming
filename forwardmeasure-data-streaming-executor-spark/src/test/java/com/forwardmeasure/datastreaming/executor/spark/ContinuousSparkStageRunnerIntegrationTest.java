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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.SparkHandoffSpecs;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@WithKafkaContainer
class ContinuousSparkStageRunnerIntegrationTest {
  @Test
  @Timeout(180)
  void restartUsesCommittedInputOffsetsAndRetainsSeparateSourceContributions(
      KafkaTestContainer kafka) throws Exception {
    String brokers = kafka.bootstrapServers();
    String prefix = "continuous-spark-" + UUID.randomUUID();
    List<String> topics = List.of(prefix + "-a", prefix + "-b");
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
      admin
          .createTopics(topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList())
          .all()
          .get(30, TimeUnit.SECONDS);
    }
    TransformSpec mapper =
        new TransformSpec(
            null,
            List.of(
                new TransformSpec.FieldRule("id", "id", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null),
                new TransformSpec.FieldRule(
                    "date", "date", null, null, "parse_yyyymmdd", null, null)));
    List<SourcePlan> sources =
        topics.stream()
            .map(
                topic ->
                    new SourcePlan(
                        topic,
                        new SourceSpec(
                            "kafka", "kafka:" + topic + "?brokers=" + brokers, null, null),
                        mapper,
                        1.0))
            .toList();
    var plan =
        new ExecutionPlan(
            new ExecutionProfile(
                SourceCardinality.CORRELATED,
                ExecutionMode.CONTINUOUS,
                DeliveryEngineKind.PEKKO_STREAMS),
            sources,
            "id",
            Optional.of(new SparkStagePlan(List.of("parse_yyyymmdd"), prefix + "-handoff")),
            null,
            new SinkSpec("kafka", "kafka:unused?brokers=" + brokers, null, null, null, Map.of()),
            null,
            null);
    var spark = SparkSession.builder().appName(prefix).master("local[2]").getOrCreate();
    try (var executor = Executors.newSingleThreadExecutor();
        var producer =
            new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", brokers),
                new StringSerializer(),
                new StringSerializer());
        var reader =
            new KafkaConsumer<String, String>(
                Map.of(
                    "bootstrap.servers",
                    brokers,
                    "enable.auto.commit",
                    false,
                    "isolation.level",
                    "read_committed"),
                new StringDeserializer(),
                new StringDeserializer())) {
      producer
          .send(
              new ProducerRecord<>(
                  topics.get(0), "x", "{\"id\":\"x\",\"name\":\"first\",\"date\":\"20240229\"}"))
          .get();
      AtomicBoolean stop = new AtomicBoolean();
      Future<?> first = executor.submit(() -> run(spark, plan, brokers, stop));
      String outputA = SparkHandoffSpecs.continuousTopic(plan, 0);
      String outputB = SparkHandoffSpecs.continuousTopic(plan, 1);
      List<TopicPartition> outputs =
          List.of(new TopicPartition(outputA, 0), new TopicPartition(outputB, 0));
      List<ConsumerRecord<String, String>> records;
      try {
        reader.assign(outputs);
        reader.seekToBeginning(outputs);
        records = awaitRecords(reader, 1);
        assertTrue(records.getFirst().value().contains("first"));
        assertTrue(records.getFirst().value().contains("2024-02-29"));
        assertEquals("x", records.getFirst().key());
      } finally {
        stop.set(true);
        first.get(30, TimeUnit.SECONDS);
      }
      producer
          .send(
              new ProducerRecord<>(
                  topics.get(1), "x", "{\"id\":\"x\",\"name\":\"second\",\"date\":\"20250301\"}"))
          .get();
      AtomicBoolean stopAgain = new AtomicBoolean();
      Future<?> second = executor.submit(() -> run(spark, plan, brokers, stopAgain));
      try {
        records.addAll(awaitRecords(reader, 1));
        // Allow a replay of source A to surface; a fresh random group would emit it again.
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline)
          reader.poll(Duration.ofMillis(200)).forEach(records::add);
        assertEquals(2, records.size());
        assertTrue(records.getLast().value().contains("2025-03-01"));
        assertEquals("x", records.getLast().key());
        assertEquals(
            Set.of(outputA, outputB),
            records.stream()
                .map(ConsumerRecord::topic)
                .collect(java.util.stream.Collectors.toSet()));
      } finally {
        stopAgain.set(true);
        second.get(30, TimeUnit.SECONDS);
      }
    } finally {
      spark.stop();
    }
  }

  private static void run(
      SparkSession spark, ExecutionPlan plan, String brokers, AtomicBoolean stop) {
    try {
      ContinuousSparkStageRunner.run(spark, plan, brokers, stop);
    } catch (Exception failure) {
      throw new CompletionException(failure);
    }
  }

  private static List<ConsumerRecord<String, String>> awaitRecords(
      KafkaConsumer<String, String> consumer, int count) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (records.size() < count && System.nanoTime() < deadline)
      consumer.poll(Duration.ofMillis(250)).forEach(records::add);
    assertEquals(count, records.size());
    return records;
  }
}
