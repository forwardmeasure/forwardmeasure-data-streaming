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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.core.SparkHandoffSpecs;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Input;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphParity;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The graph steps a Spark stage owns - every stateful parity case run entirely in Spark, bounded
 * and continuous - give exactly the outputs the delivery engines give (the shared fixtures), and a
 * bounded joining graph behind a Spark stage gets each source handed off separately.
 */
@WithKafkaContainer
class SparkStatefulGraphIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark = SparkSession.builder().appName("spark-stateful-graph").master("local[2]").getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) spark.stop();
  }

  static Stream<String> cases() {
    return StatefulGraphFixtures.all().stream().map(Case::name);
  }

  /** Every step of the graph in a bounded Spark stage. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  @Timeout(180)
  void boundedSparkRunsEveryGraphStepAsTheEnginesDo(String name, KafkaTestContainer kafka)
      throws Exception {
    Case fixture = fixture(name);
    String brokers = kafka.bootstrapServers();
    var run = StatefulGraphParity.prepare(fixture, brokers);
    ExecutionPlan plan =
        plan(fixture, run, ExecutionMode.BOUNDED, brokers, fixture.graph().chain().size());
    if (plan.sources().size() == 1) SparkIngestionRunner.run(spark, plan, brokers);
    else SparkCorrelationRunner.run(spark, plan, brokers);
    assertEquals(
        StatefulGraphFixtures.canonical(fixture.bounded()),
        StatefulGraphFixtures.canonical(read(brokers, handoff(plan))));
  }

  /**
   * Every step of the graph in a continuous Spark stage, stopped midway and started again: the
   * state on the changelog topic, committed with the offsets, carries on exactly.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  @Timeout(300)
  void continuousSparkRunsEveryGraphStepAcrossARestart(String name, KafkaTestContainer kafka)
      throws Exception {
    Case fixture = fixture(name);
    String brokers = kafka.bootstrapServers();
    var run = StatefulGraphParity.topics(fixture, brokers);
    ExecutionPlan plan =
        plan(fixture, run, ExecutionMode.CONTINUOUS, brokers, fixture.graph().chain().size());
    int half = fixture.inputs().size() / 2;
    try (var executor = Executors.newSingleThreadExecutor()) {
      StatefulGraphParity.produce(run, brokers, 0, half);
      AtomicBoolean stop = new AtomicBoolean();
      Future<?> first = executor.submit(() -> runContinuous(plan, brokers, stop));
      try {
        awaitCount(brokers, handoff(plan), outputsOf(fixture, half));
        // Let the batch that produced them commit before stopping.
        Thread.sleep(2000);
      } finally {
        stop.set(true);
        first.get(60, TimeUnit.SECONDS);
      }
      StatefulGraphParity.produce(run, brokers, half, fixture.inputs().size());
      AtomicBoolean stopAgain = new AtomicBoolean();
      Future<?> second = executor.submit(() -> runContinuous(plan, brokers, stopAgain));
      try {
        awaitCount(brokers, handoff(plan), fixture.continuous().size());
        Thread.sleep(3000);
      } finally {
        stopAgain.set(true);
        second.get(60, TimeUnit.SECONDS);
      }
    }
    assertEquals(
        StatefulGraphFixtures.canonical(fixture.continuous()),
        StatefulGraphFixtures.canonical(read(brokers, handoff(plan))));
  }

  /**
   * A bounded joining graph behind a Spark stage that owns none of its steps: each source's mapped
   * records go to their own topic, unmerged, for the delivery stage to join.
   */
  @Test
  @Timeout(180)
  void aBoundedJoinBehindSparkGetsEachSourceOnItsOwnTopic(KafkaTestContainer kafka)
      throws Exception {
    Case fixture = StatefulGraphFixtures.windowJoin();
    String brokers = kafka.bootstrapServers();
    var run = StatefulGraphParity.prepare(fixture, brokers);
    ExecutionPlan plan = plan(fixture, run, ExecutionMode.BOUNDED, brokers, 0);
    SparkCorrelationRunner.run(spark, plan, brokers);
    for (int index = 0; index < fixture.sources().size(); index++) {
      String source = fixture.sources().get(index);
      List<Map<String, Object>> expected = new ArrayList<>();
      for (Input input : fixture.inputs())
        if (input.sourceKey().equals(source)) expected.add(input.record());
      assertEquals(
          StatefulGraphFixtures.canonical(expected),
          StatefulGraphFixtures.canonical(
              withoutNulls(read(brokers, SparkHandoffSpecs.sourceTopic(plan, index)))),
          source);
    }
  }

  private static ExecutionPlan plan(
      Case fixture,
      StatefulGraphParity.Run run,
      ExecutionMode mode,
      String brokers,
      int graphSteps) {
    List<SourcePlan> sources = new ArrayList<>();
    double trust = 1.0;
    for (String source : fixture.sources()) {
      String topic = run.topics().get(source);
      SourceSpec spec =
          mode == ExecutionMode.BOUNDED
              ? new SourceSpec(
                  "kafka",
                  "n/a",
                  null,
                  null,
                  null,
                  Map.of(
                      "kafka.bootstrap.servers",
                      brokers,
                      "subscribe",
                      topic,
                      "startingOffsets",
                      "earliest",
                      "endingOffsets",
                      "latest"))
              : new SourceSpec("kafka", "kafka:" + topic + "?brokers=" + brokers, null, null);
      sources.add(
          new SourcePlan(
              source,
              spec,
              StatefulGraphFixtures.passThrough(source, fixture.inputFields()),
              trust));
      trust /= 2;
    }
    return new ExecutionPlan(
        new ExecutionProfile(
            sources.size() > 1 ? SourceCardinality.CORRELATED : SourceCardinality.SINGLE,
            mode,
            DeliveryEngineKind.KAFKA_STREAMS),
        sources,
        fixture.blockingField(),
        Optional.of(
            new SparkStagePlan(
                List.of("test_screen_against_reference"),
                "fds-spark-graph-" + UUID.randomUUID(),
                graphSteps)),
        fixture.graph(),
        new SinkSpec("kafka", brokers, "unused-destination", null, null, Map.of()),
        null,
        null);
  }

  private static String handoff(ExecutionPlan plan) {
    return plan.sparkStage().orElseThrow().handoffTopic();
  }

  private static void runContinuous(ExecutionPlan plan, String brokers, AtomicBoolean stop) {
    try {
      ContinuousSparkStageRunner.run(spark, plan, brokers, stop);
    } catch (Exception failure) {
      throw new CompletionException(failure);
    }
  }

  /** How many outputs a continuous run has written after the first {@code count} inputs. */
  private static int outputsOf(Case fixture, int count) {
    StatefulGraph graph = GraphRecords.bounded(fixture.graph(), fixture.blockingField());
    int outputs = 0;
    for (Input input : fixture.inputs().subList(0, count))
      outputs += graph.process(input.sourceKey(), input.record()).size();
    return outputs;
  }

  private static void awaitCount(String brokers, String topic, int count) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
    while (read(brokers, topic).size() < count && System.nanoTime() < deadline) Thread.sleep(500);
  }

  /** Every committed record on {@code topic}, as JSON objects. */
  private static List<Map<String, Object>> read(String brokers, String topic) throws Exception {
    List<Map<String, Object>> records = new ArrayList<>();
    try (var consumer =
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
      if (consumer.partitionsFor(topic).isEmpty()) return records;
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))
          && System.nanoTime() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250)))
          records.add(JSON.readValue(record.value(), new TypeReference<Map<String, Object>>() {}));
      }
    }
    return records;
  }

  /** A source mapper passes through every field it names; a record without one has it null. */
  private static List<Map<String, Object>> withoutNulls(List<Map<String, Object>> records) {
    List<Map<String, Object>> cleaned = new ArrayList<>();
    for (Map<String, Object> record : records) {
      Map<String, Object> copy = new java.util.LinkedHashMap<>(record);
      copy.values().removeIf(java.util.Objects::isNull);
      cleaned.add(copy);
    }
    return cleaned;
  }

  private static Case fixture(String name) {
    return StatefulGraphFixtures.all().stream()
        .filter(candidate -> candidate.name().equals(name))
        .findFirst()
        .orElseThrow();
  }
}
