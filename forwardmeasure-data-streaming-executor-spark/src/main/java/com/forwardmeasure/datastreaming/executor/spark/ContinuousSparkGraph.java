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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.CorrelationMerge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords.Sourced;
import com.forwardmeasure.datastreaming.core.stateful.InMemoryKeyedState;
import com.forwardmeasure.datastreaming.core.stateful.PrefixedKeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The graph steps a continuous Spark stage owns ({@code SparkStagePlan.graphSteps}), one poll batch
 * at a time. Map steps - the {@code HEAVY} ones among them - run distributed; correlation and
 * stateful steps run on the driver, in arrival order, with {@link StatefulGraph} deciding every
 * output exactly as on a delivery engine. Their state lives on the driver and on a compacted
 * changelog topic: each batch's changes are written in the same Kafka transaction as its outputs
 * and its input offsets, and applied to the driver's copy only once that commits - so a restarted
 * stage recovers exactly the state that matches the offsets it resumes from.
 */
final class ContinuousSparkGraph {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousSparkGraph.class);
  private static final ObjectMapper JSON =
      JsonMapper.builder().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();
  private static final TypeReference<Map<String, Object>> RECORD = new TypeReference<>() {};

  /** Not a step name (those never contain NUL), so correlation keys never meet a step's. */
  private static final String CORRELATION = "\u0000correlation";

  private final ExecutionPlan plan;
  private final JavaSparkContext context;
  private final InMemoryKeyedState committed = new InMemoryKeyedState();
  private final StagedKeyedState staged = new StagedKeyedState(committed);
  private final List<TransformGraph> steps = new ArrayList<>();
  private final List<StatefulGraph> statefulSteps = new ArrayList<>();
  private final List<SourcePlan> byTrust;
  private final boolean correlates;
  private final MalformedRecordPolicy policy;

  ContinuousSparkGraph(JavaSparkContext context, ExecutionPlan plan, String brokers)
      throws Exception {
    this.plan = plan;
    this.context = context;
    this.policy = MalformedRecordPolicy.from(plan.errors());
    TransformGraph owned = plan.transforms().first(plan.sparkStage().orElseThrow().graphSteps());
    this.correlates = plan.sources().size() > 1 && !plan.transforms().joins();
    this.byTrust =
        plan.sources().stream()
            .sorted(
                Comparator.comparingDouble(SourcePlan::trustWeight)
                    .reversed()
                    .thenComparing(SourcePlan::sourceKey))
            .toList();
    for (int index = 0; index < owned.chain().size(); index++) {
      TransformGraph step = owned.first(index + 1).after(index);
      String name = step.entryNode();
      steps.add(step);
      statefulSteps.add(
          step.chain().getFirst().kind() == TransformGraph.NodeKind.MAP
              ? null
              : new StatefulGraph(
                  step,
                  NamedTransformCatalog.installed(),
                  plan.blockingField(),
                  ignored -> new PrefixedKeyedState(name, staged)));
    }
    createTopics(brokers);
    restore(brokers);
  }

  /** The compacted topic holding the owned steps' state. */
  static String changelogTopic(ExecutionPlan plan) {
    return plan.sparkStage().orElseThrow().handoffTopic() + "-graph-state";
  }

  /**
   * One poll batch through the owned steps: the records to send in the batch's transaction - its
   * outputs, to the handoff topic, and its state changes, to the changelog. Call {@link #committed}
   * once the transaction commits, {@link #discarded} if it does not.
   */
  List<ProducerRecord<String, String>> process(
      ConsumerRecords<String, String> records, List<String> sourceTopics) throws Exception {
    try {
      return stage(records, sourceTopics);
    } catch (Exception failure) {
      staged.discard();
      throw failure;
    }
  }

  private List<ProducerRecord<String, String>> stage(
      ConsumerRecords<String, String> records, List<String> sourceTopics) throws Exception {
    List<Input> inputs = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
      int source = sourceTopics.indexOf(record.topic());
      if (source >= 0) inputs.add(new Input(source, record.value()));
    }
    List<Sourced> current = map(inputs);
    if (correlates) current = correlate(current);
    for (int index = 0; index < steps.size(); index++) {
      StatefulGraph stateful = statefulSteps.get(index);
      if (stateful == null) {
        current = mapStep(steps.get(index), current);
      } else {
        List<Sourced> next = new ArrayList<>();
        for (Sourced value : current)
          for (Map<String, Object> output :
              GraphRecords.process(stateful, value.sourceKey(), value.record(), policy))
            next.add(new Sourced(value.sourceKey(), output));
        current = next;
      }
    }
    String handoff = plan.sparkStage().orElseThrow().handoffTopic();
    List<ProducerRecord<String, String>> sends = new ArrayList<>();
    for (Sourced output : current)
      sends.add(new ProducerRecord<>(handoff, null, JSON.writeValueAsString(output.record())));
    for (StagedKeyedState.Change change : staged.changes())
      sends.add(new ProducerRecord<>(changelogTopic(plan), change.key(), change.value()));
    return sends;
  }

  /** The batch's transaction committed: its state changes are now the driver's state. */
  void committed() {
    staged.commit();
  }

  /** The batch's transaction aborted: its state changes are dropped. */
  void discarded() {
    staged.discard();
  }

  /** One input record and the index of the source it was read for. */
  private record Input(int source, String value) implements java.io.Serializable {}

  /** Each record mapped by its own source's mapper, distributed, in arrival order. */
  private List<Sourced> map(List<Input> inputs) {
    if (inputs.isEmpty()) return List.of();
    List<String> keys = plan.sources().stream().map(SourcePlan::sourceKey).toList();
    List<TransformSpec> mappers = plan.sources().stream().map(SourcePlan::mapper).toList();
    MalformedRecordPolicy malformed = policy;
    return context
        .parallelize(inputs)
        .mapPartitions(
            iterator -> {
              ObjectMapper parser = new ObjectMapper();
              FieldMappingEngine mapper = new FieldMappingEngine();
              List<Sourced> result = new ArrayList<>();
              while (iterator.hasNext()) {
                Input input = iterator.next();
                try {
                  Map<String, Object> row = parser.readValue(input.value(), RECORD);
                  result.add(
                      new Sourced(
                          keys.get(input.source()),
                          mapper.map(new MapSourceRow(row), mappers.get(input.source()))));
                } catch (Exception failure) {
                  if (malformed != MalformedRecordPolicy.SKIP) throw failure;
                  LOGGER.warn("Skipping malformed continuous Spark input", failure);
                }
              }
              return result.iterator();
            })
        .collect();
  }

  /** A map step, distributed, in order. */
  private List<Sourced> mapStep(TransformGraph step, List<Sourced> values) {
    if (values.isEmpty()) return List.of();
    String blockingField = plan.blockingField();
    MalformedRecordPolicy malformed = policy;
    return context
        .parallelize(values)
        .mapPartitions(
            iterator -> {
              StatefulGraph graph = GraphRecords.bounded(step, blockingField);
              List<Sourced> outputs = new ArrayList<>();
              while (iterator.hasNext()) {
                Sourced value = iterator.next();
                for (Map<String, Object> output :
                    GraphRecords.process(graph, value.sourceKey(), value.record(), malformed))
                  outputs.add(new Sourced(value.sourceKey(), output));
              }
              return outputs.iterator();
            })
        .collect();
  }

  /**
   * Each record's source contribution kept per blocking key, and the key's merge - every source's
   * latest contribution, by trust - emitted, as the delivery engines correlate.
   */
  private List<Sourced> correlate(List<Sourced> values) throws Exception {
    PrefixedKeyedState contributions = new PrefixedKeyedState(CORRELATION, staged);
    List<Sourced> merged = new ArrayList<>();
    for (Sourced value : values) {
      Object raw = plan.blockingField() == null ? null : value.record().get(plan.blockingField());
      String key = raw == null ? "" : String.valueOf(raw).strip().toUpperCase(Locale.ROOT);
      if (key.isBlank()) {
        IllegalArgumentException missing =
            new IllegalArgumentException(
                "Record from " + value.sourceKey() + " has no correlation key");
        if (policy == MalformedRecordPolicy.FAIL) throw missing;
        LOGGER.warn("Skipping record without a correlation key: {}", missing.getMessage());
        continue;
      }
      contributions.put(
          key + "\u0001" + value.sourceKey(), JSON.writeValueAsString(value.record()));
      List<Map<String, Object>> rows = new ArrayList<>();
      for (SourcePlan source : byTrust) {
        var stored = contributions.get(key + "\u0001" + source.sourceKey());
        if (stored.isPresent()) rows.add(JSON.readValue(stored.get(), RECORD));
      }
      merged.add(new Sourced(null, CorrelationMerge.merge(rows, plan.mergePolicy())));
    }
    return merged;
  }

  private void createTopics(String brokers) throws Exception {
    try (Admin admin = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
      for (NewTopic topic :
          List.of(
              new NewTopic(plan.sparkStage().orElseThrow().handoffTopic(), 1, (short) -1),
              new NewTopic(changelogTopic(plan), 1, (short) -1)
                  .configs(Map.of("cleanup.policy", "compact")))) {
        try {
          admin.createTopics(List.of(topic)).all().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
          if (!(failure.getCause() instanceof TopicExistsException)) throw failure;
        }
      }
    }
  }

  /** The committed state: the changelog read to its end, committed transactions only. */
  private void restore(String brokers) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(changelogTopic(plan)).stream()
              .map(info -> new TopicPartition(info.topic(), info.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      Map<TopicPartition, Long> position = new HashMap<>();
      for (TopicPartition partition : partitions)
        position.put(partition, consumer.position(partition));
      while (partitions.stream().anyMatch(p -> position.get(p) < end.get(p))) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
          if (record.value() == null) committed.remove(record.key());
          else committed.put(record.key(), record.value());
        }
        for (TopicPartition partition : partitions)
          position.put(partition, consumer.position(partition));
      }
    }
    LOGGER.info("ContinuousSparkGraph: restored state topic={}", changelogTopic(plan));
  }
}
