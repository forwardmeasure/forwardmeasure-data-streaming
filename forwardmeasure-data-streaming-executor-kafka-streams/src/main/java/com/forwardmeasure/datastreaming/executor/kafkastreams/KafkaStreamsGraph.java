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

import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.KeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * A spec's {@link TransformGraph} in a continuous Kafka Streams topology. Each node is its own
 * processor; a stateful node's records are first repartitioned, keyed by that node's key, onto one
 * partition, so one task holds the node's state and run-wide watermark, in a persistent store
 * (RocksDB with a changelog topic) that exactly-once processing commits with the input offsets. The
 * decisions are {@link StatefulGraph}'s, the same as on Pekko.
 */
final class KafkaStreamsGraph {

  /** The field a join's merged input carries its source key in, removed before processing. */
  static final String SOURCE_FIELD = "__fds_source";

  private KafkaStreamsGraph() {}

  /**
   * {@code mapped} through every node of {@code graph}. For a joining graph, every record must
   * carry {@link #SOURCE_FIELD}.
   *
   * @param prefix a run-unique prefix for this topology's store and repartition names
   */
  static KStream<String, Map<String, Object>> attach(
      StreamsBuilder builder,
      KStream<String, Map<String, Object>> mapped,
      TransformGraph graph,
      String blockingField,
      String sourceKey,
      MalformedRecordPolicy policy,
      String prefix) {
    StageRecordSerde serde = new StageRecordSerde();
    KStream<String, Map<String, Object>> current = mapped;
    List<String> names = graph.chainNames();
    List<TransformNode> nodes = graph.chain();
    for (int i = 0; i < nodes.size(); i++) {
      String name = names.get(i);
      TransformNode node = nodes.get(i);
      TransformGraph single = new TransformGraph(name, Map.of(name, node), List.of());
      if (node.kind() == TransformGraph.NodeKind.MAP) {
        StatefulGraph stateless =
            new StatefulGraph(
                single, NamedTransformCatalog.installed(), blockingField, ignored -> null);
        current =
            current.flatMapValues(
                value -> GraphRecords.process(stateless, sourceKey, value, policy));
        continue;
      }
      String keyField =
          node.kind() == TransformGraph.NodeKind.WINDOW_JOIN ? blockingField : node.key();
      String storeName = prefix + "-" + i + "-state";
      builder.addStateStore(
          Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(storeName), Serdes.String(), Serdes.String()));
      current =
          current
              .selectKey((key, value) -> String.valueOf(value.get(keyField)))
              // One partition: a node's watermark is the latest event time of the whole run, as
              // on Pekko (one entity per run), not of one input partition - so windows close and
              // joins expire identically on both engines.
              .repartition(
                  Repartitioned.with(Serdes.String(), serde)
                      .withName(prefix + "-" + i + "-by-key")
                      .withNumberOfPartitions(1))
              .processValues(
                  () -> new NodeProcessor(single, blockingField, sourceKey, policy, storeName),
                  storeName);
    }
    return current;
  }

  /** Tags a source's mapped records for a join, which needs to know which side each came from. */
  static KStream<String, Map<String, Object>> tagged(
      KStream<String, Map<String, Object>> mapped, String sourceKey) {
    return mapped.mapValues(
        value -> {
          Map<String, Object> tagged = new LinkedHashMap<>(value);
          tagged.put(SOURCE_FIELD, sourceKey);
          return tagged;
        });
  }

  private static final class NodeProcessor
      implements FixedKeyProcessor<String, Map<String, Object>, Map<String, Object>> {
    private final TransformGraph graph;
    private final String blockingField;
    private final String sourceKey;
    private final MalformedRecordPolicy policy;
    private final String storeName;
    private FixedKeyProcessorContext<String, Map<String, Object>> context;
    private StatefulGraph stateful;

    NodeProcessor(
        TransformGraph graph,
        String blockingField,
        String sourceKey,
        MalformedRecordPolicy policy,
        String storeName) {
      this.graph = graph;
      this.blockingField = blockingField;
      this.sourceKey = sourceKey;
      this.policy = policy;
      this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Map<String, Object>> context) {
      this.context = context;
      KeyValueStore<String, String> store = context.getStateStore(storeName);
      KeyedState state = new StoreKeyedState(store);
      this.stateful =
          new StatefulGraph(
              graph, NamedTransformCatalog.installed(), blockingField, ignored -> state);
    }

    @Override
    public void process(FixedKeyRecord<String, Map<String, Object>> record) {
      Map<String, Object> value = record.value();
      String source = sourceKey;
      if (value.containsKey(SOURCE_FIELD)) {
        value = new LinkedHashMap<>(value);
        source = String.valueOf(value.remove(SOURCE_FIELD));
      }
      for (Map<String, Object> output : GraphRecords.process(stateful, source, value, policy)) {
        context.forward(record.withValue(output));
      }
    }
  }

  /** A Kafka Streams key-value store as graph state; it iterates in key order. */
  private record StoreKeyedState(KeyValueStore<String, String> store) implements KeyedState {
    @Override
    public Optional<String> get(String key) {
      return Optional.ofNullable(store.get(key));
    }

    @Override
    public void put(String key, String value) {
      store.put(key, value);
    }

    @Override
    public void remove(String key) {
      store.delete(key);
    }

    @Override
    public Iterable<String> keys() {
      List<String> keys = new ArrayList<>();
      try (KeyValueIterator<String, String> all = store.all()) {
        while (all.hasNext()) {
          KeyValue<String, String> entry = all.next();
          keys.add(entry.key);
        }
      }
      keys.sort(String::compareTo);
      return keys;
    }
  }
}
