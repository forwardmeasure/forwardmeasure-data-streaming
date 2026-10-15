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

import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.PrefixedKeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;

/**
 * A continuous Pekko run's graph, with every node's state on the installation's journal. One record
 * at a time: {@link #process} computes its outputs against staged state, the caller writes them to
 * the sink, then {@link #commit} makes the record's state changes durable - or {@link #discard}
 * drops them if the write failed, so a replayed record meets the old state again.
 */
final class JournalGraph implements AutoCloseable {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final StatefulGraph graph;
  private final JournalKeyedState journal;
  private final StagedKeyedState staged;
  private final MalformedRecordPolicy policy;

  /**
   * Recovers the graph's state from the journal: one entity per run, every node's state under its
   * own key prefix, so a record's changes to all nodes are one event. {@code identity} is the run's
   * stable identity, so a restarted or replaced worker recovers the same state.
   */
  JournalGraph(
      ActorSystem system,
      String identity,
      TransformGraph transforms,
      String blockingField,
      MalformedRecordPolicy policy) {
    this.policy = policy;
    this.journal =
        JournalKeyedState.open(
            system, "fds-graph|" + identity, "fds-graph-state-" + identity, TIMEOUT);
    this.staged = new StagedKeyedState(journal);
    this.graph =
        new StatefulGraph(
            transforms,
            NamedTransformCatalog.installed(),
            blockingField,
            node -> new PrefixedKeyedState(node, staged));
  }

  /** Where an input record was read: its topic, partition and offset. */
  record InputPosition(String topic, int partition, long offset) {}

  /** Not a node name (those never contain NUL), so it never meets a node's own keys. */
  private static final String APPLIED = "\u0000applied|";

  /**
   * The record's outputs, its state changes staged. A record whose changes are already on the
   * journal - the worker stopped after committing its state but before committing its offset, and
   * the record came round again - changes nothing and emits nothing: its outputs were written
   * before its state was committed. Each partition's last applied offset is part of the same
   * journal event as the state it explains.
   */
  List<Map<String, Object>> process(
      String sourceKey, Map<String, Object> record, InputPosition position) {
    String appliedKey = APPLIED + position.topic() + "|" + position.partition();
    var applied = staged.get(appliedKey);
    if (applied.isPresent() && Long.parseLong(applied.get()) >= position.offset()) {
      return List.of();
    }
    List<Map<String, Object>> outputs = GraphRecords.process(graph, sourceKey, record, policy);
    if (!staged.changes().isEmpty()) staged.put(appliedKey, Long.toString(position.offset()));
    return outputs;
  }

  /** Persists the record's staged changes as one event, then applies them. */
  CompletionStage<Done> commit() {
    return journal
        .persist(staged.changes())
        .thenApply(
            ignored -> {
              staged.commit();
              return Done.getInstance();
            });
  }

  void discard() {
    staged.discard();
  }

  @Override
  public void close() {
    journal.close();
  }
}
