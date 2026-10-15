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

import com.forwardmeasure.datastreaming.core.stateful.KeyedState;
import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

/**
 * A graph node's {@link KeyedState} on the Pekko journal: reads come from an in-memory copy loaded
 * when it opens (the entity's recovered state); writes reach the journal only through {@link
 * #persist}, which a stream calls once a record's outputs are written.
 */
final class JournalKeyedState implements KeyedState {

  private final ActorRef<GraphStateEntity.Command> entity;
  private final ActorSystem system;
  private final Duration timeout;
  private final TreeMap<String, String> values;

  private JournalKeyedState(
      ActorRef<GraphStateEntity.Command> entity,
      ActorSystem system,
      Duration timeout,
      Map<String, String> recovered) {
    this.entity = entity;
    this.system = system;
    this.timeout = timeout;
    this.values = new TreeMap<>(recovered);
  }

  /**
   * Opens the state persisted as {@code persistenceId}, recovering it from the journal.
   *
   * @param actorName a name unique among this system's top-level actors
   */
  static JournalKeyedState open(
      ActorSystem system, String persistenceId, String actorName, Duration timeout) {
    PekkoJournalProfiles.requireJournal(system);
    ActorRef<GraphStateEntity.Command> entity =
        Adapter.spawn(system, GraphStateEntity.create(persistenceId), actorName);
    Map<String, String> recovered =
        AskPattern.<GraphStateEntity.Command, Map<String, String>>ask(
                entity, GraphStateEntity.Read::new, timeout, Adapter.toTyped(system).scheduler())
            .toCompletableFuture()
            .join();
    return new JournalKeyedState(entity, system, timeout, recovered);
  }

  /** Makes {@code changes} durable on the journal; apply them here afterwards (staged commit). */
  CompletionStage<Done> persist(List<StagedKeyedState.Change> changes) {
    if (changes.isEmpty())
      return java.util.concurrent.CompletableFuture.completedFuture(Done.getInstance());
    return AskPattern.<GraphStateEntity.Command, Done>ask(
        entity,
        replyTo -> new GraphStateEntity.Apply(List.copyOf(changes), replyTo),
        timeout,
        Adapter.toTyped(system).scheduler());
  }

  /** Stops the entity and waits until it has, so the same state can be reopened at once. */
  void close() {
    try {
      org.apache.pekko.pattern.Patterns.gracefulStop(Adapter.toClassic(entity), timeout)
          .toCompletableFuture()
          .get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted stopping graph state", interrupted);
    } catch (java.util.concurrent.ExecutionException
        | java.util.concurrent.TimeoutException failure) {
      throw new IllegalStateException("Graph state did not stop", failure);
    }
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.ofNullable(values.get(key));
  }

  @Override
  public void put(String key, String value) {
    values.put(key, value);
  }

  @Override
  public void remove(String key) {
    values.remove(key);
  }

  @Override
  public Iterable<String> keys() {
    return List.copyOf(values.keySet());
  }
}
