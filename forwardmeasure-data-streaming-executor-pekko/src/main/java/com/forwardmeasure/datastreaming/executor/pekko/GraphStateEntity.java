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

import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState.Change;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.RetentionCriteria;

/**
 * One graph node's keyed state on the installation's journal (PostgreSQL or Cassandra): every
 * committed set of changes is one event, with a snapshot every 100 events, so the state survives a
 * restart. The stream reads its own in-memory copy and only writes through this entity.
 */
final class GraphStateEntity
    extends EventSourcedBehavior<
        GraphStateEntity.Command, GraphStateEntity.Changed, GraphStateEntity.State> {

  sealed interface Command {}

  /** Persists {@code changes}, replying once they are durable. */
  record Apply(List<Change> changes, ActorRef<Done> replyTo) implements Command {}

  /** Replies with the recovered state. */
  record Read(ActorRef<Map<String, String>> replyTo) implements Command {}

  /** The event: a committed set of changes; a {@code null} value removes the key. */
  record Changed(List<Change> changes) implements JournalSerializable {}

  /** The state, and its snapshot form. */
  record State(TreeMap<String, String> values) implements JournalSerializable {
    State apply(Changed event) {
      for (Change change : event.changes()) {
        if (change.value() == null) values.remove(change.key());
        else values.put(change.key(), change.value());
      }
      return this;
    }
  }

  private GraphStateEntity(PersistenceId persistenceId) {
    super(persistenceId);
  }

  static Behavior<Command> create(String persistenceId) {
    return new GraphStateEntity(PersistenceId.ofUniqueId(persistenceId));
  }

  @Override
  public State emptyState() {
    return new State(new TreeMap<>());
  }

  @Override
  public CommandHandler<Command, Changed, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(
            Apply.class,
            (state, command) ->
                Effect()
                    .persist(new Changed(command.changes()))
                    .thenReply(command.replyTo(), ignored -> Done.getInstance()))
        .onCommand(
            Read.class,
            (state, command) -> Effect().reply(command.replyTo(), Map.copyOf(state.values())))
        .build();
  }

  @Override
  public EventHandler<State, Changed> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(Changed.class, (state, event) -> state.apply(event))
        .build();
  }

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(100, 2);
  }
}
