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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.core.CorrelationMerge;
import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.pekko.actor.ActorSystem;

/**
 * Per-source correlation state on the installation's journal (PostgreSQL or Cassandra): each
 * source's latest contribution per blocking key, recovered when a worker starts. A contribution is
 * made durable before the merged record is written, so a redelivered input record re-writes the
 * same merge. Correlated Pekko deployments run one active replica; the journal accepts one writer
 * per run identity.
 */
final class JournalCorrelationState implements AutoCloseable {

  private static final ObjectMapper JSON =
      com.fasterxml.jackson.databind.json.JsonMapper.builder()
          .enable(com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .enable(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .build();
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final JournalKeyedState journal;
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

  JournalCorrelationState(ActorSystem system, ExecutionPlan plan, String identity) {
    this.plan = plan;
    this.sources =
        plan.sources().stream()
            .sorted(
                Comparator.comparingDouble(SourcePlan::trustWeight)
                    .reversed()
                    .thenComparing(SourcePlan::sourceKey))
            .toList();
    this.journal =
        JournalKeyedState.open(
            system, "fds-correlation|" + identity, "fds-correlation-state-" + identity, TIMEOUT);
    for (String key : journal.keys()) {
      try {
        remember(JSON.readValue(journal.get(key).orElseThrow(), Contribution.class));
      } catch (JsonProcessingException failure) {
        throw new IllegalStateException("Invalid correlation state for " + identity, failure);
      }
    }
  }

  /** Serialized with the sink write so concurrent sources cannot publish an older merge last. */
  synchronized void updateAndWrite(
      Contribution contribution, Function<Map<String, Object>, CompletionStage<Void>> sink) {
    try {
      Contribution previous =
          state.getOrDefault(contribution.blockingKey(), Map.of()).get(contribution.sourceKey());
      if (previous == null
          || !previous.inputTopic().equals(contribution.inputTopic())
          || previous.partition() != contribution.partition()
          || previous.offset() < contribution.offset()) {
        String key =
            JSON.writeValueAsString(List.of(contribution.blockingKey(), contribution.sourceKey()));
        String value = JSON.writeValueAsString(contribution);
        journal
            .persist(List.of(new StagedKeyedState.Change(key, value)))
            .toCompletableFuture()
            .get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        journal.put(key, value);
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
    journal.close();
  }
}
