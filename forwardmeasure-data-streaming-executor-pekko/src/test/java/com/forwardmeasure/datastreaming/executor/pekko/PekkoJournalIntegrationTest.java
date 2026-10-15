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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.StagedKeyedState;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Input;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Continuous Pekko state on the installation's real journal - PostgreSQL (the tenant database's
 * {@code data_streaming} schema, found through the tenant registry from the worker's tenant id) and
 * Cassandra (FDS's own keyspaces, as FDS's own login) - surviving a worker restart: correlation
 * contributions and every graph node's state.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PekkoJournalIntegrationTest {

  private final Map<PekkoJournalFixture.Backend, PekkoJournalFixture> journals =
      new java.util.EnumMap<>(PekkoJournalFixture.Backend.class);

  @BeforeAll
  void startJournals() {
    for (PekkoJournalFixture.Backend backend : PekkoJournalFixture.Backend.values())
      journals.put(backend, PekkoJournalFixture.start(backend));
  }

  @AfterAll
  void stopJournals() {
    journals.values().forEach(PekkoJournalFixture::close);
  }

  // ---- correlation state ----

  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void restoresBothSourcesAndRepairsAnUnacknowledgedSinkWithoutRegressingState(
      PekkoJournalFixture.Backend backend) throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    ExecutionPlan plan = plan();
    String identity = "fds-state-test-" + UUID.randomUUID();
    var output = new AtomicReference<Map<String, Object>>();
    ActorSystem first = journal.system("correlation-first");
    try (var state = new JournalCorrelationState(first, plan, identity)) {
      state.updateAndWrite(
          row("high", 10, "winner", "one"),
          merged -> {
            output.set(merged);
            return CompletableFuture.completedFuture(null);
          });
      assertThrows(
          IllegalStateException.class,
          () ->
              state.updateAndWrite(
                  row("low", 20, "loser", "two"),
                  merged ->
                      CompletableFuture.failedFuture(
                          new IllegalStateException("sink unavailable"))));
    } finally {
      terminate(first);
    }
    ActorSystem restarted = journal.system("correlation-restarted");
    try (var restored = new JournalCorrelationState(restarted, plan, identity)) {
      // The input commit did not happen: even an earlier replay emits the restored latest merge.
      restored.updateAndWrite(
          row("low", 19, "obsolete", "old"),
          merged -> {
            output.set(merged);
            return CompletableFuture.completedFuture(null);
          });
      assertEquals("winner", output.get().get("status"));
      assertEquals(List.of("one", "two"), output.get().get("identifiers"));
    } finally {
      terminate(restarted);
    }
  }

  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void topicAndPartitionChangesResetOffsetsButSerializationFailureCannotPoisonState(
      PekkoJournalFixture.Backend backend) throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    ExecutionPlan plan = plan();
    String identity = "fds-offset-contract-" + UUID.randomUUID();
    var observed = new AtomicReference<Map<String, Object>>();
    java.util.function.Function<Map<String, Object>, java.util.concurrent.CompletionStage<Void>>
        sink =
            value -> {
              observed.set(value);
              return CompletableFuture.completedFuture(null);
            };
    ActorSystem first = journal.system("offsets-first");
    try (var state = new JournalCorrelationState(first, plan, identity)) {
      state.updateAndWrite(row("high", 100, "original", "one"), sink);
      state.updateAndWrite(
          new JournalCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 0, 0, Map.of("status", "new-topic")),
          sink);
      assertEquals("new-topic", observed.get().get("status"));
      state.updateAndWrite(
          new JournalCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 0, Map.of("status", "new-partition")),
          sink);
      assertEquals("new-partition", observed.get().get("status"));
      assertThrows(
          IllegalStateException.class,
          () ->
              state.updateAndWrite(
                  new JournalCorrelationState.Contribution(
                      "KEY", "high", "replacement-topic", 1, 1, Map.of("bad", new Object())),
                  sink));
      state.updateAndWrite(
          new JournalCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 2, Map.of("status", "recovered")),
          sink);
      assertEquals(Map.of("status", "recovered"), observed.get());
    } finally {
      terminate(first);
    }
    ActorSystem restarted = journal.system("offsets-restarted");
    try (var restored = new JournalCorrelationState(restarted, plan, identity)) {
      restored.updateAndWrite(
          new JournalCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 1, Map.of("status", "obsolete")),
          sink);
      assertEquals(Map.of("status", "recovered"), observed.get());
    } finally {
      terminate(restarted);
    }
  }

  /**
   * PostgreSQL's journal keys each event by persistence id and sequence number, so a worker that
   * was superseded (its replacement already wrote) cannot append: the write fails, nothing it
   * computed reaches the sink. Deployments run one replica with the Recreate strategy, so this is
   * the backstop, not the mechanism.
   */
  @Test
  void aSupersededWriterCannotAppendToThePostgresqlJournal() throws Exception {
    PekkoJournalFixture journal = journals.get(PekkoJournalFixture.Backend.POSTGRESQL);
    ExecutionPlan plan = plan();
    String identity = "fds-fence-test-" + UUID.randomUUID();
    ActorSystem oldWorker = journal.system("fence-old");
    ActorSystem newWorker = journal.system("fence-new");
    try (var old = new JournalCorrelationState(oldWorker, plan, identity);
        var replacement = new JournalCorrelationState(newWorker, plan, identity)) {
      replacement.updateAndWrite(
          row("high", 2, "new", "two"),
          merged -> {
            assertEquals("new", merged.get("status"));
            return CompletableFuture.completedFuture(null);
          });
      var reachedSink = new java.util.concurrent.atomic.AtomicBoolean();
      assertThrows(
          IllegalStateException.class,
          () ->
              old.updateAndWrite(
                  row("high", 1, "old", "one"),
                  merged -> {
                    reachedSink.set(true);
                    return CompletableFuture.completedFuture(null);
                  }));
      assertTrue(!reachedSink.get(), "a superseded worker must not write its merge");
    } finally {
      terminate(oldWorker);
      terminate(newWorker);
    }
  }

  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void corruptPersistedStateFailsBeforeAnySourceIsConsumed(PekkoJournalFixture.Backend backend)
      throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    String identity = "fds-invalid-state-" + UUID.randomUUID();
    ActorSystem writer = journal.system("corrupt-writer");
    try {
      JournalKeyedState raw =
          JournalKeyedState.open(
              writer, "fds-correlation|" + identity, "raw-" + identity, Duration.ofSeconds(30));
      raw.persist(List.of(new StagedKeyedState.Change("[\"KEY\",\"high\"]", "not-json")))
          .toCompletableFuture()
          .get(30, java.util.concurrent.TimeUnit.SECONDS);
      raw.close();
    } finally {
      terminate(writer);
    }
    ActorSystem reader = journal.system("corrupt-reader");
    try {
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> new JournalCorrelationState(reader, plan(), identity));
      assertTrue(failure.getMessage().contains("Invalid correlation state"), failure.getMessage());
    } finally {
      terminate(reader);
    }
  }

  @Test
  void stateNeedsTheInstallationsJournal() {
    ActorSystem unconfigured =
        ActorSystem.create("no-journal-" + UUID.randomUUID().toString().substring(0, 8));
    try {
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> new JournalCorrelationState(unconfigured, plan(), "fds-no-journal"));
      assertTrue(failure.getMessage().contains("journal"), failure.getMessage());
    } finally {
      terminate(unconfigured);
    }
  }

  // ---- graph state ----

  /**
   * Every parity case, with the worker restarted (a fresh actor system recovering from the journal)
   * after every record: the outputs are exactly an uninterrupted continuous run's.
   */
  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void graphStateSurvivesARestartAfterEveryRecord(PekkoJournalFixture.Backend backend)
      throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    for (Case fixture : StatefulGraphFixtures.all()) {
      String identity = "fds-graph-" + fixture.name().replace('_', '-') + "-" + UUID.randomUUID();
      List<Map<String, Object>> outputs = new ArrayList<>();
      for (int i = 0; i < fixture.inputs().size(); i++) {
        Input input = fixture.inputs().get(i);
        ActorSystem worker = journal.system("graph-worker");
        try (JournalGraph graph =
            new JournalGraph(
                worker,
                identity,
                fixture.graph(),
                fixture.blockingField(),
                MalformedRecordPolicy.FAIL)) {
          outputs.addAll(graph.process(input.sourceKey(), input.record(), position(input, i)));
          graph.commit().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
          terminate(worker);
        }
      }
      assertEquals(
          StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
          StatefulGraphFixtures.canonicalInOrder(outputs),
          fixture.name());
    }
  }

  /**
   * A record whose write failed is discarded, not committed: after a restart its replay meets the
   * state as it was before the record, and produces the same outputs.
   */
  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void aDiscardedRecordIsNotOnTheJournalAfterARestart(PekkoJournalFixture.Backend backend)
      throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    Case fixture = StatefulGraphFixtures.windowAggregate();
    String identity = "fds-graph-discard-" + UUID.randomUUID();
    List<Map<String, Object>> outputs = new ArrayList<>();
    List<Input> inputs = fixture.inputs();
    for (int i = 0; i < inputs.size(); i++) {
      Input input = inputs.get(i);
      if (i == 3) {
        // The record that closes the first hour: its write fails, the worker dies.
        ActorSystem failing = journal.system("graph-failing");
        try (JournalGraph graph =
            new JournalGraph(
                failing, identity, fixture.graph(), null, MalformedRecordPolicy.FAIL)) {
          assertTrue(
              !graph.process(input.sourceKey(), input.record(), position(input, i)).isEmpty());
          graph.discard();
        } finally {
          terminate(failing);
        }
      }
      ActorSystem worker = journal.system("graph-worker");
      try (JournalGraph graph =
          new JournalGraph(worker, identity, fixture.graph(), null, MalformedRecordPolicy.FAIL)) {
        outputs.addAll(graph.process(input.sourceKey(), input.record(), position(input, i)));
        graph.commit().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
      } finally {
        terminate(worker);
      }
    }
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
        StatefulGraphFixtures.canonicalInOrder(outputs));
  }

  /**
   * The crash between committing a record's state and committing its offset: every record is
   * delivered again after a restart (the offset never committed). A record already on the journal
   * changes nothing and emits nothing a second time - counts, sums, collections and joins stay
   * exactly an uninterrupted run's.
   */
  @ParameterizedTest
  @EnumSource(PekkoJournalFixture.Backend.class)
  void aRecordRedeliveredAfterItsStateWasCommittedIsAppliedOnce(PekkoJournalFixture.Backend backend)
      throws Exception {
    PekkoJournalFixture journal = journals.get(backend);
    for (Case fixture : StatefulGraphFixtures.all()) {
      String identity = "fds-graph-redelivery-" + UUID.randomUUID();
      List<Map<String, Object>> outputs = new ArrayList<>();
      for (int i = 0; i < fixture.inputs().size(); i++) {
        Input input = fixture.inputs().get(i);
        ActorSystem first = journal.system("graph-committed");
        try (JournalGraph graph =
            new JournalGraph(
                first,
                identity,
                fixture.graph(),
                fixture.blockingField(),
                MalformedRecordPolicy.FAIL)) {
          outputs.addAll(graph.process(input.sourceKey(), input.record(), position(input, i)));
          graph.commit().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
          terminate(first);
        }
        // The worker died before the offset commit: the same record arrives again.
        ActorSystem restarted = journal.system("graph-redelivered");
        try (JournalGraph graph =
            new JournalGraph(
                restarted,
                identity,
                fixture.graph(),
                fixture.blockingField(),
                MalformedRecordPolicy.FAIL)) {
          assertEquals(
              List.of(),
              graph.process(input.sourceKey(), input.record(), position(input, i)),
              fixture.name() + " record " + i);
          graph.commit().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
          terminate(restarted);
        }
      }
      assertEquals(
          StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
          StatefulGraphFixtures.canonicalInOrder(outputs),
          fixture.name());
    }
  }

  /** Each fixture source is its own one-partition topic, offsets in arrival order. */
  private static JournalGraph.InputPosition position(Input input, int arrival) {
    return new JournalGraph.InputPosition("in-" + input.sourceKey(), 0, arrival);
  }

  @Test
  void postgresqlStateLivesInTheTenantDatabasesDataStreamingSchemaAsTheRuntimeRole()
      throws Exception {
    PekkoJournalFixture journal = journals.get(PekkoJournalFixture.Backend.POSTGRESQL);
    Case fixture = StatefulGraphFixtures.deduplicate();
    String identity = "fds-graph-location-" + UUID.randomUUID();
    ActorSystem worker = journal.system("graph-location");
    try (JournalGraph graph =
        new JournalGraph(worker, identity, fixture.graph(), null, MalformedRecordPolicy.FAIL)) {
      Input input = fixture.inputs().getFirst();
      graph.process(input.sourceKey(), input.record(), position(input, 0));
      graph.commit().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      terminate(worker);
    }
    String url = journal.postgres().hostJdbcUrl();
    String tenantUrl =
        url.substring(0, url.lastIndexOf('/') + 1)
            + com.forwardmeasure.jpa.tenancy.TenantDatabase.forAlias("journal").value();
    try (Connection runtime =
            DriverManager.getConnection(
                tenantUrl, PekkoJournalFixture.RUNTIME_ROLE, PekkoJournalFixture.RUNTIME_PASSWORD);
        var statement =
            runtime.prepareStatement(
                "SELECT count(*) FROM data_streaming.event_journal WHERE persistence_id = ?")) {
      statement.setString(1, "fds-graph|" + identity);
      try (ResultSet count = statement.executeQuery()) {
        assertTrue(count.next());
        assertEquals(1, count.getInt(1));
      }
    }
  }

  @Test
  void aWorkerForAnUnregisteredTenantFindsNoJournal() {
    PekkoJournalFixture journal = journals.get(PekkoJournalFixture.Backend.POSTGRESQL);
    Map<String, String> environment = new java.util.HashMap<>(journal.workerEnvironment());
    environment.put("TENANT_ID", UUID.randomUUID().toString());
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                PekkoJournalProfiles.forEngine(
                    "pekko-postgresql",
                    environment::get,
                    PekkoJournalProfiles::registeredTenantDatabase));
    assertTrue(failure.getMessage().contains("not in the tenant registry"), failure.getMessage());
  }

  private static ExecutionPlan plan() {
    var mapper =
        new TransformSpec(
            "party",
            List.of(new TransformSpec.FieldRule("key", "key", null, null, null, null, null)));
    return ExecutionPlanCompiler.compile(
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "high",
                    new SourceSpec("kafka", "kafka:high?brokers=unused:9092", null, null),
                    mapper,
                    1),
                new SourcePlan(
                    "low",
                    new SourceSpec("kafka", "kafka:low?brokers=unused:9092", null, null),
                    mapper,
                    .5)),
            "key",
            null,
            new SinkSpec("opensearch", "http://unused", "test", null, null),
            ExecutionMode.CONTINUOUS,
            null,
            null),
        DeliveryEngineKind.PEKKO_STREAMS);
  }

  private static JournalCorrelationState.Contribution row(
      String source, long offset, String status, String identifier) {
    return new JournalCorrelationState.Contribution(
        "KEY",
        source,
        source + "-topic",
        0,
        offset,
        Map.of("status", status, "identifiers", List.of(identifier)));
  }

  private static void terminate(ActorSystem system) {
    system.terminate();
    try {
      system
          .getWhenTerminated()
          .toCompletableFuture()
          .get(30, java.util.concurrent.TimeUnit.SECONDS);
    } catch (Exception failure) {
      throw new IllegalStateException("Actor system did not stop", failure);
    }
  }
}
