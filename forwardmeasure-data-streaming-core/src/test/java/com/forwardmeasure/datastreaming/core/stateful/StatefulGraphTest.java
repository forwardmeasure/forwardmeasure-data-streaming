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
package com.forwardmeasure.datastreaming.core.stateful;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformGraph.NodeKind;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Input;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** The engine-neutral decisions of every stateful node kind, on the shared parity fixtures. */
class StatefulGraphTest {

  static List<Case> cases() {
    return StatefulGraphFixtures.all();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void aBoundedRunEmitsExactlyTheFixturesOutputsInOrder(Case fixture) {
    StatefulGraph graph = GraphRecords.bounded(fixture.graph(), fixture.blockingField());
    List<Map<String, Object>> outputs = new ArrayList<>();
    for (Input input : fixture.inputs())
      outputs.addAll(graph.process(input.sourceKey(), input.record()));
    outputs.addAll(graph.finish());
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(fixture.bounded()),
        StatefulGraphFixtures.canonicalInOrder(outputs));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void aContinuousRunEmitsOnlyWhatEventTimeHasClosed(Case fixture) {
    StatefulGraph graph = GraphRecords.bounded(fixture.graph(), fixture.blockingField());
    List<Map<String, Object>> outputs = new ArrayList<>();
    for (Input input : fixture.inputs())
      outputs.addAll(graph.process(input.sourceKey(), input.record()));
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
        StatefulGraphFixtures.canonicalInOrder(outputs));
  }

  /**
   * A worker that stops midway and recovers its committed state - a fresh graph over the same
   * stores - emits exactly what an uninterrupted run would, at every split point.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void recoveringCommittedStateMidwayChangesNoOutput(Case fixture) {
    for (int split = 0; split <= fixture.inputs().size(); split++) {
      Map<String, KeyedState> stores = new HashMap<>();
      List<Map<String, Object>> outputs = new ArrayList<>();
      List<Input> inputs = fixture.inputs();
      StatefulGraph first = graphOver(fixture, stores);
      for (Input input : inputs.subList(0, split))
        outputs.addAll(first.process(input.sourceKey(), input.record()));
      StatefulGraph recovered = graphOver(fixture, stores);
      for (Input input : inputs.subList(split, inputs.size()))
        outputs.addAll(recovered.process(input.sourceKey(), input.record()));
      assertEquals(
          StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
          StatefulGraphFixtures.canonicalInOrder(outputs),
          "split at " + split);
    }
  }

  /**
   * Continuous engines stage each record's changes and commit them only after its outputs are
   * written: a discarded record (failed write) leaves state as it was, so its replay produces the
   * same outputs again.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void aDiscardedRecordReplaysToTheSameOutputs(Case fixture) {
    InMemoryKeyedState committed = new InMemoryKeyedState();
    StagedKeyedState staged = new StagedKeyedState(committed);
    StatefulGraph graph =
        new StatefulGraph(
            fixture.graph(),
            NamedTransformCatalog.installed(),
            fixture.blockingField(),
            node -> new PrefixedKeyedState(node, staged));
    List<Map<String, Object>> outputs = new ArrayList<>();
    for (Input input : fixture.inputs()) {
      List<Map<String, Object>> attempt = graph.process(input.sourceKey(), input.record());
      staged.discard();
      List<Map<String, Object>> replay = graph.process(input.sourceKey(), input.record());
      assertEquals(
          StatefulGraphFixtures.canonicalInOrder(attempt),
          StatefulGraphFixtures.canonicalInOrder(replay));
      staged.commit();
      outputs.addAll(replay);
    }
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(fixture.continuous()),
        StatefulGraphFixtures.canonicalInOrder(outputs));
  }

  @Test
  void stagedChangesAreInvisibleToTheCommittedStateUntilCommitted() {
    InMemoryKeyedState committed = new InMemoryKeyedState();
    committed.put("kept", "1");
    committed.put("dropped", "2");
    StagedKeyedState staged = new StagedKeyedState(committed);
    staged.put("added", "3");
    staged.remove("dropped");
    assertEquals(List.of("added", "kept"), staged.keys());
    assertEquals(List.of("dropped", "kept"), committed.keys());
    assertEquals(
        List.of(
            new StagedKeyedState.Change("added", "3"),
            new StagedKeyedState.Change("dropped", null)),
        staged.changes());
    staged.commit();
    assertEquals(List.of("added", "kept"), committed.keys());
    assertTrue(staged.changes().isEmpty());
    staged.put("added", "4");
    staged.discard();
    assertEquals("3", staged.get("added").orElseThrow());
  }

  @Test
  void eachNodeSeesOnlyItsOwnKeysInASharedStore() {
    InMemoryKeyedState shared = new InMemoryKeyedState();
    PrefixedKeyedState left = new PrefixedKeyedState("left", shared);
    PrefixedKeyedState right = new PrefixedKeyedState("right", shared);
    left.put("k", "l");
    right.put("k", "r");
    assertEquals("l", left.get("k").orElseThrow());
    assertEquals(List.of("k"), left.keys());
    left.remove("k");
    assertTrue(left.get("k").isEmpty());
    assertEquals("r", right.get("k").orElseThrow());
  }

  @Test
  void eventTimeAcceptsMillisInstantsOffsetsAndDates() {
    assertEquals(1000L, StatefulGraph.eventTime(Map.of("t", 1000), "t"));
    assertEquals(1000L, StatefulGraph.eventTime(Map.of("t", " 1000 "), "t"));
    assertEquals(0L, StatefulGraph.eventTime(Map.of("t", "1970-01-01T00:00:00Z"), "t"));
    assertEquals(0L, StatefulGraph.eventTime(Map.of("t", "1970-01-01T01:00:00+01:00"), "t"));
    assertEquals(86_400_000L, StatefulGraph.eventTime(Map.of("t", "1970-01-02"), "t"));
    assertThrows(IllegalArgumentException.class, () -> StatefulGraph.eventTime(Map.of(), "t"));
    assertThrows(
        IllegalArgumentException.class, () -> StatefulGraph.eventTime(Map.of("t", "soon"), "t"));
  }

  @Test
  void aRecordWithoutKeyOrTimeFailsSkipsOrDeadLettersPerPolicy() {
    Case fixture = StatefulGraphFixtures.deduplicate();
    Map<String, Object> noKey = Map.of("ts", "2026-01-01T00:00:00Z");
    Map<String, Object> noTime = Map.of("id", "a", "ts", "whenever");
    for (Map<String, Object> bad : List.of(noKey, noTime)) {
      StatefulGraph graph = GraphRecords.bounded(fixture.graph(), null);
      assertThrows(
          IllegalArgumentException.class,
          () -> GraphRecords.process(graph, "events", bad, MalformedRecordPolicy.FAIL));
      assertTrue(GraphRecords.process(graph, "events", bad, MalformedRecordPolicy.SKIP).isEmpty());
      assertTrue(
          GraphRecords.process(graph, "events", bad, MalformedRecordPolicy.DEAD_LETTER).isEmpty());
    }
    StatefulGraph aggregate =
        GraphRecords.bounded(StatefulGraphFixtures.windowAggregate().graph(), null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            aggregate.process(
                "payments",
                Map.of(
                    "account", "A", "ts", "2026-01-01T00:00:00Z", "amount", "lots", "ref", "x")));
  }

  @Test
  void aJoinNeedsTheBlockingFieldAndOneOfItsTwoSources() {
    Case join = StatefulGraphFixtures.windowJoin();
    assertThrows(IllegalArgumentException.class, () -> GraphRecords.bounded(join.graph(), null));
    StatefulGraph graph = GraphRecords.bounded(join.graph(), join.blockingField());
    assertThrows(
        IllegalArgumentException.class,
        () -> graph.process("returns", Map.of("order_id", "o1", "ts", "2026-01-01T00:00:00Z")));
  }

  @Test
  void aBoundedJoinMeetsItsSourcesInEventTimeOrderWhateverOrderTheyWereRead() {
    Case join = StatefulGraphFixtures.windowJoin();
    List<GraphRecords.Sourced> leftThenRight = new ArrayList<>();
    for (String source : join.sources())
      for (Input input : join.inputs())
        if (input.sourceKey().equals(source))
          leftThenRight.add(new GraphRecords.Sourced(input.sourceKey(), input.record()));
    List<Map<String, Object>> outputs =
        GraphRecords.boundedCorrelated(
            join.graph(),
            join.blockingField(),
            leftThenRight,
            List.of(),
            MalformedRecordPolicy.FAIL);
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(join.bounded()),
        StatefulGraphFixtures.canonicalInOrder(outputs));
  }

  @Test
  void aNonJoiningCorrelatedGraphRunsOnTheMergedRecords() {
    Case dedupe = StatefulGraphFixtures.deduplicate();
    List<Map<String, Object>> merged = dedupe.inputs().stream().map(Input::record).toList();
    assertEquals(
        StatefulGraphFixtures.canonicalInOrder(dedupe.bounded()),
        StatefulGraphFixtures.canonicalInOrder(
            GraphRecords.boundedCorrelated(
                dedupe.graph(), "id", List.of(), merged, MalformedRecordPolicy.FAIL)));
  }

  @Test
  void graphShapesOutsideTheChainModelAreRejected() {
    TransformNode dedupe =
        new TransformNode(
            NodeKind.DEDUPLICATE, null, "id", "ts", Duration.ofMinutes(1), null, null, null, null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TransformGraph(
                "a",
                Map.of("a", dedupe, "b", dedupe, "c", dedupe),
                List.of(
                    new TransformGraph.TransformEdge("a", "b", null),
                    new TransformGraph.TransformEdge("a", "c", null))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TransformGraph(
                "a",
                Map.of("a", dedupe, "b", dedupe),
                List.of(new TransformGraph.TransformEdge("a", "b", "x == 1"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TransformGraph(
                "a",
                Map.of(
                    "a",
                    dedupe,
                    "j",
                    StatefulGraphFixtures.windowJoin().graph().nodes().values().iterator().next()),
                List.of(new TransformGraph.TransformEdge("a", "j", null))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TransformNode(
                NodeKind.WINDOW_AGGREGATE,
                null,
                "id",
                "ts",
                Duration.ofMinutes(1),
                null,
                List.of(),
                null,
                null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TransformNode(
                NodeKind.DEDUPLICATE, null, "id", "ts", Duration.ZERO, null, null, null, null));
  }

  /**
   * One record that closes several windows yields every one of them, even when a later step rejects
   * one: only the rejected value is dropped (or fails the run, under the fail policy).
   */
  @Test
  void aRejectedValueDropsOnlyItselfNotTheRecordsOtherOutputs() {
    TransformGraph graph =
        new TransformGraph(
            "hourly",
            Map.of(
                "hourly",
                new TransformNode(
                    NodeKind.WINDOW_AGGREGATE,
                    null,
                    "account",
                    "ts",
                    Duration.ofHours(1),
                    null,
                    List.of(
                        new TransformGraph.Aggregation(
                            "n", TransformGraph.AggregationOp.COUNT, null),
                        new TransformGraph.Aggregation(
                            "region", TransformGraph.AggregationOp.FIRST, "region")),
                    null,
                    null),
                "per_region",
                new TransformNode(
                    NodeKind.DEDUPLICATE,
                    null,
                    "region",
                    "window_start",
                    Duration.ofMinutes(1),
                    null,
                    null,
                    null,
                    null)),
            List.of(new TransformGraph.TransformEdge("hourly", "per_region", null)));
    List<Map<String, Object>> inputs =
        List.of(
            Map.of("account", "A", "ts", "2026-01-01T00:10:00Z", "region", "eu"),
            // B has no region, so its window's output has none for the next step to key on.
            Map.of("account", "B", "ts", "2026-01-01T00:20:00Z"),
            Map.of("account", "A", "ts", "2026-01-01T01:30:00Z", "region", "eu"));
    StatefulGraph skipping = GraphRecords.bounded(graph, null);
    List<Map<String, Object>> outputs = new ArrayList<>();
    for (Map<String, Object> input : inputs)
      outputs.addAll(GraphRecords.process(skipping, "payments", input, MalformedRecordPolicy.SKIP));
    assertEquals(1, outputs.size(), outputs.toString());
    assertEquals("A", outputs.getFirst().get("account"));
    assertEquals("2026-01-01T00:00:00Z", outputs.getFirst().get("window_start"));

    StatefulGraph failing = GraphRecords.bounded(graph, null);
    GraphRecords.process(failing, "payments", inputs.get(0), MalformedRecordPolicy.FAIL);
    GraphRecords.process(failing, "payments", inputs.get(1), MalformedRecordPolicy.FAIL);
    assertThrows(
        IllegalArgumentException.class,
        () -> GraphRecords.process(failing, "payments", inputs.get(2), MalformedRecordPolicy.FAIL));
  }

  /** Windows that close together come out in time order, then key order - across many hours. */
  @Test
  void windowsClosingTogetherAreEmittedInTimeOrder() {
    // A twelve-hour grace keeps every hourly window open until the input ends, so all close at
    // once.
    TransformGraph graph =
        new TransformGraph(
            "hourly",
            Map.of(
                "hourly",
                new TransformNode(
                    NodeKind.WINDOW_AGGREGATE,
                    null,
                    "account",
                    "ts",
                    Duration.ofHours(1),
                    Duration.ofHours(12),
                    List.of(
                        new TransformGraph.Aggregation(
                            "n", TransformGraph.AggregationOp.COUNT, null)),
                    null,
                    null)),
            List.of());
    StatefulGraph stateful = GraphRecords.bounded(graph, null);
    for (String time : List.of("05:10", "00:10", "11:10", "02:10")) {
      for (String account : List.of("B", "A")) {
        assertTrue(
            stateful
                .process(
                    "payments", Map.of("account", account, "ts", "2026-01-01T" + time + ":00Z"))
                .isEmpty());
      }
    }
    List<String> closed =
        stateful.finish().stream()
            .map(output -> output.get("window_start") + "/" + output.get("account"))
            .toList();
    assertEquals(
        List.of(
            "2026-01-01T00:00:00Z/A",
            "2026-01-01T00:00:00Z/B",
            "2026-01-01T02:00:00Z/A",
            "2026-01-01T02:00:00Z/B",
            "2026-01-01T05:00:00Z/A",
            "2026-01-01T05:00:00Z/B",
            "2026-01-01T11:00:00Z/A",
            "2026-01-01T11:00:00Z/B"),
        closed);
    List<Long> times =
        List.of(Long.MIN_VALUE, -86_400_000L, -1L, 0L, 1L, 1_767_225_600_000L, Long.MAX_VALUE);
    for (int i = 1; i < times.size(); i++)
      assertTrue(
          StatefulGraph.sortableTime(times.get(i - 1))
                  .compareTo(StatefulGraph.sortableTime(times.get(i)))
              < 0,
          times.get(i - 1) + " before " + times.get(i));
  }

  private static StatefulGraph graphOver(Case fixture, Map<String, KeyedState> stores) {
    return new StatefulGraph(
        fixture.graph(),
        NamedTransformCatalog.installed(),
        fixture.blockingField(),
        node -> stores.computeIfAbsent(node, ignored -> new InMemoryKeyedState()));
  }
}
