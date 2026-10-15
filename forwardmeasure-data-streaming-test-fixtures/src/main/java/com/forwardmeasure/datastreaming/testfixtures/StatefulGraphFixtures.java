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
package com.forwardmeasure.datastreaming.testfixtures;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformGraph.Aggregation;
import com.forwardmeasure.datastreaming.api.TransformGraph.AggregationOp;
import com.forwardmeasure.datastreaming.api.TransformGraph.NodeKind;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformEdge;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The stateful transform graphs every delivery engine must run identically (D6 parity): each case's
 * graph, its input records in arrival order, and the exact outputs of a bounded run (input ends:
 * every open window closes) and of a continuous run (windows close only as event time passes them).
 * Engine tests feed {@link Case#inputs()} through a real source and sink and compare what arrives
 * with {@link #canonical}; the core unit test runs the same cases in memory.
 */
public final class StatefulGraphFixtures {

  /** The field the {@link #withDocumentId} node writes, a sink's document id. */
  public static final String DOCUMENT_ID = "doc_id";

  private static final ObjectMapper JSON =
      JsonMapper.builder().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();

  private StatefulGraphFixtures() {}

  /** A mapped record from one source. */
  public record Input(String sourceKey, Map<String, Object> record) {}

  /**
   * One parity case.
   *
   * @param sources the source keys, in spec order (a join's left source first)
   * @param inputFields each source's record fields, which a source mapper passes through unchanged
   * @param outputFields the fields each output carries
   * @param idTemplate a template over output fields that identifies each output
   */
  public record Case(
      String name,
      List<String> sources,
      List<String> inputFields,
      String blockingField,
      TransformGraph graph,
      List<Input> inputs,
      List<Map<String, Object>> bounded,
      List<Map<String, Object>> continuous,
      List<String> outputFields,
      String idTemplate) {}

  public static List<Case> all() {
    return List.of(
        deduplicate(), windowAggregate(), windowAggregateWithGrace(), windowJoin(), chain());
  }

  /** Repeats of a key within ten minutes of its last emitted record are dropped. */
  public static Case deduplicate() {
    TransformGraph graph =
        single(
            "dedupe",
            new TransformNode(
                NodeKind.DEDUPLICATE,
                null,
                "id",
                "ts",
                Duration.ofMinutes(10),
                null,
                null,
                null,
                null));
    List<Input> inputs =
        List.of(
            in("events", "id", "a", "ts", t("00:00"), "v", 1),
            in("events", "id", "a", "ts", t("00:05"), "v", 2),
            in("events", "id", "b", "ts", t("00:06"), "v", 3),
            in("events", "id", "a", "ts", t("00:11"), "v", 4),
            in("events", "id", "a", "ts", t("00:12"), "v", 5));
    List<Map<String, Object>> expected =
        List.of(
            row("id", "a", "ts", t("00:00"), "v", 1),
            row("id", "b", "ts", t("00:06"), "v", 3),
            row("id", "a", "ts", t("00:11"), "v", 4));
    return new Case(
        "deduplicate",
        List.of("events"),
        List.of("id", "ts", "v"),
        null,
        graph,
        inputs,
        expected,
        expected,
        List.of("id", "ts", "v"),
        "{id}|{ts}");
  }

  /**
   * Hourly totals per account with every aggregation; a record for a window that already closed
   * changes nothing.
   */
  public static Case windowAggregate() {
    TransformGraph graph =
        single(
            "hourly",
            new TransformNode(
                NodeKind.WINDOW_AGGREGATE,
                null,
                "account",
                "ts",
                Duration.ofHours(1),
                null,
                List.of(
                    new Aggregation("n", AggregationOp.COUNT, null),
                    new Aggregation("total", AggregationOp.SUM, "amount"),
                    new Aggregation("smallest", AggregationOp.MIN, "amount"),
                    new Aggregation("largest", AggregationOp.MAX, "amount"),
                    new Aggregation("opening", AggregationOp.FIRST, "amount"),
                    new Aggregation("closing", AggregationOp.LAST, "amount"),
                    new Aggregation("refs", AggregationOp.COLLECT, "ref")),
                null,
                null));
    List<Input> inputs =
        List.of(
            payment("A", "00:10", 5, "r1"),
            payment("A", "00:20", 3, "r2"),
            payment("B", "00:30", 7, "r3"),
            payment("A", "01:05", 4, "r4"),
            payment("A", "00:50", 100, "late"),
            payment("C", "02:30", 1, "r5"));
    List<Map<String, Object>> continuous =
        List.of(
            hourly("A", "00:00", "01:00", 2, 8, 3, 5, 5, 3, List.of("r1", "r2")),
            hourly("B", "00:00", "01:00", 1, 7, 7, 7, 7, 7, List.of("r3")),
            hourly("A", "01:00", "02:00", 1, 4, 4, 4, 4, 4, List.of("r4")));
    List<Map<String, Object>> bounded = new ArrayList<>(continuous);
    bounded.add(hourly("C", "02:00", "03:00", 1, 1, 1, 1, 1, 1, List.of("r5")));
    return new Case(
        "window_aggregate",
        List.of("payments"),
        List.of("account", "ts", "amount", "ref"),
        null,
        graph,
        inputs,
        bounded,
        continuous,
        List.of(
            "account",
            "window_start",
            "window_end",
            "n",
            "total",
            "smallest",
            "largest",
            "opening",
            "closing",
            "refs"),
        "{account}|{window_start}");
  }

  /**
   * A window stays open for a quarter hour past its end, so a slightly late record still counts.
   */
  public static Case windowAggregateWithGrace() {
    TransformGraph graph =
        single(
            "hourly",
            new TransformNode(
                NodeKind.WINDOW_AGGREGATE,
                null,
                "account",
                "ts",
                Duration.ofHours(1),
                Duration.ofMinutes(15),
                List.of(
                    new Aggregation("n", AggregationOp.COUNT, null),
                    new Aggregation("total", AggregationOp.SUM, "amount")),
                null,
                null));
    List<Input> inputs =
        List.of(
            payment("A", "00:50", 1, "g1"),
            payment("A", "01:10", 2, "g2"),
            payment("A", "00:58", 3, "g3"),
            payment("A", "01:20", 4, "g4"));
    List<Map<String, Object>> continuous = List.of(counted("A", "00:00", "01:00", 2, 4));
    List<Map<String, Object>> bounded = new ArrayList<>(continuous);
    bounded.add(counted("A", "01:00", "02:00", 2, 6));
    return new Case(
        "window_aggregate_with_grace",
        List.of("payments"),
        List.of("account", "ts", "amount", "ref"),
        null,
        graph,
        inputs,
        bounded,
        continuous,
        List.of("account", "window_start", "window_end", "n", "total"),
        "{account}|{window_start}");
  }

  /**
   * Orders joined with their shipments within half an hour, on the spec's blocking field; the left
   * (order) record's fields win, the shipment adds what the order lacks.
   */
  public static Case windowJoin() {
    TransformGraph graph =
        single(
            "orders_with_shipments",
            new TransformNode(
                NodeKind.WINDOW_JOIN,
                null,
                null,
                "ts",
                Duration.ofMinutes(30),
                null,
                null,
                "orders",
                "shipments"));
    List<Input> inputs =
        List.of(
            in("orders", "order_id", "o1", "ts", t("00:00"), "customer", "c1", "status", "placed"),
            in(
                "shipments",
                "order_id",
                "o1",
                "ts",
                t("00:10"),
                "carrier",
                "dhl",
                "status",
                "shipped"),
            in(
                "shipments",
                "order_id",
                "o2",
                "ts",
                t("00:15"),
                "carrier",
                "ups",
                "status",
                "shipped"),
            in("orders", "order_id", "o2", "ts", t("00:20"), "customer", "c2", "status", "placed"),
            in("orders", "order_id", "o3", "ts", t("00:30"), "customer", "c3", "status", "placed"),
            in(
                "shipments",
                "order_id",
                "o3",
                "ts",
                t("01:40"),
                "carrier",
                "fedex",
                "status",
                "late"));
    List<Map<String, Object>> expected =
        List.of(
            row(
                "order_id",
                "o1",
                "ts",
                t("00:00"),
                "customer",
                "c1",
                "status",
                "placed",
                "carrier",
                "dhl"),
            row(
                "order_id",
                "o2",
                "ts",
                t("00:20"),
                "customer",
                "c2",
                "status",
                "placed",
                "carrier",
                "ups"));
    return new Case(
        "window_join",
        List.of("orders", "shipments"),
        List.of("order_id", "ts", "customer", "carrier", "status"),
        "order_id",
        graph,
        inputs,
        expected,
        expected,
        List.of("order_id", "ts", "customer", "status", "carrier"),
        "{order_id}");
  }

  /** A map step, then deduplication, then hourly aggregation of what survives. */
  public static Case chain() {
    TransformSpec rename =
        new TransformSpec(
            "reading", List.of(raw("sensor", "sensor_id"), raw("ts", "ts"), raw("value", "value")));
    TransformGraph graph =
        new TransformGraph(
            "rename",
            Map.of(
                "rename",
                new TransformNode(rename),
                "dedupe",
                new TransformNode(
                    NodeKind.DEDUPLICATE,
                    null,
                    "sensor",
                    "ts",
                    Duration.ofMinutes(5),
                    null,
                    null,
                    null,
                    null),
                "hourly",
                new TransformNode(
                    NodeKind.WINDOW_AGGREGATE,
                    null,
                    "sensor",
                    "ts",
                    Duration.ofHours(1),
                    null,
                    List.of(
                        new Aggregation("n", AggregationOp.COUNT, null),
                        new Aggregation("peak", AggregationOp.MAX, "value")),
                    null,
                    null)),
            List.of(
                new TransformEdge("rename", "dedupe", null),
                new TransformEdge("dedupe", "hourly", null)));
    List<Input> inputs =
        List.of(
            in("readings", "sensor_id", "s1", "ts", t("00:00"), "value", 5),
            in("readings", "sensor_id", "s1", "ts", t("00:02"), "value", 9),
            in("readings", "sensor_id", "s1", "ts", t("00:06"), "value", 7),
            in("readings", "sensor_id", "s2", "ts", t("00:10"), "value", 3),
            in("readings", "sensor_id", "s1", "ts", t("01:30"), "value", 1));
    List<Map<String, Object>> continuous =
        List.of(peak("s1", "00:00", "01:00", 2, 7), peak("s2", "00:00", "01:00", 1, 3));
    List<Map<String, Object>> bounded = new ArrayList<>(continuous);
    bounded.add(peak("s1", "01:00", "02:00", 1, 1));
    return new Case(
        "chain",
        List.of("readings"),
        List.of("sensor_id", "ts", "value"),
        null,
        graph,
        inputs,
        bounded,
        continuous,
        List.of("sensor", "window_start", "window_end", "n", "peak"),
        "{sensor}|{window_start}");
  }

  /**
   * {@code fixture.graph()} plus a final map step carrying each output field through unchanged and
   * adding {@link #DOCUMENT_ID} from {@code idTemplate} - what a sink writing documents by id
   * needs.
   */
  public static TransformGraph withDocumentId(Case fixture) {
    List<TransformSpec.FieldRule> fields = new ArrayList<>();
    for (String field : fixture.outputFields()) fields.add(raw(field, field));
    fields.add(
        new TransformSpec.FieldRule(
            DOCUMENT_ID, null, fixture.idTemplate(), null, null, null, null));
    Map<String, TransformNode> nodes = new LinkedHashMap<>(fixture.graph().nodes());
    nodes.put("identify", new TransformNode(new TransformSpec("output", fields)));
    List<TransformEdge> edges = new ArrayList<>(fixture.graph().edges());
    List<String> chain = fixture.graph().chainNames();
    edges.add(new TransformEdge(chain.getLast(), "identify", null));
    return new TransformGraph(fixture.graph().entryNode(), nodes, edges);
  }

  /** A source mapper that passes {@code fields} through with their types. */
  public static TransformSpec passThrough(String target, List<String> fields) {
    return new TransformSpec(target, fields.stream().map(field -> raw(field, field)).toList());
  }

  /**
   * Records as canonical JSON (keys sorted, numbers by value, {@link #DOCUMENT_ID} dropped), sorted
   * - for comparing what two engines wrote regardless of the order a sink returns documents in.
   */
  public static List<String> canonical(Collection<? extends Map<String, ?>> records) {
    List<String> canonical = new ArrayList<>();
    for (Map<String, ?> record : records) {
      Map<String, Object> copy = new LinkedHashMap<>(record);
      copy.remove(DOCUMENT_ID);
      try {
        canonical.add(JSON.writeValueAsString(JSON.readTree(JSON.writeValueAsString(copy))));
      } catch (JsonProcessingException failure) {
        throw new IllegalArgumentException("Record is not JSON: " + record, failure);
      }
    }
    canonical.sort(String::compareTo);
    return canonical;
  }

  /** Records as canonical JSON in their given order. */
  public static List<String> canonicalInOrder(List<? extends Map<String, ?>> records) {
    List<String> canonical = new ArrayList<>();
    for (Map<String, ?> record : records) canonical.add(canonical(List.of(record)).getFirst());
    return canonical;
  }

  private static TransformGraph single(String name, TransformNode node) {
    return new TransformGraph(name, Map.of(name, node), List.of());
  }

  private static TransformSpec.FieldRule raw(String target, String source) {
    return new TransformSpec.FieldRule(
        target, source, null, null, null, true, null, Map.of(), true);
  }

  private static String t(String hourMinute) {
    return "2026-01-01T" + hourMinute + ":00Z";
  }

  private static Input in(String source, Object... keysAndValues) {
    return new Input(source, row(keysAndValues));
  }

  private static Input payment(String account, String time, int amount, String ref) {
    return in("payments", "account", account, "ts", t(time), "amount", amount, "ref", ref);
  }

  private static Map<String, Object> hourly(
      String account,
      String start,
      String end,
      int n,
      int total,
      int smallest,
      int largest,
      int opening,
      int closing,
      List<String> refs) {
    return row(
        "account",
        account,
        "window_start",
        t(start),
        "window_end",
        t(end),
        "n",
        n,
        "total",
        total,
        "smallest",
        smallest,
        "largest",
        largest,
        "opening",
        opening,
        "closing",
        closing,
        "refs",
        refs);
  }

  private static Map<String, Object> counted(
      String account, String start, String end, int n, int total) {
    return row(
        "account", account, "window_start", t(start), "window_end", t(end), "n", n, "total", total);
  }

  private static Map<String, Object> peak(
      String sensor, String start, String end, int n, int peak) {
    return row(
        "sensor", sensor, "window_start", t(start), "window_end", t(end), "n", n, "peak", peak);
  }

  private static Map<String, Object> row(Object... keysAndValues) {
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      row.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return row;
  }
}
