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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformGraph.Aggregation;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Runs a spec's {@link TransformGraph} on mapped records, the same on every delivery engine: the
 * engine supplies each node's {@link KeyedState} and decides when state is committed, and this
 * class decides every output. A record goes through the chain in {@link #process}; a bounded run
 * calls {@link #finish} once its input ends, which closes every open window.
 *
 * <p>Outputs are deterministic for a given input order: windows close in window-start then key
 * order, joins emit in the order the matching records arrived.
 */
public final class StatefulGraph {

  private static final ObjectMapper JSON =
      JsonMapper.builder().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};
  private static final TypeReference<List<Map<String, Object>>> LIST = new TypeReference<>() {};
  private static final String WATERMARK = "\u0000watermark";
  private static final String SEPARATOR = "\u0001";
  private static final int PURGE_EVERY = 1024;

  private final List<String> names;
  private final List<TransformNode> nodes;
  private final List<KeyedState> states;
  private final FieldMappingEngine mapper;
  private final String joinKey;
  private long processed;

  /**
   * @param joinKey the spec's {@code blockingField}, which a {@code window_join} joins on
   * @param stateFor each node's state, by node name
   */
  public StatefulGraph(
      TransformGraph graph,
      NamedTransformCatalog catalog,
      String joinKey,
      Function<String, KeyedState> stateFor) {
    this.names = graph.chainNames();
    this.nodes = graph.chain();
    this.mapper = new FieldMappingEngine(catalog);
    this.joinKey = joinKey;
    if (graph.joins() && (joinKey == null || joinKey.isBlank()))
      throw new IllegalArgumentException("A window_join joins on the spec's blockingField");
    this.states = names.stream().map(stateFor).toList();
  }

  /** Every node name, in chain order - the state each engine must provide. */
  public List<String> nodeNames() {
    return names;
  }

  /**
   * What happens to one value a step rejects (no key, no usable event time, a value an aggregation
   * cannot use): it may throw to fail the run, or return to drop just that value.
   */
  @FunctionalInterface
  public interface Rejection {
    void reject(IllegalArgumentException failure);
  }

  /** Fails on the first rejected value. */
  public static final Rejection FAIL =
      failure -> {
        throw failure;
      };

  /** Runs one mapped record from {@code sourceKey} through the chain, failing on a rejection. */
  public List<Map<String, Object>> process(String sourceKey, Map<String, Object> record) {
    return process(sourceKey, record, FAIL);
  }

  /**
   * Runs one mapped record from {@code sourceKey} through the chain. A rejection is decided per
   * value and per step: only the rejected value is dropped, every other value - including the other
   * windows one record closes - goes on. A step changes its state only once a value is accepted, so
   * a rejected value leaves that step's state as it was.
   */
  public List<Map<String, Object>> process(
      String sourceKey, Map<String, Object> record, Rejection rejection) {
    processed++;
    List<Map<String, Object>> current = List.of(record);
    for (int i = 0; i < nodes.size(); i++) {
      List<Map<String, Object>> next = new ArrayList<>();
      for (Map<String, Object> value : current)
        next.addAll(applyOrReject(i, sourceKey, value, rejection));
      current = next;
      if (current.isEmpty()) break;
    }
    return current;
  }

  /** Closes every open window at the end of a bounded input, failing on a rejection. */
  public List<Map<String, Object>> finish() {
    return finish(FAIL);
  }

  /** Closes every open window at the end of a bounded input, running the results onward. */
  public List<Map<String, Object>> finish(Rejection rejection) {
    List<Map<String, Object>> pending = List.of();
    for (int i = 0; i < nodes.size(); i++) {
      List<Map<String, Object>> next = new ArrayList<>();
      for (Map<String, Object> value : pending)
        next.addAll(applyOrReject(i, null, value, rejection));
      next.addAll(flush(i));
      pending = next;
    }
    return pending;
  }

  private List<Map<String, Object>> applyOrReject(
      int index, String sourceKey, Map<String, Object> value, Rejection rejection) {
    try {
      return apply(index, sourceKey, value);
    } catch (IllegalArgumentException rejected) {
      rejection.reject(rejected);
      return List.of();
    }
  }

  private List<Map<String, Object>> apply(int index, String sourceKey, Map<String, Object> record) {
    TransformNode node = nodes.get(index);
    KeyedState state = states.get(index);
    return switch (node.kind()) {
      case MAP -> List.of(mapper.map(new MapSourceRow(record), node.mapper()));
      case DEDUPLICATE -> deduplicate(node, state, record);
      case WINDOW_AGGREGATE -> aggregate(node, state, record);
      case WINDOW_JOIN -> join(node, state, sourceKey, record);
    };
  }

  private List<Map<String, Object>> flush(int index) {
    TransformNode node = nodes.get(index);
    if (node.kind() != TransformGraph.NodeKind.WINDOW_AGGREGATE) return List.of();
    return closeWindows(node, states.get(index), Long.MAX_VALUE);
  }

  // ---- deduplicate ----

  private List<Map<String, Object>> deduplicate(
      TransformNode node, KeyedState state, Map<String, Object> record) {
    String key = keyOf(record, node.key());
    long time = eventTime(record, node.timeField());
    long window = node.window().toMillis();
    long watermark = Math.max(watermark(state), time);
    putWatermark(state, watermark);
    if (processed % PURGE_EVERY == 0) {
      for (String stored : state.keys()) {
        if (stored.equals(WATERMARK)) continue;
        long last = Long.parseLong(state.get(stored).orElseThrow());
        if (last < watermark - window) state.remove(stored);
      }
    }
    var last = state.get(key).map(Long::parseLong);
    if (last.isPresent() && Math.abs(time - last.get()) < window) return List.of();
    state.put(key, Long.toString(time));
    return List.of(record);
  }

  // ---- window_aggregate ----

  private List<Map<String, Object>> aggregate(
      TransformNode node, KeyedState state, Map<String, Object> record) {
    String key = keyOf(record, node.key());
    long time = eventTime(record, node.timeField());
    long size = node.window().toMillis();
    long start = Math.floorDiv(time, size) * size;
    long watermark = watermark(state);
    if (start + size <= watermark) {
      // The window already closed: a late record changes nothing that was emitted.
      return List.of();
    }
    String windowKey = sortableTime(start) + SEPARATOR + key;
    Map<String, Object> accumulator =
        state.get(windowKey).map(StatefulGraph::readMap).orElseGet(LinkedHashMap::new);
    accumulator.putIfAbsent("key", key);
    accumulator.putIfAbsent("start", start);
    for (Aggregation aggregation : node.aggregations()) {
      accumulate(accumulator, aggregation, record, time);
    }
    state.put(windowKey, write(accumulator));
    long advanced = Math.max(watermark, time - node.grace().toMillis());
    putWatermark(state, advanced);
    return closeWindows(node, state, advanced);
  }

  private List<Map<String, Object>> closeWindows(
      TransformNode node, KeyedState state, long watermark) {
    long size = node.window().toMillis();
    List<Map<String, Object>> closed = new ArrayList<>();
    for (String windowKey : state.keys()) {
      if (windowKey.equals(WATERMARK)) continue;
      Map<String, Object> accumulator = readMap(state.get(windowKey).orElseThrow());
      long start = ((Number) accumulator.get("start")).longValue();
      if (watermark != Long.MAX_VALUE && start + size > watermark) continue;
      Map<String, Object> output = new LinkedHashMap<>();
      output.put(node.key(), accumulator.get("key"));
      output.put("window_start", Instant.ofEpochMilli(start).toString());
      output.put("window_end", Instant.ofEpochMilli(start + size).toString());
      for (Aggregation aggregation : node.aggregations()) {
        output.put(aggregation.target(), result(accumulator, aggregation));
      }
      closed.add(output);
      state.remove(windowKey);
    }
    return closed;
  }

  @SuppressWarnings("unchecked")
  private static void accumulate(
      Map<String, Object> accumulator,
      Aggregation aggregation,
      Map<String, Object> record,
      long time) {
    String slot = "agg:" + aggregation.target();
    Object value = aggregation.field() == null ? null : record.get(aggregation.field());
    switch (aggregation.op()) {
      case COUNT -> accumulator.merge(slot, 1L, (a, b) -> ((Number) a).longValue() + 1L);
      case SUM -> {
        if (value == null) return;
        BigDecimal sum = new BigDecimal(String.valueOf(accumulator.getOrDefault(slot, "0")));
        accumulator.put(slot, sum.add(number(value, aggregation)).toPlainString());
      }
      case MIN, MAX -> {
        if (value == null) return;
        Object current = accumulator.get(slot);
        if (current == null
            || (aggregation.op() == TransformGraph.AggregationOp.MIN
                ? compare(value, current) < 0
                : compare(value, current) > 0)) accumulator.put(slot, value);
      }
      case FIRST, LAST -> {
        if (value == null) return;
        Map<String, Object> held = (Map<String, Object>) accumulator.get(slot);
        long heldTime = held == null ? 0 : ((Number) held.get("t")).longValue();
        boolean replace =
            held == null
                || (aggregation.op() == TransformGraph.AggregationOp.FIRST
                    ? time < heldTime
                    : time >= heldTime);
        if (replace) accumulator.put(slot, Map.of("t", time, "v", value));
      }
      case COLLECT -> {
        if (value == null) return;
        List<Object> values =
            new ArrayList<>((List<Object>) accumulator.getOrDefault(slot, List.of()));
        values.add(value);
        accumulator.put(slot, values);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Object result(Map<String, Object> accumulator, Aggregation aggregation) {
    Object slot = accumulator.get("agg:" + aggregation.target());
    return switch (aggregation.op()) {
      case COUNT -> slot == null ? 0L : ((Number) slot).longValue();
      case SUM -> slot == null ? BigDecimal.ZERO : new BigDecimal(String.valueOf(slot));
      case MIN, MAX -> slot;
      case FIRST, LAST -> slot == null ? null : ((Map<String, Object>) slot).get("v");
      case COLLECT -> slot == null ? List.of() : slot;
    };
  }

  private static BigDecimal number(Object value, Aggregation aggregation) {
    try {
      return new BigDecimal(String.valueOf(value).strip());
    } catch (NumberFormatException failure) {
      throw new IllegalArgumentException(
          "Aggregation " + aggregation.target() + " needs a number in " + aggregation.field());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compare(Object a, Object b) {
    if (a instanceof Number || b instanceof Number) {
      return new BigDecimal(String.valueOf(a)).compareTo(new BigDecimal(String.valueOf(b)));
    }
    return ((Comparable) String.valueOf(a)).compareTo(String.valueOf(b));
  }

  // ---- window_join ----

  private List<Map<String, Object>> join(
      TransformNode node, KeyedState state, String sourceKey, Map<String, Object> record) {
    boolean left;
    if (node.left().equals(sourceKey)) left = true;
    else if (node.right().equals(sourceKey)) left = false;
    else throw new IllegalArgumentException("window_join has no side for source " + sourceKey);
    String key = keyOf(record, joinKey);
    long time = eventTime(record, node.timeField());
    long window = node.window().toMillis();
    long watermark = Math.max(watermark(state), time);
    putWatermark(state, watermark);
    String own = (left ? "L" : "R") + SEPARATOR + key;
    String other = (left ? "R" : "L") + SEPARATOR + key;

    List<Map<String, Object>> outputs = new ArrayList<>();
    for (Map<String, Object> buffered : readList(state.get(other))) {
      long bufferedTime = ((Number) buffered.get("t")).longValue();
      if (Math.abs(time - bufferedTime) > window) continue;
      @SuppressWarnings("unchecked")
      Map<String, Object> match = (Map<String, Object>) buffered.get("r");
      outputs.add(left ? merge(record, match) : merge(match, record));
    }
    List<Map<String, Object>> mine = new ArrayList<>(readList(state.get(own)));
    mine.add(Map.of("t", time, "r", record));
    state.put(own, writeList(purge(mine, watermark - window)));
    List<Map<String, Object>> theirs = purge(readList(state.get(other)), watermark - window);
    if (theirs.isEmpty()) state.remove(other);
    else state.put(other, writeList(theirs));
    if (processed % PURGE_EVERY == 0) {
      for (String stored : state.keys()) {
        if (stored.equals(WATERMARK)) continue;
        List<Map<String, Object>> kept = purge(readList(state.get(stored)), watermark - window);
        if (kept.isEmpty()) state.remove(stored);
        else state.put(stored, writeList(kept));
      }
    }
    return outputs;
  }

  private static List<Map<String, Object>> purge(List<Map<String, Object>> buffered, long before) {
    return buffered.stream()
        .filter(entry -> ((Number) entry.get("t")).longValue() >= before)
        .toList();
  }

  private static Map<String, Object> merge(Map<String, Object> left, Map<String, Object> right) {
    Map<String, Object> merged = new LinkedHashMap<>(left);
    right.forEach(merged::putIfAbsent);
    return merged;
  }

  // ---- shared ----

  private static long watermark(KeyedState state) {
    return state.get(WATERMARK).map(Long::parseLong).orElse(Long.MIN_VALUE);
  }

  private static void putWatermark(KeyedState state, long watermark) {
    state.put(WATERMARK, Long.toString(watermark));
  }

  /**
   * A fixed-width text form of an epoch-millisecond time that sorts as the times do: flipping the
   * sign bit maps signed order onto unsigned order, which fixed-width hex preserves.
   */
  static String sortableTime(long millis) {
    return String.format("%016x", millis ^ Long.MIN_VALUE);
  }

  private static String keyOf(Map<String, Object> record, String field) {
    Object value = record.get(field);
    if (value == null || String.valueOf(value).isBlank())
      throw new IllegalArgumentException("Record has no value for key field '" + field + "'");
    return String.valueOf(value);
  }

  /** A record's event time in epoch milliseconds: a number, an ISO-8601 instant or a date. */
  public static long eventTime(Map<String, Object> record, String field) {
    Object value = record.get(field);
    if (value instanceof Number number) return number.longValue();
    if (value != null) {
      String text = String.valueOf(value).strip();
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException notMillis) {
        try {
          return Instant.parse(text).toEpochMilli();
        } catch (RuntimeException notInstant) {
          try {
            return OffsetDateTime.parse(text).toInstant().toEpochMilli();
          } catch (RuntimeException notOffset) {
            try {
              return LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (RuntimeException notDate) {
              // fall through
            }
          }
        }
      }
    }
    throw new IllegalArgumentException("Record has no usable event time in '" + field + "'");
  }

  private static Map<String, Object> readMap(String json) {
    try {
      return new LinkedHashMap<>(JSON.readValue(json, MAP));
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Unreadable graph state", failure);
    }
  }

  private static List<Map<String, Object>> readList(java.util.Optional<String> json) {
    if (json.isEmpty()) return List.of();
    try {
      return JSON.readValue(json.get(), LIST);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Unreadable graph state", failure);
    }
  }

  private static String write(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Record cannot be held as graph state", failure);
    }
  }

  private static String writeList(List<Map<String, Object>> values) {
    return write(values);
  }
}
