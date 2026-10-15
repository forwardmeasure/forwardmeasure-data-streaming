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
package com.forwardmeasure.datastreaming.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The transform steps a spec runs on its mapped records, after each source's own mapper and before
 * the sink: a chain from {@code entryNode} along unconditional edges. Stateful steps keep keyed,
 * event-time state, held by whichever delivery engine the installation runs - Kafka Streams state
 * stores, or Pekko Persistence on the installation's journal - with the same results on either.
 *
 * <p>Node kinds:
 *
 * <ul>
 *   <li>{@code map} - a {@link TransformSpec} applied to each record.
 *   <li>{@code deduplicate} - drops a record whose {@code key} was already emitted at an event time
 *       less than {@code window} earlier.
 *   <li>{@code window_aggregate} - groups records by {@code key} into tumbling event-time windows
 *       of {@code window}, and emits one record per key and window once the window closes (the
 *       watermark - the latest event time seen less {@code grace} - passes its end, or a bounded
 *       input ends): the key, {@code window_start}, {@code window_end} and each aggregation.
 *   <li>{@code window_join} - the entry node of a two-source spec: joins records of source {@code
 *       left} and source {@code right} that share the spec's {@code blockingField} value and whose
 *       event times are at most {@code window} apart, emitting the left record's fields plus each
 *       right field the left record lacks.
 * </ul>
 *
 * <p>Event time is the record's {@code timeField}: an ISO-8601 instant or epoch milliseconds. A
 * record without a usable event time fails per the spec's error policy.
 */
public record TransformGraph(
    String entryNode, Map<String, TransformNode> nodes, List<TransformEdge> edges)
    implements Serializable {

  public TransformGraph {
    Objects.requireNonNull(entryNode, "entryNode");
    nodes = nodes == null ? Map.of() : Map.copyOf(nodes);
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("A TransformGraph must have at least one node");
    }
    if (!nodes.containsKey(entryNode)) {
      throw new IllegalArgumentException(
          "entryNode '" + entryNode + "' is not a node in this graph");
    }
    edges = edges == null ? List.of() : List.copyOf(edges);
    for (TransformEdge edge : edges) {
      if (!nodes.containsKey(edge.from())) {
        throw new IllegalArgumentException("Edge references unknown node '" + edge.from() + "'");
      }
      if (!nodes.containsKey(edge.to())) {
        throw new IllegalArgumentException("Edge references unknown node '" + edge.to() + "'");
      }
      if (edge.condition() != null) {
        throw new IllegalArgumentException(
            "Conditional edges need output tagging, which no transform provides: edge "
                + edge.from()
                + " -> "
                + edge.to());
      }
    }
    chain(entryNode, nodes, edges);
    for (Map.Entry<String, TransformNode> node : nodes.entrySet()) {
      if (node.getValue().kind() == NodeKind.WINDOW_JOIN && !node.getKey().equals(entryNode)) {
        throw new IllegalArgumentException("A window_join node must be the entry node");
      }
    }
  }

  /** The nodes in execution order. */
  public List<TransformNode> chain() {
    return chain(entryNode, nodes, edges).stream().map(nodes::get).toList();
  }

  /** The node names in execution order, which identify each node's state. */
  public List<String> chainNames() {
    return chain(entryNode, nodes, edges);
  }

  /**
   * The chain's first {@code count} steps as their own graph - {@code null} when {@code count} is
   * zero. A stage that runs them hands their outputs to {@link #after(int)}.
   */
  public TransformGraph first(int count) {
    return slice(0, count);
  }

  /** The chain's steps after the first {@code count} - {@code null} when none remain. */
  public TransformGraph after(int count) {
    return slice(count, nodes.size());
  }

  private TransformGraph slice(int from, int to) {
    if (from < 0 || to > nodes.size() || from > to)
      throw new IllegalArgumentException("No steps " + from + ".." + to + " in this graph");
    if (from == to) return null;
    List<String> names = chainNames().subList(from, to);
    Map<String, TransformNode> slice = new java.util.LinkedHashMap<>();
    for (String name : names) slice.put(name, nodes.get(name));
    List<TransformEdge> sliceEdges = new java.util.ArrayList<>();
    for (int i = 1; i < names.size(); i++)
      sliceEdges.add(new TransformEdge(names.get(i - 1), names.get(i), null));
    return new TransformGraph(names.getFirst(), slice, sliceEdges);
  }

  public boolean stateful() {
    return nodes.values().stream().anyMatch(node -> node.kind() != NodeKind.MAP);
  }

  public boolean joins() {
    return nodes.get(entryNode).kind() == NodeKind.WINDOW_JOIN;
  }

  private static List<String> chain(
      String entryNode, Map<String, TransformNode> nodes, List<TransformEdge> edges) {
    java.util.List<String> order = new java.util.ArrayList<>();
    Set<String> seen = new java.util.HashSet<>();
    String current = entryNode;
    while (current != null) {
      if (!seen.add(current)) {
        throw new IllegalArgumentException("A TransformGraph must not loop: " + current);
      }
      order.add(current);
      String from = current;
      List<TransformEdge> next = edges.stream().filter(edge -> edge.from().equals(from)).toList();
      if (next.size() > 1) {
        throw new IllegalArgumentException(
            "Node '" + from + "' fans out; a TransformGraph runs as one chain");
      }
      current = next.isEmpty() ? null : next.getFirst().to();
    }
    if (order.size() != nodes.size()) {
      throw new IllegalArgumentException(
          "Every node must be on the chain from entryNode '" + entryNode + "'");
    }
    return order;
  }

  /** The kinds of step a node can be. */
  public enum NodeKind {
    @JsonProperty("map")
    MAP,
    @JsonProperty("deduplicate")
    DEDUPLICATE,
    @JsonProperty("window_aggregate")
    WINDOW_AGGREGATE,
    @JsonProperty("window_join")
    WINDOW_JOIN
  }

  /** One aggregation of a {@code window_aggregate} node. */
  public record Aggregation(
      @JsonProperty("target") String target,
      @JsonProperty("op") AggregationOp op,
      @JsonProperty("field") String field)
      implements Serializable {
    public Aggregation {
      Objects.requireNonNull(target, "target");
      Objects.requireNonNull(op, "op");
      if (op != AggregationOp.COUNT && (field == null || field.isBlank())) {
        throw new IllegalArgumentException("Aggregation " + op + " needs a field");
      }
    }
  }

  /** What an aggregation computes over a window's records. */
  public enum AggregationOp {
    @JsonProperty("count")
    COUNT,
    @JsonProperty("sum")
    SUM,
    @JsonProperty("min")
    MIN,
    @JsonProperty("max")
    MAX,
    @JsonProperty("first")
    FIRST,
    @JsonProperty("last")
    LAST,
    @JsonProperty("collect")
    COLLECT
  }

  /**
   * One step. {@code kind} defaults to {@code map}; each kind uses only its own fields (see the
   * class javadoc) and rejects the others' absence.
   */
  public record TransformNode(
      @JsonProperty("kind") NodeKind kind,
      @JsonProperty("mapper") TransformSpec mapper,
      @JsonProperty("key") String key,
      @JsonProperty("timeField") String timeField,
      @JsonProperty("window") Duration window,
      @JsonProperty("grace") Duration grace,
      @JsonProperty("aggregations") List<Aggregation> aggregations,
      @JsonProperty("left") String left,
      @JsonProperty("right") String right)
      implements Serializable {

    public TransformNode(TransformSpec mapper) {
      this(NodeKind.MAP, mapper, null, null, null, null, null, null, null);
    }

    public TransformNode {
      kind = kind == null ? NodeKind.MAP : kind;
      grace = grace == null ? Duration.ZERO : grace;
      aggregations = aggregations == null ? List.of() : List.copyOf(aggregations);
      switch (kind) {
        case MAP -> Objects.requireNonNull(mapper, "a map node needs a mapper");
        case DEDUPLICATE -> {
          requireText(key, "key");
          requireText(timeField, "timeField");
          requirePositive(window);
        }
        case WINDOW_AGGREGATE -> {
          requireText(key, "key");
          requireText(timeField, "timeField");
          requirePositive(window);
          if (grace.isNegative()) throw new IllegalArgumentException("grace must not be negative");
          if (aggregations.isEmpty())
            throw new IllegalArgumentException("A window_aggregate node needs aggregations");
        }
        case WINDOW_JOIN -> {
          requireText(left, "left");
          requireText(right, "right");
          requireText(timeField, "timeField");
          requirePositive(window);
          if (left.equals(right))
            throw new IllegalArgumentException("A window_join joins two different sources");
        }
      }
    }

    private static void requireText(String value, String field) {
      if (value == null || value.isBlank())
        throw new IllegalArgumentException("this node kind needs " + field);
    }

    private static void requirePositive(Duration window) {
      if (window == null || window.isZero() || window.isNegative())
        throw new IllegalArgumentException("this node kind needs a positive window");
    }
  }

  /** An edge; {@code condition} must be {@code null} (see the constructor). */
  public record TransformEdge(
      @JsonProperty("from") String from,
      @JsonProperty("to") String to,
      @JsonProperty("condition") String condition)
      implements Serializable {

    public TransformEdge {
      Objects.requireNonNull(from, "from");
      Objects.requireNonNull(to, "to");
    }
  }
}
