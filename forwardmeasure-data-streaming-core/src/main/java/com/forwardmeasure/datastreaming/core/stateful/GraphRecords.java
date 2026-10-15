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

import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** How every engine runs a record through a spec's graph and handles a record the graph rejects. */
public final class GraphRecords {

  private static final Logger LOGGER = LoggerFactory.getLogger(GraphRecords.class);

  private GraphRecords() {}

  /** A bounded run's graph: in-memory state, recomputed from the start if the run is repeated. */
  public static StatefulGraph bounded(TransformGraph graph, String blockingField) {
    return new StatefulGraph(
        graph, NamedTransformCatalog.installed(), blockingField, name -> new InMemoryKeyedState());
  }

  /** A mapped record and the source it came from. */
  public record Sourced(String sourceKey, Map<String, Object> record)
      implements java.io.Serializable {}

  /**
   * A bounded multi-source run through a graph. A joining graph receives every source's records in
   * event-time order (stable within each source's read order), so its results do not depend on the
   * order sources were read; any other graph receives the correlated, merged records in event-time
   * order ({@link #orderMerged}), so its results do not depend on the order the correlation emitted
   * them either.
   *
   * @param merged the correlated records, used only when the graph does not join
   */
  public static List<Map<String, Object>> boundedCorrelated(
      TransformGraph graph,
      String blockingField,
      List<Sourced> records,
      List<Map<String, Object>> merged,
      MalformedRecordPolicy policy) {
    StatefulGraph stateful = bounded(graph, blockingField);
    List<Map<String, Object>> outputs = new java.util.ArrayList<>();
    if (graph.joins()) {
      for (Sourced entry : joinOrder(graph, records)) {
        outputs.addAll(process(stateful, entry.sourceKey(), entry.record(), policy));
      }
    } else {
      for (Map<String, Object> record : orderMerged(graph, blockingField, merged)) {
        outputs.addAll(process(stateful, null, record, policy));
      }
    }
    outputs.addAll(finish(stateful, policy));
    return outputs;
  }

  /**
   * Correlated records in the order a graph meets them: by the event time of the graph's first
   * time-based step, then by blocking key - the same order on every engine and in the Spark stage,
   * whatever order the correlation produced them in. A graph with no time-based step keeps the
   * blocking-key order.
   */
  public static List<Map<String, Object>> orderMerged(
      TransformGraph graph, String blockingField, List<Map<String, Object>> merged) {
    String timeField =
        graph.chain().stream()
            .map(TransformGraph.TransformNode::timeField)
            .filter(java.util.Objects::nonNull)
            .findFirst()
            .orElse(null);
    List<Map<String, Object>> ordered = new java.util.ArrayList<>(merged);
    java.util.Comparator<Map<String, Object>> byKey =
        java.util.Comparator.comparing(
            record ->
                blockingField == null || record.get(blockingField) == null
                    ? ""
                    : String.valueOf(record.get(blockingField)));
    ordered.sort(
        timeField == null
            ? byKey
            : java.util.Comparator.<Map<String, Object>>comparingLong(
                    record -> timeOrMin(record, timeField))
                .thenComparing(byKey));
    return ordered;
  }

  /**
   * Every record of a bounded input through {@code graph}, then every window still open: what a
   * stage that runs some of a graph's steps over its whole input produces. Records are met in the
   * order given; a joining graph's records must already be in {@link #boundedCorrelated}'s order.
   */
  public static List<Map<String, Object>> runBounded(
      TransformGraph graph,
      String blockingField,
      java.util.Iterator<Sourced> records,
      MalformedRecordPolicy policy) {
    StatefulGraph stateful = bounded(graph, blockingField);
    List<Map<String, Object>> outputs = new java.util.ArrayList<>();
    while (records.hasNext()) {
      Sourced entry = records.next();
      outputs.addAll(process(stateful, entry.sourceKey(), entry.record(), policy));
    }
    outputs.addAll(finish(stateful, policy));
    return outputs;
  }

  /** Records from several sources, in the order a bounded joining graph meets them. */
  public static List<Sourced> joinOrder(TransformGraph graph, List<Sourced> records) {
    String timeField = graph.chain().getFirst().timeField();
    List<Sourced> ordered = new java.util.ArrayList<>(records);
    ordered.sort(java.util.Comparator.comparingLong(entry -> timeOrMin(entry.record(), timeField)));
    return ordered;
  }

  private static long timeOrMin(Map<String, Object> record, String timeField) {
    try {
      return StatefulGraph.eventTime(record, timeField);
    } catch (IllegalArgumentException missing) {
      // Ordered first; the graph then rejects it per the error policy.
      return Long.MIN_VALUE;
    }
  }

  /**
   * The graph's outputs for one mapped record. A value a step rejects (no key, no usable event
   * time) fails the run, is dead-lettered or is skipped, per {@code policy} - only that value; the
   * record's other outputs still go on.
   */
  public static List<Map<String, Object>> process(
      StatefulGraph graph,
      String sourceKey,
      Map<String, Object> record,
      MalformedRecordPolicy policy) {
    return graph.process(sourceKey, record, rejection(policy));
  }

  /** The windows still open at the end of a bounded input, with rejections handled per policy. */
  public static List<Map<String, Object>> finish(
      StatefulGraph graph, MalformedRecordPolicy policy) {
    return graph.finish(rejection(policy));
  }

  static StatefulGraph.Rejection rejection(MalformedRecordPolicy policy) {
    return rejected -> {
      switch (policy) {
        case FAIL -> throw rejected;
        case DEAD_LETTER -> LOGGER.error("graph: value dead-lettered: {}", rejected.getMessage());
        case SKIP -> LOGGER.warn("graph: value skipped: {}", rejected.getMessage());
      }
    };
  }
}
