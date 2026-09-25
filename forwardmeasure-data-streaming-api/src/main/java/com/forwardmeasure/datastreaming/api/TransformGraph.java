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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The transform side of an {@link ExecutionPlan} - a real node/edge graph, not a disguised list:
 * this org's own real pipelines need branching (validate a record, route it to a dead-letter path
 * on failure and continue the main path on success; fan a normalised record out to independent
 * extraction steps that both feed a later correlate step) - not a hypothetical, so this type models
 * it directly rather than deferring to "a future graph" while secretly being a sequence.
 *
 * <p>{@code condition} on an edge is a consumer-side-tag idiom: {@code null} means "always follow
 * this edge" (plain sequencing, or unconditional fan-out to multiple next nodes); a non-null value
 * means "follow this edge only when the upstream node's own named transform tagged its output with
 * this condition" (e.g. a validation transform tagging a record {@code "valid"} or {@code
 * "invalid"}) - the transform-tagging mechanism itself is step-2 (the planner) work, not built yet.
 */
public record TransformGraph(
    String entryNode, Map<String, TransformNode> nodes, List<TransformEdge> edges) {

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
    }
  }

  /**
   * One step in the graph. Deliberately wraps the existing {@link TransformSpec} directly rather
   * than inventing a second, competing notion of "named transform": this codebase already has
   * exactly one - {@code com.forwardmeasure.datastreaming.transforms.NamedTransform}, dispatched
   * through the one real {@code NamedTransformRegistry} inside {@code FieldMappingEngine}. A
   * field-mapping step just *is* a {@link TransformSpec}; there is no second identity space to
   * invent for it.
   *
   * <p>Non-field-mapping step kinds (correlation across multiple upstream nodes, windowed
   * aggregation) are real future work but are deliberately not modelled here yet - this repo has no
   * executable shape for either today (that's step 3-5 work in the gap-bridging plan), and guessing
   * their shape now would be exactly the kind of speculative, ahead-of-need type this repo's own
   * discipline avoids. When they're built, they get their own explicit node kind here, recognized
   * by the planner from the node's own declared shape - never by a name looked up in a new
   * registry.
   */
  public record TransformNode(TransformSpec mapper) {

    public TransformNode {
      Objects.requireNonNull(mapper, "mapper");
    }
  }

  /** {@code condition == null} means unconditional; a non-null value matches an output tag. */
  public record TransformEdge(String from, String to, String condition) {

    public TransformEdge {
      Objects.requireNonNull(from, "from");
      Objects.requireNonNull(to, "to");
    }
  }
}
