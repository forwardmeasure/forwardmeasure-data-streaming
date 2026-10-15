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
import com.forwardmeasure.datastreaming.api.TransformGraph.Aggregation;
import com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The fields a graph's records can carry after its steps - what a stage that hands a graph's
 * partial results to the next stage must pass through: a map step's targets, a deduplicated
 * record's own fields, a window's key, bounds and aggregations, a join's fields from both sides.
 */
public final class GraphFields {

  private GraphFields() {}

  /** The fields after every step of {@code graph}, given the fields its input records carry. */
  public static List<String> after(TransformGraph graph, Set<String> inputFields) {
    Set<String> fields = new LinkedHashSet<>(inputFields);
    for (TransformNode node : graph.chain()) {
      switch (node.kind()) {
        case MAP -> {
          fields = new LinkedHashSet<>();
          for (TransformSpec.FieldRule rule : node.mapper().fields()) fields.add(rule.target());
        }
        case DEDUPLICATE, WINDOW_JOIN -> {
          // Records pass through unchanged; a join's output has both sides' fields, which the
          // input fields - every source's - already are.
        }
        case WINDOW_AGGREGATE -> {
          fields = new LinkedHashSet<>();
          fields.add(node.key());
          fields.add("window_start");
          fields.add("window_end");
          for (Aggregation aggregation : node.aggregations()) fields.add(aggregation.target());
        }
      }
    }
    return List.copyOf(fields);
  }
}
