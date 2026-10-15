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
package com.forwardmeasure.datastreaming.executor.spark;

import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords.Sourced;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;

/**
 * Runs the graph steps a bounded Spark stage owns ({@code SparkStagePlan.graphSteps}) over an
 * ordered input, one step at a time, with {@link StatefulGraph} deciding every output exactly as on
 * a delivery engine.
 *
 * <ul>
 *   <li>A map step runs distributed, partition by partition, keeping record order - so a {@code
 *       HEAVY} transform runs at Spark scale.
 *   <li>A stateful step meets the whole input, in order, on one partition, and closes its open
 *       windows when the input ends - the same results a bounded delivery-engine run gives.
 * </ul>
 *
 * Feeding each step the previous step's complete output, in order, gives the same results as
 * running every record through every step: a step sees the same sequence of values either way.
 */
final class SparkGraphSteps {

  private SparkGraphSteps() {}

  /** The Spark-owned steps of {@code plan}'s graph over {@code input}, which must be in order. */
  static JavaRDD<Map<String, Object>> run(ExecutionPlan plan, JavaRDD<Sourced> input) {
    TransformGraph owned = plan.transforms().first(plan.sparkStage().orElseThrow().graphSteps());
    String blockingField = plan.blockingField();
    MalformedRecordPolicy policy = MalformedRecordPolicy.from(plan.errors());
    int parallelism = Math.max(1, input.context().defaultParallelism());
    JavaRDD<Sourced> current = input;
    for (int index = 0; index < owned.chain().size(); index++) {
      TransformGraph step = owned.first(index + 1).after(index);
      if (step.chain().getFirst().kind() == TransformGraph.NodeKind.MAP) {
        current =
            spreadInOrder(current, parallelism)
                .mapPartitions(
                    records -> {
                      StatefulGraph graph = GraphRecords.bounded(step, blockingField);
                      List<Sourced> outputs = new ArrayList<>();
                      while (records.hasNext()) {
                        Sourced record = records.next();
                        for (Map<String, Object> output :
                            GraphRecords.process(
                                graph, record.sourceKey(), record.record(), policy))
                          outputs.add(new Sourced(record.sourceKey(), output));
                      }
                      return outputs.iterator();
                    });
      } else {
        current =
            current
                .coalesce(1)
                .mapPartitions(
                    records -> {
                      List<Sourced> outputs = new ArrayList<>();
                      for (Map<String, Object> output :
                          GraphRecords.runBounded(step, blockingField, records, policy))
                        outputs.add(new Sourced(null, output));
                      return outputs.iterator();
                    });
      }
    }
    return current.map(Sourced::record);
  }

  /**
   * Several sources' mapped records in the order a joining graph meets them - each source in read
   * order, then all of them by event time - on one partition.
   */
  static JavaRDD<Sourced> joinOrder(TransformGraph graph, List<JavaRDD<Sourced>> sources) {
    JavaRDD<Sourced> all = sources.getFirst();
    for (int index = 1; index < sources.size(); index++) all = all.union(sources.get(index));
    return all.coalesce(1)
        .mapPartitions(
            records -> {
              List<Sourced> list = new ArrayList<>();
              records.forEachRemaining(list::add);
              return GraphRecords.joinOrder(graph, list).iterator();
            });
  }

  /** Correlated records in the order a graph meets them ({@link GraphRecords#orderMerged}). */
  static JavaRDD<Sourced> mergedOrder(
      TransformGraph graph, String blockingField, JavaRDD<Map<String, Object>> merged) {
    return merged
        .coalesce(1)
        .mapPartitions(
            records -> {
              List<Map<String, Object>> list = new ArrayList<>();
              records.forEachRemaining(list::add);
              List<Sourced> ordered = new ArrayList<>();
              for (Map<String, Object> record :
                  GraphRecords.orderMerged(graph, blockingField, list))
                ordered.add(new Sourced(null, record));
              return ordered.iterator();
            });
  }

  /**
   * {@code records} spread over {@code parallelism} partitions without losing their order: a range
   * partition by original position keeps each partition's records, and the partitions, in that
   * order.
   */
  private static JavaRDD<Sourced> spreadInOrder(JavaRDD<Sourced> records, int parallelism) {
    if (records.getNumPartitions() >= parallelism) return records;
    return records
        .zipWithIndex()
        .mapToPair(entry -> new scala.Tuple2<>(entry._2(), entry._1()))
        .sortByKey(true, parallelism)
        .values();
  }
}
