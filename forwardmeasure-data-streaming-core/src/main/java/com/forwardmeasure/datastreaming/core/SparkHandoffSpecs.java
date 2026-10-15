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
package com.forwardmeasure.datastreaming.core;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.core.stateful.GraphFields;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives the delivery-stage {@link IngestionSpec} a two-Job Spark-then-delivery pipeline's second
 * Job runs, from the same compiled {@link ExecutionPlan} whose {@code sparkStage} triggered the
 * first (Spark) Job - see the repo's own gap-bridging plan, "Spark: an optional distributed compute
 * stage," for the invariant this exists to satisfy: Spark always hands off via {@code
 * plan.sparkStage().get().handoffTopic()}, never writes {@code plan.destination()} directly: this
 * class builds the spec that reads that same topic and finishes the job Spark started.
 *
 * <p>Every field Spark's own mapped row carries is already in final target shape - {@code
 * FieldMappingEngine} already ran once, inside the Spark stage. The delivery Job must not run it
 * again (there is nothing left to transform, and re-running the original mapping against
 * already-mapped field names would be wrong even if it happened to not error). Its own mapper is
 * therefore pure identity: one {@link FieldRule} per distinct target field name the original plan's
 * sources declared, {@code source} set to that same name (a Spark-handoff Kafka message's own JSON
 * keys are the target field names, not the original source column names), and {@code raw: true} -
 * required, not optional, because a target field whose real value is a structured {@code
 * List<Map<String,Object>>} (a mapped {@code names}/{@code identifiers}/{@code locations} value,
 * common in party-shaped specs) would otherwise be silently mangled by {@code SourceRow#get}'s own
 * {@code String.valueOf(...)} coercion - see {@link FieldRule#raw}'s own javadoc.
 */
public final class SparkHandoffSpecs {

  private static final String HANDOFF_SOURCE_KEY = "spark-handoff";

  private SparkHandoffSpecs() {}

  /**
   * The delivery stage's spec. Spark hands off in one of three shapes:
   *
   * <ul>
   *   <li><b>Spark ran part of the graph</b> ({@code graphSteps > 0}): one stream of those steps'
   *       outputs on the handoff topic; the delivery stage runs the steps after them.
   *   <li><b>Each source separately</b> - a continuous run, or a bounded one whose graph joins:
   *       each source's mapped records on its own topic, so the delivery stage correlates or joins
   *       them as it would the sources themselves.
   *   <li><b>Correlated</b> (otherwise): one stream of merged records on the handoff topic.
   * </ul>
   *
   * @param kafkaBootstrapServers the Kafka cluster's bootstrap servers - a runtime address the
   *     caller supplies, never part of a compiled plan
   * @throws IllegalArgumentException if {@code plan} has no {@code sparkStage} to hand off from
   */
  public static IngestionSpec deliveryStageSpec(ExecutionPlan plan, String kafkaBootstrapServers) {
    if (plan.sparkStage().isEmpty()) {
      throw new IllegalArgumentException(
          "SparkHandoffSpecs: plan has no sparkStage - there is nothing to hand off from");
    }
    int graphSteps = plan.sparkStage().get().graphSteps();
    ExecutionMode mode = plan.profile().executionMode();
    if (graphSteps > 0) {
      TransformGraph graph = plan.transforms();
      Set<String> sourceFields = new LinkedHashSet<>();
      for (SourcePlan source : plan.sources())
        for (FieldRule rule : source.mapper().fields()) sourceFields.add(rule.target());
      List<String> fields = GraphFields.after(graph.first(graphSteps), sourceFields);
      SourcePlan handoff =
          new SourcePlan(
              HANDOFF_SOURCE_KEY,
              kafka(plan.sparkStage().get().handoffTopic(), kafkaBootstrapServers),
              new TransformSpec(originalMapperTarget(plan), identityRules(fields)),
              1.0);
      return new IngestionSpec(
          List.of(handoff),
          null,
          graph.after(graphSteps),
          plan.destination(),
          mode,
          plan.delivery(),
          plan.errors());
    }
    if (handsOffEachSource(plan)) {
      List<SourcePlan> sources = new ArrayList<>();
      for (int index = 0; index < plan.sources().size(); index++) {
        SourcePlan original = plan.sources().get(index);
        sources.add(
            new SourcePlan(
                original.sourceKey(),
                kafka(sourceTopic(plan, index), kafkaBootstrapServers),
                new TransformSpec(
                    original.mapper().target(), identityFieldRules(List.of(original))),
                original.trustWeight()));
      }
      return new IngestionSpec(
          sources,
          plan.blockingField(),
          plan.transforms(),
          plan.destination(),
          mode,
          plan.delivery(),
          plan.errors());
    }
    TransformSpec identityMapper =
        new TransformSpec(originalMapperTarget(plan), identityFieldRules(plan.sources()));
    SourcePlan sourcePlan =
        new SourcePlan(
            HANDOFF_SOURCE_KEY,
            kafka(plan.sparkStage().get().handoffTopic(), kafkaBootstrapServers),
            identityMapper,
            1.0);
    return new IngestionSpec(
        List.of(sourcePlan),
        null,
        plan.transforms(),
        plan.destination(),
        ExecutionMode.BOUNDED,
        plan.delivery(),
        plan.errors());
  }

  /**
   * Whether the Spark stage writes each source's mapped records to its own topic ({@link
   * #sourceTopic}) instead of one stream: a continuous run (the delivery stage correlates as the
   * records arrive), or a bounded multi-source run whose graph joins the sources - a join needs
   * both sides, which merging would lose.
   */
  public static boolean handsOffEachSource(ExecutionPlan plan) {
    if (plan.sparkStage().isEmpty() || plan.sparkStage().get().graphSteps() > 0) return false;
    if (plan.profile().executionMode() == ExecutionMode.CONTINUOUS) return true;
    return plan.sources().size() > 1 && plan.transforms() != null && plan.transforms().joins();
  }

  /** The topic the Spark stage writes source {@code sourceIndex}'s mapped records to. */
  public static String sourceTopic(ExecutionPlan plan, int sourceIndex) {
    return plan.sparkStage().orElseThrow().handoffTopic() + "-source-" + sourceIndex;
  }

  /**
   * The topic a continuous Spark stage writes source {@code sourceIndex}'s records to - {@link
   * #sourceTopic}.
   */
  public static String continuousTopic(ExecutionPlan plan, int sourceIndex) {
    return sourceTopic(plan, sourceIndex);
  }

  private static SourceSpec kafka(String topic, String kafkaBootstrapServers) {
    return new SourceSpec(
        "kafka",
        "kafka:" + topic + "?brokers=" + kafkaBootstrapServers,
        null,
        null,
        null,
        Map.of());
  }

  private static String originalMapperTarget(ExecutionPlan plan) {
    for (SourcePlan source : plan.sources()) {
      if (source.mapper().target() != null) {
        return source.mapper().target();
      }
    }
    return null;
  }

  /**
   * One identity rule per distinct target field name across every original source's own mapper -
   * deduplicated, since several original rules (e.g. a {@code names} field built from a PRIMARY
   * rule and an ALIAS rule) all collapse into the one already-accumulated value Spark wrote under
   * that single target key.
   */
  private static List<FieldRule> identityFieldRules(List<SourcePlan> sources) {
    Set<String> targets = new LinkedHashSet<>();
    for (SourcePlan source : sources) {
      for (FieldRule rule : source.mapper().fields()) {
        targets.add(rule.target());
      }
    }
    return identityRules(List.copyOf(targets));
  }

  private static List<FieldRule> identityRules(List<String> fields) {
    List<FieldRule> rules = new ArrayList<>();
    for (String field : fields) {
      rules.add(new FieldRule(field, field, null, null, null, true, false, Map.of(), true));
    }
    return rules;
  }
}
