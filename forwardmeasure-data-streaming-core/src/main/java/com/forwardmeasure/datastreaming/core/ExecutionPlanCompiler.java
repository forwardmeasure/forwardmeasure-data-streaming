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

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.Cardinality;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.ExecutionCost;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics.StateRequirement;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.mappers.TransformCharacteristicsRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The planner: compiles one author-facing {@link IngestionSpec} into the {@link ExecutionPlan} a
 * {@code DeliveryEngine} actually runs - the author names transforms, never an engine (see the
 * repo's own gap-bridging plan, "the planner" section). Reads every reachable {@link
 * TransformSpec.FieldRule#transform()} name against {@link TransformCharacteristicsRegistry}, folds
 * them, and resolves the rest against a fixed, honest table - no heuristic invented without a real
 * transform in this codebase to justify it (see this class's own {@code foldCharacteristics}).
 *
 * <p><b>{@link ExecutionMode#CONTINUOUS} always resolves to {@link
 * DeliveryEngineKind#PEKKO_STREAMS}</b> unless a {@code STATEFUL} transform forces {@code
 * KAFKA_STREAMS} - a continuous spec's source is inherently Kafka-shaped for either engine, so
 * "source connector is kafka" cannot discriminate between them the way it legitimately can in
 * {@code BOUNDED} mode. A caller that specifically wants the {@code CONTINUOUS}+{@code
 * KAFKA_STREAMS} combination (e.g. a test proving engine parity, not a real deployed spec's one
 * true automatic choice) constructs the {@link ExecutionPlan} directly instead of going through
 * this compiler - see the plan's own "Phase B addendum" for why that's the honest answer rather
 * than a fabricated 50/50 heuristic.
 */
public final class ExecutionPlanCompiler {

  private static final String KAFKA_CONNECTOR = "kafka";

  private ExecutionPlanCompiler() {}

  public static ExecutionPlan compile(IngestionSpec spec) {
    TransformCharacteristics folded = foldCharacteristics(spec);
    DeliveryEngineKind engine = resolveEngine(spec, folded);
    ExecutionProfile profile =
        new ExecutionProfile(spec.sourceCardinality(), spec.executionMode(), engine);
    Optional<SparkStagePlan> sparkStage = resolveSparkStage(spec, folded);
    return new ExecutionPlan(
        profile,
        spec.sources(),
        spec.blockingField(),
        sparkStage,
        spec.transforms(),
        spec.sink(),
        spec.delivery(),
        spec.errors());
  }

  private static DeliveryEngineKind resolveEngine(
      IngestionSpec spec, TransformCharacteristics folded) {
    if (folded.state() == StateRequirement.STATEFUL) {
      return DeliveryEngineKind.KAFKA_STREAMS;
    }
    if (spec.executionMode() == ExecutionMode.BOUNDED && anySourceIsKafka(spec)) {
      return DeliveryEngineKind.KAFKA_STREAMS;
    }
    return DeliveryEngineKind.PEKKO_STREAMS;
  }

  private static boolean anySourceIsKafka(IngestionSpec spec) {
    for (SourcePlan source : spec.sources()) {
      if (KAFKA_CONNECTOR.equals(source.source().connector())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Present only when the fold is {@link ExecutionCost#HEAVY} - the {@link
   * TransformCharacteristics.Cardinality#CORRELATED}-at-scale trigger from {@code SparkStagePlan}'s
   * own javadoc is deliberately not implemented: no real correlated-at-scale spec exists anywhere
   * in this org's data to build that heuristic against honestly (see the repo's own gap-bridging
   * plan). Not reachable by any WorldCheck/State-Street spec today - every registered transform is
   * {@code LIGHT} - so {@code handoffTopic}'s deterministic-from-spec-content naming below is
   * genuinely untested against real data; revisit once a real {@code HEAVY} transform exists to
   * exercise it.
   */
  private static Optional<SparkStagePlan> resolveSparkStage(
      IngestionSpec spec, TransformCharacteristics folded) {
    if (folded.cost() != ExecutionCost.HEAVY) {
      return Optional.empty();
    }
    List<String> heavyTransformNames = heavyTransformNames(spec);
    String handoffTopic = "fds-spark-handoff-" + Integer.toHexString(spec.hashCode());
    return Optional.of(new SparkStagePlan(heavyTransformNames, handoffTopic));
  }

  private static List<String> heavyTransformNames(IngestionSpec spec) {
    List<String> names = new ArrayList<>();
    for (TransformSpec.FieldRule rule : allFieldRules(spec)) {
      String transformName = rule.transform();
      if (transformName == null) {
        continue;
      }
      if (TransformCharacteristicsRegistry.get(transformName).cost() == ExecutionCost.HEAVY) {
        names.add(transformName);
      }
    }
    return names;
  }

  private static TransformCharacteristics foldCharacteristics(IngestionSpec spec) {
    Cardinality cardinality = Cardinality.ONE;
    StateRequirement state = StateRequirement.STATELESS;
    ExecutionCost cost = ExecutionCost.LIGHT;
    boolean orderSensitive = false;
    boolean deterministic = true;

    for (TransformSpec.FieldRule rule : allFieldRules(spec)) {
      String transformName = rule.transform();
      if (transformName == null) {
        continue;
      }
      TransformCharacteristics characteristics =
          TransformCharacteristicsRegistry.get(transformName);
      cardinality = worstCardinality(cardinality, characteristics.cardinality());
      if (characteristics.state() == StateRequirement.STATEFUL) {
        state = StateRequirement.STATEFUL;
      }
      if (characteristics.cost() == ExecutionCost.HEAVY) {
        cost = ExecutionCost.HEAVY;
      }
      orderSensitive = orderSensitive || characteristics.orderSensitive();
      deterministic = deterministic && characteristics.deterministic();
    }
    return new TransformCharacteristics(cardinality, state, cost, orderSensitive, deterministic);
  }

  private static Cardinality worstCardinality(Cardinality a, Cardinality b) {
    if (a == Cardinality.CORRELATED || b == Cardinality.CORRELATED) {
      return Cardinality.CORRELATED;
    }
    if (a == Cardinality.DATASET || b == Cardinality.DATASET) {
      return Cardinality.DATASET;
    }
    return Cardinality.ONE;
  }

  /**
   * Every {@code FieldRule} reachable from {@code spec}: each source's own mapper, plus every node
   * in {@code spec.transforms()} when a further stage graph is declared.
   */
  private static List<TransformSpec.FieldRule> allFieldRules(IngestionSpec spec) {
    List<TransformSpec.FieldRule> rules = new ArrayList<>();
    for (SourcePlan source : spec.sources()) {
      rules.addAll(source.mapper().fields());
    }
    TransformGraph transforms = spec.transforms();
    if (transforms != null) {
      for (TransformGraph.TransformNode node : transforms.nodes().values()) {
        rules.addAll(node.mapper().fields());
      }
    }
    return rules;
  }
}
