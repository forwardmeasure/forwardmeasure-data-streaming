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
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The planner: compiles one author-facing {@link IngestionSpec} into the {@link ExecutionPlan} a
 * {@code DeliveryEngine} runs. It reads every reachable {@link TransformSpec.FieldRule#transform()}
 * name against a {@link NamedTransformCatalog} (each transform declares its own characteristics)
 * and decides one thing: whether a {@code HEAVY} transform needs a Spark stage. The delivery engine
 * is not the planner's choice - the caller passes the installation's engine (the platform {@code
 * engine} setting, see {@link DeliveryEngineKind#fromPlatformEngine}), and every capability runs on
 * either engine.
 */
public final class ExecutionPlanCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPlanCompiler.class);

  private ExecutionPlanCompiler() {}

  /**
   * The environment variable every launched Job carries: the installation's {@link
   * DeliveryEngineKind} name.
   */
  public static final String DELIVERY_ENGINE_ENV = "FDS_DELIVERY_ENGINE";

  /**
   * This installation's delivery engine, from {@value #DELIVERY_ENGINE_ENV} - for a runner, such as
   * the Spark stage, that is not itself a delivery engine.
   */
  public static DeliveryEngineKind installationEngine() {
    String engine = System.getenv(DELIVERY_ENGINE_ENV);
    if (engine == null || engine.isBlank()) {
      throw new IllegalStateException(
          "required environment variable '" + DELIVERY_ENGINE_ENV + "' is not set");
    }
    return DeliveryEngineKind.valueOf(engine.strip());
  }

  /** Compiles for {@code engine} against {@link NamedTransformCatalog#installed()}. */
  public static ExecutionPlan compile(IngestionSpec spec, DeliveryEngineKind engine) {
    return compile(spec, NamedTransformCatalog.installed(), engine);
  }

  /**
   * Compiles for {@code engine} against {@code catalog}, which must know every transform {@code
   * spec} names.
   */
  public static ExecutionPlan compile(
      IngestionSpec spec, NamedTransformCatalog catalog, DeliveryEngineKind engine) {
    java.util.Objects.requireNonNull(engine, "engine");
    TransformCharacteristics folded = foldCharacteristics(spec, catalog);
    ExecutionProfile profile =
        new ExecutionProfile(spec.sourceCardinality(), spec.executionMode(), engine);
    Optional<SparkStagePlan> sparkStage = resolveSparkStage(spec, folded, catalog, engine);
    ExecutionPlan plan =
        new ExecutionPlan(
            profile,
            spec.sources(),
            spec.blockingField(),
            sparkStage,
            spec.transforms(),
            spec.sink(),
            spec.delivery(),
            spec.errors(),
            com.forwardmeasure.datastreaming.api.MergePolicy.load(spec.mergePolicyUri()));
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          "plan.compiled engine={} cardinality={} mode={} sparkStage={} cost={} state={}",
          engine,
          profile.sourceCardinality(),
          profile.executionMode(),
          sparkStage.map(SparkStagePlan::handoffTopic).orElse("absent"),
          folded.cost(),
          folded.state());
    }
    return plan;
  }

  /**
   * Present only when the fold is {@link ExecutionCost#HEAVY} - the {@link
   * TransformCharacteristics.Cardinality#CORRELATED}-at-scale trigger from {@code SparkStagePlan}'s
   * own javadoc is deliberately not implemented: no real correlated-at-scale spec exists anywhere
   * in this org's data to build that heuristic against honestly (see the repo's own gap-bridging
   * plan).
   */
  private static Optional<SparkStagePlan> resolveSparkStage(
      IngestionSpec spec,
      TransformCharacteristics folded,
      NamedTransformCatalog catalog,
      DeliveryEngineKind engine) {
    if (folded.cost() != ExecutionCost.HEAVY) {
      return Optional.empty();
    }
    List<String> heavyTransformNames = heavyTransformNames(spec, catalog);
    ExecutionPlan identityPlan =
        new ExecutionPlan(
            new ExecutionProfile(spec.sourceCardinality(), spec.executionMode(), engine),
            spec.sources(),
            spec.blockingField(),
            Optional.empty(),
            spec.transforms(),
            spec.sink(),
            spec.delivery(),
            spec.errors(),
            com.forwardmeasure.datastreaming.api.MergePolicy.load(spec.mergePolicyUri()));
    String handoffTopic =
        com.forwardmeasure.datastreaming.api.ExecutionIdentity.of(
            identityPlan, "fds-spark-handoff-", System.getenv("FDS_EXECUTION_ID"));
    return Optional.of(
        new SparkStagePlan(
            heavyTransformNames, handoffTopic, sparkGraphSteps(spec.transforms(), catalog)));
  }

  private static List<String> heavyTransformNames(
      IngestionSpec spec, NamedTransformCatalog catalog) {
    List<String> names = new ArrayList<>();
    for (TransformSpec.FieldRule rule : allFieldRules(spec)) {
      String transformName = rule.transform();
      if (transformName == null) {
        continue;
      }
      if (catalog.characteristics(transformName).cost() == ExecutionCost.HEAVY) {
        names.add(transformName);
      }
    }
    return names;
  }

  private static TransformCharacteristics foldCharacteristics(
      IngestionSpec spec, NamedTransformCatalog catalog) {
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
      TransformCharacteristics characteristics = catalog.characteristics(transformName);
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
   * How many graph steps, from the entry step, the Spark stage runs: through the last map step that
   * uses a {@code HEAVY} transform, so every such step runs at Spark scale - stateful steps before
   * it included, with the same semantics as on a delivery engine. Zero when no graph step needs
   * Spark.
   */
  static int sparkGraphSteps(TransformGraph transforms, NamedTransformCatalog catalog) {
    if (transforms == null) return 0;
    List<TransformGraph.TransformNode> chain = transforms.chain();
    int steps = 0;
    for (int i = 0; i < chain.size(); i++) {
      TransformGraph.TransformNode node = chain.get(i);
      if (node.mapper() == null) continue;
      for (TransformSpec.FieldRule rule : node.mapper().fields()) {
        if (rule.transform() != null
            && catalog.characteristics(rule.transform()).cost() == ExecutionCost.HEAVY) {
          steps = i + 1;
        }
      }
    }
    return steps;
  }

  /**
   * Every {@code FieldRule} reachable from {@code spec}: each source's own mapper, plus every map
   * step of {@code spec.transforms()} - the other step kinds carry no field rules.
   */
  private static List<TransformSpec.FieldRule> allFieldRules(IngestionSpec spec) {
    List<TransformSpec.FieldRule> rules = new ArrayList<>();
    for (SourcePlan source : spec.sources()) {
      rules.addAll(source.mapper().fields());
    }
    if (spec.transforms() != null) rules.addAll(graphFieldRules(spec.transforms()));
    return rules;
  }

  private static List<TransformSpec.FieldRule> graphFieldRules(TransformGraph transforms) {
    List<TransformSpec.FieldRule> rules = new ArrayList<>();
    for (TransformGraph.TransformNode node : transforms.nodes().values()) {
      if (node.mapper() != null) rules.addAll(node.mapper().fields());
    }
    return rules;
  }
}
