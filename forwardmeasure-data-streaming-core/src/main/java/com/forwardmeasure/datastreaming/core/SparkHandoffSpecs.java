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
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
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
 * real for every WorldCheck/Customer-Master spec built so far) would otherwise be silently mangled
 * by {@code SourceRow#get}'s own {@code String.valueOf(...)} coercion - see {@link FieldRule#raw}'s
 * own javadoc.
 */
public final class SparkHandoffSpecs {

  private static final String HANDOFF_SOURCE_KEY = "spark-handoff";

  private SparkHandoffSpecs() {}

  /**
   * @param kafkaBootstrapServers the real Kafka cluster's own bootstrap servers - deliberately a
   *     caller-supplied runtime address, not read off {@code plan} itself, matching {@code
   *     SparkIngestionRunner}'s own established convention that a compiled plan never carries
   *     runtime infrastructure addresses.
   * @throws IllegalArgumentException if {@code plan} has no {@code sparkStage} to hand off from
   */
  public static IngestionSpec deliveryStageSpec(ExecutionPlan plan, String kafkaBootstrapServers) {
    if (plan.sparkStage().isEmpty()) {
      throw new IllegalArgumentException(
          "SparkHandoffSpecs: plan has no sparkStage - there is nothing to hand off from");
    }
    String handoffTopic = plan.sparkStage().get().handoffTopic();
    TransformSpec identityMapper =
        new TransformSpec(originalMapperTarget(plan), identityFieldRules(plan));
    SourceSpec kafkaSource =
        new SourceSpec(
            "kafka",
            "kafka:" + handoffTopic + "?brokers=" + kafkaBootstrapServers,
            null,
            null,
            null,
            Map.of());
    SourcePlan sourcePlan = new SourcePlan(HANDOFF_SOURCE_KEY, kafkaSource, identityMapper, 1.0);
    return new IngestionSpec(
        List.of(sourcePlan),
        null,
        null,
        plan.destination(),
        ExecutionMode.BOUNDED,
        plan.delivery(),
        plan.errors());
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
  private static List<FieldRule> identityFieldRules(ExecutionPlan plan) {
    Set<String> targets = new LinkedHashSet<>();
    for (SourcePlan source : plan.sources()) {
      for (FieldRule rule : source.mapper().fields()) {
        targets.add(rule.target());
      }
    }
    List<FieldRule> rules = new ArrayList<>();
    for (String target : targets) {
      rules.add(new FieldRule(target, target, null, null, null, true, false, Map.of(), true));
    }
    return rules;
  }
}
