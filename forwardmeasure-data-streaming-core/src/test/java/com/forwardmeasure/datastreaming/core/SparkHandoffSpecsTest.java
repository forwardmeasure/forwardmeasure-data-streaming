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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A manually-constructed {@link ExecutionPlan} (bypassing {@link ExecutionPlanCompiler}'s own real
 * HEAVY-transform detection - no real transform is registered {@code HEAVY} yet, see that class's
 * own test) exercises {@link SparkHandoffSpecs} in isolation: given a plan whose {@code sparkStage}
 * is already present, does the derived delivery spec have the right shape.
 */
class SparkHandoffSpecsTest {

  @Test
  void derivesAKafkaSourcedIdentityPassthroughSpecFromTheOriginalMappers() {
    ExecutionPlan plan = planWithTwoSourcesSharingATargetField();

    IngestionSpec delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "kafka:9092");

    assertEquals(1, delivery.sources().size());
    SourcePlan deliverySource = delivery.sources().get(0);
    assertEquals("kafka", deliverySource.source().connector());
    assertEquals("kafka:handoff-topic?brokers=kafka:9092", deliverySource.source().uri());
    assertEquals(ExecutionMode.BOUNDED, delivery.executionMode());
    assertEquals(plan.destination(), delivery.sink());

    // "names" is contributed by two original rules (across two sources) but must collapse into
    // exactly one identity rule - Spark already accumulated it into one final value.
    List<FieldRule> fields = deliverySource.mapper().fields();
    assertEquals(2, fields.size(), "uid + names, deduplicated");
    for (FieldRule rule : fields) {
      assertTrue(rule.isRaw(), "every identity rule must be raw - see FieldRule#raw's own javadoc");
      assertEquals(rule.target(), rule.source(), "identity: source name == target name");
      assertTrue(rule.isOptional());
      assertEquals(false, rule.isRepeated());
    }
  }

  @Test
  void rejectsAPlanWithNoSparkStage() {
    ExecutionPlan plan = planWithoutSparkStage();

    assertThrows(
        IllegalArgumentException.class, () -> SparkHandoffSpecs.deliveryStageSpec(plan, "k:9092"));
  }

  private static ExecutionPlan planWithTwoSourcesSharingATargetField() {
    TransformSpec mapperOne =
        new TransformSpec(
            "party",
            List.of(
                new FieldRule("uid", "ID", null, null, null, null, null),
                new FieldRule("names", "primaryName", null, null, null, null, true)));
    TransformSpec mapperTwo =
        new TransformSpec(
            "party", List.of(new FieldRule("names", "aliasName", null, null, null, null, true)));
    SourcePlan sourceOne =
        new SourcePlan("one", new SourceSpec("file", "/tmp/one.csv", null, null), mapperOne, 1.0);
    SourcePlan sourceTwo =
        new SourcePlan("two", new SourceSpec("file", "/tmp/two.csv", null, null), mapperTwo, 1.0);
    ExecutionProfile profile =
        new ExecutionProfile(
            SourceCardinality.CORRELATED, ExecutionMode.BOUNDED, DeliveryEngineKind.PEKKO_STREAMS);
    return new ExecutionPlan(
        profile,
        List.of(sourceOne, sourceTwo),
        "blockingKey",
        Optional.of(new SparkStagePlan(List.of("some_heavy_transform"), "handoff-topic")),
        null,
        sink(),
        null,
        null);
  }

  private static ExecutionPlan planWithoutSparkStage() {
    TransformSpec mapper =
        new TransformSpec(
            "party", List.of(new FieldRule("uid", "ID", null, null, null, null, null)));
    SourcePlan source =
        new SourcePlan("single", new SourceSpec("file", "/tmp/x.csv", null, null), mapper, 1.0);
    ExecutionProfile profile =
        new ExecutionProfile(
            SourceCardinality.SINGLE, ExecutionMode.BOUNDED, DeliveryEngineKind.PEKKO_STREAMS);
    return new ExecutionPlan(
        profile, List.of(source), null, Optional.empty(), null, sink(), null, null);
  }

  private static SinkSpec sink() {
    return new SinkSpec("opensearch", "http://localhost:9200", "party-index", null, null);
  }
}
