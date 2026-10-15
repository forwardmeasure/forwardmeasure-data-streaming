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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SparkPlanningContractTest {
  @Test
  void actualScreeningTransformRequiresSparkAndStableDestinationSpecificHandoff() {
    var spec = spec(ExecutionMode.BOUNDED, "one", false);
    var plan = ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.PEKKO_STREAMS);
    var spark = plan.sparkStage().orElseThrow();
    assertEquals(List.of("test_screen_against_reference"), spark.transformNames());
    assertEquals(DeliveryEngineKind.PEKKO_STREAMS, plan.profile().deliveryEngine());
    assertEquals(
        spark.handoffTopic(),
        ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.PEKKO_STREAMS)
            .sparkStage()
            .orElseThrow()
            .handoffTopic());
    assertNotEquals(
        spark.handoffTopic(),
        ExecutionPlanCompiler.compile(
                spec(ExecutionMode.BOUNDED, "two", false), DeliveryEngineKind.PEKKO_STREAMS)
            .sparkStage()
            .orElseThrow()
            .handoffTopic());
    var delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "broker:9092");
    var names = List.of(Map.of("value", "Jane Doe", "name_type", "PRIMARY"));
    var result =
        new FieldMappingEngine()
            .map(
                new MapSourceRow(Map.of("uid", "42", "names", names, "matches", List.of("other"))),
                delivery.sources().getFirst().mapper());
    assertEquals(Map.of("uid", "42", "names", names, "matches", List.of("other")), result);
    assertTrue(
        ExecutionPlanCompiler.compile(delivery, DeliveryEngineKind.PEKKO_STREAMS)
            .sparkStage()
            .isEmpty(),
        "Delivery must not run screening again");
  }

  @Test
  void continuousCorrelatedHandoffKeepsSeparateSourcesKeysWeightsAndBlocking() {
    var original = spec(ExecutionMode.CONTINUOUS, "one", true);
    var plan = ExecutionPlanCompiler.compile(original, DeliveryEngineKind.PEKKO_STREAMS);
    var delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "broker:9092");
    assertEquals(ExecutionMode.CONTINUOUS, delivery.executionMode());
    assertEquals("uid", delivery.blockingField());
    assertEquals(original.sink(), delivery.sink());
    assertEquals(original.delivery(), delivery.delivery());
    assertEquals(original.errors(), delivery.errors());
    for (int i = 0; i < 2; i++) {
      var source = delivery.sources().get(i);
      assertEquals(original.sources().get(i).sourceKey(), source.sourceKey());
      assertEquals(original.sources().get(i).trustWeight(), source.trustWeight());
      assertEquals(
          "kafka:"
              + plan.sparkStage().orElseThrow().handoffTopic()
              + "-source-"
              + i
              + "?brokers=broker:9092",
          source.source().uri());
      assertTrue(source.mapper().fields().stream().allMatch(FieldRule::isRaw));
    }
    assertNotEquals(
        delivery.sources().get(0).source().uri(), delivery.sources().get(1).source().uri());
  }

  @Test
  void graphsCompileButUnknownTransformsCannotBecomeExecutablePlans() {
    var spec = spec(ExecutionMode.BOUNDED, "one", false);
    var graph =
        new TransformGraph(
            "node",
            Map.of("node", new TransformGraph.TransformNode(spec.sources().getFirst().mapper())),
            List.of());
    assertEquals(
        graph,
        ExecutionPlanCompiler.compile(
                new IngestionSpec(
                    spec.sources(), null, graph, spec.sink(), spec.executionMode(), null, null),
                DeliveryEngineKind.PEKKO_STREAMS)
            .transforms());
    var bad =
        new SourcePlan(
            "one",
            new SourceSpec("file", "/input.csv", null, null),
            new TransformSpec(
                "target",
                List.of(
                    new FieldRule(
                        "name", "name", null, null, "unknown_vendor_transform", false, false))),
            1.0);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExecutionPlanCompiler.compile(
                new IngestionSpec(
                    List.of(bad), null, null, spec.sink(), spec.executionMode(), null, null),
                DeliveryEngineKind.PEKKO_STREAMS));
  }

  private static IngestionSpec spec(ExecutionMode mode, String destination, boolean correlated) {
    var mapper =
        new TransformSpec(
            null,
            List.of(
                new FieldRule("uid", "id", null, null, null, false, false),
                new FieldRule("names", "name", null, null, "parse_semicolon_list", false, false),
                new FieldRule(
                    "matches", "name", null, null, "test_screen_against_reference", true, false)));
    var one = new SourcePlan("one", new SourceSpec("file", "/input.csv", null, null), mapper, 0.9);
    var two = new SourcePlan("two", new SourceSpec("file", "/other.csv", null, null), mapper, 0.4);
    return new IngestionSpec(
        correlated ? List.of(one, two) : List.of(one),
        correlated ? "uid" : null,
        null,
        new SinkSpec("opensearch", "http://localhost:9200", destination, null, null),
        mode,
        null,
        null);
  }
}
