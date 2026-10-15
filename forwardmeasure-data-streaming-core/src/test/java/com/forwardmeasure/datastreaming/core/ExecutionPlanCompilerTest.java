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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The planner never picks the delivery engine; it only decides whether to add a Spark stage. */
class ExecutionPlanCompilerTest {

  @Test
  void theEngineIsTheInstallationsForEverySourceAndMode() {
    List<IngestionSpec> specs =
        List.of(
            boundedSpec("file", "/tmp/source.csv"),
            boundedSpec("kafka", "kafka:input-topic?brokers=localhost:9092"),
            new IngestionSpec(
                List.of(singleSource("kafka", "kafka:input-topic?brokers=localhost:9092")),
                null,
                null,
                sink(),
                ExecutionMode.CONTINUOUS,
                null,
                null));
    for (DeliveryEngineKind engine : DeliveryEngineKind.values()) {
      for (IngestionSpec spec : specs) {
        ExecutionPlan plan = ExecutionPlanCompiler.compile(spec, engine);
        assertEquals(engine, plan.profile().deliveryEngine());
        assertEquals(spec.executionMode(), plan.profile().executionMode());
        assertTrue(plan.sparkStage().isEmpty(), "no HEAVY transform - no Spark stage expected");
      }
    }
    assertEquals(
        SourceCardinality.SINGLE,
        ExecutionPlanCompiler.compile(
                boundedSpec("file", "/tmp/source.csv"), DeliveryEngineKind.PEKKO_STREAMS)
            .profile()
            .sourceCardinality());
  }

  @Test
  void aStatefulTransformDoesNotForceAnEngine() {
    NamedTransformCatalog catalog =
        NamedTransformCatalog.builtIns()
            .with(
                java.util.Map.of(
                    "stateful_thing",
                    NamedTransform.of(
                        new TransformCharacteristics(
                            TransformCharacteristics.Cardinality.ONE,
                            TransformCharacteristics.StateRequirement.STATEFUL,
                            TransformCharacteristics.ExecutionCost.LIGHT,
                            true,
                            true),
                        inputs -> inputs.get("value"))));
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule(
                    "uid", "ID", null, null, "stateful_thing", null, null)));
    IngestionSpec spec =
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "single", new SourceSpec("file", "/tmp/x.csv", null, null), mapper, 1.0)),
            null,
            null,
            sink(),
            ExecutionMode.BOUNDED,
            null,
            null);
    for (DeliveryEngineKind engine : DeliveryEngineKind.values()) {
      assertEquals(
          engine, ExecutionPlanCompiler.compile(spec, catalog, engine).profile().deliveryEngine());
    }
  }

  @Test
  void platformEngineValuesMapToDeliveryEngines() {
    assertEquals(
        DeliveryEngineKind.KAFKA_STREAMS, DeliveryEngineKind.fromPlatformEngine("kafka-streams"));
    assertEquals(
        DeliveryEngineKind.PEKKO_STREAMS,
        DeliveryEngineKind.fromPlatformEngine("pekko-postgresql"));
    assertEquals(
        DeliveryEngineKind.PEKKO_STREAMS, DeliveryEngineKind.fromPlatformEngine("pekko-cassandra"));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> DeliveryEngineKind.fromPlatformEngine("pekko"));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> DeliveryEngineKind.fromPlatformEngine(null));
  }

  @Test
  void plainFieldCopyWithNoNamedTransformDoesNotBreakCompilation() {
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null)));
    IngestionSpec spec =
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "single", new SourceSpec("file", "/tmp/x.csv", null, null), mapper, 1.0)),
            null,
            null,
            sink(),
            ExecutionMode.BOUNDED,
            null,
            null);

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.KAFKA_STREAMS);

    assertEquals(DeliveryEngineKind.KAFKA_STREAMS, plan.profile().deliveryEngine());
  }

  private static IngestionSpec boundedSpec(String connector, String uri) {
    return new IngestionSpec(
        List.of(singleSource(connector, uri)),
        null,
        null,
        sink(),
        ExecutionMode.BOUNDED,
        null,
        null);
  }

  private static SourcePlan singleSource(String connector, String uri) {
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule(
                    "uid", "ID", null, null, "resolve_iso2_country", null, null)));
    return new SourcePlan("single", new SourceSpec(connector, uri, null, null), mapper, 1.0);
  }

  private static SinkSpec sink() {
    return new SinkSpec("opensearch", "http://localhost:9200", "party-index", null, null);
  }

  // ---- transform graphs ----

  private static IngestionSpec graphSpec(
      com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case fixture,
      ExecutionMode mode,
      String sourceTransform) {
    List<SourcePlan> sources = new java.util.ArrayList<>();
    for (String source : fixture.sources()) {
      List<TransformSpec.FieldRule> rules = new java.util.ArrayList<>();
      for (String field : fixture.inputFields())
        rules.add(new TransformSpec.FieldRule(field, field, null, null, null, true, null));
      if (sourceTransform != null)
        rules.add(
            new TransformSpec.FieldRule(
                "screening", "name", null, null, sourceTransform, true, null));
      sources.add(
          new SourcePlan(
              source,
              new SourceSpec("kafka", "kafka:" + source + "?brokers=broker:9092", null, null),
              new TransformSpec(source, rules),
              1.0));
    }
    return new IngestionSpec(
        sources,
        fixture.blockingField(),
        fixture.graph(),
        new SinkSpec("opensearch", "http://search:9200", "out", null, null),
        mode,
        null,
        null);
  }

  /** Stateful steps carry no mapper; planning reads field rules only from map steps. */
  @Test
  void everyStatefulGraphPlansOnBothEnginesInBothModes() {
    for (var fixture : com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.all()) {
      for (ExecutionMode mode : ExecutionMode.values()) {
        for (DeliveryEngineKind engine : DeliveryEngineKind.values()) {
          ExecutionPlan plan =
              ExecutionPlanCompiler.compile(graphSpec(fixture, mode, null), engine);
          assertEquals(fixture.graph(), plan.transforms(), fixture.name());
          assertTrue(plan.sparkStage().isEmpty(), fixture.name());
        }
      }
    }
  }

  /** The chain fixture with its first (map) step screening each reading - a HEAVY transform. */
  static IngestionSpec heavyStepSpec(ExecutionMode mode) {
    var fixture = com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.chain();
    var nodes = new java.util.LinkedHashMap<>(fixture.graph().nodes());
    nodes.put(
        "rename",
        new com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode(
            new TransformSpec(
                "reading",
                List.of(
                    new TransformSpec.FieldRule(
                        "sensor",
                        "sensor_id",
                        null,
                        null,
                        null,
                        true,
                        null,
                        java.util.Map.of(),
                        true),
                    new TransformSpec.FieldRule(
                        "ts", "ts", null, null, null, true, null, java.util.Map.of(), true),
                    new TransformSpec.FieldRule(
                        "value", "value", null, null, null, true, null, java.util.Map.of(), true),
                    new TransformSpec.FieldRule(
                        "screening",
                        "sensor_id",
                        null,
                        null,
                        "test_screen_against_reference",
                        true,
                        null)))));
    var heavyGraph =
        new com.forwardmeasure.datastreaming.api.TransformGraph(
            fixture.graph().entryNode(), nodes, fixture.graph().edges());
    IngestionSpec base = graphSpec(fixture, mode, null);
    return new IngestionSpec(
        base.sources(),
        base.blockingField(),
        heavyGraph,
        base.sink(),
        base.executionMode(),
        base.delivery(),
        base.errors());
  }

  /**
   * A HEAVY transform inside a graph puts the graph, through that step, in the Spark stage; the
   * delivery stage runs the steps after it, and reads exactly the fields those steps produced.
   */
  @Test
  void aHeavyGraphStepRunsInSparkAndDeliveryRunsTheRest() {
    for (ExecutionMode mode : ExecutionMode.values()) {
      for (DeliveryEngineKind engine : DeliveryEngineKind.values()) {
        ExecutionPlan plan = ExecutionPlanCompiler.compile(heavyStepSpec(mode), engine);
        assertTrue(plan.sparkStage().isPresent());
        assertEquals(1, plan.sparkStage().get().graphSteps());
        assertEquals(
            List.of("test_screen_against_reference"), plan.sparkStage().get().transformNames());
        IngestionSpec delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "broker:9092");
        assertEquals(1, delivery.sources().size());
        assertEquals(mode, delivery.executionMode());
        assertEquals(List.of("dedupe", "hourly"), delivery.transforms().chainNames());
        assertEquals(
            List.of("sensor", "ts", "value", "screening"),
            delivery.sources().getFirst().mapper().fields().stream()
                .map(TransformSpec.FieldRule::target)
                .toList());
        // The delivery stage needs no second Spark stage: nothing HEAVY is left.
        assertTrue(ExecutionPlanCompiler.compile(delivery, engine).sparkStage().isEmpty());
      }
    }
  }

  /**
   * A bounded join behind a Spark stage gets each source's records on its own topic, as a
   * continuous one always has, so the delivery stage joins the two sides.
   */
  @Test
  void aJoinBehindASparkStageGetsEachSourceSeparately() {
    var join = com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.windowJoin();
    for (ExecutionMode mode : ExecutionMode.values()) {
      ExecutionPlan plan =
          ExecutionPlanCompiler.compile(
              graphSpec(join, mode, "test_screen_against_reference"),
              DeliveryEngineKind.PEKKO_STREAMS);
      assertEquals(0, plan.sparkStage().orElseThrow().graphSteps());
      assertTrue(SparkHandoffSpecs.handsOffEachSource(plan));
      IngestionSpec delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "broker:9092");
      assertEquals(
          List.of("orders", "shipments"),
          delivery.sources().stream().map(SourcePlan::sourceKey).toList());
      assertTrue(delivery.sources().get(1).source().uri().contains("-source-1?"));
      assertEquals(join.graph(), delivery.transforms());
      assertEquals(mode, delivery.executionMode());
      ExecutionPlanCompiler.compile(delivery, DeliveryEngineKind.KAFKA_STREAMS);
    }
  }

  /** A HEAVY step after a join puts the join itself in Spark: one joined stream is handed off. */
  @Test
  void aHeavyStepAfterAJoinPutsTheJoinInSpark() {
    var join = com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.windowJoin();
    var nodes = new java.util.LinkedHashMap<>(join.graph().nodes());
    nodes.put(
        "screen",
        new com.forwardmeasure.datastreaming.api.TransformGraph.TransformNode(
            new TransformSpec(
                "order",
                List.of(
                    new TransformSpec.FieldRule(
                        "order_id", "order_id", null, null, null, true, null),
                    new TransformSpec.FieldRule(
                        "screening",
                        "customer",
                        null,
                        null,
                        "test_screen_against_reference",
                        true,
                        null)))));
    var edges = new java.util.ArrayList<>(join.graph().edges());
    edges.add(
        new com.forwardmeasure.datastreaming.api.TransformGraph.TransformEdge(
            join.graph().entryNode(), "screen", null));
    var graph =
        new com.forwardmeasure.datastreaming.api.TransformGraph(
            join.graph().entryNode(), nodes, edges);
    IngestionSpec base = graphSpec(join, ExecutionMode.BOUNDED, null);
    IngestionSpec spec =
        new IngestionSpec(
            base.sources(),
            base.blockingField(),
            graph,
            base.sink(),
            base.executionMode(),
            base.delivery(),
            base.errors());
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.KAFKA_STREAMS);
    assertEquals(2, plan.sparkStage().orElseThrow().graphSteps());
    assertTrue(!SparkHandoffSpecs.handsOffEachSource(plan));
    IngestionSpec delivery = SparkHandoffSpecs.deliveryStageSpec(plan, "broker:9092");
    assertEquals(1, delivery.sources().size());
    org.junit.jupiter.api.Assertions.assertNull(delivery.transforms());
  }
}
