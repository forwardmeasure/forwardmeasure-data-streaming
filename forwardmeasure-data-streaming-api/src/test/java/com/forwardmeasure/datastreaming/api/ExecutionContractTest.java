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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExecutionContractTest {
  private static final TransformSpec MAPPING =
      new TransformSpec(
          "entity",
          List.of(new TransformSpec.FieldRule("uid", "id", null, null, null, false, false)));
  private static final SinkSpec SINK =
      new SinkSpec("kafka", "kafka:out?brokers=broker:9092", "out", null, null);
  private static final ExecutionProfile SINGLE =
      new ExecutionProfile(
          SourceCardinality.SINGLE, ExecutionMode.CONTINUOUS, DeliveryEngineKind.PEKKO_STREAMS);
  private static final ExecutionProfile CORRELATED =
      new ExecutionProfile(
          SourceCardinality.CORRELATED, ExecutionMode.BOUNDED, DeliveryEngineKind.KAFKA_STREAMS);

  @Test
  void restartIdentityIsStableAcrossMapOrderingButSeparatesTenantsAndDestinations() {
    var firstOptions = new LinkedHashMap<String, String>();
    firstOptions.put("encoding", "UTF-8");
    firstOptions.put("delimiter", "|");
    var secondOptions = new LinkedHashMap<String, String>();
    secondOptions.put("delimiter", "|");
    secondOptions.put("encoding", "UTF-8");
    var first = plan(List.of(source("a", firstOptions)), SINGLE, SINK);
    var reordered = plan(List.of(source("a", secondOptions)), SINGLE, SINK);
    String identity = ExecutionIdentity.of(first, "run-", "tenant-a/revision-1");
    assertEquals(identity, ExecutionIdentity.of(reordered, "run-", "tenant-a/revision-1"));
    assertNotEquals(identity, ExecutionIdentity.of(first, "run-", "tenant-b/revision-1"));
    assertNotEquals(identity, ExecutionIdentity.of(first, "run-", "tenant-a/revision-2"));
    var otherSink = new SinkSpec("kafka", "kafka:other?brokers=broker:9092", "other", null, null);
    assertNotEquals(
        identity,
        ExecutionIdentity.of(
            plan(first.sources(), SINGLE, otherSink), "run-", "tenant-a/revision-1"));
    assertTrue(identity.matches("run-[a-f0-9]{32}"));
    assertThrows(
        IllegalArgumentException.class, () -> ExecutionIdentity.of(null, "run-", "invalid"));
  }

  @Test
  void sourceCardinalityAndUnsupportedGraphsFailBeforeExecution() {
    var source = source("a", Map.of());
    assertThrows(IllegalArgumentException.class, () -> plan(null, SINGLE, SINK));
    assertThrows(IllegalArgumentException.class, () -> plan(List.of(), SINGLE, SINK));
    assertThrows(IllegalArgumentException.class, () -> plan(List.of(source), CORRELATED, SINK));
    assertThrows(
        IllegalArgumentException.class,
        () -> plan(List.of(source, source("b", Map.of())), SINGLE, SINK));
    var graph =
        new TransformGraph("map", Map.of("map", new TransformGraph.TransformNode(MAPPING)), null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExecutionPlan(SINGLE, List.of(source), null, null, graph, SINK, null, null));
    var sources = new ArrayList<>(List.of(source, source("b", Map.of())));
    var transforms = new ArrayList<>(List.of("correlate"));
    var stage = new SparkStagePlan(transforms, "handoff");
    var accepted =
        new ExecutionPlan(
            CORRELATED,
            sources,
            "uid",
            Optional.of(stage),
            null,
            SINK,
            DeliverySemantics.defaults(),
            new ErrorPolicy("fail", "fail"),
            null);
    sources.clear();
    transforms.clear();
    assertEquals(2, accepted.sources().size());
    assertEquals(List.of("correlate"), accepted.sparkStage().orElseThrow().transformNames());
    assertEquals(MergePolicy.defaults(), accepted.mergePolicy());
    for (List<String> names : java.util.Arrays.<List<String>>asList(null, List.of()))
      assertThrows(IllegalArgumentException.class, () -> new SparkStagePlan(names, "handoff"));
    assertThrows(
        IllegalArgumentException.class, () -> new SparkStagePlan(List.of("correlate"), " "));
  }

  @Test
  void admissionRejectsAmbiguousSourcesAndMissingCorrelationKeys() {
    var a = source("a", Map.of());
    var b = source("b", Map.of());
    for (List<SourcePlan> invalid :
        java.util.Arrays.<List<SourcePlan>>asList(null, List.of(), List.of(a, a)))
      assertThrows(IllegalArgumentException.class, () -> spec(invalid, "uid", null));
    for (String key : new String[] {null, " "})
      assertThrows(IllegalArgumentException.class, () -> spec(List.of(a, b), key, null));
    assertThrows(IllegalArgumentException.class, () -> source(" ", Map.of()));
    assertThrows(
        IllegalArgumentException.class, () -> new SourcePlan("a", a.source(), MAPPING, Double.NaN));
    assertEquals(MergePolicy.DEFAULT_URI, spec(List.of(a), null, " ").mergePolicyUri());
    assertEquals(
        SourceCardinality.CORRELATED,
        spec(List.of(a, b), "uid", MergePolicy.DEFAULT_URI).sourceCardinality());
    assertThrows(IllegalArgumentException.class, () -> IngestionSpec.parseYaml("sources: [broken"));
  }

  @Test
  void kafkaUrisPreserveBrokerListsAndRejectMissingConnectionData() {
    var uri =
        KafkaConnectorUri.parse(
            "kafka:in?brokers=host1%3A9092%2Chost2%3A9092&&flag&consumerGroup=g");
    assertEquals("in", uri.topic());
    assertEquals("host1:9092,host2:9092", uri.bootstrapServers());
    for (String invalid :
        List.of(
            "http://in",
            "kafka:in",
            "kafka:in?flag",
            "kafka:in?brokers",
            "kafka:in?brokers=",
            "kafka:?brokers=host:9092",
            "kafka:in?brokers=%XX"))
      assertThrows(IllegalArgumentException.class, () -> KafkaConnectorUri.parse(invalid), invalid);
    assertThrows(IllegalArgumentException.class, () -> new KafkaConnectorUri(" ", "host:9092"));
  }

  @Test
  void graphsRejectDanglingEdgesAndDoNotRetainMutableConfiguration() {
    var node = new TransformGraph.TransformNode(MAPPING);
    for (Map<String, TransformGraph.TransformNode> nodes :
        java.util.Arrays.<Map<String, TransformGraph.TransformNode>>asList(null, Map.of()))
      assertThrows(IllegalArgumentException.class, () -> new TransformGraph("start", nodes, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TransformGraph("missing", Map.of("start", node), null));
    for (var edge :
        List.of(
            new TransformGraph.TransformEdge("missing", "start", null),
            new TransformGraph.TransformEdge("start", "missing", "valid")))
      assertThrows(
          IllegalArgumentException.class,
          () -> new TransformGraph("start", Map.of("start", node), List.of(edge)));
    var nodes = new LinkedHashMap<>(Map.of("start", node, "end", node));
    var edges = new ArrayList<>(List.of(new TransformGraph.TransformEdge("start", "end", "valid")));
    var graph = new TransformGraph("start", nodes, edges);
    nodes.clear();
    edges.clear();
    assertEquals(2, graph.nodes().size());
    assertEquals("valid", graph.edges().getFirst().condition());
  }

  @Test
  void aliasMergeRulesRequireCompleteClassificationAndRejectUntrustedLocations() {
    for (String[] values :
        new String[][] {
          {null, "PRIMARY", "ALIAS"},
          {"", "PRIMARY", "ALIAS"},
          {"kind", null, "ALIAS"},
          {"kind", "", "ALIAS"},
          {"kind", "PRIMARY", null},
          {"kind", "PRIMARY", ""}
        })
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new MergePolicy.FieldRule(
                  MergePolicy.Strategy.ALIASES, values[0], values[1], values[2]));
    var aliases =
        new MergePolicy.FieldRule(MergePolicy.Strategy.ALIASES, "kind", "PRIMARY", "ALIAS");
    assertEquals(aliases, new MergePolicy(1, Map.of("names", aliases)).fields().get("names"));
    assertEquals(Map.of(), new MergePolicy(1, null).fields());
    assertThrows(IllegalArgumentException.class, () -> new MergePolicy(2, null));
    assertEquals(MergePolicy.defaults(), MergePolicy.load(null));
    assertEquals(MergePolicy.defaults(), MergePolicy.load(" "));
    assertThrows(
        IllegalArgumentException.class,
        () -> MergePolicy.load("https://example.invalid/merge.yaml"));
    assertThrows(
        IllegalArgumentException.class,
        () -> MergePolicy.load("classpath:/absent-merge-policy.yaml"));
  }

  private static SourcePlan source(String key, Map<String, String> options) {
    return new SourcePlan(
        key,
        new SourceSpec(
            "file", "/tmp/input.csv", new SourceSpec.FormatSpec("csv"), null, null, options),
        MAPPING,
        1);
  }

  private static ExecutionPlan plan(
      List<SourcePlan> sources, ExecutionProfile profile, SinkSpec sink) {
    return new ExecutionPlan(profile, sources, "uid", null, null, sink, null, null);
  }

  private static IngestionSpec spec(List<SourcePlan> sources, String blocking, String mergeUri) {
    return new IngestionSpec(
        sources, blocking, null, SINK, ExecutionMode.BOUNDED, null, null, mergeUri);
  }
}
