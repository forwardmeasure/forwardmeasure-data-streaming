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
package com.forwardmeasure.datastreaming.launcher.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class IngestionLaunchPlannerTest {
  private static final String IMAGE = "example/worker@sha256:" + "a".repeat(64);
  private final IngestionLaunchPlanner planner =
      new IngestionLaunchPlanner(
          IngestionJobPolicy.configured(Set.of("workers"), Set.of(IMAGE)),
          IMAGE,
          "pekko",
          IMAGE,
          "kafka",
          IMAGE,
          "spark",
          "broker:9092",
          List.of("registry"));

  private IngestionSpec heavy() {
    return TestCustomerMasterFixtures.boundedKafkaSpecWithScreening(
        "broker:9092", "input", "http://search:9200", Path.of(""));
  }

  @Test
  void stagesPreserveResourcesAndSeparateConcurrentRuns() {
    var request =
        new DirectLaunchRequest(
            "run", "workers", heavy(), Map.of("memory", "1Gi"), Map.of("memory", "2Gi"), 600L);
    Map<String, Object> first = planner.workflowInput(request, "tenant-a:run");
    Map<String, Object> second = planner.workflowInput(request, "tenant-b:run");
    assertEquals(true, first.get("hasDelivery"));
    assertNotEquals(first.get("deploymentName"), second.get("deploymentName"));
    for (String stage : List.of("primary", "delivery")) {
      Map<?, ?> worker = (Map<?, ?>) first.get(stage);
      assertEquals(600L, worker.get("activeDeadlineSeconds"));
      assertEquals(
          Map.of("requests", request.resourceRequests(), "limits", request.resourceLimits()),
          worker.get("resources"));
      assertEquals(List.of("registry"), worker.get("imagePullSecretNames"));
      Map<?, ?> env = (Map<?, ?>) worker.get("env");
      assertTrue(env.containsKey("MERGE_POLICY_YAML_BASE64"));
      assertEquals("tenant-a:run", env.get("FDS_EXECUTION_ID"));
    }
    Map<?, ?> envA = (Map<?, ?>) ((Map<?, ?>) first.get("primary")).get("env");
    Map<?, ?> envB = (Map<?, ?>) ((Map<?, ?>) second.get("primary")).get("env");
    assertNotEquals(envA.get("SPARK_HANDOFF_TOPIC"), envB.get("SPARK_HANDOFF_TOPIC"));
  }

  @Test
  void continuousSparkKeepsIndependentComputeAndDeliveryWorkers() {
    var spec = heavy();
    var continuous =
        new IngestionSpec(
            spec.sources().stream()
                .map(
                    source ->
                        new com.forwardmeasure.datastreaming.api.SourcePlan(
                            source.sourceKey(),
                            new com.forwardmeasure.datastreaming.api.SourceSpec(
                                "kafka",
                                "kafka:input?brokers=broker:9092",
                                source.source().format(),
                                source.source().schema(),
                                source.source().query(),
                                source.source().options()),
                            source.mapper(),
                            source.trustWeight()))
                .toList(),
            spec.blockingField(),
            spec.transforms(),
            spec.sink(),
            ExecutionMode.CONTINUOUS,
            spec.delivery(),
            spec.errors(),
            spec.mergePolicyUri());
    var input =
        planner.workflowInput(
            new DirectLaunchRequest("run", "workers", continuous, Map.of(), Map.of(), null),
            "tenant:run");
    assertEquals(true, input.get("continuous"));
    assertEquals(true, input.get("hasDelivery"));
    Map<?, ?> compute = (Map<?, ?>) input.get("primary");
    Map<?, ?> delivery = (Map<?, ?>) input.get("delivery");
    assertEquals(1, compute.get("replicas"));
    assertEquals(1, delivery.get("replicas"));
    assertNotEquals(compute.get("name"), delivery.get("name"));
    Map<?, ?> env = (Map<?, ?>) delivery.get("env");
    String yaml =
        new String(
            java.util.Base64.getDecoder().decode((String) env.get("INGESTION_SPEC_YAML_BASE64")),
            java.nio.charset.StandardCharsets.UTF_8);
    var deliverySpec = IngestionSpec.parseYaml(yaml);
    assertEquals(ExecutionMode.CONTINUOUS, deliverySpec.executionMode());
    assertTrue(deliverySpec.sources().getFirst().source().uri().contains("-source-0?"));
    assertNull(deliverySpec.transforms());
  }

  @Test
  void neverDispatchesAPlanThatWouldSilentlyIgnoreAGraph() {
    var spec = heavy();
    TransformGraph graph =
        new TransformGraph(
            "map",
            Map.of("map", new TransformGraph.TransformNode(spec.sources().getFirst().mapper())),
            List.of());
    var unsupported =
        new IngestionSpec(
            spec.sources(),
            spec.blockingField(),
            graph,
            spec.sink(),
            spec.executionMode(),
            spec.delivery(),
            spec.errors());
    var failure =
        assertThrows(
            IllegalArgumentException.class, () -> planner.compile(unsupported, "tenant:run"));
    assertTrue(failure.getMessage().contains("TransformGraph"));
  }

  @Test
  void rejectsNamespaceBeforeConstructingAnyDispatch() {
    assertThrows(
        SecurityException.class,
        () ->
            planner.workflowInput(
                new DirectLaunchRequest("run", "other", heavy(), Map.of(), Map.of(), null),
                "tenant:run"));
  }
}
