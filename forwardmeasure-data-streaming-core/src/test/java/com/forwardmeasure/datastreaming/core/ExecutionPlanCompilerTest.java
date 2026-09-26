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
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Real proof of {@link ExecutionPlanCompiler}'s automatic engine selection against every branch
 * genuinely reachable today. The {@code STATEFUL}/{@code HEAVY}-triggered branches (forcing {@code
 * KAFKA_STREAMS} regardless of mode, inserting a {@code SparkStagePlan}) are deliberately not
 * exercised here - no transform registered in {@code TransformCharacteristicsRegistry} is {@code
 * STATEFUL} or {@code HEAVY} yet (every real WorldCheck/Customer-Master transform is {@code
 * LIGHT}/{@code STATELESS}), so there is no real spec that can reach them without fabricating a
 * transform name the registry would reject. Revisit once a real one exists.
 */
class ExecutionPlanCompilerTest {

  @Test
  void boundedFileSourcedSpecResolvesToPekkoStreams() {
    IngestionSpec spec = boundedSpec("file", "/tmp/source.csv");

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    assertEquals(DeliveryEngineKind.PEKKO_STREAMS, plan.profile().deliveryEngine());
    assertEquals(ExecutionMode.BOUNDED, plan.profile().executionMode());
    assertEquals(SourceCardinality.SINGLE, plan.profile().sourceCardinality());
    assertTrue(plan.sparkStage().isEmpty(), "no HEAVY transform - no Spark stage expected");
  }

  @Test
  void boundedKafkaSourcedSpecResolvesToKafkaStreams() {
    IngestionSpec spec = boundedSpec("kafka", "kafka:input-topic?brokers=localhost:9092");

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    assertEquals(
        DeliveryEngineKind.KAFKA_STREAMS,
        plan.profile().deliveryEngine(),
        "a kafka-sourced BOUNDED spec is the one real signal pointing at Kafka Streams");
  }

  @Test
  void continuousKafkaSourcedSpecStillDefaultsToPekkoStreams() {
    IngestionSpec spec =
        new IngestionSpec(
            List.of(singleSource("kafka", "kafka:input-topic?brokers=localhost:9092")),
            null,
            null,
            sink(),
            ExecutionMode.CONTINUOUS,
            null,
            null);

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    assertEquals(
        DeliveryEngineKind.PEKKO_STREAMS,
        plan.profile().deliveryEngine(),
        "CONTINUOUS mode's source is inherently kafka-shaped for either engine, so that signal"
            + " can't discriminate the way it does in BOUNDED mode - PEKKO_STREAMS is the honest"
            + " default until a STATEFUL transform forces KAFKA_STREAMS");
    assertEquals(ExecutionMode.CONTINUOUS, plan.profile().executionMode());
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

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    assertEquals(DeliveryEngineKind.PEKKO_STREAMS, plan.profile().deliveryEngine());
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
}
