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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

class LoaderHandoffSpecsTest {

  private static final TransformSpec MAPPER =
      new TransformSpec(
          "party",
          List.of(
              new TransformSpec.FieldRule(
                  "uid", "ID", null, null, "parse_tilde_list", null, null)));

  private static IngestionSpec spec(ExecutionMode mode, SourcePlan... sources) {
    return new IngestionSpec(
        List.of(sources),
        sources.length > 1 ? "uid" : null,
        null,
        new SinkSpec("opensearch", "http://search:9200", "party", null, null),
        mode,
        null,
        null);
  }

  private static SourcePlan file(String key) {
    return new SourcePlan(key, new SourceSpec("file", "/" + key + ".csv", null, null), MAPPER, 0.7);
  }

  private static SourcePlan kafka(String key) {
    return new SourcePlan(
        key, new SourceSpec("kafka", "kafka:" + key + "?brokers=b:9092", null, null), MAPPER, 0.3);
  }

  @Test
  void onlyABoundedKafkaStreamsPlanReadingANonKafkaSourceNeedsALoader() {
    var bounded = spec(ExecutionMode.BOUNDED, file("a"));
    assertTrue(
        LoaderHandoffSpecs.needsLoader(
            ExecutionPlanCompiler.compile(bounded, DeliveryEngineKind.KAFKA_STREAMS)));
    assertFalse(
        LoaderHandoffSpecs.needsLoader(
            ExecutionPlanCompiler.compile(bounded, DeliveryEngineKind.PEKKO_STREAMS)));
    assertFalse(
        LoaderHandoffSpecs.needsLoader(
            ExecutionPlanCompiler.compile(
                spec(ExecutionMode.BOUNDED, kafka("k")), DeliveryEngineKind.KAFKA_STREAMS)));
    assertFalse(
        LoaderHandoffSpecs.needsLoader(
            ExecutionPlanCompiler.compile(
                spec(ExecutionMode.CONTINUOUS, kafka("k")), DeliveryEngineKind.KAFKA_STREAMS)));
  }

  @Test
  void deliveryReadsLoadedSourcesFromTheirTopicsAndKeepsEverythingElse() {
    var original = spec(ExecutionMode.BOUNDED, file("a"), kafka("k"), file("b"));
    var plan = ExecutionPlanCompiler.compile(original, DeliveryEngineKind.KAFKA_STREAMS);

    var delivery = LoaderHandoffSpecs.deliveryStageSpec(plan, "broker:9092", "fds-load-x");

    assertEquals(3, delivery.sources().size());
    assertEquals(
        "kafka:fds-load-x-source-0?brokers=broker:9092", delivery.sources().get(0).source().uri());
    assertEquals(original.sources().get(1), delivery.sources().get(1));
    assertEquals(
        "kafka:fds-load-x-source-2?brokers=broker:9092", delivery.sources().get(2).source().uri());
    for (int i = 0; i < 3; i++) {
      assertEquals(original.sources().get(i).sourceKey(), delivery.sources().get(i).sourceKey());
      assertEquals(original.sources().get(i).mapper(), delivery.sources().get(i).mapper());
      assertEquals(
          original.sources().get(i).trustWeight(), delivery.sources().get(i).trustWeight());
    }
    assertEquals("uid", delivery.blockingField());
    assertEquals(original.sink(), delivery.sink());
    assertEquals(ExecutionMode.BOUNDED, delivery.executionMode());
    assertFalse(
        LoaderHandoffSpecs.needsLoader(
            ExecutionPlanCompiler.compile(delivery, DeliveryEngineKind.KAFKA_STREAMS)),
        "the delivery stage never loads again");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LoaderHandoffSpecs.deliveryStageSpec(
                ExecutionPlanCompiler.compile(original, DeliveryEngineKind.PEKKO_STREAMS),
                "broker:9092",
                "fds-load-x"));
  }
}
