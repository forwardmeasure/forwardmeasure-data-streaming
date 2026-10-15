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
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The planner sees test-fixtures' HEAVY transform through the provider SPI, like a product's. */
class TestFixturePlanningTest {

  @Test
  void boundedFileSpecWithScreeningCompilesToAPlanWithARealSparkStage() {
    IngestionSpec spec =
        TestCustomerMasterFixtures.boundedFileSpecWithScreening(
            "file:/tmp/test-customer-master.csv",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    assertTrue(
        spec.sources().get(0).mapper().fields().stream()
            .anyMatch(f -> "screening_hits".equals(f.target())),
        "expected the real screening_hits field rule to survive parsing");

    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.PEKKO_STREAMS);

    assertTrue(
        plan.sparkStage().isPresent(),
        "test_screen_against_reference is HEAVY - the compiler must insert a real"
            + " SparkStagePlan, not silently ignore it");
  }

  @Test
  void boundedKafkaSpecWithScreeningCompilesToAPlanWithARealSparkStage() {
    IngestionSpec spec =
        TestCustomerMasterFixtures.boundedKafkaSpecWithScreening(
            "kafka:9092",
            "test-customer-master-rows",
            "http://opensearch:9200",
            Path.of("/tmp/index.json"));
    assertEquals("kafka", spec.sources().get(0).source().connector());
    assertEquals(
        "kafka:9092", spec.sources().get(0).source().options().get("kafka.bootstrap.servers"));
    assertEquals(
        "test-customer-master-rows", spec.sources().get(0).source().options().get("subscribe"));
    assertTrue(
        ExecutionPlanCompiler.compile(spec, DeliveryEngineKind.PEKKO_STREAMS)
            .sparkStage()
            .isPresent());
  }
}
