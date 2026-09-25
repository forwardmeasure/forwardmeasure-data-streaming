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
package com.forwardmeasure.datastreaming.executor.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.ExecutionProfile;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceCardinality;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.SparkStagePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link SparkIngestionRunner#run} genuinely honors {@code
 * execution.failure} on the Spark engine, mirroring {@code PekkoExecutionWiringIntegrationTest}'s
 * own Pekko-side proof. Same real finding applies here too: every real {@code
 * NamedTransformRegistry} function is defensive (never throws on bad data), so a genuinely
 * malformed row in this system is a spec-level mistake (an unregistered transform name), uniform
 * across every row - not per-row bad data.
 *
 * <p><b>Retired 2026-09-21</b>: this class used to also prove {@code sinkFailure: retry} eventually
 * succeeding against a real flaky HTTP server backing the old {@code opensearch} sink's
 * per-document write loop - deleted along with that sink (see {@link SparkSinks}' own javadoc). The
 * retry *mechanism* itself ({@code IngestionPipeline#withSinkFailureHandlingBlocking}) is already
 * proven generically by {@code IngestionPipelineTest} in {@code
 * forwardmeasure-data-streaming-core}, independent of any one connector; {@code writeKafka}'s
 * single whole-batch {@code .save()} call has no natural per-attempt flakiness to inject against a
 * real broker the way the old per-document loop did, so re-proving retry-specifically-on-Spark here
 * would need real chaos engineering (pausing/resuming the broker container) for a mechanism already
 * covered - not built, flagged here rather than silently dropped. {@code sinkFailure: fail}
 * propagation is still proven below, now against an unreachable Kafka broker instead of Postgres.
 */
@WithKafkaContainer
class SparkExecutionWiringIntegrationTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-execution-wiring-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void malformedRecordSkipLetsTheRunCompleteWithNothingWritten(
      @TempDir Path tempDir, KafkaTestContainer kafka) throws Exception {
    String brokers = kafka.bootstrapServers();
    ExecutionPlan plan = planWithUnregisteredTransform(tempDir, "skip", brokers);

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, plan, brokers);

    assertEquals(0, result.recordsWritten(), "every row hits the same unregistered transform");
  }

  @Test
  void malformedRecordFailFailsTheWholeRun(@TempDir Path tempDir, KafkaTestContainer kafka)
      throws Exception {
    String brokers = kafka.bootstrapServers();
    ExecutionPlan plan = planWithUnregisteredTransform(tempDir, "fail", brokers);

    assertThrows(Exception.class, () -> SparkIngestionRunner.run(spark, plan, brokers));
  }

  @Test
  void sinkFailureFailPropagatesARealWriteFailureRatherThanSilentlySucceeding(@TempDir Path tempDir)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\n", StandardCharsets.UTF_8);

    ExecutionPlan plan =
        new ExecutionPlan(
            new ExecutionProfile(
                SourceCardinality.SINGLE, ExecutionMode.BOUNDED, DeliveryEngineKind.PEKKO_STREAMS),
            List.of(
                new SourcePlan(
                    "single",
                    new SourceSpec("file", sourceCsv.toString(), null, null),
                    new TransformSpec(
                        "party",
                        List.of(
                            new TransformSpec.FieldRule(
                                "uid", "ID", null, null, null, null, null))),
                    1.0)),
            null,
            Optional.of(
                new SparkStagePlan(List.of("test-heavy-transform"), "unreachable-handoff-topic")),
            null,
            new SinkSpec("opensearch", "http://unused", "unused", null, null, java.util.Map.of()),
            new DeliverySemantics(true, null, null),
            new ErrorPolicy(null, "fail"));

    // No real Kafka broker listening on this port - a real, deterministic connection failure.
    assertThrows(Exception.class, () -> SparkIngestionRunner.run(spark, plan, "127.0.0.1:1"));
  }

  private static ExecutionPlan planWithUnregisteredTransform(
      Path tempDir, String malformedRecord, String brokers) throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);
    String handoffTopic = "fds-spark-execution-wiring-test-" + UUID.randomUUID();

    return new ExecutionPlan(
        new ExecutionProfile(
            SourceCardinality.SINGLE, ExecutionMode.BOUNDED, DeliveryEngineKind.PEKKO_STREAMS),
        List.of(
            new SourcePlan(
                "single",
                new SourceSpec("file", sourceCsv.toString(), null, null),
                new TransformSpec(
                    "party",
                    List.of(
                        new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                        new TransformSpec.FieldRule(
                            "name",
                            "FULL_NAME",
                            null,
                            null,
                            "this_transform_was_never_registered",
                            null,
                            null))),
                1.0)),
        null,
        Optional.of(new SparkStagePlan(List.of("test-heavy-transform"), handoffTopic)),
        null,
        new SinkSpec("opensearch", "http://unused", "unused", null, null, java.util.Map.of()),
        new DeliverySemantics(true, null, null),
        new ErrorPolicy(malformedRecord, null));
  }
}
