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
package com.forwardmeasure.datastreaming.executor.kafkastreams;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.executor.streaming.CompletedExecutionHandle;
import com.forwardmeasure.datastreaming.executor.streaming.DeliveryEngine;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SinkRowWriter;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real Kafka Streams {@link DeliveryEngine} - collapses the old {@code
 * KafkaStreamsStageRunnerProvider} (continuous-only, topic-in/topic-out) into an engine that
 * branches on {@link ExecutionMode} and delegates to one of two named, equally-discoverable runner
 * classes (2026-09-25, mirroring {@code PekkoStreamsDeliveryEngine}'s own bounded/continuous
 * split): {@link ContinuousKafkaStreamsConsumerRunner} keeps the same real Kafka Streams topology
 * (same {@link FieldMappingEngine} mapping, {@code EXACTLY_ONCE_V2} default) terminating in a real
 * sink write via {@link SinkRowWriter}; {@link BoundedKafkaStreamsConsumerRunner} is a plain
 * consumer/producer poll loop against a real {@link InputFrontier}, since the Kafka Streams DSL
 * runtime itself has no "stop at these offsets" concept.
 *
 * <p><b>Correlation</b>: both modes now support {@code sources.size() > 1} - {@code BOUNDED} via
 * {@link BoundedKafkaStreamsCorrelationRunner} (2026-09-25), {@code CONTINUOUS} via {@link
 * ContinuousKafkaStreamsCorrelationRunner} (same day, real native {@code KTable} outer joins, not a
 * poll loop - see that class's own javadoc for the full mechanism and why it accommodates any
 * number of sources, not just two). Both use the same real merge semantics {@code
 * PekkoCorrelationEngine}/{@code SparkCorrelationEngine} already use.
 *
 * <p>Deliberately still scoped to no-Spark-stage plans - correlated/Spark-staged dispatch through
 * this engine remains real future work, not silently unsupported.
 */
public final class KafkaStreamsDeliveryEngine implements DeliveryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsDeliveryEngine.class);

  private final Map<String, Object> streamsConfigOverrides;
  private final FieldMappingEngine mapper = new FieldMappingEngine();

  public KafkaStreamsDeliveryEngine(Map<String, Object> streamsConfigOverrides) {
    this.streamsConfigOverrides =
        Map.copyOf(Objects.requireNonNull(streamsConfigOverrides, "streamsConfigOverrides"));
  }

  @Override
  public ExecutionHandle execute(ExecutionPlan plan, ExecutionMode mode) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(mode, "mode");
    if (plan.sparkStage().isPresent()) {
      throw new UnsupportedOperationException(
          "KafkaStreamsDeliveryEngine: a plan with a Spark compute stage is not dispatched here"
              + " yet - see this engine's own javadoc");
    }
    return mode == ExecutionMode.CONTINUOUS ? executeContinuous(plan) : executeBounded(plan);
  }

  private ExecutionHandle executeBounded(ExecutionPlan plan) {
    String id = "fds-kafka-streams-bounded-" + UUID.randomUUID();
    if (plan.sources().size() == 1) {
      BoundedKafkaStreamsConsumerRunner.run(
          plan.sources().get(0), plan.destination(), plan.errors(), mapper);
    } else {
      BoundedKafkaStreamsCorrelationRunner.run(
          plan.sources(), plan.blockingField(), plan.destination(), plan.errors(), mapper);
    }
    return new CompletedExecutionHandle(id);
  }

  private ExecutionHandle executeContinuous(ExecutionPlan plan) {
    return plan.sources().size() == 1
        ? new ContinuousKafkaStreamsConsumerRunner(streamsConfigOverrides).run(plan)
        : new ContinuousKafkaStreamsCorrelationRunner(streamsConfigOverrides).run(plan);
  }

  /**
   * The real, deployable entrypoint for this engine's own container image - no such image has ever
   * existed before this class (unlike the Pekko side, which reused {@code PekkoIngestionRunner}'s
   * own long-standing entrypoint). Same {@code INGESTION_SPEC_PATH} convention, same
   * bounded-exits/continuous-blocks shape as {@code PekkoStreamsDeliveryEngine#main} - see that
   * class's own javadoc for why.
   */
  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    int exitCode = 0;
    try {
      ExecutionHandle handle =
          new KafkaStreamsDeliveryEngine(Map.of()).execute(plan, spec.executionMode());
      if (handle.isRunning()) {
        awaitShutdown(handle);
      }
      LOGGER.info("KafkaStreamsDeliveryEngine: run completed, id={}", handle.id());
    } catch (Exception e) {
      LOGGER.error("KafkaStreamsDeliveryEngine: run failed", e);
      exitCode = 1;
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  private static void awaitShutdown(ExecutionHandle handle) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  handle.stop();
                  latch.countDown();
                }));
    latch.await();
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
