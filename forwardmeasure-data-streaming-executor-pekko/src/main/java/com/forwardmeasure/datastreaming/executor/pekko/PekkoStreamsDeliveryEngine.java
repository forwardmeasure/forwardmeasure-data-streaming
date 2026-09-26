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
package com.forwardmeasure.datastreaming.executor.pekko;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.executor.streaming.DeliveryEngine;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.apache.pekko.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real Pekko Streams {@link DeliveryEngine} - branches on {@link ExecutionMode} and delegates
 * to named, equally-discoverable runner classes (2026-09-25, mirroring {@code
 * KafkaStreamsDeliveryEngine}'s own bounded/continuous split): {@link
 * BoundedPekkoStreamsConsumerRunner} reconstructs an equivalent {@link IngestionSpec} from {@code
 * plan} and delegates to the existing, proven {@code PekkoIngestionRunner}/{@code
 * PekkoCorrelationRunner}; {@link ContinuousPekkoStreamsConsumerRunner} is real Kafka
 * commit-after-write via Pekko Connectors Kafka. {@link ContinuousPekkoStreamsCorrelationRunner}
 * (same day) closes the continuous-correlation gap this class used to reject outright - see its own
 * javadoc for the real, harder-than-Kafka-Streams mechanism (no native stateful join operator in
 * Pekko Streams, so it builds one).
 */
public final class PekkoStreamsDeliveryEngine implements DeliveryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoStreamsDeliveryEngine.class);

  private final ActorSystem system;

  public PekkoStreamsDeliveryEngine(ActorSystem system) {
    this.system = Objects.requireNonNull(system, "system");
  }

  @Override
  public ExecutionHandle execute(ExecutionPlan plan, ExecutionMode mode) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(mode, "mode");
    if (mode == ExecutionMode.CONTINUOUS) {
      if (plan.sparkStage().isPresent()) {
        throw new UnsupportedOperationException(
            "PekkoStreamsDeliveryEngine: a plan with a Spark compute stage is not dispatched here"
                + " yet - see this engine's own javadoc");
      }
      return plan.sources().size() == 1
          ? ContinuousPekkoStreamsConsumerRunner.run(plan, system)
          : ContinuousPekkoStreamsCorrelationRunner.run(plan, system);
    }
    return BoundedPekkoStreamsConsumerRunner.run(plan, mode, system);
  }

  /**
   * The real, deployable entrypoint for this engine's own container image - reads the same {@code
   * INGESTION_SPEC_PATH} convention {@link PekkoIngestionRunner#main} uses, compiles it, and
   * dispatches. For {@code BOUNDED} mode, {@link #execute} already blocks until the run completes
   * (see {@link BoundedPekkoStreamsConsumerRunner}), so {@code main()} exits as soon as it returns
   * - correct for a one-shot Kubernetes {@code Job}. For {@code CONTINUOUS} mode, {@link #execute}
   * returns immediately with a live handle (see {@link ContinuousPekkoStreamsConsumerRunner}) -
   * {@code main()} blocks on a shutdown hook instead, so the process stays up for a Kubernetes
   * {@code Deployment} and stops the running stream cleanly on {@code SIGTERM} rather than the JVM
   * just exiting underneath it.
   */
  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    ActorSystem system = ActorSystem.create("fds-pekko-streams-delivery-engine");
    int exitCode = 0;
    try {
      ExecutionHandle handle =
          new PekkoStreamsDeliveryEngine(system).execute(plan, spec.executionMode());
      if (handle.isRunning()) {
        awaitShutdown(handle);
      }
      LOGGER.info("PekkoStreamsDeliveryEngine: run completed, id={}", handle.id());
    } catch (Exception e) {
      LOGGER.error("PekkoStreamsDeliveryEngine: run failed", e);
      exitCode = 1;
    } finally {
      system.terminate();
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /**
   * Blocks {@code main()} until a {@code SIGTERM} (Kubernetes' own {@code Deployment} pod-eviction
   * signal) arrives, then stops {@code handle} before letting the JVM actually exit - the shutdown
   * hook is the only reliable place to do this cleanly, since a plain infinite sleep would leave no
   * chance to call {@link ExecutionHandle#stop()} at all.
   */
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
