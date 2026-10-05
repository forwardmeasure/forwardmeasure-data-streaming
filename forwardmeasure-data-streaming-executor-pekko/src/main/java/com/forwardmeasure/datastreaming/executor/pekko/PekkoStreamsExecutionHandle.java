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

import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.Done;
import org.apache.pekko.kafka.javadsl.Consumer;

/**
 * Wraps one real, running Pekko Connectors Kafka graph (a real {@link CamelBridge} on the sink
 * side, live for as long as the graph runs) - {@code CONTINUOUS} mode only.
 *
 * <p>A real, honest limitation, not smoothed over: unlike Kafka Streams' own explicit state machine
 * ({@code KafkaStreams.State}), a running {@link Consumer.DrainingControl} can only report whether
 * its completion signal is still pending, not "is actively rebalanced and consuming" - {@link
 * #isRunning()} is a materially weaker claim than {@code KafkaStreamsExecutionHandle#isRunning()}'s
 * own (see this repo's own gap-bridging plan for the same asymmetry, previously documented on the
 * now-retired {@code PekkoStreamingStageHandle}).
 */
final class PekkoStreamsExecutionHandle implements ExecutionHandle {

  private final String id;
  private final Consumer.DrainingControl<Done> control;
  private final CamelBridge bridge;

  PekkoStreamsExecutionHandle(
      String id, Consumer.DrainingControl<Done> control, CamelBridge bridge) {
    this.id = Objects.requireNonNull(id, "id");
    this.control = Objects.requireNonNull(control, "control");
    this.bridge = Objects.requireNonNull(bridge, "bridge");
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public boolean isRunning() {
    return !control.streamCompletion().toCompletableFuture().isDone();
  }

  @Override
  public java.util.Optional<Throwable> failure() {
    var completion = control.streamCompletion().toCompletableFuture();
    return completion.isDone()
        ? java.util.Optional.ofNullable(completion.handle((done, error) -> error).join())
        : java.util.Optional.empty();
  }

  @Override
  public void stop() {
    try {
      control
          .drainAndShutdown(java.util.concurrent.ForkJoinPool.commonPool())
          .toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
    } catch (Exception ignored) {
      // Best-effort shutdown, mirroring the now-retired PekkoStreamingStageHandle#close.
    } finally {
      try {
        bridge.close();
      } catch (Exception ignored) {
        // Best-effort close - the stream is already stopped/stopping either way.
      }
    }
  }
}
