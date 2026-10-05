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

import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import java.util.Objects;
import org.apache.kafka.streams.KafkaStreams;

/** Wraps one real, running {@link KafkaStreams} instance - {@code CONTINUOUS} mode only. */
final class KafkaStreamsExecutionHandle implements ExecutionHandle {

  private final String id;
  private final KafkaStreams streams;
  private final java.util.concurrent.atomic.AtomicReference<Throwable> failure =
      new java.util.concurrent.atomic.AtomicReference<>();

  KafkaStreamsExecutionHandle(String id, KafkaStreams streams) {
    this.id = Objects.requireNonNull(id, "id");
    this.streams = Objects.requireNonNull(streams, "streams");
    streams.setUncaughtExceptionHandler(
        error -> {
          failure.compareAndSet(null, error);
          return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
              .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public boolean isRunning() {
    KafkaStreams.State state = streams.state();
    return state != KafkaStreams.State.NOT_RUNNING && state != KafkaStreams.State.ERROR;
  }

  @Override
  public java.util.Optional<Throwable> failure() {
    return java.util.Optional.ofNullable(failure.get());
  }

  @Override
  public void stop() {
    streams.close();
  }
}
