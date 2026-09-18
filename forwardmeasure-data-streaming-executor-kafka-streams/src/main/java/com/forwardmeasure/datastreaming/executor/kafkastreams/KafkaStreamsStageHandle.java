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

import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageHandle;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageHealth;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageHealth.HealthState;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;

/** Wraps one real, running {@link KafkaStreams} instance for one stage. */
final class KafkaStreamsStageHandle implements StreamingStageHandle {
  private final String stageName;
  private final KafkaStreams streams;

  KafkaStreamsStageHandle(String stageName, KafkaStreams streams) {
    this.stageName = stageName;
    this.streams = streams;
  }

  @Override
  public String stageName() {
    return stageName;
  }

  @Override
  public StreamingStageHealth health() {
    KafkaStreams.State state = streams.state();
    boolean live = state != KafkaStreams.State.NOT_RUNNING && state != KafkaStreams.State.ERROR;
    boolean ready = state == KafkaStreams.State.RUNNING;
    HealthState healthState =
        ready ? HealthState.UP : live ? HealthState.DEGRADED : HealthState.DOWN;
    return new StreamingStageHealth(
        healthState, live, ready, Instant.now(), Map.of("kafkaStreamsState", state.name()));
  }

  @Override
  public void close() {
    streams.close();
  }
}
