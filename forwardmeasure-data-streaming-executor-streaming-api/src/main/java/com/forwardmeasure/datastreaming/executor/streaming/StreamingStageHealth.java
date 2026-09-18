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
package com.forwardmeasure.datastreaming.executor.streaming;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * One running stage's own liveness/readiness report - one instance per {@link
 * StreamingStageHandle}, not per provider: unlike fowf's execution engines (many concurrent
 * executions behind one provider-wide health check), each stage here is a single long-running
 * topology whose health is independently meaningful.
 */
public record StreamingStageHealth(
    HealthState state,
    boolean live,
    boolean ready,
    Instant observedAt,
    Map<String, String> details) {

  public StreamingStageHealth {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(observedAt, "observedAt");
    details = Map.copyOf(Objects.requireNonNull(details, "details"));
    if (ready && !live) {
      throw new IllegalArgumentException("a ready stage must be live");
    }
    if (state == HealthState.UP && (!live || !ready)) {
      throw new IllegalArgumentException("UP requires live and ready");
    }
  }

  public enum HealthState {
    UP,
    DEGRADED,
    DOWN
  }
}
