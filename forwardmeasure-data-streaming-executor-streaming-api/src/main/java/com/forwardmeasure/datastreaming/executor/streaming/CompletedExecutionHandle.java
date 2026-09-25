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

import java.util.Objects;

/**
 * A real {@link ExecutionHandle} for a {@code BOUNDED} run that has already finished by the time
 * {@link DeliveryEngine#execute} returns - matching every real bounded runner in this repo today
 * ({@code PekkoIngestionRunner#run}, {@code SparkIngestionRunner#run}), which blocks the calling
 * thread until the run completes (a {@code main()} calling one of these runs to completion, then
 * the JVM exits - real Kubernetes Job semantics, not a background execution a caller polls later).
 * Shared here (not duplicated per engine) since both {@code PekkoStreamsDeliveryEngine} and {@code
 * KafkaStreamsDeliveryEngine} need the identical "already done" shape for their own {@code BOUNDED}
 * branch.
 */
public final class CompletedExecutionHandle implements ExecutionHandle {

  private final String id;

  public CompletedExecutionHandle(String id) {
    this.id = Objects.requireNonNull(id, "id");
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public void stop() {
    // Already finished - nothing to stop.
  }
}
