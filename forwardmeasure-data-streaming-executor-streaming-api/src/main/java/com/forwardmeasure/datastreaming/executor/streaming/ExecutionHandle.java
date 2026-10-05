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

/**
 * One running {@link DeliveryEngine} execution, bounded or continuous. For {@code BOUNDED} runs,
 * {@link #isRunning()} turns {@code false} on its own once the run reaches a terminal state
 * (SUCCEEDED/FAILED); for {@code CONTINUOUS} runs it stays {@code true} until {@link #stop()} is
 * called. Deliberately smaller than the old {@code StreamingStageHandle} (no engine-specific health
 * shape baked in here) - a mode-agnostic handle, not a continuous-only one.
 */
public interface ExecutionHandle {
  String id();

  boolean isRunning();

  /** Terminal failure, when the engine exposes its cause. */
  default java.util.Optional<Throwable> failure() {
    return java.util.Optional.empty();
  }

  void stop();
}
