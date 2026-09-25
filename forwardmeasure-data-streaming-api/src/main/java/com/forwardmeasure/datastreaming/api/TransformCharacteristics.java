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
package com.forwardmeasure.datastreaming.api;

import java.util.Objects;

/**
 * What a named transform declares about itself, so the planner can resolve {@link
 * DeliveryEngineKind} and whether a {@link SparkStagePlan} is needed automatically - the author
 * names a transform, never an engine (see the repo's own gap-bridging plan, "the planner" section).
 * No engine-selection table is implemented against this yet; this type exists so one can be built
 * without changing the declarative surface later.
 */
public record TransformCharacteristics(
    Cardinality cardinality,
    StateRequirement state,
    ExecutionCost cost,
    boolean orderSensitive,
    boolean deterministic) {

  public TransformCharacteristics {
    Objects.requireNonNull(cardinality, "cardinality");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(cost, "cost");
  }

  /** Whether this transform reads one row, a whole dataset, or needs more than one source. */
  public enum Cardinality {
    ONE,
    DATASET,
    CORRELATED
  }

  /**
   * Stateless transforms never force {@link DeliveryEngineKind#KAFKA_STREAMS}; stateful ones do.
   */
  public enum StateRequirement {
    STATELESS,
    STATEFUL
  }

  /** {@code HEAVY} is the signal the planner uses to insert a {@link SparkStagePlan}. */
  public enum ExecutionCost {
    LIGHT,
    HEAVY
  }
}
