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
 * The resolved identity of one compiled {@code ExecutionPlan} - the 2x2x2 cube's own coordinates
 * (see the repo's own gap-bridging plan). Not something an {@code IngestionSpec} author writes
 * directly: {@code sourceCardinality} is derived from {@code sources.size()}, {@code
 * deliveryEngine} is resolved by the planner from the transform graph's own declared {@link
 * TransformCharacteristics}, and only {@code executionMode} stays author-declared.
 */
public record ExecutionProfile(
    SourceCardinality sourceCardinality,
    ExecutionMode executionMode,
    DeliveryEngineKind deliveryEngine) {

  public ExecutionProfile {
    Objects.requireNonNull(sourceCardinality, "sourceCardinality");
    Objects.requireNonNull(executionMode, "executionMode");
    Objects.requireNonNull(deliveryEngine, "deliveryEngine");
  }
}
