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

import java.util.List;
import java.util.Objects;

/**
 * Present in an {@link ExecutionPlan} only when the planner decided at least one transform is
 * {@link TransformCharacteristics.ExecutionCost#HEAVY} or {@link
 * TransformCharacteristics.Cardinality#CORRELATED} at scale. Never a delivery engine - always hands
 * off to whichever {@link DeliveryEngineKind} the plan selected via {@code handoffTopic}, a real
 * Kafka topic (see the repo's own gap-bridging plan, "Spark: an optional distributed compute
 * stage") - {@code SparkSinks#writeKafka} is the only sink-write path a Spark stage ever uses; it
 * never writes to {@link ExecutionPlan#destination()} directly.
 *
 * <p>{@code graphSteps} is how many of the plan's transform-graph steps, from the entry step, run
 * in the Spark stage: through the last step that uses a {@code HEAVY} transform, so every such step
 * runs at Spark scale; zero when only the sources' mappers need Spark. The delivery engine runs the
 * steps after them.
 */
public record SparkStagePlan(List<String> transformNames, String handoffTopic, int graphSteps) {

  public SparkStagePlan {
    transformNames = transformNames == null ? List.of() : List.copyOf(transformNames);
    if (transformNames.isEmpty()) {
      throw new IllegalArgumentException("A Spark stage must run at least one transform");
    }
    Objects.requireNonNull(handoffTopic, "handoffTopic");
    if (handoffTopic.isBlank()) {
      throw new IllegalArgumentException("handoffTopic must not be blank");
    }
    if (graphSteps < 0) {
      throw new IllegalArgumentException("graphSteps must not be negative");
    }
  }

  /** A Spark stage that runs only the sources' mappers. */
  public SparkStagePlan(List<String> transformNames, String handoffTopic) {
    this(transformNames, handoffTopic, 0);
  }

  /** The same stage, handing off to {@code topic}. */
  public SparkStagePlan withHandoffTopic(String topic) {
    return new SparkStagePlan(transformNames, topic, graphSteps);
  }
}
