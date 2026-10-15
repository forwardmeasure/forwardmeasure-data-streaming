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
package com.forwardmeasure.datastreaming.core;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The two-stage shape of a bounded load on a Kafka Streams installation: a loader stage writes each
 * non-Kafka source's rows, unmapped, to its own per-run topic, and the Kafka Streams delivery stage
 * then reads those topics with the original mappers, correlation and sink. Kafka sources are read
 * where they are. Mirrors {@link SparkHandoffSpecs}, the other first stage the launcher sequences.
 */
public final class LoaderHandoffSpecs {

  private static final String KAFKA_CONNECTOR = "kafka";

  private LoaderHandoffSpecs() {}

  /**
   * Whether {@code plan} needs a loader stage: a bounded plan on Kafka Streams, with no Spark stage
   * (which already hands off through Kafka), reading at least one non-Kafka source.
   */
  public static boolean needsLoader(ExecutionPlan plan) {
    return plan.profile().deliveryEngine() == DeliveryEngineKind.KAFKA_STREAMS
        && plan.profile().executionMode() == ExecutionMode.BOUNDED
        && plan.sparkStage().isEmpty()
        && plan.sources().stream().anyMatch(LoaderHandoffSpecs::needsLoading);
  }

  /** Whether {@code source} is read by the loader (anything not already on Kafka). */
  public static boolean needsLoading(SourcePlan source) {
    return !KAFKA_CONNECTOR.equals(source.source().connector());
  }

  /** The topic the loader writes source {@code sourceIndex}'s rows to. */
  public static String sourceTopic(String loadTopic, int sourceIndex) {
    return loadTopic + "-source-" + sourceIndex;
  }

  /** The delivery stage's spec: each loaded source replaced by a Kafka source on its topic. */
  public static IngestionSpec deliveryStageSpec(
      ExecutionPlan plan, String kafkaBootstrapServers, String loadTopic) {
    if (!needsLoader(plan)) {
      throw new IllegalArgumentException("LoaderHandoffSpecs: plan has nothing to load");
    }
    List<SourcePlan> sources = new ArrayList<>();
    for (int index = 0; index < plan.sources().size(); index++) {
      SourcePlan original = plan.sources().get(index);
      if (!needsLoading(original)) {
        sources.add(original);
        continue;
      }
      SourceSpec kafka =
          new SourceSpec(
              KAFKA_CONNECTOR,
              "kafka:" + sourceTopic(loadTopic, index) + "?brokers=" + kafkaBootstrapServers,
              null,
              null,
              null,
              Map.of());
      sources.add(
          new SourcePlan(original.sourceKey(), kafka, original.mapper(), original.trustWeight()));
    }
    return new IngestionSpec(
        sources,
        plan.blockingField(),
        plan.transforms(),
        plan.destination(),
        ExecutionMode.BOUNDED,
        plan.delivery(),
        plan.errors());
  }
}
