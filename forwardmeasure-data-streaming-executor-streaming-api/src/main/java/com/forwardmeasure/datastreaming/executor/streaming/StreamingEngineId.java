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
 * Stable identity for a {@link StreamingStageRunnerProvider} implementation. Deliberately not a
 * reuse of fowf's own {@code EngineId} ({@code openworkflow-engine-api}): that module pulls in
 * fowf's whole workflow-definition DSL as a transitive dependency for what is otherwise a two-field
 * kebab-case identifier value type - not worth the coupling here.
 */
public record StreamingEngineId(String value) implements Comparable<StreamingEngineId> {
  public static final StreamingEngineId KAFKA_STREAMS = new StreamingEngineId("kafka-streams");
  public static final StreamingEngineId PEKKO = new StreamingEngineId("pekko");

  public StreamingEngineId {
    Objects.requireNonNull(value, "value");
    if (!value.matches("[a-z][a-z0-9-]{0,62}")) {
      throw new IllegalArgumentException("engine id must be a lowercase kebab-case identifier");
    }
  }

  @Override
  public int compareTo(StreamingEngineId other) {
    return value.compareTo(other.value);
  }
}
