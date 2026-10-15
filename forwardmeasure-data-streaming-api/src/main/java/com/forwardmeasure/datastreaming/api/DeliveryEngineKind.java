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

/**
 * The only two engines that ever own final write-to-sink, in every cell of the capability matrix
 * including bounded ones. Spark is deliberately not a member of this enum - it is an optional
 * compute stage that always hands off to one of these two, never a delivery engine itself. Never
 * author-specified and never chosen by the planner: an installation runs exactly one engine, the
 * platform's {@code engine} setting ({@code kafka-streams | pekko-postgresql | pekko-cassandra}),
 * read through {@link #fromPlatformEngine}.
 */
public enum DeliveryEngineKind {
  PEKKO_STREAMS,
  KAFKA_STREAMS;

  /** The delivery engine for the platform {@code engine} value. */
  public static DeliveryEngineKind fromPlatformEngine(String platformEngine) {
    if (platformEngine == null) {
      throw new IllegalArgumentException("the platform engine setting is required");
    }
    return switch (platformEngine.strip()) {
      case "kafka-streams" -> KAFKA_STREAMS;
      case "pekko-postgresql", "pekko-cassandra" -> PEKKO_STREAMS;
      default ->
          throw new IllegalArgumentException(
              "unknown platform engine '"
                  + platformEngine
                  + "': expected kafka-streams, pekko-postgresql or pekko-cassandra");
    };
  }
}
