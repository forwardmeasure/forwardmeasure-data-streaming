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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * How a run executes: which engine ({@code pekko} or {@code spark} - {@code kafka-streams}
 * intentionally not offered yet, see D4), its concurrency bounds, backpressure/flow-control limits,
 * and failure handling per record/sink outcome.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ExecutionSpec(
    @JsonProperty("engine") String engine,
    @JsonProperty("concurrency") ConcurrencySpec concurrency,
    @JsonProperty("flowControl") FlowControlSpec flowControl,
    @JsonProperty("failure") FailureSpec failure)
    implements Serializable {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ConcurrencySpec(
      @JsonProperty("preferred") Integer preferred, @JsonProperty("maximum") Integer maximum)
      implements Serializable {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FlowControlSpec(@JsonProperty("maxInFlightRecords") Integer maxInFlightRecords)
      implements Serializable {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FailureSpec(
      @JsonProperty("malformedRecord") String malformedRecord,
      @JsonProperty("sinkFailure") String sinkFailure)
      implements Serializable {}
}
