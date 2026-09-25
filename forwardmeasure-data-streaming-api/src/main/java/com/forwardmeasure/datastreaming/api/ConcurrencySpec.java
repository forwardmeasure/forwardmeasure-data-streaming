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
 * Same shape as the old {@code ExecutionSpec.ConcurrencySpec}, promoted to a top-level type and
 * carried on {@link DeliverySemantics} now that engine selection is automatic rather than
 * author-specified (see the repo's own gap-bridging plan) - this isn't new policy, the field shape
 * and {@code IngestionPipeline.effectiveParallelism} semantics are unchanged.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConcurrencySpec(
    @JsonProperty("preferred") Integer preferred, @JsonProperty("maximum") Integer maximum)
    implements Serializable {}
