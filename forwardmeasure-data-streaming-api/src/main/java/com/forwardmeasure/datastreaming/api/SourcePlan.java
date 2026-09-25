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
 * One source in an {@link ExecutionPlan}'s {@code sources} list - one element means {@link
 * SourceCardinality#SINGLE}, more than one means {@link SourceCardinality#CORRELATED}. Same shape
 * as {@code CorrelationSpec.SourceEntry} (field-for-field), promoted to a top-level type now that
 * one list serves both cardinalities instead of two separate spec types (see the repo's own
 * gap-bridging plan, "a single IngestionSpec, not four types").
 */
public record SourcePlan(
    String sourceKey, SourceSpec source, TransformSpec mapper, double trustWeight) {

  public SourcePlan {
    Objects.requireNonNull(sourceKey, "sourceKey");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(mapper, "mapper");
  }
}
