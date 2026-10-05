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
import java.util.Optional;

/**
 * The compiled result a planner produces from one {@code IngestionSpec} - not a second
 * author-facing spec (see the repo's own gap-bridging plan, "a single IngestionSpec, not four
 * types"). {@code sources.size()} determines {@link ExecutionProfile#sourceCardinality}; {@code
 * sparkStage} is present only when the planner decided at least one transform needs it; {@code
 * destination} deliberately reuses {@link SinkSpec} directly rather than a near-empty wrapper type,
 * since nothing yet needs more than {@code SinkSpec} already carries.
 *
 * <p>{@code transforms} is nullable, mirroring {@link IngestionSpec#transforms()} - the single most
 * common real case (a spec with no further stage graph beyond each source's own field mapping) has
 * nothing here to compile into a graph; the planner does not invent one.
 *
 * <p>{@code blockingField} carries {@link IngestionSpec#blockingField()} through unchanged (added
 * 2026-09-21) - a correlated plan's own delivery engine (today: {@code PekkoStreamsDeliveryEngine},
 * via the real, existing {@code PekkoCorrelationRunner}) needs it to actually correlate rows; only
 * {@code null} for a single-source plan, matching {@code IngestionSpec}'s own convention exactly.
 * Every field an {@code IngestionSpec} carries is present here in some form, so a {@code
 * DeliveryEngine} can always reconstruct an equivalent {@code IngestionSpec} from a plan when its
 * own bounded logic was built against that type first (see {@code PekkoStreamsDeliveryEngine}'s own
 * javadoc for why that reconstruction is a legitimate reuse of already-proven code, not a hack).
 */
public record ExecutionPlan(
    ExecutionProfile profile,
    List<SourcePlan> sources,
    String blockingField,
    Optional<SparkStagePlan> sparkStage,
    TransformGraph transforms,
    SinkSpec destination,
    DeliverySemantics delivery,
    ErrorPolicy errors,
    MergePolicy mergePolicy) {

  public ExecutionPlan(
      ExecutionProfile profile,
      List<SourcePlan> sources,
      String blockingField,
      Optional<SparkStagePlan> sparkStage,
      TransformGraph transforms,
      SinkSpec destination,
      DeliverySemantics delivery,
      ErrorPolicy errors) {
    this(
        profile,
        sources,
        blockingField,
        sparkStage,
        transforms,
        destination,
        delivery,
        errors,
        MergePolicy.defaults());
  }

  public ExecutionPlan {
    if (transforms != null) {
      throw new IllegalArgumentException(
          "TransformGraph execution is not supported: declare executable field transforms in each"
              + " source mapper");
    }
    mergePolicy = mergePolicy == null ? MergePolicy.defaults() : mergePolicy;
    Objects.requireNonNull(profile, "profile");
    sources = sources == null ? List.of() : List.copyOf(sources);
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("An ExecutionPlan must have at least one source");
    }
    boolean correlated = sources.size() > 1;
    if (correlated != (profile.sourceCardinality() == SourceCardinality.CORRELATED)) {
      throw new IllegalArgumentException(
          "sources.size() must agree with profile.sourceCardinality()");
    }
    sparkStage = sparkStage == null ? Optional.empty() : sparkStage;
    Objects.requireNonNull(destination, "destination");
    delivery = delivery == null ? DeliverySemantics.defaults() : delivery;
  }
}
