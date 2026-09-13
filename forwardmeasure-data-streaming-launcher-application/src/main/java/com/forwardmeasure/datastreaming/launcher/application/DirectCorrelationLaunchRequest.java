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
package com.forwardmeasure.datastreaming.launcher.application;

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import java.util.Map;
import java.util.Objects;

/**
 * Direct mode's Spark-correlation counterpart to {@link DirectLaunchRequest}: run a {@link
 * CorrelationSpec} (several sources, correlated on a blocking key) as one real Kubernetes Job via
 * {@link DirectCorrelationLauncher}, bypassing fowf entirely - the same bypass form {@link
 * DirectLaunchRequest}/{@link DirectIngestionLauncher} already provide for the single-source Pekko
 * path, added 2026-09-13 once {@code CorrelationSpec} and a real Spark runner entrypoint both
 * existed to launch (see the design plan's own live gap tracker for why neither did before).
 */
public record DirectCorrelationLaunchRequest(
    String correlationId,
    String namespace,
    CorrelationSpec correlationSpec,
    Map<String, String> resourceRequests,
    Map<String, String> resourceLimits,
    Long activeDeadlineSeconds) {

  public DirectCorrelationLaunchRequest {
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(namespace, "namespace");
    Objects.requireNonNull(correlationSpec, "correlationSpec");
    resourceRequests = resourceRequests == null ? Map.of() : Map.copyOf(resourceRequests);
    resourceLimits = resourceLimits == null ? Map.of() : Map.copyOf(resourceLimits);
  }
}
