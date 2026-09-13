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

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import java.util.Map;
import java.util.Objects;

/**
 * A direct-mode launch: run {@code ingestionSpec} as one Kubernetes Job, bypassing fowf's workflow
 * engine entirely. {@code correlationId} is the caller's own idempotency key - the same id always
 * resolves to the same Job name ({@link DirectIngestionLauncher#deterministicJobName}), so a caller
 * can safely retry a launch call without risking a duplicate run.
 */
public record DirectLaunchRequest(
    String correlationId,
    String namespace,
    IngestionSpec ingestionSpec,
    Map<String, String> resourceRequests,
    Map<String, String> resourceLimits,
    Long activeDeadlineSeconds) {

  public DirectLaunchRequest {
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(namespace, "namespace");
    Objects.requireNonNull(ingestionSpec, "ingestionSpec");
    resourceRequests = resourceRequests == null ? Map.of() : Map.copyOf(resourceRequests);
    resourceLimits = resourceLimits == null ? Map.of() : Map.copyOf(resourceLimits);
  }
}
