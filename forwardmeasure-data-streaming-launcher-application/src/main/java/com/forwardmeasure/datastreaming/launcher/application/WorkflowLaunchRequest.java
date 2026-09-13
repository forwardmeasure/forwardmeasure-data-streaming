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

import java.util.Objects;
import java.util.UUID;

/**
 * A workflow-mode launch: start an execution of an already-published fowf {@code
 * WorkflowDefinition} revision, through fowf's own {@code POST /v1/executions} - this launcher
 * never talks Kubernetes directly in this mode; whatever the workflow definition's own {@code
 * correlated-worker}/{@code kubernetes-job} steps do is entirely fowf's concern (the same real
 * {@code AsyncApiKubernetesJobOperationExecutor} this project's own direct mode shares its
 * Job-lifecycle library with).
 *
 * <p>{@code revisionId} must be a real, published, non-deprecated {@code WorkflowDefinition}
 * revision id - fowf's own API rejects anything else. {@code input} is that workflow's own,
 * definition-specific input document (e.g. an {@code IngestionSpec} rendered to a plain {@code
 * Map<String,Object>}, if the target definition expects one) - this class has no opinion on its
 * shape. {@code idempotencyKey} and {@code correlationId} map directly to fowf's own required
 * {@code Idempotency-Key}/{@code X-Correlation-ID} headers.
 */
public record WorkflowLaunchRequest(
    UUID revisionId, Object input, String idempotencyKey, String correlationId) {

  public WorkflowLaunchRequest {
    Objects.requireNonNull(revisionId, "revisionId");
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey");
    Objects.requireNonNull(correlationId, "correlationId");
  }
}
