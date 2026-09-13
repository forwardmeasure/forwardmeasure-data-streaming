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

import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import java.util.Objects;
import java.util.UUID;

/**
 * Workflow mode: invokes ingestion by starting a real fowf workflow execution, through fowf's own
 * generated {@link ExecutionsApi} client SDK (Apache HttpClient, built from {@code
 * execution-management.openapi.yaml}) - no hand-rolled HTTP, no reimplementation of anything fowf
 * already does. This class owns nothing beyond the thin translation from this launcher's own
 * request/response shape to fowf's own {@code POST/GET /v1/executions[...]} contract; it never
 * touches Kubernetes (see {@link DirectIngestionLauncher} for the bypass form).
 *
 * <p>Deliberately does not build or configure the {@link ExecutionsApi}/{@code ApiClient} itself -
 * how a caller obtains a bearer token (Keycloak client-credentials, a forwarded user token, etc.)
 * and which fowf base URL to call are real, environment-specific decisions this framework-agnostic
 * module has no business making; a caller constructs and configures {@link ExecutionsApi} (see its
 * own {@code ApiClient.setBasePath}/{@code setBearerToken}) and hands it to this class already to
 * go.
 */
public final class WorkflowIngestionLauncher {

  private final ExecutionsApi executionsApi;

  public WorkflowIngestionLauncher(ExecutionsApi executionsApi) {
    this.executionsApi = Objects.requireNonNull(executionsApi, "executionsApi");
  }

  /**
   * Starts the execution. Returns fowf's own {@link Execution} resource (admitted, not completed).
   */
  public Execution launch(WorkflowLaunchRequest request) throws ApiException {
    ExecutionStart start =
        new ExecutionStart().revisionId(request.revisionId()).input(request.input());
    return executionsApi.startExecution(request.idempotencyKey(), request.correlationId(), start);
  }

  /** One-shot status check - fowf's own current {@link Execution} resource for this id. */
  public Execution observe(UUID executionId) throws ApiException {
    return executionsApi.getExecution(executionId);
  }

  /**
   * Cancels the execution. {@code ifMatch} must be the execution's current {@code ETag}/version
   * (from a prior {@link #launch}/{@link #observe} call) - fowf's own optimistic-concurrency
   * contract, not something this class works around.
   */
  public Execution cancel(UUID executionId, String ifMatch, String correlationId, String reason)
      throws ApiException {
    ExecutionControl control = reason == null ? null : new ExecutionControl().reason(reason);
    return executionsApi.cancelExecution(ifMatch, correlationId, executionId, control);
  }
}
