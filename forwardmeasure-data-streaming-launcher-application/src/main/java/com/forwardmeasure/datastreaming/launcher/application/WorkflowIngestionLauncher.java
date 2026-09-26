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

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.openworkflow.execution.api.model.SubjectActor;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecution;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionStart;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Workflow mode: invokes ingestion by starting a real fowf workflow execution, through fowf's own
 * generated {@link WorkflowExecutionsApi} client SDK (Apache HttpClient, built from {@code
 * execution-management.openapi.yaml}) - no hand-rolled HTTP, no reimplementation of anything fowf
 * already does. This class owns nothing beyond the thin translation from this launcher's own
 * request/response shape to fowf's own {@code POST/GET /v1/executions[...]} contract; it never
 * touches Kubernetes (see {@link DirectIngestionLauncher} for the bypass form).
 *
 * <p>Deliberately does not build or configure the {@link WorkflowExecutionsApi}/{@code ApiClient}
 * itself - how a caller obtains a bearer token (Keycloak client-credentials, a forwarded user
 * token, etc.) and which fowf base URL to call are real, environment-specific decisions this
 * framework-agnostic module has no business making; a caller constructs and configures {@link
 * WorkflowExecutionsApi} (see its own {@code ApiClient.setBasePath}/{@code setBearerToken}) and
 * hands it to this class already to go.
 */
public final class WorkflowIngestionLauncher {

  private final WorkflowExecutionsApi executionsApi;
  private final AuthorizationService authorization;

  public WorkflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi, AuthorizationService authorization) {
    this.executionsApi = Objects.requireNonNull(executionsApi, "executionsApi");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  /**
   * Starts the execution. Returns fowf's own {@link WorkflowExecution} resource (admitted, not
   * completed). Authorized against {@code request.correlationId()} - fowf hasn't assigned a real
   * execution id yet at this point (see docs/fds-authorization-remediation-guide.md, added
   * 2026-09-14 to close a confirmed real caller-authorization gap).
   *
   * <p>{@code subjectActor} (wired 2026-09-26, per {@code forwardmeasure-openworkflow}'s own
   * docs/subject-actor-fei-adoption-handoff-2026-09-25.md) asserts which real human {@code actor}
   * this call is genuinely for - {@code actor} was already resolved and used for this class's own
   * authorization check above, but previously dropped after that, never reaching fowf itself. fowf
   * attributes every {@code GET .../history} event to the real authenticated caller (this
   * launcher's own service-account identity) automatically regardless; {@code subjectActor} is the
   * *additional*, opt-in assertion of the human on whose behalf that service account is acting -
   * fowf checks the caller holds {@code execution:assert-subject} before honoring it, fail-closed;
   * omitting it (or a caller lacking that grant) changes nothing about the call itself. {@link
   * ActiveOrganization} has no display-name field, so only {@code actorId} is ever populated -
   * {@code SubjectActor .displayName} stays unset, matching its own documented optionality.
   */
  public WorkflowExecution launch(WorkflowLaunchRequest request, ActiveOrganization actor)
      throws ApiException {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.workflowRun(request.correlationId()),
            AuthorizationAction.WORKFLOW_RUN_LAUNCH,
            request.correlationId(),
            Map.of()));
    WorkflowExecutionStart start =
        new WorkflowExecutionStart()
            .revisionId(request.revisionId())
            .input(request.input())
            .subjectActor(subjectActorFor(actor));
    return executionsApi.startWorkflowExecution(
        request.idempotencyKey(), request.correlationId(), start);
  }

  /** One-shot status check - fowf's own current {@link WorkflowExecution} resource for this id. */
  public WorkflowExecution observe(UUID executionId, ActiveOrganization actor) throws ApiException {
    String executionIdText = executionId.toString();
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.workflowRun(executionIdText),
            AuthorizationAction.WORKFLOW_RUN_READ,
            executionIdText,
            Map.of()));
    return executionsApi.getWorkflowExecution(executionId);
  }

  /**
   * Cancels the execution. {@code ifMatch} must be the execution's current {@code ETag}/version
   * (from a prior {@link #launch}/{@link #observe} call) - fowf's own optimistic-concurrency
   * contract, not something this class works around. {@code subjectActor} is always attached (see
   * {@link #launch} javadoc) - previously {@code control} was built only when {@code reason} was
   * non-null, which silently dropped subjectActor on a reasonless cancel.
   */
  public WorkflowExecution cancel(
      UUID executionId,
      String ifMatch,
      String correlationId,
      String reason,
      ActiveOrganization actor)
      throws ApiException {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            DataStreamingAuthorizationResources.workflowRun(executionId.toString()),
            AuthorizationAction.WORKFLOW_RUN_CANCEL,
            correlationId,
            Map.of()));
    WorkflowExecutionControl control =
        new WorkflowExecutionControl().reason(reason).subjectActor(subjectActorFor(actor));
    return executionsApi.cancelWorkflowExecution(ifMatch, correlationId, executionId, control);
  }

  private static SubjectActor subjectActorFor(ActiveOrganization actor) {
    return new SubjectActor().actorId(actor.actorId());
  }
}
