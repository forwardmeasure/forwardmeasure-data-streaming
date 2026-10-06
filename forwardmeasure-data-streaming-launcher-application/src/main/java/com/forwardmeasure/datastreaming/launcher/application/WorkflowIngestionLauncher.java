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

  private final java.util.function.Function<
          com.forwardmeasure.jpa.tenancy.TenantId, WorkflowExecutionsApi>
      executionsApis;
  private final AuthorizationService authorization;
  private final boolean assertSubjectActor;
  private final IngestionLaunchPlanner planner;

  /**
   * Defaults {@code assertSubjectActor} to {@code false} - see the 3-arg constructor's own javadoc
   * for why {@code false} is the only safe default today. Every one of this class's own 9 real
   * production call sites (fei's/FDS's own Quarkus/Spring/Micronaut framework bindings) uses this
   * constructor - none of them need to change to pick up the 2026-09-26 fix below.
   */
  public WorkflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi, AuthorizationService authorization) {
    this(executionsApi, authorization, false);
  }

  /**
   * {@code assertSubjectActor} - added 2026-09-26 as a genuine, real bug fix, not a speculative
   * safeguard: this class used to attach {@code subjectActor} to every real {@link #launch}/{@link
   * #cancel} call unconditionally, on the documented (but, confirmed by direct read, WRONG)
   * assumption that fowf silently drops the field when the caller's identity lacks {@code
   * execution:assert-subject}. It does not. {@code
   * WorkflowExecutionManagementService.resolveSubjectActor} calls {@code authorizer.authorize(...,
   * ASSERT_SUBJECT, ...)} the moment a caller supplies ANY {@code subjectActor} at all, and that
   * call is fail-closed ({@code AuthorizationService.requireAuthorized} throws) - so a caller
   * lacking the grant gets the WHOLE {@code launch}/{@code cancel} call rejected (fowf surfaces
   * this as a 500 {@code AuthorizationDeniedException}), not a silently-ignored field. Every real
   * fei/FDS service-account identity lacks this grant today (see
   * docs/subject-actor-fei-adoption-handoff-2026-09-25.md's own "real, separate infrastructure
   * step" callout in forwardmeasure-openworkflow) - so unconditional assertion would break every
   * real {@code launch}/{@code cancel} call, not leave {@code subjectActor} merely inert. Defaults
   * to {@code false} for exactly that reason; pass {@code true} only once a caller's own deployment
   * has confirmed {@code execution:assert-subject} is actually granted to its own service-account
   * identity.
   */
  public WorkflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi,
      AuthorizationService authorization,
      boolean assertSubjectActor) {
    this(executionsApi, authorization, assertSubjectActor, null);
  }

  public WorkflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi,
      AuthorizationService authorization,
      boolean assertSubjectActor,
      IngestionLaunchPlanner planner) {
    this.planner = planner;
    Objects.requireNonNull(executionsApi, "executionsApi");
    this.executionsApis = ignored -> executionsApi;
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.assertSubjectActor = assertSubjectActor;
  }

  public WorkflowIngestionLauncher(
      com.forwardmeasure.datastreaming.launcher.application.auth.TenantWorkflowExecutions clients,
      AuthorizationService authorization,
      boolean assertSubjectActor) {
    this(clients, authorization, assertSubjectActor, null);
  }

  public WorkflowIngestionLauncher(
      com.forwardmeasure.datastreaming.launcher.application.auth.TenantWorkflowExecutions clients,
      AuthorizationService authorization,
      boolean assertSubjectActor,
      IngestionLaunchPlanner planner) {
    Objects.requireNonNull(clients, "clients");
    this.executionsApis = clients::forTenant;
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.assertSubjectActor = assertSubjectActor;
    this.planner = planner;
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
   * *additional* assertion of the human on whose behalf that service account is acting - real,
   * confirmed fail-closed at the point of assertion (see the {@code assertSubjectActor}
   * constructor's own javadoc for why this is opt-in, not unconditional). {@link
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
    Object input = request.input();
    if (request.ingestion() != null) {
      if (planner == null)
        throw new IllegalStateException("This launcher has no ingestion planner configured");
      input =
          planner.workflowInput(
              request.ingestion(), actor.tenantId() + ":" + request.idempotencyKey());
    }
    WorkflowExecutionStart start =
        new WorkflowExecutionStart()
            .revisionId(request.revisionId())
            .input(input)
            .subjectActor(assertSubjectActor ? subjectActorFor(actor) : null);
    return executionsApis
        .apply(actor.tenantId())
        .startWorkflowExecution(request.idempotencyKey(), request.correlationId(), start);
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
    return executionsApis.apply(actor.tenantId()).getWorkflowExecution(executionId);
  }

  /**
   * Cancels the execution. {@code ifMatch} must be the execution's current {@code ETag}/version
   * (from a prior {@link #launch}/{@link #observe} call) - fowf's own optimistic-concurrency
   * contract, not something this class works around. {@code subjectActor} is attached whenever
   * {@code assertSubjectActor} is enabled (see {@link #launch} javadoc), regardless of whether
   * {@code reason} is set - {@code control} is always built (not only when {@code reason} is
   * non-null), so a reasonless cancel doesn't silently drop it either.
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
        new WorkflowExecutionControl()
            .reason(reason)
            .subjectActor(assertSubjectActor ? subjectActorFor(actor) : null);
    return executionsApis
        .apply(actor.tenantId())
        .cancelWorkflowExecution(ifMatch, correlationId, executionId, control);
  }

  private static SubjectActor subjectActorFor(ActiveOrganization actor) {
    return new SubjectActor().actorId(actor.actorId());
  }
}
