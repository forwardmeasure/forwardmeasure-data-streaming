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
package com.forwardmeasure.datastreaming.launcher.jaxrs;

import com.forwardmeasure.authzen.ActiveOrganizationProvider;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecution;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.UUID;

/**
 * Workflow mode's REST surface over {@link WorkflowIngestionLauncher} - the through-fowf form,
 * {@code POST /workflow-runs} (202 Accepted + {@code Location} + fowf's own {@link
 * WorkflowExecution} body), {@code GET /workflow-runs/{executionId}} (fowf's current {@link
 * WorkflowExecution}), {@code POST /workflow-runs/{executionId}/cancel} (requires the real {@code
 * If-Match} header - fowf's own optimistic-concurrency contract, not something this resource works
 * around).
 *
 * <p>{@code reason} on cancel is an optional query parameter rather than a request body, since it's
 * the only field a caller might send and a body felt like unwarranted ceremony for one optional
 * string.
 *
 * <p>Every {@link ApiException} this class's own methods can throw (fowf rejecting the request, or
 * a connectivity failure) is translated by this module's {@code mapper} package, not here.
 */
@Path("/workflow-runs")
public class WorkflowRunResource
    implements com.forwardmeasure.datastreaming.launcher.contract.WorkflowRunsApi {

  private final WorkflowIngestionLauncher launcher;
  private final ActiveOrganizationProvider organizations;

  public WorkflowRunResource(
      WorkflowIngestionLauncher launcher, ActiveOrganizationProvider organizations) {
    this.launcher = Objects.requireNonNull(launcher, "launcher");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @Override
  public Response createWorkflowRun(WorkflowLaunchRequest request) throws ApiException {
    return create(request);
  }

  @Override
  public Response getWorkflowRun(UUID executionId) throws ApiException {
    return get(executionId);
  }

  @Override
  public Response cancelWorkflowRun(
      UUID executionId, String ifMatch, String correlationId, String reason) throws ApiException {
    return cancel(executionId, ifMatch, correlationId, reason);
  }

  public Response create(WorkflowLaunchRequest request) throws ApiException {
    WorkflowExecution execution = launcher.launch(request, organizations.current());
    return Response.accepted()
        .header("Location", "/workflow-runs/" + execution.getId())
        .entity(execution)
        .build();
  }

  public Response get(UUID executionId) throws ApiException {
    return Response.ok(launcher.observe(executionId, organizations.current())).build();
  }

  public Response cancel(UUID executionId, String ifMatch, String correlationId, String reason)
      throws ApiException {
    if (ifMatch == null || ifMatch.isBlank()) {
      throw new BadRequestException("the 'If-Match' header is required");
    }
    if (correlationId == null || correlationId.isBlank()) {
      throw new BadRequestException("the 'correlationId' query parameter is required");
    }
    return Response.ok(
            launcher.cancel(executionId, ifMatch, correlationId, reason, organizations.current()))
        .build();
  }
}
