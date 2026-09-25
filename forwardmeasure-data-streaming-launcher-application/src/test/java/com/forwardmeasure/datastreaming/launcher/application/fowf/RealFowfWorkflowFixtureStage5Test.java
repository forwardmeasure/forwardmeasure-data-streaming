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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.ApiClient;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Stage 5 only: the real {@code openworkflow-definition-management-quarkus} image, publishing a
 * real {@code WorkflowDefinition} through fowf's own generated Java client SDK - the exact same
 * create -&gt; validate -&gt; submit -&gt; publish governance sequence {@code
 * forwardmeasure-entity-intelligence}'s own real, already-proven {@code
 * WorkflowDefinitionPublisherMain} uses (mirrored here, not reinvented). The workflow itself is the
 * real 2-step apply+await {@code kubernetes-deployment} pattern from {@code
 * docs/fowf-handover-2026-09-17.md} - this stage proves it genuinely compiles and publishes against
 * a real definition-management service; Stage 6 proves the operation-adapter actually dispatches
 * it.
 */
class RealFowfWorkflowFixtureStage5Test {

  private static final String ROLE = "workflow-run-launcher";
  private static final String CORRELATION_NAME = "fds-phase-d-worker";

  @Test
  @Timeout(300)
  void definitionManagementCompilesAndPublishesTheRealTwoStepDeploymentWorkflow() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage5", ROLE)) {
      GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();

      String asyncApiUrl =
          fixture.startAsyncApiDocumentServer(
              "/stream-worker-kubernetes-deployment.yaml", ASYNCAPI_DOCUMENT);
      String workflowSource = workflowSource(asyncApiUrl);

      String baseUrl =
          "http://"
              + definitionManagement.getHost()
              + ":"
              + definitionManagement.getMappedPort(8080);

      ApiClient authorApiClient = new ApiClient();
      authorApiClient.setBasePath(baseUrl);
      authorApiClient.setBearerToken(fixture.keycloak().mintUserToken());
      ApiClient reviewerApiClient = new ApiClient();
      reviewerApiClient.setBasePath(baseUrl);
      reviewerApiClient.setBearerToken(fixture.provisionReviewerToken(ROLE));

      WorkflowsApi workflows = new WorkflowsApi(authorApiClient);
      WorkflowDefinitionsApi definitions = new WorkflowDefinitionsApi(authorApiClient);
      WorkflowDefinitionGovernanceApi governance =
          new WorkflowDefinitionGovernanceApi(authorApiClient);
      WorkflowDefinitionGovernanceApi reviewerGovernance =
          new WorkflowDefinitionGovernanceApi(reviewerApiClient);

      Workflow workflow =
          workflows.createWorkflow(
              new CreateWorkflowRequest()
                  .name("fds-phase-d-continuous-stream-worker")
                  .title("FDS Phase D continuous stream worker")
                  .description(
                      "Real Phase D fixture workflow - RealFowfWorkflowFixtureStage5Test."));

      WorkflowDefinition created =
          definitions.createWorkflowDefinition(
              workflow.getId(),
              new CreateWorkflowDefinitionRequest().version("1.0.0").source(workflowSource));

      WorkflowDefinitionValidation validation =
          governance.validateWorkflowDefinition(
              ifMatch(created.getRevision()), workflow.getId(), created.getId());
      assertTrue(
          Boolean.TRUE.equals(validation.getValid()),
          "expected the real workflow document to compile cleanly: " + validation.getViolations());

      WorkflowDefinition submitted =
          governance.submitWorkflowDefinition(
              ifMatch(created.getRevision()), workflow.getId(), created.getId());

      WorkflowDefinition published =
          reviewerGovernance.publishWorkflowDefinition(
              ifMatch(submitted.getRevision()), workflow.getId(), created.getId());

      assertEquals("PUBLISHED", published.getStatus().name());
    }
  }

  private static String ifMatch(Long revision) {
    return "\"" + revision + "\"";
  }

  // Both the workflow source and the AsyncAPI document below are byte-for-byte the real, live-
  // verified shape from fowf's own RealAsyncApiKubernetesDeploymentDispatchTest (forwardmeasure-
  // openworkflow, openworkflow-operation-adapter-kubernetes-deployment), not the (stale) summary in
  // docs/fowf-handover-2026-09-17.md. Confirmed live 2026-09-21, in this order: the handover doc's
  // own subscription: {} shape fails real compiler validation (422, "required property 'consume'
  // not found"); a subscription.consume + subscription.payload sibling shape also fails (422,
  // "additional properties ['payload'] not allowed") - the real, pinned CNCF Serverless Workflow
  // schema's $defs/asyncApiSubscription only ever allows filter/consume/foreach. Confirmed the
  // identical failure independently reproduced fowf's own real dispatch test failing live for the
  // same reason - a genuine, then-currently-broken bug in fowf itself, fixed there directly (see
  // this plan's own fowf-schema-fix section) by having
  // AsyncApiKubernetesDeploymentOperationExecutor
  // evaluate subscription.filter as a real jq expression instead. This fix lives entirely in the
  // compiler-independent executor/materializer read path - real-schema compiler validation (what
  // this Stage 5 test exercises) was already correct before the fix, so no new image is needed
  // here; only the SUBSCRIBE step's own YAML shape (filter instead of literal fields) had to change
  // to become schema-legal, and a SEPARATE events channel from the commands channel used for apply.
  private static String workflowSource(String asyncApiUrl) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-phase-d
      name: continuous-stream-worker
      version: '1.0.0'
    do:
      - applyStreamWorker:
          call: asyncapi
          with:
            document:
              endpoint: %s
            channel: stream-worker.commands
            message:
              payload:
                namespace: streaming
                name: %s
                image: docker.io/library/busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
                replicas: 1
                command: ["sh", "-c", "sleep 300"]
      - watchStreamWorker:
          call: asyncapi
          with:
            document:
              endpoint: %s
            channel: stream-worker.events
            subscription:
              consume:
                amount: 1
              filter: '${ {namespace: "streaming", name: "%s", readinessTimeoutSeconds: 120} }'
    """
        .formatted(asyncApiUrl, CORRELATION_NAME, asyncApiUrl, CORRELATION_NAME);
  }

  private static final String ASYNCAPI_DOCUMENT =
      """
      asyncapi: 2.6.0
      info:
        title: Stream Worker Deployment (kubernetes-deployment)
        version: 1.0.0
      servers:
        cluster:
          url: k8s://in-cluster
          protocol: kubernetes-deployment
      channels:
        stream-worker.commands:
          servers: [cluster]
          publish:
            message: {name: ApplyStreamWorkerDeployment}
        stream-worker.events:
          servers: [cluster]
          subscribe:
            message: {name: StreamWorkerDeploymentReadiness}
      """;
}
