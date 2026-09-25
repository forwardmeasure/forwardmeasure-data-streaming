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
package com.forwardmeasure.datastreaming.launcher.micronaut.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.launcher.application.fowf.RealFowfWorkflowFixture;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * The Kafka-Streams-engine sibling of {@code WorkflowContinuousMatrixMicronautPekkoSmokeTest} - see
 * the Quarkus sibling ({@code WorkflowContinuousMatrixQuarkusKafkaStreamsSmokeTest}) for why this
 * isn't blocked on the `correlated-worker` actor-context bug.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowContinuousMatrixMicronautKafkaStreamsSmokeTest implements TestPropertyProvider {

  private static final String ROLE = "micronaut-workflow-continuous-ks-smoke-role";
  private static final String NAMESPACE = "fds-micronaut-workflow-continuous-ks-smoke";
  private static final String CORRELATION_NAME = "fds-micronaut-cont-ks-worker";
  private static final String IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static volatile RealFowfWorkflowFixture fixture;
  static volatile WorkflowDefinition published;

  @Inject
  @Client("/")
  HttpClient http;

  @Override
  public Map<String, String> getProperties() {
    fixture = RealFowfWorkflowFixture.start("fds-micronaut-wf-cont-ks-smoke", ROLE);
    try (var k8s = fixture.kubernetes().createClient()) {
      k8s.namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }
    CurrentKubernetesTestContainer.set(fixture.kubernetes());

    GenericContainer<?> executionManagement = fixture.startExecutionManagement("kafka-streams");
    fixture.startEngineKafkaStreams();
    GenericContainer<?> definitionManagement = fixture.startDefinitionManagement();
    fixture.startOperationAdapter(NAMESPACE, IMAGE, 1, false);

    String asyncApiUrl =
        fixture.startAsyncApiDocumentServer(
            "/stream-worker-kubernetes-deployment.yaml", asyncApiDocument());

    try {
      published = publish(fixture, definitionManagement, asyncApiUrl);
    } catch (Exception failure) {
      throw new IllegalStateException("failed to publish the real workflow definition", failure);
    }

    fixture
        .keycloak()
        .grantResourceAuthorization(
            fixture.organizationId(),
            "datastreaming-workflow-run",
            "workflow-runs",
            "workflow-continuous-permission",
            ROLE,
            Set.of(
                AuthorizationAction.WORKFLOW_RUN_LAUNCH.scope(),
                AuthorizationAction.WORKFLOW_RUN_READ.scope()));

    String executionManagementBaseUrl =
        "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

    return Map.ofEntries(
        Map.entry(
            "micronaut.security.token.jwt.signatures.jwks.keycloak.url",
            fixture.keycloak().issuer().toString() + "/protocol/openid-connect/certs"),
        Map.entry(
            "datastreaming.launcher.authorization.organization-client-id",
            AuthzenKeycloakFixture.CLIENT_ID),
        Map.entry(
            "datastreaming.launcher.authorization.issuer", fixture.keycloak().issuer().toString()),
        Map.entry(
            "datastreaming.launcher.authorization.client-id",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
        Map.entry(
            "datastreaming.launcher.authorization.client-secret",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.fowf.base-url", executionManagementBaseUrl),
        Map.entry(
            "datastreaming.launcher.fowf.keycloak.token-url",
            fixture.keycloak().issuer() + "/protocol/openid-connect/token"),
        Map.entry(
            "datastreaming.launcher.fowf.keycloak.client-id",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
        Map.entry(
            "datastreaming.launcher.fowf.keycloak.client-secret",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
        Map.entry("datastreaming.launcher.k8s.images", IMAGE),
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        Map.entry("datastreaming.launcher.k8s.host-aliases", "unused=127.0.0.1"),
        Map.entry("datastreaming.launcher.pekko.image", IMAGE),
        Map.entry("datastreaming.launcher.pekko.command", "true #"),
        Map.entry("datastreaming.launcher.kafka-streams.image", IMAGE),
        Map.entry("datastreaming.launcher.kafka-streams.command", "true #"));
  }

  @AfterAll
  static void stopFixtures() {
    CurrentKubernetesTestContainer.clear();
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  @Timeout(300)
  void realHttpCallThroughFowfAppliesARealDeploymentThatReachesAvailable() throws Exception {
    String correlationId = "wf-cont-ks-smoke-" + UUID.randomUUID();
    String token = fixture.keycloak().mintUserToken();

    Map<String, Object> requestBody =
        Map.of(
            "revisionId",
            published.getId(),
            "input",
            Map.of(),
            "idempotencyKey",
            "idempotency-" + correlationId,
            "correlationId",
            correlationId);
    String requestBodyJson = MAPPER.writeValueAsString(requestBody);

    HttpResponse<String> dispatchResponse =
        http.toBlocking()
            .exchange(
                HttpRequest.POST("/workflow-runs", requestBodyJson)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bearerAuth(token),
                String.class);
    assertEquals(HttpStatus.ACCEPTED.getCode(), dispatchResponse.code());
    @SuppressWarnings("unchecked")
    Map<String, Object> dispatched = MAPPER.readValue(dispatchResponse.body(), Map.class);
    String executionId = String.valueOf(dispatched.get("id"));

    String state = pollUntilTerminal(token, executionId);
    assertEquals("COMPLETED", state, "expected the real 2-step apply+watch workflow to complete");

    try (KubernetesClient k8s = fixture.kubernetes().createClient()) {
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      Integer readyReplicas = null;
      while (System.nanoTime() < deadline) {
        var deployments =
            k8s.apps()
                .deployments()
                .inNamespace(NAMESPACE)
                .withLabel("openworkflow.io/execution-id", executionId)
                .list()
                .getItems();
        assertEquals(
            1, deployments.size(), "expected exactly one real Deployment for this execution");
        var deployment = deployments.get(0);
        readyReplicas =
            deployment.getStatus() == null ? null : deployment.getStatus().getReadyReplicas();
        if (Integer.valueOf(1).equals(readyReplicas)) {
          return;
        }
        Thread.sleep(500);
      }
      assertTrue(
          Integer.valueOf(1).equals(readyReplicas),
          "the real Deployment must actually be Available (readyReplicas=1) - last observed "
              + readyReplicas);
    }
  }

  private String pollUntilTerminal(String token, String executionId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
    String state = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<Map> response =
            http.toBlocking()
                .exchange(
                    HttpRequest.GET("/workflow-runs/" + executionId)
                        .bearerAuth(fixture.keycloak().mintUserToken()),
                    Map.class);
        if (response.code() == 200) {
          Object rawState = response.body().get("state");
          state = rawState == null ? "UNKNOWN" : rawState.toString();
          if ("COMPLETED".equals(state) || "FAILED".equals(state)) {
            return state;
          }
        }
      } catch (HttpClientResponseException notYetTerminal) {
        // Tolerant poll, matching the other Micronaut smoke tests' own shape.
      }
      Thread.sleep(1000);
    }
    return state;
  }

  private static WorkflowDefinition publish(
      RealFowfWorkflowFixture fixture, GenericContainer<?> definitionManagement, String asyncApiUrl)
      throws Exception {
    String baseUrl =
        "http://" + definitionManagement.getHost() + ":" + definitionManagement.getMappedPort(8080);

    var authorApiClient =
        new com.forwardmeasure.openworkflow.definition.management.client.ApiClient();
    authorApiClient.setBasePath(baseUrl);
    authorApiClient.setBearerToken(fixture.keycloak().mintUserToken());
    var reviewerApiClient =
        new com.forwardmeasure.openworkflow.definition.management.client.ApiClient();
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
                .name("fds-micronaut-workflow-continuous-ks-stream-worker")
                .title("FDS Micronaut workflow-continuous stream worker (Kafka-Streams)")
                .description(
                    "Real Phase G matrix cell -"
                        + " WorkflowContinuousMatrixMicronautKafkaStreamsSmokeTest."));

    WorkflowDefinition created =
        definitions.createWorkflowDefinition(
            workflow.getId(),
            new CreateWorkflowDefinitionRequest()
                .version("1.0.0")
                .source(workflowSource(asyncApiUrl)));

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
    return published;
  }

  private static String ifMatch(Long revision) {
    return "\"" + revision + "\"";
  }

  private static String workflowSource(String asyncApiUrl) {
    return """
    document:
      dsl: '1.0.3'
      namespace: fds-micronaut-workflow-continuous-ks
      name: stream-worker
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
                namespace: fds-micronaut-workflow-continuous-ks-smoke
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
              filter: '${ {namespace: "fds-micronaut-workflow-continuous-ks-smoke", name: "%s", readinessTimeoutSeconds: 120} }'
    """
        .formatted(asyncApiUrl, CORRELATION_NAME, asyncApiUrl, CORRELATION_NAME);
  }

  private static String asyncApiDocument() {
    return """
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
}
