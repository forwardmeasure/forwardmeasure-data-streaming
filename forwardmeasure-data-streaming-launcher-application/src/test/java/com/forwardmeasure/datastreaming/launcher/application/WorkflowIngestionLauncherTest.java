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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.testkit.StubAuthorizationService;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocking-framework proof at the actual HTTP protocol level: a plain JDK {@link
 * HttpServer} stands in for fowf's own execution-management service, and {@link
 * WorkflowIngestionLauncher} is exercised against fowf's own generated {@link ExecutionsApi} client
 * hitting it over a real socket - proving this class sends the right method/path/headers/ body and
 * correctly parses fowf's own {@link Execution} response shape, not a hand-mocked substitute for
 * either. Not a live fowf deployment (that's a materially bigger ask than this class's own real
 * dependency, deliberately not attempted here) - the actual Job-lifecycle mechanics fowf's side of
 * this contract runs on are already proven for real elsewhere (see {@code
 * RealKubernetesJobOperationExecutorTest} in fowf itself).
 */
final class WorkflowIngestionLauncherTest {

  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          TenantDatabase.forAlias("test-tenant"),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  private HttpServer server;
  private WorkflowIngestionLauncher launcher;
  private final AtomicReference<String> lastMethod = new AtomicReference<>();
  private final AtomicReference<String> lastPath = new AtomicReference<>();
  private final AtomicReference<String> lastIdempotencyKey = new AtomicReference<>();
  private final AtomicReference<String> lastCorrelationId = new AtomicReference<>();
  private final AtomicReference<String> lastAuthorization = new AtomicReference<>();
  private final AtomicReference<String> lastBody = new AtomicReference<>();

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/executions", this::respond);
    server.start();

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
    apiClient.setBearerToken("test-token");
    launcher =
        new WorkflowIngestionLauncher(
            new ExecutionsApi(apiClient), StubAuthorizationService.permitAll());
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void launchSendsTheRealStartExecutionRequestAndParsesTheResponse() throws Exception {
    UUID revisionId = UUID.randomUUID();
    WorkflowLaunchRequest request =
        new WorkflowLaunchRequest(
            revisionId, Map.of("sourceUri", "file:///test.csv"), "idem-1", "corr-1");

    Execution execution = launcher.launch(request, ACTOR);

    assertEquals("POST", lastMethod.get());
    assertTrue(
        lastPath.get().startsWith("/v1/executions"),
        "expected /v1/executions, got " + lastPath.get());
    assertEquals("idem-1", lastIdempotencyKey.get());
    assertEquals("corr-1", lastCorrelationId.get());
    assertEquals("Bearer test-token", lastAuthorization.get());
    assertTrue(
        lastBody.get().contains(revisionId.toString()), "request body should carry the revisionId");
    assertTrue(lastBody.get().contains("file:///test.csv"), "request body should carry the input");

    assertNotNull(execution);
    assertEquals(ExecutionState.RUNNING, execution.getState());
  }

  @Test
  void observeSendsAGetAndParsesTheResponse() throws Exception {
    UUID executionId = UUID.randomUUID();

    Execution execution = launcher.observe(executionId, ACTOR);

    assertEquals("GET", lastMethod.get());
    assertTrue(lastPath.get().contains(executionId.toString()));
    assertEquals(ExecutionState.RUNNING, execution.getState());
  }

  @Test
  void cancelSendsAPostToTheCancelSubResource() throws Exception {
    UUID executionId = UUID.randomUUID();

    Execution execution =
        launcher.cancel(executionId, "\"1\"", "corr-2", "no longer needed", ACTOR);

    assertEquals("POST", lastMethod.get());
    assertTrue(
        lastPath.get().endsWith("/cancel"), "expected a .../cancel path, got " + lastPath.get());
    assertEquals("corr-2", lastCorrelationId.get());
    assertNotNull(execution);
  }

  private void respond(HttpExchange exchange) throws IOException {
    lastMethod.set(exchange.getRequestMethod());
    lastPath.set(exchange.getRequestURI().getPath());
    lastIdempotencyKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
    lastCorrelationId.set(exchange.getRequestHeaders().getFirst("X-Correlation-ID"));
    lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
    lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

    UUID id = UUID.randomUUID();
    String json =
        """
        {"id":"%s","workflowId":"%s","revisionId":"%s","revisionDigest":"abc123",
        "engineId":"test-engine","state":"RUNNING","version":1,"correlationId":"corr",
        "input":{},"createdAt":"2026-09-13T12:00:00.000Z","updatedAt":"2026-09-13T12:00:00.000Z"}
        """
            .formatted(id, UUID.randomUUID(), UUID.randomUUID());
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(
        exchange.getRequestMethod().equals("GET") ? 200 : 202, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }
}
