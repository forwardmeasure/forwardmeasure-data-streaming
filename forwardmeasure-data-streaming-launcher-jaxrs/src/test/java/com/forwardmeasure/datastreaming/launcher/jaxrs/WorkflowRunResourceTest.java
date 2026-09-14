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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.testkit.StubAuthorizationService;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
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
 * Real, no-mocking-framework proof at the actual HTTP protocol level - same plain JDK {@link
 * HttpServer} stand-in for fowf's own execution-management service that {@code
 * WorkflowIngestionLauncherTest} already uses, this time exercised through {@link
 * WorkflowRunResource} to prove the HTTP-facing wiring (status codes, {@code Location},
 * header/query validation) on top of the already-proven launcher.
 */
final class WorkflowRunResourceTest {

  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  private HttpServer server;
  private WorkflowRunResource resource;
  private final AtomicReference<String> lastMethod = new AtomicReference<>();
  private final AtomicReference<String> lastPath = new AtomicReference<>();
  private final AtomicReference<String> lastIfMatch = new AtomicReference<>();

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/executions", this::respond);
    server.start();

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
    apiClient.setBearerToken("test-token");
    resource =
        new WorkflowRunResource(
            new WorkflowIngestionLauncher(
                new ExecutionsApi(apiClient), StubAuthorizationService.permitAll()),
            () -> ACTOR);
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void createReturnsAcceptedWithLocationAndTheExecutionBody() throws Exception {
    WorkflowLaunchRequest request =
        new WorkflowLaunchRequest(
            UUID.randomUUID(), Map.of("sourceUri", "file:///test.csv"), "idem-1", "corr-1");

    Response response = resource.create(request);

    assertEquals(202, response.getStatus());
    assertNotNull(response.getHeaderString("Location"));
    Execution execution = (Execution) response.getEntity();
    assertTrue(response.getHeaderString("Location").endsWith(execution.getId().toString()));
  }

  @Test
  void getReturnsTheCurrentExecution() throws Exception {
    Response response = resource.get(UUID.randomUUID());

    assertEquals(200, response.getStatus());
    assertEquals("GET", lastMethod.get());
  }

  @Test
  void cancelSendsToTheCancelSubResourceWithIfMatch() throws Exception {
    UUID executionId = UUID.randomUUID();

    Response response = resource.cancel(executionId, "\"3\"", "corr-2", "no longer needed");

    assertEquals(200, response.getStatus());
    assertEquals("\"3\"", lastIfMatch.get());
    assertTrue(lastPath.get().endsWith("/cancel"));
  }

  @Test
  void cancelRejectsAMissingIfMatchOrCorrelationId() {
    UUID executionId = UUID.randomUUID();

    assertThrows(
        BadRequestException.class, () -> resource.cancel(executionId, null, "corr-2", null));
    assertThrows(BadRequestException.class, () -> resource.cancel(executionId, "\"1\"", " ", null));
  }

  private void respond(HttpExchange exchange) throws IOException {
    lastMethod.set(exchange.getRequestMethod());
    lastPath.set(exchange.getRequestURI().getPath());
    lastIfMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
    exchange.getRequestBody().readAllBytes();

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
