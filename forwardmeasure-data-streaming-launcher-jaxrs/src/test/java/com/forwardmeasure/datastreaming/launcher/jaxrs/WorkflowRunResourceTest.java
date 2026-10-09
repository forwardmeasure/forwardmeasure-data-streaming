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
import com.forwardmeasure.authzen.AuthorizationDecision;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecution;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Resource adapter test: invokes the resource directly and uses a JDK HTTP server for the upstream
 * FOWF protocol. Verifies response construction and outgoing paths/headers; this is not evidence of
 * incoming HTTP routing or real authorization. Those are covered by the three framework deployment
 * suites and packaged runtime acceptance.
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
    server.createContext("/v1/workflow-executions", this::respond);
    server.start();

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
    apiClient.setBearerToken("test-token");
    resource =
        new WorkflowRunResource(
            new WorkflowIngestionLauncher(
                new WorkflowExecutionsApi(apiClient), new PermitAllAuthorizationService()),
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
    assertEquals("POST", lastMethod.get());
    assertEquals("/v1/workflow-executions", lastPath.get());
    assertNotNull(response.getHeaderString("Location"));
    WorkflowExecution execution = (WorkflowExecution) response.getEntity();
    assertTrue(response.getHeaderString("Location").endsWith(execution.getId().toString()));
  }

  @Test
  void getReturnsTheCurrentExecution() throws Exception {
    UUID executionId = UUID.randomUUID();
    Response response = resource.get(executionId);

    assertEquals(200, response.getStatus());
    assertEquals("GET", lastMethod.get());
    assertEquals("/v1/workflow-executions/" + executionId, lastPath.get());
    assertEquals(executionId, ((WorkflowExecution) response.getEntity()).getId());
  }

  @Test
  void cancelSendsToTheCancelSubResourceWithIfMatch() throws Exception {
    UUID executionId = UUID.randomUUID();

    Response response = resource.cancel(executionId, "\"3\"", "corr-2", "no longer needed");

    assertEquals(200, response.getStatus());
    assertEquals("\"3\"", lastIfMatch.get());
    assertEquals("POST", lastMethod.get());
    assertEquals("/v1/workflow-executions/" + executionId + "/cancel", lastPath.get());
    assertEquals(executionId, ((WorkflowExecution) response.getEntity()).getId());
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

    String[] path = exchange.getRequestURI().getPath().split("/");
    UUID id = path.length > 3 ? UUID.fromString(path[3]) : UUID.randomUUID();
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

  /**
   * A local, file-scoped stand-in for {@link AuthorizationService} - deliberately not a shared,
   * importable-from-anywhere stub class. The shared {@code StubAuthorizationService} this class
   * used to import was removed repo-wide 2026-09-20: it masked a real Keycloak Organizations-group
   * authorization bug elsewhere in the product (native Role policies don't see roles granted only
   * via Organization membership). What's under test here is {@link WorkflowRunResource}'s real
   * HTTP-status-code/Location/header wiring against a plain JDK {@link HttpServer}, not the
   * authorization decision itself, so a permissive local fake - not a real Keycloak-backed check -
   * is the right amount of realism.
   */
  private static final class PermitAllAuthorizationService implements AuthorizationService {
    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      return new AuthorizationDecision(true, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }
}
