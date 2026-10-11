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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * The launcher's non-happy-path answers through the real Micronaut binding: a caller whose
 * organization role lacks the action's scope is a 403 problem, an unreachable fowf is a 502
 * problem, a verified token with no active Organization is a 401 problem, and the workflow-run
 * cancel preconditions are 400 problems. The caller's role holds only the two read scopes.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LauncherRequestOutcomesMicronautTest implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-outcomes-role";
  private static final String NAMESPACE = "fds-micronaut-outcomes";
  private static final String ORPHAN_CLIENT_ID = "fds-micronaut-outcomes-orphan";
  private static final String ORPHAN_CLIENT_SECRET = "orphan-" + UUID.randomUUID();
  private static final String FOWF_CLIENT_ID = "fds-micronaut-outcomes-fowf";
  private static final String FOWF_CLIENT_SECRET = "fowf-" + UUID.randomUUID();
  private static final String FOWF_ROLE_NAME = "micronaut-outcomes-fowf-role";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  static volatile AuthzenKeycloakFixture fixture;
  static volatile KubernetesTestContainer kubernetes;

  @Inject EmbeddedServer server;

  @Override
  public Map<String, String> getProperties() {
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    String organizationId = fixture.provisionTenant("micronaut-outcomes-org", tenantDid, ROLE_NAME);
    fixture.grantResourceAuthorization(
        organizationId,
        "datastreaming-ingestion-run",
        "ingestion-runs",
        "outcomes-ingestion-read",
        ROLE_NAME,
        Set.of(AuthorizationAction.INGESTION_RUN_READ.scope()));
    fixture.grantResourceAuthorization(
        organizationId,
        "datastreaming-workflow-run",
        "workflow-runs",
        "outcomes-workflow-read",
        ROLE_NAME,
        Set.of(AuthorizationAction.WORKFLOW_RUN_READ.scope()));
    fixture.createServiceAccountClient(ORPHAN_CLIENT_ID, ORPHAN_CLIENT_SECRET);
    // fds calls fowf as a machine identity of the caller's own Organization.
    fixture.createServiceAccountClient(FOWF_CLIENT_ID, FOWF_CLIENT_SECRET);
    fixture.addServiceAccountToOrganization(organizationId, FOWF_CLIENT_ID, FOWF_ROLE_NAME);
    fixture.grantOrganizationClientRole(organizationId, FOWF_ROLE_NAME);

    kubernetes = new KubernetesTestContainer().start();
    CurrentKubernetesTestContainer.set(kubernetes);
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }

    return Map.ofEntries(
        Map.entry(
            "micronaut.security.token.jwt.signatures.jwks.keycloak.url",
            fixture.issuer().toString() + "/protocol/openid-connect/certs"),
        Map.entry(
            "datastreaming.launcher.authorization.organization-client-id",
            AuthzenKeycloakFixture.CLIENT_ID),
        Map.entry("datastreaming.launcher.authorization.issuer", fixture.issuer().toString()),
        Map.entry(
            "datastreaming.launcher.authorization.client-id",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
        Map.entry(
            "datastreaming.launcher.authorization.client-secret",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
        Map.entry("datastreaming.launcher.k8s.images", "unused"),
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        // A port nothing listens on: every fowf call fails to connect.
        Map.entry("datastreaming.launcher.fowf.base-url", "http://127.0.0.1:" + closedPort()),
        Map.entry(
            "datastreaming.launcher.fowf.keycloak.token-url",
            fixture.issuer() + "/protocol/openid-connect/token"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", FOWF_CLIENT_ID),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", FOWF_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.pekko.image", "unused"),
        Map.entry("datastreaming.launcher.pekko.command", "unused"),
        Map.entry("datastreaming.launcher.kafka-streams.image", "unused"),
        Map.entry("datastreaming.launcher.kafka-streams.command", "unused"));
  }

  @AfterAll
  static void stopFixtures() {
    CurrentKubernetesTestContainer.clear();
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  void launchingAnIngestionRunWithoutTheLaunchScopeIsForbidden() throws Exception {
    String body =
        MAPPER.writeValueAsString(
            Map.of(
                "correlationId",
                "outcomes-" + UUID.randomUUID(),
                "namespace",
                NAMESPACE,
                "ingestionSpec",
                WorldCheckFixtures.boundedFileSpec(
                    "file:/tmp?fileName=source.csv&noop=true", "http://unused:9200", Path.of(""))));
    assertProblem(post("ingestion-runs", body, userToken()), 403, "Forbidden");
  }

  @Test
  void cancellingAnIngestionRunWithoutTheCancelScopeIsForbidden() throws Exception {
    assertProblem(
        post("ingestion-runs/outcomes-run/cancel?namespace=" + NAMESPACE, "", userToken()),
        403,
        "Forbidden");
  }

  @Test
  void launchingAWorkflowRunWithoutTheLaunchScopeIsForbidden() throws Exception {
    String body =
        MAPPER.writeValueAsString(
            Map.of(
                "revisionId",
                UUID.randomUUID().toString(),
                "input",
                Map.of(),
                "idempotencyKey",
                "outcomes-" + UUID.randomUUID(),
                "correlationId",
                "outcomes-" + UUID.randomUUID()));
    assertProblem(post("workflow-runs", body, userToken()), 403, "Forbidden");
  }

  @Test
  void readingAWorkflowRunWhileFowfIsUnreachableIsABadGateway() throws Exception {
    assertProblem(get("workflow-runs/" + UUID.randomUUID(), userToken()), 502, "Bad Gateway");
  }

  @Test
  void cancellingAWorkflowRunWithoutIfMatchIsABadRequest() throws Exception {
    JsonNode problem =
        assertProblem(
            post(
                "workflow-runs/" + UUID.randomUUID() + "/cancel?correlationId=outcomes",
                "",
                userToken()),
            400,
            "Bad Request");
    assertEquals("the 'If-Match' header is required", problem.path("detail").asText());
  }

  @Test
  void cancellingAWorkflowRunWithoutACorrelationIdIsABadRequest() throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            request("workflow-runs/" + UUID.randomUUID() + "/cancel", userToken())
                .header("If-Match", "\"1\"")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    JsonNode problem = assertProblem(response, 400, "Bad Request");
    assertEquals(
        "the 'correlationId' query parameter is required", problem.path("detail").asText());
  }

  @Test
  void aVerifiedTokenWithNoActiveOrganizationIsUnauthorized() throws Exception {
    String token = fixture.clientCredentialsToken(ORPHAN_CLIENT_ID, ORPHAN_CLIENT_SECRET);
    assertProblem(get("workflow-runs/" + UUID.randomUUID(), token), 401, "Unauthorized");
  }

  private static String userToken() {
    return fixture.mintUserToken();
  }

  private HttpRequest.Builder request(String path, String token) {
    return HttpRequest.newBuilder(URI.create(server.getURL() + "/").resolve(path))
        .header("Authorization", "Bearer " + token);
  }

  private HttpResponse<String> get(String path, String token)
      throws IOException, InterruptedException {
    return HTTP.send(request(path, token).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(String path, String json, String token)
      throws IOException, InterruptedException {
    HttpRequest.Builder builder = request(path, token).header("Content-Type", "application/json");
    builder.POST(
        json.isEmpty()
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(json));
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static JsonNode assertProblem(HttpResponse<String> response, int status, String title)
      throws IOException {
    assertEquals(status, response.statusCode(), "unexpected response: " + response.body());
    assertTrue(
        response
            .headers()
            .firstValue("Content-Type")
            .orElse("")
            .startsWith("application/problem+json"),
        "expected an RFC 9457 problem: " + response.headers().map());
    JsonNode problem = MAPPER.readTree(response.body());
    assertEquals(status, problem.path("status").asInt(), response.body());
    assertEquals(title, problem.path("title").asText(), response.body());
    return problem;
  }

  private static int closedPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException("could not reserve a local port", e);
    }
  }
}
