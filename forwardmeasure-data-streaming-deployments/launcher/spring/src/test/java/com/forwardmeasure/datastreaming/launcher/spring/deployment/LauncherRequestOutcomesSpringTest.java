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
package com.forwardmeasure.datastreaming.launcher.spring.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The launcher's non-happy-path answers through the real Spring binding: a caller whose
 * organization role lacks the action's scope is a 403 problem, an unreachable fowf is a 502
 * problem, a verified token with no active Organization is a 401 problem, and the workflow-run
 * cancel preconditions are 400 problems. The caller's role holds only the two read scopes.
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.context.annotation.Import(
    LauncherRequestOutcomesSpringTest.TestKubernetesClientConfiguration.class)
class LauncherRequestOutcomesSpringTest {

  private static final String ROLE_NAME = "spring-outcomes-role";
  private static final String NAMESPACE = "fds-spring-outcomes";
  private static final String ORPHAN_CLIENT_ID = "fds-spring-outcomes-orphan";
  private static final String ORPHAN_CLIENT_SECRET = "orphan-" + UUID.randomUUID();
  private static final String FOWF_CLIENT_ID = "fds-spring-outcomes-fowf";
  private static final String FOWF_CLIENT_SECRET = "fowf-" + UUID.randomUUID();
  private static final String FOWF_ROLE_NAME = "spring-outcomes-fowf-role";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static AuthzenKeycloakFixture fixture;
  private static KubernetesTestContainer kubernetes;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() {
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    String organizationId = fixture.provisionTenant("spring-outcomes-org", tenantDid, ROLE_NAME);
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
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }
  }

  @AfterAll
  static void stopFixtures() {
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @DynamicPropertySource
  static void registerDynamicProperties(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> fixture.issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.organization-client-id",
        () -> AuthzenKeycloakFixture.CLIENT_ID);
    registry.add("datastreaming.launcher.authorization.issuer", () -> fixture.issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.client-id",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID);
    registry.add(
        "datastreaming.launcher.authorization.client-secret",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET);
    registry.add("datastreaming.launcher.k8s.namespaces", () -> NAMESPACE);
    registry.add("datastreaming.launcher.k8s.images", () -> "unused");
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    // A port nothing listens on: every fowf call fails to connect.
    int fowfPort = closedPort();
    registry.add("datastreaming.launcher.fowf.base-url", () -> "http://127.0.0.1:" + fowfPort);
    registry.add(
        "datastreaming.launcher.fowf.keycloak.token-url",
        () -> fixture.issuer() + "/protocol/openid-connect/token");
    registry.add("datastreaming.launcher.fowf.keycloak.client-id", () -> FOWF_CLIENT_ID);
    registry.add("datastreaming.launcher.fowf.keycloak.client-secret", () -> FOWF_CLIENT_SECRET);
    registry.add("datastreaming.launcher.pekko.image", () -> "unused");
    registry.add("datastreaming.launcher.pekko.command", () -> "unused");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> "unused");
    registry.add("datastreaming.launcher.kafka-streams.command", () -> "unused");
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
    return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/").resolve(path))
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

  /** Points the launcher at this class's own disposable K3s, never an ambient kubeconfig. */
  @TestConfiguration
  static class TestKubernetesClientConfiguration {
    @Bean
    @Primary
    KubernetesClient testKubernetesClient() {
      return new KubernetesClientBuilder()
          .withConfig(Config.fromKubeconfig(kubernetes.kubeConfigYaml()))
          .build();
    }
  }
}
