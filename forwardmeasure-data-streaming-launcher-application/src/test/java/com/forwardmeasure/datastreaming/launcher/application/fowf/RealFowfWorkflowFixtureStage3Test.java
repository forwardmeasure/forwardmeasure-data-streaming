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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionPage;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

/**
 * Stage 3 only: each real Quarkus, Spring and Micronaut execution-management image booted against
 * this fixture's real Postgres + Keycloak (no engine up yet). Proves the whole request path up to
 * (not including) engine dispatch works for real: HTTP -> production bearer-token validation ->
 * production active-organization/tenant resolution -> the real tenant-schema-routed query layer -
 * by making a real, authenticated, read-only call ({@code listExecutions}) and getting a real empty
 * page back, not an auth rejection or a 5xx.
 */
class RealFowfWorkflowFixtureStage3Test {

  private static final String ROLE = "workflow-run-launcher";

  @ParameterizedTest
  @EnumSource(RealFowfWorkflowFixture.Framework.class)
  @Timeout(300)
  void executionManagementAcceptsARealAuthenticatedRequestForTheRealTenant(
      RealFowfWorkflowFixture.Framework framework) throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage3", ROLE)) {
      GenericContainer<?> executionManagement =
          fixture.startExecutionManagement("kafka-streams", framework);
      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      WorkflowExecutionsApi executionsApi = new WorkflowExecutionsApi(apiClient);

      var anonymous =
          assertThrows(
              ApiException.class,
              () -> executionsApi.listWorkflowExecutions(null, null, null, null, null, null, null));
      assertEquals(401, anonymous.getCode(), "Execution queries require an authenticated caller");
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());

      WorkflowExecutionPage page =
          executionsApi.listWorkflowExecutions(null, null, null, null, null, null, null);
      assertEquals(
          0,
          page.getItems().size(),
          "a freshly onboarded tenant must have zero real executions, not an error");

      // Mint another real token from the same Keycloak realm/key through the alternate local
      // loopback hostname. Only its issuer changes: an invalid signature cannot make this pass.
      var issuer = fixture.keycloak().issuer();
      var alternate =
          new URI(
              issuer.getScheme(),
              null,
              "127.0.0.1".equals(issuer.getHost()) ? "localhost" : "127.0.0.1",
              issuer.getPort(),
              issuer.getPath() + "/protocol/openid-connect/token",
              null,
              null);
      String form =
          Map.of(
                  "grant_type",
                  "password",
                  "client_id",
                  AuthzenKeycloakFixture.CLIENT_ID,
                  "username",
                  AuthzenKeycloakFixture.USERNAME,
                  "password",
                  AuthzenKeycloakFixture.PASSWORD)
              .entrySet()
              .stream()
              .map(
                  entry ->
                      URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                          + "="
                          + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
              .collect(Collectors.joining("&"));
      try (var http = HttpClient.newHttpClient()) {
        var response =
            http.send(
                HttpRequest.newBuilder(alternate)
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(form))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(
            200, response.statusCode(), "Alternate hostname must reach the same real token issuer");
        String otherToken =
            new ObjectMapper().readTree(response.body()).path("access_token").asText();
        var claims =
            new ObjectMapper().readTree(Base64.getUrlDecoder().decode(otherToken.split("\\.")[1]));
        assertNotEquals(issuer.toString(), claims.path("iss").asText());
        apiClient.setBearerToken(otherToken);
        var rejected =
            assertThrows(
                ApiException.class,
                () ->
                    executionsApi.listWorkflowExecutions(null, null, null, null, null, null, null));
        assertEquals(
            401, rejected.getCode(), "A genuine token from an unexpected issuer must be rejected");
      }
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      assertEquals(
          0,
          executionsApi
              .listWorkflowExecutions(null, null, null, null, null, null, null)
              .getItems()
              .size());
    }
  }

  @ParameterizedTest
  @EnumSource(RealFowfWorkflowFixture.Framework.class)
  @Timeout(300)
  void executionManagementReturnsARealNotFoundForAnUnknownExecution(
      RealFowfWorkflowFixture.Framework framework) throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage3b", ROLE)) {
      GenericContainer<?> executionManagement =
          fixture.startExecutionManagement("kafka-streams", framework);
      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      WorkflowExecutionsApi executionsApi = new WorkflowExecutionsApi(apiClient);

      ApiException notFound =
          org.junit.jupiter.api.Assertions.assertThrows(
              ApiException.class,
              () -> executionsApi.getWorkflowExecution(java.util.UUID.randomUUID()));
      assertEquals(404, notFound.getCode());
    }
  }
}
