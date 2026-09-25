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

import com.forwardmeasure.openworkflow.execution.api.model.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Stage 3 only: the real {@code openworkflow-execution-management-quarkus} image booted against
 * this fixture's real Postgres + Keycloak (no engine up yet). Proves the whole request path up to
 * (not including) engine dispatch works for real: HTTP -> Quarkus OIDC bearer-token validation ->
 * {@code QuarkusActiveOrganizationProvider}/tenant resolution -> the real tenant-schema-routed
 * query layer - by making a real, authenticated, read-only call ({@code listExecutions}) and
 * getting a real empty page back, not an auth rejection or a 5xx.
 */
class RealFowfWorkflowFixtureStage3Test {

  private static final String ROLE = "workflow-run-launcher";

  @Test
  @Timeout(300)
  void executionManagementAcceptsARealAuthenticatedRequestForTheRealTenant() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage3", ROLE)) {
      GenericContainer<?> executionManagement = fixture.startExecutionManagement();
      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      ExecutionsApi executionsApi = new ExecutionsApi(apiClient);

      ExecutionPage page = executionsApi.listExecutions(null, null, null, null, null, null, null);
      assertEquals(
          0,
          page.getItems().size(),
          "a freshly onboarded tenant must have zero real executions, not an error");
    }
  }

  @Test
  @Timeout(300)
  void executionManagementReturnsARealNotFoundForAnUnknownExecution() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage3b", ROLE)) {
      GenericContainer<?> executionManagement = fixture.startExecutionManagement();
      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);

      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      ExecutionsApi executionsApi = new ExecutionsApi(apiClient);

      ApiException notFound =
          org.junit.jupiter.api.Assertions.assertThrows(
              ApiException.class, () -> executionsApi.getExecution(java.util.UUID.randomUUID()));
      assertEquals(404, notFound.getCode());
    }
  }
}
