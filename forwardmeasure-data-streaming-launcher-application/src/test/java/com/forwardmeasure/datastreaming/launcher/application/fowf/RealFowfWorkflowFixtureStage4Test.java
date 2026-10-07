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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionStart;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Starts both services and proves an unknown revision is rejected by public admission. Actual
 * engine dispatch requires a published definition and is covered by the end-to-end fixture tests.
 */
class RealFowfWorkflowFixtureStage4Test {

  private static final String ROLE = "workflow-run-launcher";

  @Test
  @Timeout(300)
  void unknownRevisionIsRejectedByAdmissionWithTheEngineRunning() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage4", ROLE)) {
      GenericContainer<?> executionManagement = fixture.startExecutionManagement();
      fixture.startEngineKafkaStreams();

      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      WorkflowExecutionsApi executionsApi = new WorkflowExecutionsApi(apiClient);

      UUID unknownRevisionId = UUID.randomUUID();
      WorkflowExecutionStart start =
          new WorkflowExecutionStart().revisionId(unknownRevisionId).input(java.util.Map.of());

      ApiException failure =
          assertThrows(
              ApiException.class,
              () ->
                  executionsApi.startWorkflowExecution(
                      "stage4-idempotency-key", "stage4-correlation-id", start));
      assertEquals(404, failure.getCode());
      var problem =
          new com.fasterxml.jackson.databind.ObjectMapper().readTree(failure.getResponseBody());
      assertTrue(
          problem.path("detail").asText().contains("revision"),
          "The rejection must identify the missing publication, not an authentication or routing"
              + " failure");
    }
  }
}
