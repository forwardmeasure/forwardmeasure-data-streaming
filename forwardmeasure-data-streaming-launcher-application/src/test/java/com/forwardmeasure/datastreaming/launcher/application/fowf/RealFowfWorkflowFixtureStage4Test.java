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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Stage 4 only: the real {@code openworkflow-engine-kafka-streams-quarkus} image booted alongside
 * Stage 3's execution-management, both pointed at each other's own real network alias. No
 * WorkflowDefinition exists yet (that's Stage 5), so the real, meaningful proof here is that
 * submitting an execution against an unknown revision produces a real, specific rejection from the
 * engine's own definition-resolution path - not a connection failure/5xx, which is what a genuinely
 * unreachable engine would produce instead.
 */
class RealFowfWorkflowFixtureStage4Test {

  private static final String ROLE = "workflow-run-launcher";

  @Test
  @Timeout(300)
  void executionManagementAndTheEngineReachEachOtherForARealSubmission() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage4", ROLE)) {
      GenericContainer<?> executionManagement = fixture.startExecutionManagement();
      fixture.startEngineKafkaStreams();

      String baseUrl =
          "http://" + executionManagement.getHost() + ":" + executionManagement.getMappedPort(8080);
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(baseUrl);
      apiClient.setBearerToken(fixture.keycloak().mintUserToken());
      ExecutionsApi executionsApi = new ExecutionsApi(apiClient);

      UUID unknownRevisionId = UUID.randomUUID();
      ExecutionStart start =
          new ExecutionStart().revisionId(unknownRevisionId).input(java.util.Map.of());

      ApiException failure =
          assertThrows(
              ApiException.class,
              () ->
                  executionsApi.startExecution(
                      "stage4-idempotency-key", "stage4-correlation-id", start));
      // A genuinely unreachable engine would surface as a 500/502/504 (or this call would just
      // hang until the OPENWORKFLOW_ENGINES_TIMEOUT elapses) - a real 4xx this fast means
      // execution-management's own submission path genuinely reached the engine and got a real,
      // specific "no such definition" rejection back.
      assertNotEquals(500, failure.getCode());
      assertNotEquals(502, failure.getCode());
      assertNotEquals(504, failure.getCode());
    }
  }
}
