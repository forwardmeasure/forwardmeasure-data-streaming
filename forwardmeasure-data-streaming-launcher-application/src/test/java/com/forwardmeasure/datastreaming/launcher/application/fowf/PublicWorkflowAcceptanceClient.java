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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.client.ApiClient;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionGovernanceApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowDefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.client.api.WorkflowsApi;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.testcontainers.containers.GenericContainer;

/** Public HTTP boundary helpers shared by cross-product acceptance fixtures. */
public final class PublicWorkflowAcceptanceClient {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private PublicWorkflowAcceptanceClient() {}

  public static UUID publish(GenericContainer<?> definition, String token, String source) {
    var client = new ApiClient();
    client.setBasePath(endpoint(definition));
    client.setBearerToken(token);
    var workflows = new WorkflowsApi(client);
    var definitions = new WorkflowDefinitionsApi(client);
    var governance = new WorkflowDefinitionGovernanceApi(client);
    // Catalogue name deliberately differs from document.name, exercising durable definition
    // identity.
    var workflow =
        workflows.createWorkflow(
            new CreateWorkflowRequest()
                .name("catalogue-" + UUID.randomUUID())
                .title("Runtime acceptance"));
    var created =
        definitions.createWorkflowDefinition(
            workflow.getId(),
            new CreateWorkflowDefinitionRequest().version("1.0.0").source(source));
    String match = "\"" + created.getRevision() + "\"";
    var validated = governance.validateWorkflowDefinition(match, workflow.getId(), created.getId());
    assertTrue(Boolean.TRUE.equals(validated.getValid()), validated.toString());
    return governance.publishWorkflowDefinition(match, workflow.getId(), created.getId()).getId();
  }

  public static String endpoint(GenericContainer<?> service) {
    return "http://" + service.getHost() + ":" + service.getMappedPort(8080);
  }

  public static JsonNode request(
      String endpoint,
      String path,
      String token,
      String method,
      Object body,
      String key,
      int expected)
      throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create(endpoint + path))
            .timeout(Duration.ofSeconds(30))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json");
    if (key != null) request.header("Idempotency-Key", key);
    request.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    var response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    assertEquals(expected, response.statusCode(), response.body());
    return response.body().isBlank() ? JSON.nullNode() : JSON.readTree(response.body());
  }
}
