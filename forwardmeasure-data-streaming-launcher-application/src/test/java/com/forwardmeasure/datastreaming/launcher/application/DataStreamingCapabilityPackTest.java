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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class DataStreamingCapabilityPackTest {
  @Test
  void deployedPolicyCoversExactlyTheLaunchersResourcesAndActions() throws Exception {
    try (var stream =
        getClass().getResourceAsStream("/META-INF/data-streaming/capability-pack.json")) {
      assertNotNull(stream);
      JsonNode pack = new ObjectMapper().readTree(stream);
      Set<String> scopes = new HashSet<>();
      Set<String> resources = new HashSet<>();
      for (JsonNode resource : pack.required("resources")) {
        resources.add(resource.required("type").asText() + ":" + resource.required("id").asText());
        Set<String> declared = new HashSet<>();
        resource.required("scopes").forEach(scope -> declared.add(scope.asText()));
        Set<String> granted = new HashSet<>();
        pack.required("resourceGrants")
            .required("ingestion-control")
            .required(resource.required("id").asText())
            .forEach(scope -> granted.add(scope.asText()));
        assertEquals(declared, granted);
        scopes.addAll(granted);
      }
      assertEquals(
          Arrays.stream(AuthorizationAction.values())
              .map(AuthorizationAction::scope)
              .collect(Collectors.toSet()),
          scopes);
      var ingestion = DataStreamingAuthorizationResources.ingestionRun("test");
      var workflow = DataStreamingAuthorizationResources.workflowRun("test");
      assertEquals(
          Set.of(ingestion.type() + ":" + ingestion.id(), workflow.type() + ":" + workflow.id()),
          resources);
    }
  }
}
