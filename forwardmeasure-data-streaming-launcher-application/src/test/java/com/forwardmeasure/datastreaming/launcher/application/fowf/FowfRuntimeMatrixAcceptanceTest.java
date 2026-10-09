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

import static com.forwardmeasure.datastreaming.launcher.application.fowf.PublicWorkflowAcceptanceClient.endpoint;
import static com.forwardmeasure.datastreaming.launcher.application.fowf.PublicWorkflowAcceptanceClient.publish;
import static com.forwardmeasure.datastreaming.launcher.application.fowf.PublicWorkflowAcceptanceClient.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Cross-product public FOWF acceptance hosted beside the shared runtime fixture used by FDS. */
class FowfRuntimeMatrixAcceptanceTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  record Runtime(
      RealFowfWorkflowFixture.Framework framework,
      String engine,
      RealFowfWorkflowFixture.PekkoPersistence persistence) {}

  static Stream<Runtime> runtimes() {
    var all =
        Stream.of(RealFowfWorkflowFixture.Framework.values())
            .flatMap(
                framework ->
                    Stream.of(
                        new Runtime(
                            framework,
                            "kafka-streams",
                            RealFowfWorkflowFixture.PekkoPersistence.POSTGRESQL),
                        new Runtime(
                            framework,
                            "pekko",
                            RealFowfWorkflowFixture.PekkoPersistence.POSTGRESQL),
                        new Runtime(
                            framework,
                            "pekko",
                            RealFowfWorkflowFixture.PekkoPersistence.CASSANDRA)))
            .toList();
    String selection = System.getProperty("fowf.acceptance.runtime", "");
    var selected =
        all.stream()
            .filter(
                runtime ->
                    selection.isBlank()
                        || selection.equals(
                            runtime.framework().name().toLowerCase(java.util.Locale.ROOT)
                                + "/"
                                + runtime.engine()
                                + "/"
                                + runtime.persistence().name().toLowerCase(java.util.Locale.ROOT)))
            .toList();
    if (selected.isEmpty())
      throw new IllegalArgumentException("Unknown FOWF acceptance runtime: " + selection);
    return selected.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("runtimes")
  @Timeout(900)
  void publicWorkflowRunsAdapterEffectsAndRecoversWithoutChangingAdmission(Runtime selected)
      throws Exception {
    String marker = UUID.randomUUID().toString();
    var observed = new ConcurrentHashMap<String, JsonNode>();
    var receiver = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
    receiver.createContext(
        "/effect",
        exchange -> {
          try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
              exchange.sendResponseHeaders(405, -1);
              return;
            }
            var body = JSON.readTree(exchange.getRequestBody());
            observed.put(body.path("phase").asText(), body);
            byte[] response = JSON.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
          }
        });
    receiver.start();
    try (var runtime =
        RealFowfWorkflowFixture.start(
            "matrix-" + marker, "matrix-caller", selected.framework(), selected.persistence())) {
      var kc = runtime.keycloak();
      String organizationB =
          runtime.provisionAdditionalTenant("isolated-" + marker, "matrix-caller");
      // Dedicated single-organization identities avoid ambiguous tokens when the fixture's
      // administrative user belongs to both organizations.
      var execution = runtime.startExecutionManagement(selected.engine());
      boolean pekko = "pekko".equals(selected.engine());
      var engine = pekko ? runtime.startEnginePekko() : runtime.startEngineKafkaStreams();
      var definition = runtime.startDefinitionManagement();
      var adapter = runtime.startHttpOperationAdapter(pekko);
      if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
      String tokenA = caller(kc, runtime.organizationId(), "matrix-a");
      String tokenB = caller(kc, organizationB, "matrix-b");
      String endpoint = endpoint(execution);
      String effect = "http://host.docker.internal:" + receiver.getAddress().getPort() + "/effect";
      UUID revision = publish(definition, tokenA, workflow(effect));
      Map<String, Object> body = Map.of("revisionId", revision, "input", Map.of("marker", marker));
      String key = UUID.randomUUID().toString();
      // A real same-key JWT with a wrong issuer must be rejected at the deployed HTTP boundary.
      request(
          endpoint,
          "/v1/workflow-executions",
          runtime.keycloak().mintUserTokenWithAlternateIssuer(),
          "POST",
          body,
          key,
          401);
      kc.createServiceAccountClient("matrix-denied", "matrix-denied-test-only");
      kc.grantOrganizationClientRole(runtime.organizationId(), "matrix-no-permission");
      kc.addServiceAccountToOrganization(
          runtime.organizationId(), "matrix-denied", "matrix-no-permission");
      kc.grantTokenAudience("matrix-denied", AuthzenKeycloakFixture.CLIENT_ID);
      request(
          endpoint,
          "/v1/workflow-executions",
          kc.clientCredentialsToken("matrix-denied", "matrix-denied-test-only"),
          "POST",
          body,
          key,
          403);
      var admitted = request(endpoint, "/v1/workflow-executions", tokenA, "POST", body, key, 202);
      String id = admitted.path("id").asText();
      assertFalse(id.isBlank());
      assertEquals(selected.engine(), admitted.path("engineId").asText());
      var replay = request(endpoint, "/v1/workflow-executions", tokenA, "POST", body, key, 202);
      assertEquals(id, replay.path("id").asText());
      request(
          endpoint,
          "/v1/workflow-executions",
          tokenA,
          "POST",
          Map.of("revisionId", revision, "input", Map.of("marker", "different")),
          key,
          409);
      long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      while (!observed.containsKey("before") && System.nanoTime() < deadline) Thread.sleep(500);
      assertTrue(
          observed.containsKey("before"), "The actual operation adapter must reach the receiver");
      assertEquals(marker, observed.get("before").path("marker").asText());
      var running =
          request(endpoint, "/v1/workflow-executions/" + id, tokenA, "GET", null, null, 200);
      assertNotEquals(
          "COMPLETED",
          running.path("state").asText(),
          "Restart must interrupt the waiting execution");
      // Restart the existing container, retaining its Kafka state directory / selected journal.
      engine.getDockerClient().restartContainerCmd(engine.getContainerId()).exec();
      if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
      JsonNode completed = null;
      deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
      while (System.nanoTime() < deadline) {
        completed =
            request(endpoint, "/v1/workflow-executions/" + id, tokenA, "GET", null, null, 200);
        assertNotEquals("FAILED", completed.path("state").asText(), completed.toString());
        if ("COMPLETED".equals(completed.path("state").asText())) break;
        Thread.sleep(1000);
      }
      assertEquals("COMPLETED", completed.path("state").asText(), completed.toString());
      assertEquals(admitted.path("revisionDigest"), completed.path("revisionDigest"));
      assertEquals(admitted.path("revisionId"), completed.path("revisionId"));
      assertEquals(marker, completed.path("output").path("marker").asText());
      assertEquals("after", completed.path("output").path("phase").asText());
      assertTrue(
          observed.containsKey("after"),
          "Recovery must execute the subsequent real adapter effect");
      assertEquals(marker, observed.get("after").path("marker").asText());
      var history =
          request(
              endpoint,
              "/v1/workflow-executions/" + id + "/history",
              tokenA,
              "GET",
              null,
              null,
              200);
      assertTrue(history.path("items").size() > 1, history.toString());
      var finalReplay =
          request(endpoint, "/v1/workflow-executions", tokenA, "POST", body, key, 202);
      assertEquals(id, finalReplay.path("id").asText());
      // A fully authorized actor in another migrated tenant cannot read/control A's execution,
      // or admit a workflow using A's published definition.
      for (String suffix : new String[] {"", "/history"}) {
        request(endpoint, "/v1/workflow-executions/" + id + suffix, tokenB, "GET", null, null, 404);
      }
      request(
          endpoint,
          "/v1/workflow-executions/" + id + "/cancel",
          tokenB,
          "POST",
          Map.of(),
          null,
          404,
          Map.of("If-Match", "\"" + completed.path("version").asLong() + "\""));
      request(endpoint, "/v1/workflow-executions", tokenB, "POST", body, key, 404);
      UUID isolatedRevision = publish(definition, tokenB, isolatedWorkflow());
      var isolated =
          request(
              endpoint,
              "/v1/workflow-executions",
              tokenB,
              "POST",
              Map.of("revisionId", isolatedRevision, "input", Map.of("marker", "isolated")),
              key,
              202);
      String isolatedId = isolated.path("id").asText();
      assertFalse(isolatedId.isBlank());
      assertNotEquals(id, isolatedId, "Idempotency keys must be scoped to a tenant");
      request(endpoint, "/v1/workflow-executions/" + isolatedId, tokenA, "GET", null, null, 404);
      JsonNode isolatedResult = null;
      deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      while (System.nanoTime() < deadline) {
        isolatedResult =
            request(
                endpoint, "/v1/workflow-executions/" + isolatedId, tokenB, "GET", null, null, 200);
        assertNotEquals("FAILED", isolatedResult.path("state").asText(), isolatedResult.toString());
        if ("COMPLETED".equals(isolatedResult.path("state").asText())) break;
        Thread.sleep(500);
      }
      assertEquals("COMPLETED", isolatedResult.path("state").asText(), isolatedResult.toString());
      assertEquals("isolated", isolatedResult.path("output").path("marker").asText());
    } finally {
      receiver.stop(0);
    }
  }

  private static String caller(
      AuthzenKeycloakFixture identity, String organization, String client) {
    String secret = "matrix-identity-test-only";
    identity.createServiceAccountClient(client, secret);
    identity.addServiceAccountToOrganization(organization, client, "matrix-caller");
    identity.grantTokenAudience(client, AuthzenKeycloakFixture.CLIENT_ID);
    return identity.clientCredentialsToken(client, secret);
  }

  private static String isolatedWorkflow() {
    return """
    document:
      dsl: '1.0.3'
      namespace: runtime-acceptance
      name: isolated-tenant
      version: '1.0.0'
    do:
      - preserveInput:
          wait: PT1S
    """;
  }

  private static String workflow(String effect) {
    return """
    document:
      dsl: '1.0.3'
      namespace: runtime-acceptance
      name: adapter-and-recovery
      version: '1.0.0'
    do:
      - beforeRestart:
          call: http
          with:
            method: POST
            endpoint: %s
            output: content
            body:
              marker: '${ .marker }'
              phase: before
      - durableWait:
          wait: PT45S
      - afterRestart:
          call: http
          with:
            method: POST
            endpoint: %s
            output: content
            body:
              marker: '${ .marker }'
              phase: after
    """
        .formatted(effect, effect);
  }
}
