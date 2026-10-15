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
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.WorkflowErrorTypes;
import com.forwardmeasure.testcontainers.gcs.GcsEmulatorTestContainer;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Start INPUT, not protocol response overflow: public HTTP, real packaged engines and storage. */
class FowfStartInputRuntimeMatrixAcceptanceTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String BUCKET = "start-inputs";

  static Stream<FowfRuntimeMatrixAcceptanceTest.Runtime> runtimes() {
    return FowfRuntimeMatrixAcceptanceTest.runtimes();
  }

  @ParameterizedTest(name = "start-input/{0}")
  @MethodSource("runtimes")
  @Timeout(1200)
  void publicStartResolvesLargeInputValidatesItAndRecoversAcrossEngines(
      FowfRuntimeMatrixAcceptanceTest.Runtime selected) throws Exception {
    String run = UUID.randomUUID().toString();
    try (var runtime =
            RealFowfWorkflowFixture.start(
                "start-" + run, "matrix-caller", selected.framework(), selected.persistence());
        var storage = new GcsEmulatorTestContainer(runtime.network(), "gcs").start()) {
      storageRequest(
          storage,
          "POST",
          "/storage/v1/b?project=acceptance",
          JSON.writeValueAsBytes(Map.of("name", BUCKET)),
          200);
      // Larger than 32 KiB by design, so passing cannot depend on lowering the protocol threshold.
      runtime.configureOverflow(storage.networkEndpoint().toString(), BUCKET, 1_000_000, 2_000_000);
      String otherOrganization = runtime.provisionAdditionalTenant("other-" + run, "matrix-caller");
      boolean pekko = selected.engine().equals("pekko");
      var execution = runtime.startExecutionManagement(selected.engine());
      var engine = pekko ? runtime.startEnginePekko() : runtime.startEngineKafkaStreams();
      var definition = runtime.startDefinitionManagement();
      // Pekko readiness includes its operation-adapter cluster member even for a set-only flow.
      var adapter = runtime.startHttpOperationAdapter(pekko);
      if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
      String api = endpoint(execution);
      String token = caller(runtime, runtime.organizationId(), "start-a");
      String otherToken = caller(runtime, otherOrganization, "start-b");
      UUID revision = publish(definition, token, workflow(false));

      for (int bytes : new int[] {32767, 32768, 32769, 65536}) {
        ObjectNode input = inputBytes(bytes);
        String key = UUID.randomUUID().toString();
        var body = Map.of("revisionId", revision, "input", input);
        // Authentication must still run before any input is admitted or offloaded.
        request(
            api,
            "/v1/workflow-executions",
            runtime.keycloak().mintUserTokenWithAlternateIssuer(),
            "POST",
            body,
            key,
            401);
        JsonNode admitted = request(api, "/v1/workflow-executions", token, "POST", body, key, 202);
        String id = admitted.path("id").asText();
        assertFalse(id.isBlank());
        assertEquals(selected.engine(), admitted.path("engineId").asText());
        assertOutput(awaitTerminal(api, token, id), input);
        assertEquals(
            id,
            request(api, "/v1/workflow-executions", token, "POST", body, key, 202)
                .path("id")
                .asText());
        request(
            api,
            "/v1/workflow-executions",
            token,
            "POST",
            Map.of("revisionId", revision, "input", input.deepCopy().put("marker", "changed")),
            key,
            409);
        request(api, "/v1/workflow-executions/" + id, otherToken, "GET", null, null, 404);
        request(
            api, "/v1/workflow-executions/" + id + "/history", otherToken, "GET", null, null, 404);
        if (!pekko) {
          List<String> artifacts = artifactsFor(storage, input.path("marker").asText());
          assertEquals(
              bytes > 32768,
              !artifacts.isEmpty(),
              "Start input inline boundary in serialized bytes");
          for (String artifact : artifacts)
            assertTrue(
                artifact.startsWith("runtime-acceptance/" + runtime.tenantId().value() + "/"),
                artifact);
        }
      }

      ObjectNode unicode =
          JSON.createObjectNode()
              .put("marker", "unicode-" + run)
              .put("alphabet", "é")
              .put("expectedCharacters", 20000)
              .put("blob", "é".repeat(20000));
      assertTrue(unicode.toString().length() < 32768);
      assertTrue(JSON.writeValueAsBytes(unicode).length > 32768);
      var admittedUnicode =
          request(
              api,
              "/v1/workflow-executions",
              token,
              "POST",
              Map.of("revisionId", revision, "input", unicode),
              UUID.randomUUID().toString(),
              202);
      assertOutput(awaitTerminal(api, token, admittedUnicode.path("id").asText()), unicode);

      // Validate the original large input, not the encoded DataReference object.
      var invalid = inputBytes(65536).put("blob", 42).put("padding", "x".repeat(65536));
      var rejected =
          request(
              api,
              "/v1/workflow-executions",
              token,
              "POST",
              Map.of("revisionId", revision, "input", invalid),
              UUID.randomUUID().toString(),
              202);
      var failed = awaitTerminal(api, token, rejected.path("id").asText());
      assertEquals("FAILED", failed.path("state").asText(), failed.toString());
      failed = awaitFailureProjection(api, token, rejected.path("id").asText());
      assertFalse(
          failed.path("error").isNull() || failed.path("error").isMissingNode(), failed.toString());
      assertEquals(WorkflowErrorTypes.VALIDATION, failed.path("error").path("code").asText());
      assertEquals("/input/schema", failed.path("error").path("taskPath").asText());
      assertFalse(failed.path("error").path("retryable").asBoolean());

      UUID recoveryRevision = publish(definition, token, workflow(true));
      ObjectNode recoveryInput = inputBytes(65536);
      String recoveryKey = UUID.randomUUID().toString();
      var recoveryBody = Map.of("revisionId", recoveryRevision, "input", recoveryInput);
      var started =
          request(api, "/v1/workflow-executions", token, "POST", recoveryBody, recoveryKey, 202);
      String recoveryId = started.path("id").asText();
      awaitTimer(api, token, recoveryId);
      engine.getDockerClient().stopContainerCmd(engine.getContainerId()).exec();
      engine.getDockerClient().startContainerCmd(engine.getContainerId()).exec();
      if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
      var completed = awaitTerminal(api, token, recoveryId);
      assertOutput(completed, recoveryInput);
      assertEquals(started.path("revisionDigest"), completed.path("revisionDigest"));
      assertEquals(
          recoveryId,
          request(api, "/v1/workflow-executions", token, "POST", recoveryBody, recoveryKey, 202)
              .path("id")
              .asText());

      UUID otherRevision = publish(definition, otherToken, workflow(false));
      var isolated =
          request(
              api,
              "/v1/workflow-executions",
              otherToken,
              "POST",
              Map.of("revisionId", otherRevision, "input", recoveryInput),
              recoveryKey,
              202);
      assertNotEquals(recoveryId, isolated.path("id").asText());
      assertOutput(awaitTerminal(api, otherToken, isolated.path("id").asText()), recoveryInput);
      request(
          api,
          "/v1/workflow-executions/" + isolated.path("id").asText(),
          token,
          "GET",
          null,
          null,
          404);
    }
  }

  private static ObjectNode inputBytes(int bytes) throws Exception {
    var input =
        JSON.createObjectNode()
            .put("marker", UUID.randomUUID().toString())
            .put("alphabet", "x")
            .put("expectedCharacters", 10000)
            .put("blob", "");
    int length = bytes - JSON.writeValueAsBytes(input).length;
    input.put("expectedCharacters", length).put("blob", "x".repeat(length));
    assertEquals(bytes, JSON.writeValueAsBytes(input).length);
    return input;
  }

  private static void assertOutput(JsonNode terminal, JsonNode input) {
    assertEquals("COMPLETED", terminal.path("state").asText(), terminal.toString());
    var output = terminal.path("output");
    assertEquals(input.path("marker"), output.path("marker"));
    assertEquals(input.path("expectedCharacters").asInt(), output.path("length").asInt());
    assertTrue(output.path("contentMatches").asBoolean(), output.toString());
  }

  private static JsonNode awaitTerminal(String api, String token, String id) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
    JsonNode last = null;
    while (System.nanoTime() < deadline) {
      last = request(api, "/v1/workflow-executions/" + id, token, "GET", null, null, 200);
      if (List.of("COMPLETED", "FAILED", "CANCELLED").contains(last.path("state").asText()))
        return last;
      Thread.sleep(250);
    }
    throw new AssertionError("No terminal execution: " + last);
  }

  private static JsonNode awaitFailureProjection(String api, String token, String id)
      throws Exception {
    // A Pekko acknowledgement can mark the execution FAILED before its journal projection has
    // delivered the error. Require convergence of both the result and the persisted failure fact.
    long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
    JsonNode last = null;
    while (System.nanoTime() < deadline) {
      last = request(api, "/v1/workflow-executions/" + id, token, "GET", null, null, 200);
      assertEquals("FAILED", last.path("state").asText(), last.toString());
      if (last.path("error").isObject()) {
        var history =
            request(
                api, "/v1/workflow-executions/" + id + "/history", token, "GET", null, null, 200);
        for (var event : history.path("items")) {
          if (!"FAILED".equals(event.path("type").asText())) continue;
          assertEquals(last.path("error").path("code"), event.path("data").path("code"));
          assertEquals(last.path("error").path("message"), event.path("data").path("message"));
          return last;
        }
      }
      Thread.sleep(250);
    }
    throw new AssertionError("Failed execution did not project its error and history: " + last);
  }

  private static void awaitTimer(String api, String token, String id) throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
    while (System.nanoTime() < deadline) {
      var execution = request(api, "/v1/workflow-executions/" + id, token, "GET", null, null, 200);
      assertNotEquals("FAILED", execution.path("state").asText(), execution.toString());
      assertNotEquals("COMPLETED", execution.path("state").asText(), "Missed restart window");
      var history =
          request(api, "/v1/workflow-executions/" + id + "/history", token, "GET", null, null, 200);
      for (var event : history.path("items")) {
        if ("TIMER_SCHEDULED".equals(event.path("type").asText())
            && event.path("taskPath").asText().contains("restoreWindow")) return;
      }
      Thread.sleep(250);
    }
    fail("Start input did not reach a durable timer before restart");
  }

  private static String caller(
      RealFowfWorkflowFixture runtime, String organization, String client) {
    var identity = runtime.keycloak();
    String secret = "start-fixture-only";
    identity.createServiceAccountClient(client, secret);
    identity.addServiceAccountToOrganization(organization, client, "matrix-caller");
    identity.grantTokenAudience(
        client, com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture.CLIENT_ID);
    return identity.clientCredentialsToken(client, secret);
  }

  private static String workflow(boolean recovery) {
    return """
    document:
      dsl: '1.0.3'
      namespace: start-input
      name: start-%s
      version: '1.0.0'
    input:
      schema:
        format: json
        document:
          type: object
          required: [blob, marker, alphabet, expectedCharacters]
          properties:
            blob: {type: string}
            marker: {type: string}
            alphabet: {type: string}
            expectedCharacters: {type: integer}
      from: '${ . }'
    do:
    %s
      - observe:
          set:
            marker: '${ .marker }'
            length: '${ .blob | length }'
            contentMatches: '${ .blob == (.alphabet * .expectedCharacters) }'
    """
        .formatted(UUID.randomUUID(), recovery ? "  - restoreWindow:\n      wait: PT90S" : "");
  }

  private static List<String> artifactsFor(GcsEmulatorTestContainer storage, String marker)
      throws Exception {
    var listing =
        JSON.readTree(storageRequest(storage, "GET", "/storage/v1/b/" + BUCKET + "/o", null, 200));
    var found = new ArrayList<String>();
    for (var object : listing.path("items")) {
      String name = object.path("name").asText();
      byte[] content =
          storageRequest(
              storage,
              "GET",
              "/storage/v1/b/"
                  + BUCKET
                  + "/o/"
                  + URLEncoder.encode(name, StandardCharsets.UTF_8)
                  + "?alt=media",
              null,
              200);
      if (new String(content, StandardCharsets.UTF_8).contains(marker)) found.add(name);
    }
    return found;
  }

  private static byte[] storageRequest(
      GcsEmulatorTestContainer storage, String method, String path, byte[] body, int expected)
      throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      var response =
          client.send(
              HttpRequest.newBuilder(URI.create(storage.hostEndpoint() + path))
                  .timeout(Duration.ofSeconds(15))
                  .header("Content-Type", "application/json")
                  .method(
                      method,
                      body == null
                          ? HttpRequest.BodyPublishers.noBody()
                          : HttpRequest.BodyPublishers.ofByteArray(body))
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());
      assertEquals(
          expected, response.statusCode(), new String(response.body(), StandardCharsets.UTF_8));
      return response.body();
    }
  }
}
