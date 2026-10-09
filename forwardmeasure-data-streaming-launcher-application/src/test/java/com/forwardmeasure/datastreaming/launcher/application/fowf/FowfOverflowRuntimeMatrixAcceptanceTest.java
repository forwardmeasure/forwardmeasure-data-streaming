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
import com.forwardmeasure.testcontainers.gcs.GcsEmulatorTestContainer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Packaged services, public admission, real GCS protocol storage and durable engine recovery. */
class FowfOverflowRuntimeMatrixAcceptanceTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private static final String BUCKET = "runtime-overflow";

  private enum Scenario {
    INLINE,
    RECOVER,
    CORRUPT,
    MISSING,
    MAXIMUM,
    STORAGE_WRITE_FAILURE
  }

  static Stream<FowfRuntimeMatrixAcceptanceTest.Runtime> runtimes() {
    return FowfRuntimeMatrixAcceptanceTest.runtimes();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("runtimes")
  @Timeout(1200)
  void deployedOverflowSurvivesRestartAndFailsClosedOnDataLoss(
      FowfRuntimeMatrixAcceptanceTest.Runtime selected) throws Exception {
    String run = UUID.randomUUID().toString();
    var inputs = new ConcurrentHashMap<String, byte[]>();
    var effects = new ConcurrentHashMap<String, JsonNode>();
    var server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
    server.createContext(
        "/source/",
        exchange -> {
          try (exchange) {
            byte[] bytes =
                inputs.get(exchange.getRequestURI().getPath().substring("/source/".length()));
            if (bytes == null) {
              exchange.sendResponseHeaders(404, -1);
              return;
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
          }
        });
    server.createContext(
        "/effect",
        exchange -> {
          try (exchange) {
            JsonNode body = JSON.readTree(exchange.getRequestBody());
            effects.put(body.path("marker").asText(), body);
            byte[] bytes = JSON.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
          }
        });
    server.start();
    try (var runtime =
            RealFowfWorkflowFixture.start(
                "overflow-" + run,
                "overflow-caller",
                selected.framework(),
                selected.persistence());
        var storage = new GcsEmulatorTestContainer(runtime.network(), "gcs").start()) {
      storageRequest(
          storage,
          "POST",
          "/storage/v1/b?project=acceptance",
          JSON.writeValueAsBytes(Map.of("name", BUCKET)),
          200);
      runtime.configureOverflow(storage.networkEndpoint().toString(), BUCKET, 1024, 16384);
      var execution = runtime.startExecutionManagement(selected.engine());
      boolean pekko = selected.engine().equals("pekko");
      var engine = pekko ? runtime.startEnginePekko() : runtime.startEngineKafkaStreams();
      var definition = runtime.startDefinitionManagement();
      var adapter = runtime.startHttpOperationAdapter(pekko);
      if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
      String api = endpoint(execution);
      String source = "http://host.docker.internal:" + server.getAddress().getPort();
      for (Scenario scenario : Scenario.values()) {
        String marker = run + "-" + scenario;
        int size = scenario == Scenario.INLINE ? 32 : scenario == Scenario.MAXIMUM ? 32768 : 8192;
        byte[] payload = JSON.writeValueAsBytes(Map.of("marker", marker, "blob", "x".repeat(size)));
        inputs.put(marker, payload);
        boolean recovery =
            scenario == Scenario.RECOVER
                || scenario == Scenario.CORRUPT
                || scenario == Scenario.MISSING;
        if (scenario == Scenario.STORAGE_WRITE_FAILURE) {
          // Make the real bucket unavailable. Do not fake a storage client's exception.
          for (String key : objectKeys(storage)) deleteObject(storage, key);
          storageRequest(storage, "DELETE", "/storage/v1/b/" + BUCKET, null, 204);
        }
        UUID revision =
            publish(
                definition,
                runtime.keycloak().mintUserToken(),
                workflow(marker, source + "/source/" + marker, source + "/effect", recovery));
        String key = UUID.randomUUID().toString();
        Map<String, Object> admission = Map.of("revisionId", revision, "input", Map.of());
        JsonNode started =
            request(
                api,
                "/v1/workflow-executions",
                runtime.keycloak().mintUserToken(),
                "POST",
                admission,
                key,
                202);
        String id = started.path("id").asText();
        assertFalse(id.isBlank());
        assertEquals(selected.engine(), started.path("engineId").asText());
        if (recovery) {
          awaitDurableWait(runtime, api, id);
          List<String> artifacts = objectsContaining(storage, marker);
          assertFalse(artifacts.isEmpty(), "The real response must have been offloaded");
          String prefix = "runtime-acceptance/" + runtime.tenantId().value() + "/";
          for (String artifact : artifacts) assertTrue(artifact.startsWith(prefix), artifact);
          // Stop before mutating, so no live in-memory copy can mask a failed restore.
          engine.getDockerClient().stopContainerCmd(engine.getContainerId()).exec();
          if (scenario == Scenario.CORRUPT) {
            for (String artifact : artifacts) {
              byte[] original =
                  storageRequest(storage, "GET", objectPath(artifact) + "?alt=media", null, 200);
              byte[] changed = original.clone();
              // Preserve length and valid JSON. A size-only check cannot catch this corruption.
              for (int i = 0; i < changed.length; i++) {
                if (changed[i] == 'x') {
                  changed[i] = 'y';
                  break;
                }
              }
              assertFalse(java.util.Arrays.equals(original, changed));
              storageRequest(
                  storage,
                  "POST",
                  "/upload/storage/v1/b/" + BUCKET + "/o?uploadType=media&name=" + encode(artifact),
                  changed,
                  200);
            }
          } else if (scenario == Scenario.MISSING) {
            for (String artifact : artifacts) deleteObject(storage, artifact);
          }
          engine.getDockerClient().startContainerCmd(engine.getContainerId()).exec();
          if (pekko) runtime.awaitPekkoClusterReady(engine, adapter);
        }
        boolean success = scenario == Scenario.INLINE || scenario == Scenario.RECOVER;
        JsonNode terminal = awaitTerminal(runtime, api, id);
        assertEquals(
            success ? "COMPLETED" : "FAILED",
            terminal.path("state").asText(),
            scenario + ": " + terminal);
        assertEquals(started.path("revisionDigest"), terminal.path("revisionDigest"));
        if (success) {
          assertEquals(marker, terminal.path("output").path("marker").asText());
          assertEquals(size, terminal.path("output").path("length").asInt());
          assertEquals(size, effects.get(marker).path("length").asInt());
          if (scenario == Scenario.INLINE) assertTrue(objectsContaining(storage, marker).isEmpty());
        } else {
          assertFalse(
              effects.containsKey(marker), "Failed restore/capture must not run the next effect");
          assertFalse(terminal.path("error").isMissingNode());
          assertFalse(terminal.path("error").isNull(), terminal.toString());
          if (scenario == Scenario.MAXIMUM)
            assertTrue(
                objectsContaining(storage, marker).isEmpty(),
                "An over-limit response must not leave a partial artifact");
        }
        var replay =
            request(
                api,
                "/v1/workflow-executions",
                runtime.keycloak().mintUserToken(),
                "POST",
                admission,
                key,
                202);
        assertEquals(id, replay.path("id").asText());
      }
    } finally {
      server.stop(0);
    }
  }

  private static void awaitDurableWait(RealFowfWorkflowFixture runtime, String api, String id)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
    JsonNode last = null;
    while (System.nanoTime() < deadline) {
      last =
          request(
              api,
              "/v1/workflow-executions/" + id,
              runtime.keycloak().mintUserToken(),
              "GET",
              null,
              null,
              200);
      assertNotEquals("FAILED", last.path("state").asText(), last.toString());
      assertNotEquals("COMPLETED", last.path("state").asText(), "Recovery window was missed");
      if (!last.path("timers").isEmpty()) return;
      Thread.sleep(250);
    }
    fail("No durable timer was exposed before restart: " + last);
  }

  private static JsonNode awaitTerminal(RealFowfWorkflowFixture runtime, String api, String id)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
    JsonNode last = null;
    while (System.nanoTime() < deadline) {
      last =
          request(
              api,
              "/v1/workflow-executions/" + id,
              runtime.keycloak().mintUserToken(),
              "GET",
              null,
              null,
              200);
      if (List.of("COMPLETED", "FAILED", "CANCELLED").contains(last.path("state").asText()))
        return last;
      Thread.sleep(500);
    }
    throw new AssertionError("Execution did not terminate: " + last);
  }

  private static String workflow(String name, String source, String effect, boolean wait) {
    return """
    document:
      dsl: '1.0.3'
      namespace: runtime-overflow
      name: bounded-response-%s
      version: '1.0.0'
    do:
      - receive:
          call: http
          with:
            method: GET
            endpoint: %s
            output: content
    %s
      - observe:
          call: http
          with:
            method: POST
            endpoint: %s
            output: content
            body:
              marker: '${ .marker }'
              length: '${ .blob | length }'
    """
        .formatted(name, source, wait ? "  - restoreWindow:\n      wait: PT90S" : "", effect);
  }

  private static List<String> objectKeys(GcsEmulatorTestContainer storage) throws Exception {
    var result =
        JSON.readTree(storageRequest(storage, "GET", "/storage/v1/b/" + BUCKET + "/o", null, 200));
    var keys = new ArrayList<String>();
    result.path("items").forEach(item -> keys.add(item.path("name").asText()));
    return keys;
  }

  private static List<String> objectsContaining(GcsEmulatorTestContainer storage, String marker)
      throws Exception {
    var keys = new ArrayList<String>();
    for (String key : objectKeys(storage)) {
      String content =
          new String(
              storageRequest(storage, "GET", objectPath(key) + "?alt=media", null, 200),
              StandardCharsets.UTF_8);
      if (content.contains(marker)) keys.add(key);
    }
    return keys;
  }

  private static void deleteObject(GcsEmulatorTestContainer storage, String key) throws Exception {
    storageRequest(storage, "DELETE", objectPath(key), null, 204);
  }

  private static String objectPath(String key) {
    return "/storage/v1/b/" + BUCKET + "/o/" + encode(key);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static byte[] storageRequest(
      GcsEmulatorTestContainer storage, String method, String path, byte[] body, int expected)
      throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create(storage.hostEndpoint() + path))
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json")
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body))
            .build();
    var response = HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());
    assertEquals(
        expected, response.statusCode(), new String(response.body(), StandardCharsets.UTF_8));
    return response.body();
  }
}
