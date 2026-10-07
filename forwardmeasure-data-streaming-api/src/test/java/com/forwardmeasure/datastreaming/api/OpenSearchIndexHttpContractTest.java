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
package com.forwardmeasure.datastreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OpenSearchIndexHttpContractTest {
  @Test
  void authenticatedCreationAndReuseCarryTheSameContractAndCredentials() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var requests = new CopyOnWriteArrayList<String>();
    var authorization = new CopyOnWriteArrayList<String>();
    var created = new AtomicBoolean();
    String body =
        "{\"mappings\":{\"_meta\":{\"owner\":\"tenant-a\",\"keys\":[\"uid\"]}},\"settings\":{\"index\":{\"number_of_shards\":1}}}";
    var receivedBody = new AtomicReference<String>();
    server.createContext(
        "/owned",
        exchange -> {
          requests.add(exchange.getRequestMethod());
          authorization.add(exchange.getRequestHeaders().getFirst("Authorization"));
          String response = "{}";
          int status = 200;
          if (exchange.getRequestMethod().equals("HEAD")) status = created.get() ? 200 : 404;
          else if (exchange.getRequestMethod().equals("PUT")) {
            receivedBody.set(
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            created.set(true);
          } else if (exchange.getRequestURI().getPath().endsWith("_mapping"))
            response =
                "{\"owned\":{\"mappings\":{\"_meta\":{\"owner\":\"tenant-a\",\"keys\":[\"uid\"]}}}}";
          else response = "{\"owned\":{\"settings\":{\"index\":{\"number_of_shards\":\"1\"}}}}";
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(
              status, exchange.getRequestMethod().equals("HEAD") ? -1 : bytes.length);
          if (!exchange.getRequestMethod().equals("HEAD")) exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();
    try {
      var options = new java.util.HashMap<>(inline(body));
      options.put("user", "fixture-user");
      options.put("password", "fixture-password");
      OpenSearchIndexInitializer.ensureIndex(url(server), "owned", options);
      OpenSearchIndexInitializer.ensureIndex(url(server), "owned", options);
      assertEquals(body, receivedBody.get());
      assertEquals(java.util.List.of("HEAD", "PUT", "HEAD", "GET", "GET"), requests);
      String expected =
          "Basic "
              + Base64.getEncoder()
                  .encodeToString("fixture-user:fixture-password".getBytes(StandardCharsets.UTF_8));
      assertTrue(authorization.stream().allMatch(expected::equals));
      assertThrows(
          IllegalStateException.class,
          () ->
              OpenSearchIndexInitializer.ensureIndex(
                  url(server), "owned", inline(body.replace("[\"uid\"]", "[\"other\"]"))));
      assertThrows(
          IllegalStateException.class,
          () ->
              OpenSearchIndexInitializer.ensureIndex(
                  url(server), "owned", inline(body.replace(":1", ":2"))));
      assertEquals(1, requests.stream().filter("PUT"::equals).count());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void transportFailuresNeverBecomePermissionToCreateOrReuseAnIndex() throws Exception {
    for (String failing : java.util.List.of("HEAD", "PUT", "GET")) {
      var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      var requests = new CopyOnWriteArrayList<String>();
      server.createContext(
          "/owned",
          exchange -> {
            String method = exchange.getRequestMethod();
            requests.add(method);
            int status = method.equals(failing) ? 503 : failing.equals("GET") ? 200 : 404;
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
          });
      server.start();
      try {
        assertThrows(
            RuntimeException.class,
            () -> OpenSearchIndexInitializer.ensureIndex(url(server), "owned", inline("{}")));
        assertEquals(
            failing.equals("HEAD") ? java.util.List.of("HEAD") : java.util.List.of("HEAD", failing),
            requests);
      } finally {
        server.stop(0);
      }
    }
  }

  @Test
  void cancellationPreservesInterruptionDuringEveryNetworkStage() throws Exception {
    for (String blocked : java.util.List.of("HEAD", "PUT", "GET")) {
      var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      var arrived = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var interrupted = new AtomicBoolean();
      var failure = new AtomicReference<Throwable>();
      server.createContext(
          "/owned",
          exchange -> {
            if (exchange.getRequestMethod().equals(blocked)) {
              arrived.countDown();
              try {
                release.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            exchange.sendResponseHeaders(blocked.equals("GET") ? 200 : 404, -1);
            exchange.close();
          });
      server.start();
      Thread worker =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      OpenSearchIndexInitializer.ensureIndex(url(server), "owned", inline("{}"));
                    } catch (RuntimeException e) {
                      failure.set(e);
                      interrupted.set(Thread.currentThread().isInterrupted());
                    }
                  });
      try {
        assertTrue(arrived.await(10, TimeUnit.SECONDS), blocked);
        worker.interrupt();
        worker.join(5000);
        assertTrue(failure.get() != null, blocked + " must fail on cancellation");
        assertTrue(interrupted.get(), blocked + " swallowed thread interruption");
      } finally {
        release.countDown();
        worker.interrupt();
        worker.join(5000);
        server.stop(0);
      }
    }
  }

  @Test
  void invalidNamesSettingsAndAmbiguousSourcesFailBeforeNetwork() {
    for (String name : new String[] {null, "Upper", "a/b", "_hidden", ""})
      assertThrows(
          IllegalArgumentException.class,
          () ->
              OpenSearchIndexInitializer.ensureIndex("http://unused.invalid", name, inline("{}")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OpenSearchIndexInitializer.ensureIndex(
                "http://unused.invalid",
                "owned",
                Map.of("indexSettingsFile", "file", "indexSettingsJsonBase64", "e30=")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OpenSearchIndexInitializer.ensureIndex(
                "http://unused.invalid", "owned", inline(" ".repeat(1048577))));
    assertThrows(
        java.io.UncheckedIOException.class,
        () ->
            OpenSearchIndexInitializer.ensureIndex(
                "http://unused.invalid", "owned", inline("not-json")));
    OpenSearchIndexInitializer.ensureIndex(
        "http://unused.invalid",
        "owned",
        Map.of("indexSettingsFile", " ", "indexSettingsJsonBase64", " "));
  }

  private static Map<String, String> inline(String body) {
    return Map.of(
        "indexSettingsJsonBase64",
        Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)));
  }

  private static String url(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }
}
