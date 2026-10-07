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
package com.forwardmeasure.datastreaming.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

@WithOpenSearchContainer
class OpenSearchSinkRowWriterContractTest {
  @Test
  void replayPreservesExactIdsAndRawPayload(OpenSearchTestContainer opensearch) throws Exception {
    String endpoint = opensearch.hostEndpoint().toString();
    String index = "writer-" + UUID.randomUUID();
    var ids = List.of("source a", "source+a", "source/a?b#é");
    try (var writer =
            new OpenSearchSinkRowWriter(
                sink(
                    endpoint,
                    index,
                    Map.of(
                        "idField",
                        "uid",
                        "indexSettingsJsonBase64",
                        Base64.getEncoder()
                            .encodeToString(
                                "{\"mappings\":{\"properties\":{\"source_data\":{\"type\":\"object\",\"dynamic\":false}}}}"
                                    .getBytes(StandardCharsets.UTF_8)))));
        var client = HttpClient.newHttpClient()) {
      for (String id : ids) {
        writer.write(Map.of("uid", id, "revision", 1));
        writer.write(
            Map.of(
                "uid", id, "revision", 2, "source_data", Map.of("vendor", List.of(0, false, ""))));
      }
      for (String id : ids) {
        var response =
            client.send(
                HttpRequest.newBuilder(
                        URI.create(
                            endpoint
                                + "/"
                                + index
                                + "/_doc/"
                                + URLEncoder.encode(id, StandardCharsets.UTF_8)
                                    .replace("+", "%20")))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        var document = new ObjectMapper().readTree(response.body());
        assertEquals(id, document.path("_id").asText());
        assertEquals(id, document.path("_source").path("uid").asText());
        assertEquals(2, document.path("_source").path("revision").asInt());
        assertEquals(
            "[0,false,\"\"]",
            document.path("_source").path("source_data").path("vendor").toString());
      }
    }
  }

  @Test
  void authenticationAndRejectedWritesAreVisibleToTheCaller() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var requests = new AtomicInteger();
    var auth = new AtomicReference<String>();
    var contentType = new AtomicReference<String>();
    server.createContext(
        "/owned/_doc/",
        exchange -> {
          requests.incrementAndGet();
          auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
          exchange.sendResponseHeaders(429, -1);
          exchange.close();
        });
    server.start();
    try (var writer =
        new OpenSearchSinkRowWriter(
            sink(
                url(server),
                "owned",
                Map.of("idField", "uid", "user", "fixture", "password", "password")))) {
      var failure =
          assertThrows(IllegalStateException.class, () -> writer.write(Map.of("uid", "123")));
      assertTrue(failure.getMessage().contains("429"));
      assertEquals(
          "Basic "
              + Base64.getEncoder()
                  .encodeToString("fixture:password".getBytes(StandardCharsets.UTF_8)),
          auth.get());
      assertEquals("application/json", contentType.get());
      assertThrows(IllegalArgumentException.class, () -> writer.write(Map.of("other", "123")));
      assertThrows(
          java.io.UncheckedIOException.class,
          () -> writer.write(Map.of("uid", "123", "unsupported", new Object())));
      assertEquals(1, requests.get(), "Invalid rows must never reach the sink");
    } finally {
      server.stop(0);
    }
    for (var options : List.of(Map.<String, String>of(), Map.of("idField", " ")))
      assertThrows(
          IllegalArgumentException.class,
          () -> new OpenSearchSinkRowWriter(sink("http://unused.invalid", "owned", options)));
  }

  @Test
  void cancellationRemainsVisibleToTheDeliveryLoop() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var arrived = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var interrupted = new AtomicBoolean();
    var failure = new AtomicReference<Throwable>();
    server.createContext(
        "/owned/_doc/",
        exchange -> {
          arrived.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          exchange.close();
        });
    server.start();
    try (var writer =
        new OpenSearchSinkRowWriter(sink(url(server), "owned", Map.of("idField", "uid")))) {
      var worker =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      writer.write(Map.of("uid", "123"));
                    } catch (RuntimeException e) {
                      failure.set(e);
                      interrupted.set(Thread.currentThread().isInterrupted());
                    }
                  });
      try {
        assertTrue(arrived.await(10, TimeUnit.SECONDS));
        worker.interrupt();
        worker.join(5000);
        assertFalse(worker.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertTrue(interrupted.get(), "Cancelled delivery must not lose its interrupt flag");
      } finally {
        release.countDown();
        worker.interrupt();
        worker.join(5000);
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void unavailableSinkCannotAcknowledgeDelivery() throws Exception {
    int port;
    try (var reserved = new java.net.ServerSocket(0)) {
      port = reserved.getLocalPort();
    }
    try (var writer =
        new OpenSearchSinkRowWriter(
            sink(
                "http://127.0.0.1:" + port,
                "owned",
                Map.of("idField", "uid", "user", "fixture")))) {
      var failure =
          assertThrows(IllegalStateException.class, () -> writer.write(Map.of("uid", "123")));
      assertInstanceOf(java.io.IOException.class, failure.getCause());
    }
  }

  private static SinkSpec sink(String url, String index, Map<String, String> options) {
    return new SinkSpec("opensearch", url, index, null, null, options);
  }

  private static String url(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }
}
