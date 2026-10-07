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
package com.forwardmeasure.datastreaming.executor.pekko;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;

class PekkoSinkHttpContractTest {
  @Test
  void batchesFinishOnlyAfterEveryAuthenticatedWriteIsAcknowledged() throws Exception {
    var system = ActorSystem.create("http-sink-contract");
    try {
      for (var batching :
          List.of(
              new SinkSpec.BatchingSpec(null, null),
              new SinkSpec.BatchingSpec(2, null),
              new SinkSpec.BatchingSpec(2, java.time.Duration.ofMillis(10)))) {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var paths = new CopyOnWriteArrayList<String>();
        var credentials = new CopyOnWriteArrayList<String>();
        var arrived = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        server.createContext(
            "/owned/_doc/",
            exchange -> {
              // Real HTTP Basic challenge: exercise Camel's configured credentials on the wire.
              String auth = exchange.getRequestHeaders().getFirst("Authorization");
              if (auth == null) {
                exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=fixture");
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
              }
              credentials.add(auth);
              paths.add(exchange.getRequestURI().getPath());
              arrived.countDown();
              try {
                release.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              exchange.getRequestBody().readAllBytes();
              exchange.sendResponseHeaders(201, -1);
              exchange.close();
            });
        server.start();
        try (var bridge = new CamelBridge()) {
          var count = new AtomicLong();
          var sink =
              new SinkSpec(
                  "opensearch",
                  url(server),
                  "owned",
                  null,
                  batching,
                  Map.of("idField", "uid", "user", "fixture", "password", "password"));
          var stream =
              Source.from(
                      List.<Map<String, Object>>of(
                          Map.of("uid", "source a"), Map.of("uid", "source+a")))
                  .runWith(
                      PekkoIngestionRunner.buildSink(
                          bridge,
                          sink,
                          DeliverySemantics.defaults(),
                          new ErrorPolicy("fail", "fail"),
                          new ObjectMapper(),
                          count),
                      system)
                  .toCompletableFuture();
          assertTrue(arrived.await(10, TimeUnit.SECONDS));
          assertFalse(stream.isDone(), "Pending writes must prevent terminal success");
          release.countDown();
          stream.get(10, TimeUnit.SECONDS);
          assertEquals(2, count.get());
          assertEquals(
              java.util.Set.of("/owned/_doc/source a", "/owned/_doc/source+a"),
              java.util.Set.copyOf(paths));
          assertTrue(
              credentials.stream()
                  .allMatch(
                      ("Basic "
                              + Base64.getEncoder()
                                  .encodeToString(
                                      "fixture:password".getBytes(StandardCharsets.UTF_8)))
                          ::equals));
        } finally {
          release.countDown();
          server.stop(0);
        }
      }
    } finally {
      system.terminate();
    }
  }

  @Test
  void rejectedHttpWritesAndMissingDocumentIdentityFailCompletion() throws Exception {
    var system = ActorSystem.create("http-sink-rejection-contract");
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var requests = new AtomicLong();
    server.createContext(
        "/",
        exchange -> {
          requests.incrementAndGet();
          exchange.sendResponseHeaders(429, -1);
          exchange.close();
        });
    server.start();
    try (var bridge = new CamelBridge()) {
      for (var options :
          List.of(
              Map.of("idField", "uid"),
              Map.of("idField", "uid", "user", "fixture"),
              Map.of("idField", "uid", "password", "password"))) {
        var spec = new SinkSpec("opensearch", url(server), "owned", null, null, options);
        var sink =
            PekkoIngestionRunner.buildSink(
                bridge,
                spec,
                DeliverySemantics.defaults(),
                new ErrorPolicy("fail", "fail"),
                new ObjectMapper(),
                new AtomicLong());
        assertThrows(
            ExecutionException.class,
            () ->
                Source.single(Map.<String, Object>of("uid", "123"))
                    .runWith(sink, system)
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS));
      }
      long rejectedRequests = requests.get();
      assertTrue(
          rejectedRequests >= 3, "Each write must reach the server; HTTP may retry throttling");
      for (var options :
          List.of(Map.<String, String>of(), Map.of("idField", " "), Map.of("idField", "uid"))) {
        var spec = new SinkSpec("opensearch", url(server), "owned", null, null, options);
        var sink =
            PekkoIngestionRunner.buildSink(
                bridge,
                spec,
                DeliverySemantics.defaults(),
                new ErrorPolicy("fail", "fail"),
                new ObjectMapper(),
                new AtomicLong());
        assertThrows(
            ExecutionException.class,
            () ->
                Source.single(Map.<String, Object>of("name", "Missing ID"))
                    .runWith(sink, system)
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS));
      }
      assertEquals(rejectedRequests, requests.get(), "Invalid identity must never be sent");
    } finally {
      server.stop(0);
      system.terminate();
    }
  }

  @Test
  void retryPolicyObservesExchangeFailuresAndCompletesOnlyAfterAcceptance() throws Exception {
    var system = ActorSystem.create("http-sink-retry-contract");
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var requests = new AtomicLong();
    server.createContext(
        "/",
        exchange -> {
          // 400 is not retried by the HTTP client's throttling policy, so these are pipeline
          // retries.
          exchange.sendResponseHeaders(requests.incrementAndGet() < 3 ? 400 : 201, -1);
          exchange.close();
        });
    server.start();
    try (var bridge = new CamelBridge()) {
      var spec =
          new SinkSpec("opensearch", url(server), "owned", null, null, Map.of("idField", "uid"));
      var written = new AtomicLong();
      var sink =
          PekkoIngestionRunner.buildSink(
              bridge,
              spec,
              DeliverySemantics.defaults(),
              new ErrorPolicy("fail", "retry"),
              new ObjectMapper(),
              written);
      Source.single(Map.<String, Object>of("uid", "123"))
          .runWith(sink, system)
          .toCompletableFuture()
          .get(10, TimeUnit.SECONDS);
      assertEquals(3, requests.get());
      assertEquals(1, written.get());
    } finally {
      server.stop(0);
      system.terminate();
    }
  }

  private static String url(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }
}
