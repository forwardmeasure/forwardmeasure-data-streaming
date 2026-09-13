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
package com.forwardmeasure.datastreaming.launcher.application.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocking-framework proof at the actual HTTP protocol level - same plain JDK {@link
 * HttpServer} stand-in technique already used for fowf's own execution-management service in {@code
 * WorkflowIngestionLauncherTest}, this time standing in for a Keycloak token endpoint.
 */
final class KeycloakClientCredentialsTokenSupplierTest {

  private HttpServer server;
  private final AtomicInteger requestCount = new AtomicInteger();
  private final AtomicReference<String> lastBody = new AtomicReference<>();
  private volatile String tokenToReturn = "token-1";
  private volatile long expiresInSeconds = 3600;

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/token", this::respond);
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void fetchesAndReturnsARealAccessToken() {
    KeycloakClientCredentialsTokenSupplier supplier = supplier(Clock.systemUTC());

    String token = supplier.get();

    assertEquals("token-1", token);
    assertEquals(1, requestCount.get());
    assertTrue(lastBody.get().contains("grant_type=client_credentials"));
    assertTrue(lastBody.get().contains("client_id=test-client"));
    assertTrue(lastBody.get().contains("client_secret=test-secret"));
  }

  @Test
  void cachesTheTokenUntilShortlyBeforeItExpires() {
    expiresInSeconds = 3600;
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    Clock movable = Clock.fixed(now.get(), ZoneOffset.UTC);
    KeycloakClientCredentialsTokenSupplier supplier =
        new KeycloakClientCredentialsTokenSupplier(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/token"),
            "test-client",
            "test-secret",
            HttpClient.newHttpClient(),
            new MovableClock(now));

    String first = supplier.get();
    now.set(now.get().plusSeconds(10));
    String second = supplier.get();

    assertEquals(first, second);
    assertEquals(1, requestCount.get(), "expected the cached token, not a second fetch");
  }

  @Test
  void refetchesOnceTheCachedTokenIsNearExpiry() {
    expiresInSeconds = 60;
    tokenToReturn = "token-1";
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    KeycloakClientCredentialsTokenSupplier supplier =
        new KeycloakClientCredentialsTokenSupplier(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/token"),
            "test-client",
            "test-secret",
            HttpClient.newHttpClient(),
            new MovableClock(now));

    String first = supplier.get();
    tokenToReturn = "token-2";
    now.set(now.get().plusSeconds(45));
    String second = supplier.get();

    assertEquals("token-1", first);
    assertEquals("token-2", second);
    assertEquals(2, requestCount.get());
  }

  private KeycloakClientCredentialsTokenSupplier supplier(Clock clock) {
    return new KeycloakClientCredentialsTokenSupplier(
        URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/token"),
        "test-client",
        "test-secret",
        HttpClient.newHttpClient(),
        clock);
  }

  private void respond(HttpExchange exchange) throws IOException {
    requestCount.incrementAndGet();
    lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    String json =
        "{\"access_token\":\"" + tokenToReturn + "\",\"expires_in\":" + expiresInSeconds + "}";
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  /** A {@link Clock} whose instant tracks a mutable reference, so a test can advance time. */
  private static final class MovableClock extends Clock {
    private final AtomicReference<Instant> now;

    MovableClock(AtomicReference<Instant> now) {
      this.now = now;
    }

    @Override
    public java.time.ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now.get();
    }
  }
}
