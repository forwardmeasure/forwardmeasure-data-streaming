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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A real OAuth2 client-credentials grant against a Keycloak (or any compliant) token endpoint -
 * framework-agnostic (plain {@link HttpClient}, no CDI/Spring/Micronaut annotation), so every
 * framework binding can hand the same {@link #get()} directly to fowf's generated {@code
 * ApiClient#setBearerToken(java.util.function.Supplier)} rather than each reimplementing this.
 *
 * <p>Caches the token until shortly before its own {@code expires_in}, then transparently refetches
 * - {@code ApiClient} calls {@link #get()} fresh on every request, so a caller never needs its own
 * refresh scheduling.
 */
public final class KeycloakClientCredentialsTokenSupplier implements Supplier<String> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long EXPIRY_SAFETY_MARGIN_SECONDS = 30;

  private final URI tokenUri;
  private final String clientId;
  private final String clientSecret;
  private final HttpClient httpClient;
  private final Clock clock;
  private volatile CachedToken cached;

  public KeycloakClientCredentialsTokenSupplier(
      URI tokenUri, String clientId, String clientSecret) {
    this(tokenUri, clientId, clientSecret, HttpClient.newHttpClient(), Clock.systemUTC());
  }

  KeycloakClientCredentialsTokenSupplier(
      URI tokenUri, String clientId, String clientSecret, HttpClient httpClient, Clock clock) {
    this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri");
    this.clientId = Objects.requireNonNull(clientId, "clientId");
    this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public String get() {
    CachedToken current = cached;
    if (current != null && clock.instant().isBefore(current.expiresAt())) {
      return current.accessToken();
    }
    synchronized (this) {
      current = cached;
      if (current != null && clock.instant().isBefore(current.expiresAt())) {
        return current.accessToken();
      }
      CachedToken fetched = fetch();
      cached = fetched;
      return fetched.accessToken();
    }
  }

  private CachedToken fetch() {
    String form =
        "grant_type=client_credentials"
            + "&client_id="
            + encode(clientId)
            + "&client_secret="
            + encode(clientSecret);
    HttpRequest request =
        HttpRequest.newBuilder(tokenUri)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();
    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new UncheckedIOException(
          "failed to reach the Keycloak token endpoint at " + tokenUri, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while fetching a Keycloak token", e);
    }
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "Keycloak token endpoint returned HTTP "
              + response.statusCode()
              + ": "
              + response.body());
    }
    JsonNode body;
    try {
      body = OBJECT_MAPPER.readTree(response.body());
    } catch (IOException e) {
      throw new UncheckedIOException("Keycloak token response was not valid JSON", e);
    }
    String accessToken = body.required("access_token").asText();
    long expiresInSeconds = body.path("expires_in").asLong(60);
    Instant expiresAt =
        clock.instant().plusSeconds(Math.max(1, expiresInSeconds - EXPIRY_SAFETY_MARGIN_SECONDS));
    return new CachedToken(accessToken, expiresAt);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private record CachedToken(String accessToken, Instant expiresAt) {}
}
