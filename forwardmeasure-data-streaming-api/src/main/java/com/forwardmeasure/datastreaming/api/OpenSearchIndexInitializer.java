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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

/**
 * Creates a real {@code opensearch} sink's target index with a real mapping/settings document
 * before any row is written - added 2026-09-14, closing a real gap found while trying to reuse
 * {@code entity-intelligence-specifications}' own real, checked-in WorldCheck indexing spec ({@code
 * screening-records-worldcheck-opensearch-indexing-spec.json} - custom ngram/phonetic name
 * analyzers, nested {@code identifiers}/{@code locations}/{@code names} objects, typed dates)
 * against FDS: neither engine's own {@code opensearch} sink ever created the index at all before
 * this - both just started PUTting documents, leaving OpenSearch's own dynamic mapping to improvise
 * a flatter, wrong-typed index no real mapping/settings document could ever apply to after the fact
 * (an index's own analysis settings are fixed at creation).
 *
 * <p>Shared between both engines (JDK-only, no Camel/Spark/Pekko dependency) since the actual HTTP
 * mechanics - check if the index exists, PUT the settings/mapping document if not - are identical
 * regardless of which engine ends up writing documents into it afterward; each engine's own sink
 * code calls {@link #ensureIndex} once, before it starts writing rows, not per-row.
 *
 * <p>{@code indexSettingsFile} (a {@code sink.options()} entry naming a local JSON file's path -
 * the same "generic per-connector options bag" convention every other connector metadata field
 * already uses) is optional: a sink with none behaves exactly as before (OpenSearch's own dynamic
 * mapping), so no existing spec's behavior changes. {@code user}/{@code password} go through {@link
 * SecretRefs#resolve}, the same as every other real credential read in this library.
 */
public final class OpenSearchIndexInitializer {

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
  private static final ObjectMapper JSON = new ObjectMapper();

  private OpenSearchIndexInitializer() {}

  /**
   * Creates an absent index from the admitted settings, or verifies mapping, ownership metadata and
   * settings before reusing an existing index. With no settings source, initialization is a no-op.
   * Failed existence checks never authorize creation.
   */
  public static void ensureIndex(String baseUrl, String index, Map<String, String> options) {
    String settingsFile = options.get("indexSettingsFile");
    String inline = options.get("indexSettingsJsonBase64");
    if ((settingsFile == null || settingsFile.isBlank()) && (inline == null || inline.isBlank()))
      return;
    if (settingsFile != null && inline != null)
      throw new IllegalArgumentException("Choose one index settings source");
    if (index == null || !index.matches("[a-z0-9][a-z0-9-]{0,254}"))
      throw new IllegalArgumentException("Invalid index name");
    String authHeader = basicAuthHeader(options);
    String indexUri = baseUrl + "/" + index;
    try {
      String body =
          inline == null
              ? Files.readString(Path.of(settingsFile), StandardCharsets.UTF_8)
              : new String(Base64.getDecoder().decode(inline), StandardCharsets.UTF_8);
      if (body.length() > 1_048_576)
        throw new IllegalArgumentException("Index settings exceed 1 MiB");
      JsonNode expected = JSON.readTree(body);
      if (!indexExists(indexUri, authHeader)) {
        createIndex(indexUri, body, authHeader);
        return;
      }
      JsonNode mappings = getJson(indexUri + "/_mapping", authHeader).path(index).path("mappings");
      if (!contains(mappings, expected.path("mappings"))) {
        throw new IllegalStateException(
            "Existing index ownership or mapping differs from the admitted contract");
      }
      JsonNode settings =
          getJson(indexUri + "/_settings", authHeader).path(index).path("settings").path("index");
      JsonNode wanted = expected.path("settings");
      if (wanted.has("index")) wanted = wanted.path("index");
      if (!contains(settings, wanted))
        throw new IllegalStateException(
            "Existing index settings differ from the admitted contract");
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot prepare index contract", failure);
    }
  }

  private static boolean contains(JsonNode actual, JsonNode expected) {
    if (expected.isMissingNode()) return true;
    if (expected.isObject()) {
      for (var entry : expected.properties())
        if (!contains(actual.path(entry.getKey()), entry.getValue())) return false;
      return true;
    }
    return expected.isValueNode()
        ? !actual.isMissingNode() && expected.asText().equals(actual.asText())
        : expected.equals(actual);
  }

  private static JsonNode getJson(String uri, String auth) throws IOException {
    var builder = HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(30)).GET();
    if (auth != null) builder.header("Authorization", auth);
    try {
      var response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2)
        throw new IOException("Index inspection HTTP " + response.statusCode());
      return JSON.readTree(response.body());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Index inspection interrupted", interrupted);
    }
  }

  private static boolean indexExists(String indexUri, String authHeader) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(indexUri))
            .timeout(Duration.ofSeconds(30))
            .method("HEAD", HttpRequest.BodyPublishers.noBody());
    if (authHeader != null) {
      requestBuilder.header("Authorization", authHeader);
    }
    HttpResponse<Void> response;
    try {
      response = HTTP_CLIENT.send(requestBuilder.build(), HttpResponse.BodyHandlers.discarding());
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "OpenSearchIndexInitializer: HEAD " + indexUri + " failed", e);
    }
    if (response.statusCode() == 200) return true;
    if (response.statusCode() == 404) return false;
    throw new IllegalStateException("Index existence check HTTP " + response.statusCode());
  }

  private static void createIndex(String indexUri, String body, String authHeader) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(indexUri))
            .timeout(Duration.ofSeconds(30))
            .PUT(HttpRequest.BodyPublishers.ofString(body))
            .header("Content-Type", "application/json");
    if (authHeader != null) {
      requestBuilder.header("Authorization", authHeader);
    }
    HttpResponse<String> response;
    try {
      response = HTTP_CLIENT.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new IllegalStateException("OpenSearchIndexInitializer: PUT " + indexUri + " failed", e);
    }
    if (response.statusCode() >= 300) {
      throw new IllegalStateException(
          "OpenSearchIndexInitializer: PUT "
              + indexUri
              + " failed: "
              + response.statusCode()
              + " "
              + response.body());
    }
  }

  private static String basicAuthHeader(Map<String, String> options) {
    String user = SecretRefs.resolve(options, "user");
    String password = SecretRefs.resolve(options, "password");
    if (user == null || password == null) {
      return null;
    }
    String credentials = user + ":" + password;
    return "Basic "
        + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }
}
