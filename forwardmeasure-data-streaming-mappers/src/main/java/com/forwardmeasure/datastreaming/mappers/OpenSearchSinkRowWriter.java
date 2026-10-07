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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.OpenSearchIndexInitializer;
import com.forwardmeasure.datastreaming.api.SecretRefs;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Real, per-row {@code PUT .../{index}/_doc/{id}} write against a real OpenSearch node - the same
 * proven shape {@code SparkSinks#writeOpenSearch} already uses (ported, not reinvented), now shared
 * by both continuous {@code DeliveryEngine} implementations via {@link SinkRowWriter}. Per-document
 * PUT by id is genuinely idempotent (indexing the same id twice just overwrites), which is exactly
 * why an at-least-once continuous pipeline built on this writer doesn't need Kafka transactions to
 * be safe in practice - see the repo's own gap-bridging plan, "Phase B addendum" and the bounded
 * Kafka Streams `InputFrontier` design for the same reasoning applied elsewhere.
 *
 * <p>Deliberately scoped to {@code opensearch} only, matching this repo's own real Phase G/H test
 * scope - not a general multi-connector sink dispatcher the way {@code SparkSinks#write} is.
 */
public final class OpenSearchSinkRowWriter implements SinkRowWriter {

  private final String idField;
  private final String baseUrl;
  private final String index;
  private final String authHeader;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final HttpClient client = HttpClient.newHttpClient();

  public OpenSearchSinkRowWriter(SinkSpec sink) {
    Objects.requireNonNull(sink, "sink");
    this.idField = requiredOption(sink.options(), "idField");
    this.baseUrl = sink.uri();
    this.index = sink.index();
    this.authHeader = basicAuthHeader(sink.options());
    OpenSearchIndexInitializer.ensureIndex(baseUrl, index, sink.options());
  }

  @Override
  public void write(Map<String, Object> row) {
    Object idValue = row.get(idField);
    if (idValue == null) {
      throw new IllegalArgumentException(
          "OpenSearchSinkRowWriter: a row is missing its own id field '" + idField + "'");
    }
    String docUri =
        baseUrl
            + "/"
            + index
            + "/_doc/"
            + URLEncoder.encode(String.valueOf(idValue), StandardCharsets.UTF_8)
                .replace("+", "%20");
    String json = writeValueAsString(row);
    putOneDocument(docUri, json);
  }

  @Override
  public void close() {
    client.close();
  }

  private void putOneDocument(String docUri, String json) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(docUri))
            .PUT(HttpRequest.BodyPublishers.ofString(json))
            .header("Content-Type", "application/json");
    if (authHeader != null) {
      requestBuilder.header("Authorization", authHeader);
    }
    HttpResponse<String> response;
    try {
      response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("OpenSearchSinkRowWriter: PUT " + docUri + " failed", e);
    }
    if (response.statusCode() >= 300) {
      throw new IllegalStateException(
          "OpenSearchSinkRowWriter: PUT "
              + docUri
              + " failed: "
              + response.statusCode()
              + " "
              + response.body());
    }
  }

  private String writeValueAsString(Map<String, Object> row) {
    try {
      return objectMapper.writeValueAsString(row);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
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

  private static String requiredOption(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "OpenSearchSinkRowWriter: '"
              + key
              + "' is required in the sink's own options map for this connector");
    }
    return value;
  }
}
