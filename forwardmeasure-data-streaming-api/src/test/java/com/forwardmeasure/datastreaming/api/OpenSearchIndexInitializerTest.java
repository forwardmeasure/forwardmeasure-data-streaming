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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link OpenSearchIndexInitializer#ensureIndex} genuinely applies a real
 * mapping/settings document to a real OpenSearch node before any row is written - added 2026-09-14,
 * closing a real gap found while trying to reuse {@code entity-intelligence-specifications}' own
 * real WorldCheck indexing spec against FDS: neither engine's own {@code opensearch} sink ever
 * created the target index at all before this method existed.
 */
@WithOpenSearchContainer
class OpenSearchIndexInitializerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  @Test
  void noIndexSettingsFileOptionLeavesTheIndexUncreated(OpenSearchTestContainer opensearch)
      throws Exception {
    String index = "no-settings-test";
    OpenSearchIndexInitializer.ensureIndex(opensearch.hostEndpoint().toString(), index, Map.of());

    assertFalse(
        indexExists(opensearch, index),
        "ensureIndex with no indexSettingsFile option must be a real no-op, not create an empty"
            + " index and leave dynamic mapping to improvise as before");
  }

  @Test
  void realIndexSettingsFileIsAppliedBeforeAnyDocumentIsWritten(
      OpenSearchTestContainer opensearch, @TempDir Path tempDir) throws Exception {
    String index = "real-settings-test";
    Path settingsFile = tempDir.resolve("index-settings.json");
    Files.writeString(settingsFile, INDEX_SETTINGS_JSON, StandardCharsets.UTF_8);

    OpenSearchIndexInitializer.ensureIndex(
        opensearch.hostEndpoint().toString(),
        index,
        Map.of("indexSettingsFile", settingsFile.toString()));

    assertTrue(indexExists(opensearch, index), "the index must now genuinely exist");
    JsonNode mapping = fetchMapping(opensearch, index);
    assertEquals(
        "name_analyzer",
        mapping
            .path(index)
            .path("mappings")
            .path("properties")
            .path("name")
            .path("analyzer")
            .asText(),
        "the real custom analyzer from the settings file must be the one actually applied - proof"
            + " this isn't OpenSearch's own dynamic mapping improvising a plain 'text' field");
  }

  @Test
  void callingItTwiceWithAnAlreadyExistingIndexIsIdempotent(
      OpenSearchTestContainer opensearch, @TempDir Path tempDir) throws Exception {
    String index = "idempotent-settings-test";
    Path settingsFile = tempDir.resolve("index-settings.json");
    Files.writeString(settingsFile, INDEX_SETTINGS_JSON, StandardCharsets.UTF_8);
    Map<String, String> options = Map.of("indexSettingsFile", settingsFile.toString());

    OpenSearchIndexInitializer.ensureIndex(opensearch.hostEndpoint().toString(), index, options);
    // A second call against an index whose analysis settings are already fixed must not throw -
    // this is the real-world case of a run that might create the same sink's index more than once.
    OpenSearchIndexInitializer.ensureIndex(opensearch.hostEndpoint().toString(), index, options);

    assertTrue(indexExists(opensearch, index));
  }

  private static boolean indexExists(OpenSearchTestContainer opensearch, String index)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(opensearch.hostEndpoint() + "/" + index))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build();
    HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    return response.statusCode() == 200;
  }

  private static JsonNode fetchMapping(OpenSearchTestContainer opensearch, String index)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(opensearch.hostEndpoint() + "/" + index + "/_mapping"))
            .GET()
            .build();
    HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return MAPPER.readTree(response.body());
  }

  private static final String INDEX_SETTINGS_JSON =
      """
      {
        "settings": {
          "analysis": {
            "analyzer": {
              "name_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"]
              }
            }
          }
        },
        "mappings": {
          "properties": {
            "name": {
              "type": "text",
              "analyzer": "name_analyzer"
            },
            "date_of_birth": {
              "type": "date",
              "format": "yyyy-MM-dd"
            }
          }
        }
      }
      """;
}
