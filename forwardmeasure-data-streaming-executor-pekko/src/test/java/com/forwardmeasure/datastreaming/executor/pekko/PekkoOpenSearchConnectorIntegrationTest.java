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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof of the {@code opensearch} sink added 2026-09-13: {@link
 * PekkoIngestionRunner#run(IngestionSpec, org.apache.pekko.actor.ActorSystem)} indexes each mapped
 * row as a real document (PUT {@code {baseUrl}/{index}/_doc/{id}} via {@code camel-http}, {@code
 * idField} naming which mapped field is the document id) against a real, unsecured OpenSearch node
 * - verified with a plain real-time GET by id (bypasses OpenSearch's own refresh interval, unlike a
 * search query would, so no artificial wait/retry is needed here).
 */
@WithOpenSearchContainer
class PekkoOpenSearchConnectorIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  @Test
  void sinkIndexesEachMappedRowAsARealOpenSearchDocument(
      OpenSearchTestContainer opensearch, @TempDir Path tempDir) throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv,
        "ID,FULL_NAME,CATEGORY\nS1,Alice Anderson,person\nS2,Bob Baker,person\n",
        StandardCharsets.UTF_8);

    String index = "party-connector-test";
    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                null,
                null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule("name", "FULL_NAME", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "category", "CATEGORY", null, null, null, null, null))),
            new SinkSpec(
                "opensearch",
                opensearch.hostEndpoint().toString(),
                index,
                null,
                null,
                Map.of("idField", "uid")),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(2, 4), null, null));

    ActorSystem system = ActorSystem.create("opensearch-sink-integration-test");
    try {
      new PekkoIngestionRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    JsonNode s1 = fetchDocument(opensearch, index, "S1");
    assertEquals("Alice Anderson", s1.path("_source").path("name").asText());
    assertEquals("person", s1.path("_source").path("category").asText());

    JsonNode s2 = fetchDocument(opensearch, index, "S2");
    assertEquals("Bob Baker", s2.path("_source").path("name").asText());
  }

  private static JsonNode fetchDocument(OpenSearchTestContainer opensearch, String index, String id)
      throws Exception {
    URI uri = URI.create(opensearch.hostEndpoint() + "/" + index + "/_doc/" + id);
    HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200, response.statusCode(), "expected document '" + id + "' to exist: " + response.body());
    return MAPPER.readTree(response.body());
  }
}
