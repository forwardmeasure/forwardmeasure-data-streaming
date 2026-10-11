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
package com.forwardmeasure.datastreaming.launcher.micronaut.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * When the AuthZEN decision point cannot be reached with this service's credentials, an
 * authenticated caller gets a 503 problem - never a 403 (which would claim a decision) and never a
 * 500. The launcher's AuthZEN client secret is deliberately wrong.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LauncherAuthorizationUnavailableMicronautTest implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-unavailable-role";
  private static final String NAMESPACE = "fds-micronaut-unavailable";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  static volatile AuthzenKeycloakFixture fixture;
  static volatile KubernetesTestContainer kubernetes;

  @Inject EmbeddedServer server;

  @Override
  public Map<String, String> getProperties() {
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    fixture.provisionTenant("micronaut-unavailable-org", tenantDid, ROLE_NAME);
    kubernetes = new KubernetesTestContainer().start();
    CurrentKubernetesTestContainer.set(kubernetes);

    return Map.ofEntries(
        Map.entry(
            "micronaut.security.token.jwt.signatures.jwks.keycloak.url",
            fixture.issuer().toString() + "/protocol/openid-connect/certs"),
        Map.entry(
            "datastreaming.launcher.authorization.organization-client-id",
            AuthzenKeycloakFixture.CLIENT_ID),
        Map.entry("datastreaming.launcher.authorization.issuer", fixture.issuer().toString()),
        Map.entry(
            "datastreaming.launcher.authorization.client-id",
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID),
        Map.entry(
            "datastreaming.launcher.authorization.client-secret",
            "not-" + AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
        Map.entry("datastreaming.launcher.k8s.images", "unused"),
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
        Map.entry("datastreaming.launcher.pekko.image", "unused"),
        Map.entry("datastreaming.launcher.pekko.command", "unused"),
        Map.entry("datastreaming.launcher.kafka-streams.image", "unused"),
        Map.entry("datastreaming.launcher.kafka-streams.command", "unused"));
  }

  @AfterAll
  static void stopFixtures() {
    CurrentKubernetesTestContainer.clear();
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  void launchingAnIngestionRunIsServiceUnavailable() throws Exception {
    String body =
        MAPPER.writeValueAsString(
            Map.of(
                "correlationId",
                "unavailable-" + UUID.randomUUID(),
                "namespace",
                NAMESPACE,
                "ingestionSpec",
                WorldCheckFixtures.boundedFileSpec(
                    "file:/tmp?fileName=source.csv&noop=true", "http://unused:9200", Path.of(""))));
    HttpResponse<String> response =
        HTTP.send(
            request("ingestion-runs")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertServiceUnavailable(response);
  }

  @Test
  void readingAWorkflowRunIsServiceUnavailable() throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            request("workflow-runs/" + UUID.randomUUID()).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertServiceUnavailable(response);
  }

  private HttpRequest.Builder request(String path) {
    return HttpRequest.newBuilder(URI.create(server.getURL() + "/").resolve(path))
        .header("Authorization", "Bearer " + fixture.mintUserToken());
  }

  private static void assertServiceUnavailable(HttpResponse<String> response) throws IOException {
    assertEquals(503, response.statusCode(), "unexpected response: " + response.body());
    assertTrue(
        response
            .headers()
            .firstValue("Content-Type")
            .orElse("")
            .startsWith("application/problem+json"),
        "expected an RFC 9457 problem: " + response.headers().map());
    JsonNode problem = MAPPER.readTree(response.body());
    assertEquals(503, problem.path("status").asInt(), response.body());
    assertEquals("Service Unavailable", problem.path("title").asText(), response.body());
  }
}
