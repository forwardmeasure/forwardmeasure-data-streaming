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
package com.forwardmeasure.datastreaming.launcher.spring.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * When the AuthZEN decision point cannot be reached with this service's credentials, an
 * authenticated caller gets a 503 problem - never a 403 (which would claim a decision) and never a
 * 500. The launcher's AuthZEN client secret is deliberately wrong.
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.context.annotation.Import(
    LauncherAuthorizationUnavailableSpringTest.TestKubernetesClientConfiguration.class)
class LauncherAuthorizationUnavailableSpringTest {

  private static final String ROLE_NAME = "spring-unavailable-role";
  private static final String NAMESPACE = "fds-spring-unavailable";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static AuthzenKeycloakFixture fixture;
  private static KubernetesTestContainer kubernetes;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() {
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    fixture.provisionTenant("spring-unavailable-org", tenantDid, ROLE_NAME);
    kubernetes = new KubernetesTestContainer().start();
  }

  @AfterAll
  static void stopFixtures() {
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @DynamicPropertySource
  static void registerDynamicProperties(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> fixture.issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.organization-client-id",
        () -> AuthzenKeycloakFixture.CLIENT_ID);
    registry.add("datastreaming.launcher.authorization.issuer", () -> fixture.issuer().toString());
    registry.add(
        "datastreaming.launcher.authorization.client-id",
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID);
    registry.add(
        "datastreaming.launcher.authorization.client-secret",
        () -> "not-" + AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET);
    registry.add("datastreaming.launcher.k8s.namespaces", () -> NAMESPACE);
    registry.add("datastreaming.launcher.k8s.images", () -> "unused");
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    registry.add("datastreaming.launcher.fowf.keycloak.client-id", () -> "unused");
    registry.add("datastreaming.launcher.fowf.keycloak.client-secret", () -> "unused");
    registry.add("datastreaming.launcher.engine", () -> "pekko-postgresql");
    registry.add("datastreaming.launcher.spark.image", () -> "unused-spark");
    registry.add("datastreaming.launcher.spark.command", () -> "false");
    registry.add("datastreaming.launcher.kafka.bootstrap-servers", () -> "unused:9092");
    registry.add("datastreaming.launcher.pekko.image", () -> "unused");
    registry.add("datastreaming.launcher.pekko.command", () -> "unused");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> "unused");
    registry.add("datastreaming.launcher.kafka-streams.command", () -> "unused");
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
    return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/").resolve(path))
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

  /** Points the launcher at this class's own disposable K3s, never an ambient kubeconfig. */
  @TestConfiguration
  static class TestKubernetesClientConfiguration {
    @Bean
    @Primary
    KubernetesClient testKubernetesClient() {
      return new KubernetesClientBuilder()
          .withConfig(Config.fromKubeconfig(kubernetes.kubeConfigYaml()))
          .build();
    }
  }
}
