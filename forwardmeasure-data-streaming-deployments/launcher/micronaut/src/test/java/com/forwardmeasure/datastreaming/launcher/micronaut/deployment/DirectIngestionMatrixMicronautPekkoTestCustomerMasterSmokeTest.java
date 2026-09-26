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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/**
 * Phase H's own sibling of {@code DirectIngestionMatrixMicronautPekkoSmokeTest} - identical real
 * plumbing proof, seeded with a real Customer Master row instead of WorldCheck.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DirectIngestionMatrixMicronautPekkoTestCustomerMasterSmokeTest
    implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-direct-cm-smoke-role";
  private static final String NAMESPACE = "fds-micronaut-cm-smoke";
  private static final String DOCUMENT_ID = "smoke-doc-5";
  private static final String CURL_IMAGE =
      "curlimages/curl@sha256:83a505ba2ba62f208ed6e410c268b7b9aa48f0f7b403c8108b9773b44199dbba";

  private static final String PEKKO_IMAGE =
      "docker.io/forwardmeasure/data-streaming-executor-pekko@sha256:"
          + "1b3d5e60475535643db17ce9038c293252eba3f0be1a48e2e982ad09c96f36af";

  private static final String PULL_SECRET_NAME = "dockerhub-pull-secret";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static volatile AuthzenKeycloakFixture fixture;
  static volatile KubernetesTestContainer kubernetes;
  static volatile OpenSearchTestContainer opensearch;
  static volatile boolean realImageMode;
  static volatile String openSearchUrlForPod;

  @Inject
  @Client("/")
  HttpClient http;

  @Override
  public Map<String, String> getProperties() {
    fixture = AuthzenKeycloakFixture.start();
    UUID tenantId = UUID.randomUUID();
    String organizationId =
        fixture.provisionTenant("micronaut-direct-cm-smoke-org", tenantId, ROLE_NAME);
    fixture.grantResourceAuthorization(
        organizationId,
        "datastreaming-ingestion-run",
        "ingestion-runs",
        "smoke-permission",
        ROLE_NAME,
        Set.of(
            AuthorizationAction.INGESTION_RUN_LAUNCH.scope(),
            AuthorizationAction.INGESTION_RUN_READ.scope()));

    kubernetes = new KubernetesTestContainer().start();
    CurrentKubernetesTestContainer.set(kubernetes);
    String username = System.getenv("DOCKER_HUB_USERNAME");
    String token = System.getenv("DOCKER_HUB_TOKEN");
    realImageMode = username != null && token != null;
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
      if (realImageMode) {
        client
            .secrets()
            .inNamespace(NAMESPACE)
            .resource(dockerConfigSecret(username, token))
            .create();
      }
    }

    opensearch = new OpenSearchTestContainer().start();
    openSearchUrlForPod = "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
    String gatewayIp = dockerBridgeGatewayIp();

    String pekkoImage = realImageMode ? PEKKO_IMAGE : CURL_IMAGE;
    String pekkoCommand =
        realImageMode
            ? realRowSeedAndRunCommand()
            : "curl -sf -X PUT "
                + openSearchUrlForPod
                + "/smoke-index/_doc/"
                + DOCUMENT_ID
                + " -H Content-Type:application/json -d {\\\"marker\\\":\\\""
                + DOCUMENT_ID
                + "\\\"} #";

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
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET),
        Map.entry("datastreaming.launcher.k8s.namespaces", NAMESPACE),
        Map.entry("datastreaming.launcher.k8s.images", pekkoImage),
        Map.entry(
            "datastreaming.launcher.k8s.image-pull-secrets",
            realImageMode ? PULL_SECRET_NAME : "unused"),
        Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
        Map.entry("datastreaming.launcher.pekko.image", pekkoImage),
        Map.entry("datastreaming.launcher.pekko.command", pekkoCommand),
        Map.entry("datastreaming.launcher.kafka-streams.image", CURL_IMAGE),
        Map.entry("datastreaming.launcher.kafka-streams.command", "true #"));
  }

  @AfterAll
  static void stopFixtures() {
    CurrentKubernetesTestContainer.clear();
    if (opensearch != null) {
      opensearch.close();
    }
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  @Timeout(180)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = fixture.mintUserToken();

    var spec =
        realImageMode
            ? TestCustomerMasterFixtures.boundedFileSpec(
                "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
                openSearchUrlForPod,
                Path.of(""))
            : TestCustomerMasterFixtures.boundedFileSpec(
                "file:/tmp?fileName=unused.csv&noop=true",
                "http://unused:9200",
                Path.of("/tmp/unused.json"));

    Map<String, Object> requestBody =
        Map.of(
            "correlationId",
            correlationId,
            "namespace",
            NAMESPACE,
            "ingestionSpec",
            spec,
            "resourceRequests",
            Map.of(),
            "resourceLimits",
            Map.of());
    String requestBodyJson = MAPPER.writeValueAsString(requestBody);

    HttpResponse<String> dispatchResponse =
        http.toBlocking()
            .exchange(
                HttpRequest.POST("/ingestion-runs", requestBodyJson)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bearerAuth(token),
                String.class);
    assertEquals(HttpStatus.ACCEPTED.getCode(), dispatchResponse.code());

    String phase = pollUntilTerminal(token, correlationId);
    if (!"SUCCEEDED".equals(phase)) {
      throw new AssertionError(
          "the real dispatched Job must reach a real terminal SUCCEEDED phase, got "
              + phase
              + " - pod logs:\n"
              + podLogs());
    }

    String documentUri =
        realImageMode
            ? opensearch.hostEndpoint() + "/test-customer-master-screening-records/_doc/KYC22438AML"
            : opensearch.hostEndpoint() + "/smoke-index/_doc/" + DOCUMENT_ID;
    java.net.http.HttpResponse<String> document =
        java.net.http.HttpClient.newHttpClient()
            .send(
                java.net.http.HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document written by the dispatched pod: " + document.body());
    assertTrue(
        document.body().contains(realImageMode ? "\"entity_kind\":\"organization\"" : DOCUMENT_ID),
        "unexpected document body: " + document.body());
  }

  private String pollUntilTerminal(String token, String correlationId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
    String phase = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<Map> response =
            http.toBlocking()
                .exchange(
                    HttpRequest.GET("/ingestion-runs/" + correlationId + "?namespace=" + NAMESPACE)
                        .bearerAuth(token),
                    Map.class);
        if (response.code() == 200) {
          Object rawPhase = response.body().get("phase");
          phase = rawPhase == null ? "UNKNOWN" : rawPhase.toString();
          if ("SUCCEEDED".equals(phase) || "FAILED".equals(phase)) {
            return phase;
          }
        }
      } catch (HttpClientResponseException notYetTerminal) {
        // Tolerant poll, matching the other Micronaut smoke tests' own shape.
      }
      Thread.sleep(1000);
    }
    return phase;
  }

  /**
   * Real Customer Master org row (ss-3) from {@link TestCustomerMasterFixtures#SAMPLE_CSV},
   * base64-transported.
   */
  private static String realRowSeedAndRunCommand() {
    String[] lines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
    String seedCsv = lines[0] + "\n" + lines[3] + "\n";
    String seedCsvBase64 =
        Base64.getEncoder().encodeToString(seedCsv.getBytes(StandardCharsets.UTF_8));
    return "sh -c 'echo "
        + seedCsvBase64
        + " | base64 -d > /tmp/source.csv && "
        + "exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml'";
  }

  private static Secret dockerConfigSecret(String username, String token) {
    String auth =
        Base64.getEncoder()
            .encodeToString((username + ":" + token).getBytes(StandardCharsets.UTF_8));
    String dockerConfigJson =
        "{\"auths\":{\"https://index.docker.io/v1/\":{\"username\":\""
            + username
            + "\",\"password\":\""
            + token
            + "\",\"auth\":\""
            + auth
            + "\"}}}";
    return new SecretBuilder()
        .withNewMetadata()
        .withName(PULL_SECRET_NAME)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withType("kubernetes.io/dockerconfigjson")
        .addToData(
            ".dockerconfigjson",
            Base64.getEncoder().encodeToString(dockerConfigJson.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

  private static String podLogs() {
    try (KubernetesClient client = kubernetes.createClient()) {
      var pods = client.pods().inNamespace(NAMESPACE).list().getItems();
      StringBuilder logs = new StringBuilder();
      for (var pod : pods) {
        logs.append("--- ").append(pod.getMetadata().getName()).append(" ---\n");
        try {
          logs.append(
              client.pods().inNamespace(NAMESPACE).withName(pod.getMetadata().getName()).getLog());
        } catch (RuntimeException e) {
          logs.append("(failed to fetch logs: ").append(e).append(")\n");
        }
      }
      return logs.toString();
    }
  }

  private static String dockerBridgeGatewayIp() {
    try {
      Process process =
          new ProcessBuilder(
                  "docker",
                  "network",
                  "inspect",
                  "bridge",
                  "--format",
                  "{{(index .IPAM.Config 0).Gateway}}")
              .redirectErrorStream(true)
              .start();
      String output =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
      process.waitFor();
      return output;
    } catch (Exception e) {
      throw new IllegalStateException("failed to discover the real Docker bridge gateway IP", e);
    }
  }
}
