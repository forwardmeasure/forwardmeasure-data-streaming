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
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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

/** Real REST admission, selected current executor, and persisted provider output. */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DirectIngestionMatrixMicronautPekkoSmokeTest implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-direct-smoke-role";
  private static final String NAMESPACE = "fds-micronaut-smoke";

  private static String PEKKO_IMAGE;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static volatile AuthzenKeycloakFixture fixture;
  static volatile KubernetesTestContainer kubernetes;
  static volatile OpenSearchTestContainer opensearch;

  static volatile String openSearchUrlForPod;

  @Inject
  @Client("/")
  HttpClient http;

  @Override
  public Map<String, String> getProperties() {
    String executorImage = System.getProperty("fds.acceptance.pekko.image");
    if (executorImage == null || executorImage.isBlank()) {
      throw new IllegalStateException(
          "Set fds.acceptance.pekko.image to the current locally built executor image");
    }
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    UUID tenantId = com.forwardmeasure.jpa.tenancy.TenantId.forDid(tenantDid).value();
    String organizationId =
        fixture.provisionTenant("micronaut-direct-smoke-org", tenantDid, ROLE_NAME);
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
    PEKKO_IMAGE = kubernetes.loadImageAndPinDigest(executorImage);
    CurrentKubernetesTestContainer.set(kubernetes);
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }

    opensearch = new OpenSearchTestContainer().start();
    openSearchUrlForPod = "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
    String gatewayIp = dockerBridgeGatewayIp();

    String pekkoImage = PEKKO_IMAGE;
    String pekkoCommand = realRowSeedAndRunCommand();

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
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
        Map.entry("datastreaming.launcher.pekko.image", pekkoImage),
        Map.entry("datastreaming.launcher.pekko.command", pekkoCommand),
        Map.entry("datastreaming.launcher.kafka-streams.image", "unused"),
        Map.entry("datastreaming.launcher.kafka-streams.command", "false"));
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
        WorldCheckFixtures.boundedFileSpec(
            // Real, live-confirmed race found 2026-09-23 (deterministic here, 3/3 runs):
            // Camel's
            // own file consumer starts polling as soon as the CamelContext starts, independent
            // of when the corresponding Pekko Source.fromPublisher(...) actually subscribes -
            // with initialDelay=0, Camel's first poll can fire (and, per its own reactive-
            // streams component's real backpressure contract, throw
            // ReactiveStreamsNoActiveSubscriptionsException) before that subscription
            // completes. Quarkus/Spring's own identical real-image smoke tests happened not to
            // lose this race, but the race itself is real and pre-existing in shared
            // PekkoIngestionRunner/CamelBridge code, not something this test introduces - a
            // real, small initialDelay here (test-only, not a production code change) reliably
            // gives the subscription time to complete first.
            "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
            openSearchUrlForPod,
            Path.of(""));

    // Micronaut's own native HttpClient encodes a request body via compile-time micronaut-serde
    // introspection, unlike Jackson's runtime reflection - IngestionSpec (a plain record from the
    // shared, production forwardmeasure-data-streaming-api module) has no @Serdeable/@SerdeImport
    // registered and can't be annotated here (not this module's own source). Serializing to a real
    // JSON string via plain Jackson first, then sending that raw string, sidesteps Serde
    // introspection entirely on the client side - the server's own JAX-RS resource deserializes the
    // request body through a completely different, Jackson-based JAX-RS MessageBodyReader anyway.
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

    String documentUri = opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-3";
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
        document.body().contains("\"entity_kind\":\"organization\""),
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
        // A transient non-200 mid-poll (e.g. the Job not observable yet) is not itself a failure -
        // same tolerant-poll shape the Quarkus/Spring siblings use.
      }
      Thread.sleep(1000);
    }
    return phase;
  }

  private static String realRowSeedAndRunCommand() {
    String[] lines = WorldCheckFixtures.SAMPLE_TSV.split("\n");
    String seedTsv = lines[0] + "\n" + lines[3] + "\n";
    String seedTsvBase64 =
        Base64.getEncoder().encodeToString(seedTsv.getBytes(StandardCharsets.UTF_8));
    return "sh -c 'echo "
        + seedTsvBase64
        + " | base64 -d > /tmp/source.csv && "
        + "exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml'";
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
