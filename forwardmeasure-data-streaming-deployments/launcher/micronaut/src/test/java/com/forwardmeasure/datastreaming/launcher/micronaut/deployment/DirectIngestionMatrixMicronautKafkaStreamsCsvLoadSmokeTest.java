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
import com.forwardmeasure.datastreaming.testfixtures.TestTransformImages;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
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

/**
 * A CSV file load on a Kafka Streams installation: REST admission, the real loader Job writing the
 * file's rows to the run's load topic, then the real Kafka Streams delivery Job writing the mapped
 * rows to OpenSearch. Neither image carries Pekko (enforced by their builds).
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DirectIngestionMatrixMicronautKafkaStreamsCsvLoadSmokeTest implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-ks-csv-smoke-role";
  private static final String NAMESPACE = "fds-micronaut-ks-csv-smoke";

  private static String KAFKA_STREAMS_IMAGE;
  private static String LOADER_IMAGE;
  static volatile KafkaTestContainer kafka;

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
    String executorImage = requiredProperty("fds.acceptance.kafka-streams.image");
    String loaderImage = requiredProperty("fds.acceptance.loader.image");
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    UUID tenantId = com.forwardmeasure.jpa.tenancy.TenantId.forDid(tenantDid).value();
    String organizationId =
        fixture.provisionTenant("micronaut-ks-csv-smoke-org", tenantDid, ROLE_NAME);
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
    KAFKA_STREAMS_IMAGE =
        kubernetes.loadImageAndPinDigest(TestTransformImages.withTestTransforms(executorImage));
    LOADER_IMAGE = kubernetes.loadImageAndPinDigest(loaderImage);
    CurrentKubernetesTestContainer.set(kubernetes);
    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
    }

    opensearch = new OpenSearchTestContainer().start();
    kafka =
        new KafkaTestContainer(
                com.forwardmeasure.testcontainers.kafka.KafkaContainerConfiguration.defaults()
                    .withHostDockerInternalListener())
            .start();
    openSearchUrlForPod = "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
    String gatewayIp = dockerBridgeGatewayIp();

    String kafkaStreamsImage = KAFKA_STREAMS_IMAGE;
    String loaderCommand = realRowSeedAndRunCommand();

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
        Map.entry("datastreaming.launcher.k8s.images", kafkaStreamsImage + "," + LOADER_IMAGE),
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
        Map.entry("datastreaming.launcher.engine", "kafka-streams"),
        Map.entry("datastreaming.launcher.spark.image", "unused-spark"),
        Map.entry("datastreaming.launcher.spark.command", "false"),
        Map.entry(
            "datastreaming.launcher.kafka.bootstrap-servers",
            kafka.hostDockerInternalBootstrapServers()),
        Map.entry("datastreaming.launcher.pekko.image", "unused"),
        Map.entry("datastreaming.launcher.pekko.command", "false"),
        Map.entry("datastreaming.launcher.kafka-streams.image", kafkaStreamsImage),
        Map.entry(
            "datastreaming.launcher.kafka-streams.command",
            "java -jar /deployments/application.jar"),
        Map.entry("datastreaming.launcher.loader.image", LOADER_IMAGE),
        Map.entry("datastreaming.launcher.loader.command", loaderCommand));
  }

  @AfterAll
  static void stopFixtures() {
    CurrentKubernetesTestContainer.clear();
    if (kafka != null) {
      kafka.close();
    }
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
  @Timeout(300)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = fixture.mintUserToken();

    var spec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp?fileName=source.csv&noop=true&initialDelay=1000&delay=100",
            openSearchUrlForPod,
            Path.of(""));

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
        opensearch.hostEndpoint() + "/test-customer-master-screening-records/_doc/KYC22438AML";
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
    long deadline = System.nanoTime() + Duration.ofSeconds(240).toNanos();
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
   * Real Customer Master org row (cm-3) from {@link TestCustomerMasterFixtures#SAMPLE_CSV},
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

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Set " + name + " to the current locally built image");
    }
    return value;
  }
}
