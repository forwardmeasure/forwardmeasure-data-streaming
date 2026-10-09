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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kafka.KafkaContainerConfiguration;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/** Real REST admission, selected current executor, and persisted provider output. */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DirectIngestionMatrixMicronautKafkaStreamsSmokeTest implements TestPropertyProvider {

  private static final String ROLE_NAME = "micronaut-kstreams-smoke-role";
  private static final String NAMESPACE = "fds-micronaut-kstreams-smoke";

  private static final String KAFKA_TOPIC = "worldcheck-rows";

  private static String KAFKA_STREAMS_IMAGE;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static volatile AuthzenKeycloakFixture fixture;
  static volatile KubernetesTestContainer kubernetes;
  static volatile OpenSearchTestContainer opensearch;
  static volatile KafkaTestContainer kafka;

  static volatile String openSearchUrlForPod;

  @Inject
  @Client("/")
  HttpClient http;

  @Override
  public Map<String, String> getProperties() {
    String executorImage = System.getProperty("fds.acceptance.kafka-streams.image");
    if (executorImage == null || executorImage.isBlank()) {
      throw new IllegalStateException(
          "Set fds.acceptance.kafka-streams.image to the current locally built executor image");
    }
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    UUID tenantId = com.forwardmeasure.jpa.tenancy.TenantId.forDid(tenantDid).value();
    String organizationId =
        fixture.provisionTenant("micronaut-kstreams-smoke-org", tenantDid, ROLE_NAME);
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
    KAFKA_STREAMS_IMAGE = kubernetes.loadImageAndPinDigest(executorImage);
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
                KafkaContainerConfiguration.defaults().withHostDockerInternalListener())
            .start();

    openSearchUrlForPod = "http://host.docker.internal:" + opensearch.hostEndpoint().getPort();
    String gatewayIp = dockerBridgeGatewayIp();

    String kafkaStreamsImage = KAFKA_STREAMS_IMAGE;
    String kafkaStreamsCommand = "java -jar /deployments/application.jar";

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
        Map.entry("datastreaming.launcher.k8s.images", kafkaStreamsImage),
        Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
        Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
        Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
        Map.entry("datastreaming.launcher.pekko.image", "unused"),
        Map.entry("datastreaming.launcher.pekko.command", "false"),
        Map.entry("datastreaming.launcher.kafka-streams.image", kafkaStreamsImage),
        Map.entry("datastreaming.launcher.kafka-streams.command", kafkaStreamsCommand));
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
  @Timeout(180)
  void dispatchesARealKafkaStreamsJobThatReachesRealOpenSearchThroughHostAliases()
      throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = fixture.mintUserToken();

    seedOneRealWorldCheckRow(kafka.bootstrapServers());

    var spec =
        WorldCheckFixtures.boundedKafkaSpec(
            "kafka:" + KAFKA_TOPIC + "?brokers=" + kafka.hostDockerInternalBootstrapServers(),
            openSearchUrlForPod,
            Path.of(""));

    // Same client-side plain-Jackson sidestep DirectIngestionMatrixMicronautPekkoSmokeTest uses -
    // see that class's own javadoc for why.
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
    assertEquals(
        "SUCCEEDED", phase, "the real dispatched Job must reach a real terminal SUCCEEDED phase");

    String documentUri = opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-1";
    java.net.http.HttpResponse<String> document =
        java.net.http.HttpClient.newHttpClient()
            .send(
                java.net.http.HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document written by the dispatched pod: " + document.body());
    assertTrue(document.body().contains("Smith"), "unexpected document body: " + document.body());
  }

  /** Real WorldCheck row wc-1 from {@code WorldCheckFixtures#SAMPLE_TSV}, JSON-encoded. */
  // One answer on every framework (fowf handoff doc section 4b): a path no service serves is a
  // 401 problem without a token and a 404 problem with one; health is open, but a token sent to it
  // must verify; this service's own client errors are problems too.

  @Test
  void rejectsAGenuineTokenFromAnUnexpectedIssuer() throws Exception {
    assertUnauthorizedProblem(
        HttpRequest.GET("/ingestion-runs/issuer-check")
            .bearerAuth(fixture.mintUserTokenWithAlternateIssuer()));
    unknownPath_authenticatedIsNotFound();
  }

  @Test
  void unknownPath_unauthenticatedIsRejected() throws Exception {
    assertUnauthorizedProblem(HttpRequest.GET("/no-such-resource"));
  }

  @Test
  void unknownPath_authenticatedIsNotFound() throws Exception {
    JsonNode problem =
        problem(
            assertProblem(
                HttpRequest.GET("/no-such-resource").bearerAuth(fixture.mintUserToken()), 404));
    assertEquals(404, problem.path("status").asInt());
  }

  @Test
  void health_isOpenWithoutAToken() {
    assertEquals(200, http.toBlocking().exchange(HttpRequest.GET("/health"), String.class).code());
  }

  @Test
  void health_invalidTokenIsRejected() throws Exception {
    assertUnauthorizedProblem(HttpRequest.GET("/health").bearerAuth("not-a-jwt"));
  }

  @Test
  void aMissingNamespaceIsABadRequestProblem() throws Exception {
    JsonNode problem =
        problem(
            assertProblem(
                HttpRequest.GET("/ingestion-runs/some-run").bearerAuth(fixture.mintUserToken()),
                400));
    assertEquals("Bad Request", problem.path("title").asText());
    assertEquals("the 'namespace' query parameter is required", problem.path("detail").asText());
  }

  private void assertUnauthorizedProblem(HttpRequest<?> request) throws Exception {
    HttpClientResponseException thrown = assertProblem(request, 401);
    assertEquals("Bearer", thrown.getResponse().getHeaders().get("WWW-Authenticate"));
    assertEquals("Unauthorized", problem(thrown).path("title").asText());
  }

  private HttpClientResponseException assertProblem(HttpRequest<?> request, int status) {
    HttpClientResponseException thrown =
        assertThrows(
            HttpClientResponseException.class,
            () -> http.toBlocking().exchange(request, String.class));
    assertEquals(
        status,
        thrown.getStatus().getCode(),
        () -> "response body: " + thrown.getResponse().getBody(String.class).orElse(""));
    assertEquals(
        "application/problem+json",
        thrown.getResponse().getContentType().map(Object::toString).orElse(""));
    return thrown;
  }

  private static JsonNode problem(HttpClientResponseException thrown) throws Exception {
    JsonNode problem = MAPPER.readTree(thrown.getResponse().getBody(String.class).orElse(""));
    assertEquals("about:blank", problem.path("type").asText());
    return problem;
  }

  private static void seedOneRealWorldCheckRow(String bootstrapServers) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("UID", "wc-1");
    row.put("LAST NAME", "Smith");
    row.put("FIRST NAME", "Jose");
    row.put("CATEGORY", "INDIVIDUAL");
    row.put("E/I", "M");
    row.put("DOB", "1975/03/15");
    row.put("CITIZENSHIP", "UNITED STATES;RUSSIA");
    row.put("COUNTRIES", "");
    row.put("ALIASES", "Johnny Smith;J. Smith");
    row.put("SSN", "123-45-6789");
    row.put("POSITION", "Businessman");
    row.put("PEP ROLES", "Senator~Governor");
    row.put("PEP STATUS", "Former PEP");
    row.put("KEYWORDS", "Fraud~Bribery");
    row.put("EXTERNAL SOURCES", "https://example.com/report1");
    row.put("FURTHER INFORMATION", "Subject of an investigation.");
    row.put("SPECIAL INTEREST CATEGORIES", "Sanctions Related;PEP");
    row.put("LOCATIONS", "~ Moscow, Moscow Oblast ~ RUSSIA");

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      String json = new ObjectMapper().writeValueAsString(row);
      producer.send(new ProducerRecord<>(KAFKA_TOPIC, "wc-1", json)).get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed the real WorldCheck row onto Kafka", e);
    }
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
        // same tolerant-poll shape the Pekko sibling uses.
      }
      Thread.sleep(1000);
    }
    return phase;
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
