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

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kafka.KafkaContainerConfiguration;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.restassured.specification.RequestSpecification;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Real REST admission, selected current executor, and persisted provider output. */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(DirectIngestionMatrixSpringKafkaStreamsSmokeTest.TestKubernetesClientConfiguration.class)
class DirectIngestionMatrixSpringKafkaStreamsSmokeTest {

  private static final String ROLE_NAME = "spring-kstreams-smoke-role";
  private static final String NAMESPACE = "fds-spring-kstreams-smoke";

  private static final String KAFKA_TOPIC = "worldcheck-rows";

  private static String KAFKA_STREAMS_IMAGE;

  private static AuthzenKeycloakFixture fixture;
  private static KubernetesTestContainer kubernetes;
  private static OpenSearchTestContainer opensearch;
  private static KafkaTestContainer kafka;

  private static String openSearchUrlForPod;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() {
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
        fixture.provisionTenant("spring-kstreams-smoke-org", tenantDid, ROLE_NAME);
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
  }

  @AfterAll
  static void stopFixtures() {
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

  @DynamicPropertySource
  static void registerDynamicProperties(DynamicPropertyRegistry registry) throws Exception {
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
        () -> AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET);
    registry.add("datastreaming.launcher.k8s.namespaces", () -> NAMESPACE);
    registry.add("datastreaming.launcher.fowf.keycloak.client-id", () -> "unused");
    registry.add("datastreaming.launcher.fowf.keycloak.client-secret", () -> "unused");

    String gatewayIp = dockerBridgeGatewayIp();
    registry.add(
        "datastreaming.launcher.k8s.host-aliases", () -> "host.docker.internal=" + gatewayIp);

    String kafkaStreamsImage = KAFKA_STREAMS_IMAGE;
    registry.add("datastreaming.launcher.k8s.images", () -> kafkaStreamsImage);
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    registry.add("datastreaming.launcher.pekko.image", () -> "unused");
    registry.add("datastreaming.launcher.pekko.command", () -> "false");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> kafkaStreamsImage);
    registry.add(
        "datastreaming.launcher.kafka-streams.command",
        () -> "java -jar /deployments/application.jar");
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

    given()
        .port(port)
        .header("Authorization", "Bearer " + token)
        .contentType("application/json")
        .body(requestBody)
        .when()
        .post("/ingestion-runs")
        .then()
        .statusCode(202);

    String phase = pollUntilTerminal(token, correlationId);
    assertEquals(
        "SUCCEEDED", phase, "the real dispatched Job must reach a real terminal SUCCEEDED phase");

    String documentUri = opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-1";
    HttpResponse<String> document =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document written by the dispatched pod: " + document.body());
    assertTrue(document.body().contains("Smith"), "unexpected document body: " + document.body());
  }

  private String pollUntilTerminal(String token, String correlationId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
    String phase = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      var response =
          given()
              .port(port)
              .header("Authorization", "Bearer " + token)
              .when()
              .get("/ingestion-runs/" + correlationId + "?namespace=" + NAMESPACE);
      if (response.statusCode() == 200) {
        phase = response.jsonPath().getString("phase");
        if ("SUCCEEDED".equals(phase) || "FAILED".equals(phase)) {
          return phase;
        }
      }
      Thread.sleep(1000);
    }
    return phase;
  }

  // One answer on every framework (fowf handoff doc section 4b): a path no service serves is a
  // 401 problem without a token and a 404 problem with one; health is open, but a token sent to it
  // must verify; this service's own client errors are problems too.

  @Test
  void rejectsAGenuineTokenFromAnUnexpectedIssuer() {
    assertUnauthorizedProblem(
        given()
            .port(port)
            .header("Authorization", "Bearer " + fixture.mintUserTokenWithAlternateIssuer()),
        "/ingestion-runs/issuer-check");
    unknownPath_authenticatedIsNotFound();
  }

  @Test
  void unknownPath_unauthenticatedIsRejected() {
    assertUnauthorizedProblem(given().port(port), "/no-such-resource");
  }

  @Test
  void unknownPath_authenticatedIsNotFound() {
    given()
        .port(port)
        .header("Authorization", "Bearer " + fixture.mintUserToken())
        .when()
        .get("/no-such-resource")
        .then()
        .statusCode(404)
        .contentType("application/problem+json")
        .body("type", equalTo("about:blank"))
        .body("status", equalTo(404));
  }

  @Test
  void health_isOpenWithoutAToken() {
    given().port(port).when().get("/actuator/health").then().statusCode(200);
  }

  @Test
  void health_invalidTokenIsRejected() {
    assertUnauthorizedProblem(
        given().port(port).header("Authorization", "Bearer not-a-jwt"), "/actuator/health");
  }

  @Test
  void aMissingNamespaceIsABadRequestProblem() {
    given()
        .port(port)
        .header("Authorization", "Bearer " + fixture.mintUserToken())
        .when()
        .get("/ingestion-runs/some-run")
        .then()
        .statusCode(400)
        .contentType("application/problem+json")
        .body("title", equalTo("Bad Request"))
        .body("detail", equalTo("the 'namespace' query parameter is required"));
  }

  private static void assertUnauthorizedProblem(RequestSpecification request, String path) {
    request
        .when()
        .get(path)
        .then()
        .statusCode(401)
        .contentType("application/problem+json")
        .header("WWW-Authenticate", "Bearer")
        .body("type", equalTo("about:blank"))
        .body("title", equalTo("Unauthorized"))
        .body("status", equalTo(401));
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
      String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(row);
      producer.send(new ProducerRecord<>(KAFKA_TOPIC, "wc-1", json)).get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed the real WorldCheck row onto Kafka", e);
    }
  }

  private static String dockerBridgeGatewayIp() throws Exception {
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
  }

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
