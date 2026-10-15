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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import com.forwardmeasure.datastreaming.testfixtures.TestTransformImages;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * A CSV file load on a Kafka Streams installation: REST admission, the real loader Job writing the
 * file's rows to the run's load topic, then the real Kafka Streams delivery Job writing the mapped
 * rows to OpenSearch. Neither image carries Pekko (enforced by their builds).
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.context.annotation.Import(
    DirectIngestionMatrixSpringKafkaStreamsCsvLoadSmokeTest.TestKubernetesClientConfiguration.class)
class DirectIngestionMatrixSpringKafkaStreamsCsvLoadSmokeTest {

  private static final String ROLE_NAME = "spring-ks-csv-smoke-role";
  private static final String NAMESPACE = "fds-spring-ks-csv-smoke";

  private static String KAFKA_STREAMS_IMAGE;
  private static String LOADER_IMAGE;
  static volatile KafkaTestContainer kafka;

  private static AuthzenKeycloakFixture fixture;
  private static KubernetesTestContainer kubernetes;
  private static OpenSearchTestContainer opensearch;

  private static String openSearchUrlForPod;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() throws Exception {
    String executorImage = requiredProperty("fds.acceptance.kafka-streams.image");
    String loaderImage = requiredProperty("fds.acceptance.loader.image");
    fixture = AuthzenKeycloakFixture.start();
    var tenantDid =
        com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
    UUID tenantId = com.forwardmeasure.jpa.tenancy.TenantId.forDid(tenantDid).value();
    String organizationId =
        fixture.provisionTenant("spring-ks-csv-smoke-org", tenantDid, ROLE_NAME);
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
    registry.add("datastreaming.launcher.k8s.images", () -> kafkaStreamsImage + "," + LOADER_IMAGE);
    registry.add("datastreaming.launcher.k8s.image-pull-secrets", () -> "unused");
    registry.add("datastreaming.launcher.engine", () -> "kafka-streams");
    registry.add("datastreaming.launcher.spark.image", () -> "unused-spark");
    registry.add("datastreaming.launcher.spark.command", () -> "false");
    registry.add(
        "datastreaming.launcher.kafka.bootstrap-servers",
        () -> kafka.hostDockerInternalBootstrapServers());
    registry.add("datastreaming.launcher.pekko.image", () -> "unused");
    registry.add("datastreaming.launcher.pekko.command", () -> "false");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> kafkaStreamsImage);
    registry.add(
        "datastreaming.launcher.kafka-streams.command",
        () -> "java -jar /deployments/application.jar");
    registry.add("datastreaming.launcher.loader.image", () -> LOADER_IMAGE);
    registry.add("datastreaming.launcher.loader.command", () -> realRowSeedAndRunCommand());
  }

  @Test
  @Timeout(300)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = fixture.mintUserToken();

    var spec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp?fileName=source.csv&noop=true&initialDelay=0&delay=100",
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

    String documentUri =
        opensearch.hostEndpoint() + "/test-customer-master-screening-records/_doc/KYC22438AML";
    HttpResponse<String> document =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
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

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Set " + name + " to the current locally built image");
    }
    return value;
  }
}
