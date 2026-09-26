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
package com.forwardmeasure.datastreaming.launcher.quarkus;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.launcher.application.AuthorizationAction;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Phase H's own sibling of {@code DirectIngestionMatrixQuarkusKafkaStreamsSmokeTest} - the
 * identical {@code KAFKA_STREAMS} delivery-engine plumbing proof, seeded with a real Customer
 * Master row instead of WorldCheck.
 */
@QuarkusTest
@QuarkusTestResource(
    value = DirectIngestionMatrixQuarkusKafkaStreamsTestCustomerMasterSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class DirectIngestionMatrixQuarkusKafkaStreamsTestCustomerMasterSmokeTest {

  private static final String DOCUMENT_ID = "smoke-doc-2";
  private static final String KAFKA_TOPIC = "test-customer-master-rows";

  @Test
  @Timeout(180)
  void dispatchesARealKafkaStreamsJobThatReachesRealOpenSearchThroughHostAliases()
      throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = SmokeResource.fixture.mintUserToken();

    if (SmokeResource.realImageMode) {
      seedOneRealTestCustomerMasterRow(SmokeResource.kafka.bootstrapServers());
    }

    var spec =
        SmokeResource.realImageMode
            ? TestCustomerMasterFixtures.boundedKafkaSpec(
                "kafka:"
                    + KAFKA_TOPIC
                    + "?brokers="
                    + SmokeResource.kafka.hostDockerInternalBootstrapServers(),
                SmokeResource.openSearchUrlForPod,
                Path.of(""))
            : TestCustomerMasterFixtures.boundedKafkaSpec(
                "kafka:" + KAFKA_TOPIC + "?brokers=unused:9092",
                "http://unused:9200",
                Path.of("/tmp/unused.json"));

    Map<String, Object> requestBody =
        Map.of(
            "correlationId",
            correlationId,
            "namespace",
            SmokeResource.NAMESPACE,
            "ingestionSpec",
            spec,
            "resourceRequests",
            Map.of(),
            "resourceLimits",
            Map.of());

    given()
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
        SmokeResource.realImageMode
            ? SmokeResource.opensearch.hostEndpoint()
                + "/test-customer-master-screening-records/_doc/KYC85310AML"
            : SmokeResource.opensearch.hostEndpoint() + "/smoke-index/_doc/" + DOCUMENT_ID;
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
        document.body().contains(SmokeResource.realImageMode ? "DOSHAY" : DOCUMENT_ID),
        "unexpected document body: " + document.body());
  }

  /**
   * Real Customer Master row ss-1 ("Steven DOSHAY") from {@link TestCustomerMasterFixtures},
   * JSON-encoded.
   */
  private static void seedOneRealTestCustomerMasterRow(String bootstrapServers) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("ENTITYS_UNIQUE_ID", "KYC85310AML");
    row.put("BUSINESS_ENTITY_RECORD_ID", "f1321672-c82e-4595-93be-be072caf2481");
    row.put("ENTITYS_FULL_NAME", "Steven DOSHAY");
    row.put("ENTITY_FIRST_NAME", "Steven");
    row.put("ENTITY_LAST_NAME", "DOSHAY");
    row.put("ENTITY_LOCATION_1_ADDRESS_LINE_1", "3521 Malaga Ct");
    row.put("ENTITY_LOCATION_1_CITY", "CA");
    row.put("ENTITY_LOCATION_1_STATE_PROVINCE", "California");
    row.put("ENTITY_LOCATION_1_COUNTRY", "United States (the)");
    row.put("DATE_OF_BIRTH_DATE_OF_INCORPORATION", "19531231");
    row.put("JOB_TITLE", "Senior Councel");
    row.put("ENTITYS_TYPE", "Individual");
    row.put("ENTITY_PUBLIC_IDENTIFIER", "{SSN}XXX-XX-9875");
    row.put("ENTITY_CLASSIFICATION", "Associated Third Party");

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(row);
      producer.send(new ProducerRecord<>(KAFKA_TOPIC, "KYC85310AML", json)).get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed the real Customer Master row onto Kafka", e);
    }
  }

  private static String pollUntilTerminal(String token, String correlationId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
    String phase = "UNKNOWN";
    while (System.nanoTime() < deadline) {
      var response =
          given()
              .header("Authorization", "Bearer " + token)
              .when()
              .get("/ingestion-runs/" + correlationId + "?namespace=" + SmokeResource.NAMESPACE);
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

  public static final class SmokeResource implements QuarkusTestResourceLifecycleManager {
    static final String ROLE_NAME = "quarkus-kstreams-cm-smoke-role";
    static final String NAMESPACE = "fds-quarkus-kstreams-cm-smoke";
    static final String CURL_IMAGE =
        "curlimages/curl@sha256:83a505ba2ba62f208ed6e410c268b7b9aa48f0f7b403c8108b9773b44199dbba";

    static final String KAFKA_STREAMS_IMAGE =
        "docker.io/forwardmeasure/data-streaming-executor-kafka-streams@sha256:"
            + "fa7235432b6991a2d717d0e8cb5f6b5a39786023782d9834e681afb41ea001fd";

    static final String PULL_SECRET_NAME = "dockerhub-pull-secret";

    static volatile AuthzenKeycloakFixture fixture;
    static volatile KubernetesTestContainer kubernetes;
    static volatile OpenSearchTestContainer opensearch;
    static volatile KafkaTestContainer kafka;
    static volatile boolean realImageMode;
    static volatile String openSearchUrlForPod;

    @Override
    public Map<String, String> start() {
      fixture = AuthzenKeycloakFixture.start();
      UUID tenantId = UUID.randomUUID();
      String organizationId =
          fixture.provisionTenant("quarkus-kstreams-cm-smoke-org", tenantId, ROLE_NAME);
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
      try (var client = kubernetes.createClient()) {
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
      if (realImageMode) {
        kafka =
            new KafkaTestContainer(
                    com.forwardmeasure.testcontainers.kafka.KafkaContainerConfiguration.defaults()
                        .withHostDockerInternalListener())
                .start();
      }
      int opensearchPort = opensearch.hostEndpoint().getPort();
      String gatewayIp = dockerBridgeGatewayIp();
      openSearchUrlForPod = "http://host.docker.internal:" + opensearchPort;

      String kafkaStreamsImage = realImageMode ? KAFKA_STREAMS_IMAGE : CURL_IMAGE;
      String kafkaStreamsCommand =
          realImageMode
              ? "java -jar /deployments/application.jar"
              : "curl -sf -X PUT "
                  + openSearchUrlForPod
                  + "/smoke-index/_doc/"
                  + DOCUMENT_ID
                  + " -H Content-Type:application/json -d {\\\"marker\\\":\\\""
                  + DOCUMENT_ID
                  + "\\\"} #";

      return Map.ofEntries(
          Map.entry("quarkus.oidc.auth-server-url", fixture.issuer().toString()),
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
          Map.entry(
              "datastreaming.launcher.k8s.image-pull-secrets",
              realImageMode ? PULL_SECRET_NAME : "unused"),
          Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
          Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
          Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
          Map.entry("datastreaming.launcher.pekko.image", CURL_IMAGE),
          Map.entry("datastreaming.launcher.pekko.command", "true #"),
          Map.entry("datastreaming.launcher.kafka-streams.image", kafkaStreamsImage),
          Map.entry("datastreaming.launcher.kafka-streams.command", kafkaStreamsCommand),
          Map.entry("datastreaming.launcher.spark.image", "unused"),
          Map.entry("datastreaming.launcher.spark.command", "unused"),
          Map.entry("datastreaming.launcher.kafka.bootstrap-servers", "unused"));
    }

    @Override
    public void stop() {
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
