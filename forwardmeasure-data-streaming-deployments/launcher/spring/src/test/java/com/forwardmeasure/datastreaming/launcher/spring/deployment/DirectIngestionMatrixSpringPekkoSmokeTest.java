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
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
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
 * The Spring sibling of {@code DirectIngestionMatrixQuarkusPekkoSmokeTest} (forwardmeasure-data-
 * streaming-deployments/launcher/quarkus) - first-ever real, no-mocks boot test for this launcher's
 * Spring deployment (confirmed: no test source existed in this module before). Same real proof: a
 * real HTTP call against a real, booted Spring app -&gt; real Keycloak-authenticated JWT -&gt; real
 * AuthZEN authorization -&gt; real {@code DirectIngestionLauncher} dispatch into a real K3s
 * testcontainer -&gt; the dispatched pod reaches a real, sibling OpenSearch testcontainer via
 * {@code hostAliases} (a real gap found and fixed in {@code LauncherSpringBinding} while building
 * this test - that binding had never wired {@code hostAliases} through to {@code
 * DirectIngestionLauncher} at all, unlike the Quarkus binding).
 *
 * <p>Real business-logic proof when Docker Hub credentials are available, same fallback discipline
 * as the Quarkus sibling: dispatches the real, pushed Pekko executor image and seeds a real, ASCII-
 * safe WorldCheck row (wc-3, "Acme Holdings") when {@code DOCKER_HUB_USERNAME}/{@code
 * DOCKER_HUB_TOKEN} are set, otherwise falls back to a public curl stand-in image proving only the
 * dispatch/reachability plumbing.
 *
 * <p><b>The K3s-pointed {@code KubernetesClient} override has no existing precedent anywhere in
 * this org</b> (confirmed via research) - Spring's own {@code @TestConfiguration}+{@code @Primary}
 * bean override is used instead of Quarkus's {@code @Alternative @Priority} CDI mechanism, and it
 * must be brought in via an explicit {@code @Import} on this class - see the real, live-verified
 * near-miss documented on that annotation below (a bare nested {@code @TestConfiguration}, with no
 * {@code @Import}, is silently never applied at all, and the real dispatch genuinely reached this
 * machine's own real production GKE cluster before this fix).
 */
@SpringBootTest(
    classes = LauncherSpringApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// Real, live-verified near-miss (2026-09-21), two real bugs found in sequence: (1) a bare nested
// @TestConfiguration is NOT picked up by LauncherSpringApplication's own @ComponentScan (which
// deliberately excludes @TestConfiguration classes, precisely to stop a stray test config leaking
// into every other test) - without this explicit @Import, IngestionRunResource kept getting the
// real, ambient-kubeconfig-pointed production KubernetesClient bean, and a real dispatched POST
// reached this machine's own real GKE production cluster (401, only because gcloud auth had
// separately expired - not a safety mechanism this test can rely on, same real class of near-miss
// the Quarkus sibling test already documents). (2) Once imported, the override bean's method was
// ALSO named kubernetesClient() - Spring resolves a bean's name from its factory method name by
// default, so this collided with the production bean's own identically-named definition and threw
// BeanDefinitionOverrideException rather than the two coexisting as @Primary-resolved candidates;
// renamed to testKubernetesClient() below to fix.
@org.springframework.context.annotation.Import(
    DirectIngestionMatrixSpringPekkoSmokeTest.TestKubernetesClientConfiguration.class)
class DirectIngestionMatrixSpringPekkoSmokeTest {

  private static final String ROLE_NAME = "spring-direct-smoke-role";
  private static final String NAMESPACE = "fds-spring-smoke";
  private static final String DOCUMENT_ID = "smoke-doc-3";
  private static final String CURL_IMAGE =
      "curlimages/curl@sha256:83a505ba2ba62f208ed6e410c268b7b9aa48f0f7b403c8108b9773b44199dbba";

  /** Same digest {@code DirectIngestionLauncherRealImageIntegrationTest} pins. */
  private static final String PEKKO_IMAGE =
      "docker.io/forwardmeasure/data-streaming-executor-pekko@sha256:"
          + "1b3d5e60475535643db17ce9038c293252eba3f0be1a48e2e982ad09c96f36af";

  private static final String PULL_SECRET_NAME = "dockerhub-pull-secret";

  private static AuthzenKeycloakFixture fixture;
  private static KubernetesTestContainer kubernetes;
  private static OpenSearchTestContainer opensearch;
  private static boolean realImageMode;
  private static String openSearchUrlForPod;

  @LocalServerPort private int port;

  @BeforeAll
  static void startFixtures() throws Exception {
    fixture = AuthzenKeycloakFixture.start();
    UUID tenantId = UUID.randomUUID();
    String organizationId = fixture.provisionTenant("spring-direct-smoke-org", tenantId, ROLE_NAME);
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
  }

  @AfterAll
  static void stopFixtures() {
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

    boolean real = realImageMode;
    String pekkoImage = real ? PEKKO_IMAGE : CURL_IMAGE;
    registry.add("datastreaming.launcher.k8s.images", () -> pekkoImage);
    registry.add(
        "datastreaming.launcher.k8s.image-pull-secrets", () -> real ? PULL_SECRET_NAME : "unused");
    registry.add("datastreaming.launcher.pekko.image", () -> pekkoImage);
    registry.add(
        "datastreaming.launcher.pekko.command",
        () ->
            real
                ? realRowSeedAndRunCommand()
                : "curl -sf -X PUT "
                    + openSearchUrlForPod
                    + "/smoke-index/_doc/"
                    + DOCUMENT_ID
                    + " -H Content-Type:application/json -d {\\\"marker\\\":\\\""
                    + DOCUMENT_ID
                    + "\\\"} #");
    registry.add("datastreaming.launcher.kafka-streams.image", () -> CURL_IMAGE);
    registry.add("datastreaming.launcher.kafka-streams.command", () -> "true #");
  }

  @Test
  @Timeout(180)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = fixture.mintUserToken();

    var spec =
        realImageMode
            ? WorldCheckFixtures.boundedFileSpec(
                "file:/tmp?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                openSearchUrlForPod,
                Path.of(""))
            : WorldCheckFixtures.boundedFileSpec(
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
        realImageMode
            ? opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-3"
            : opensearch.hostEndpoint() + "/smoke-index/_doc/" + DOCUMENT_ID;
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
        document.body().contains(realImageMode ? "\"entity_kind\":\"organization\"" : DOCUMENT_ID),
        "unexpected document body: " + document.body());
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

  /**
   * Same real wc-3 row + base64 transport {@code DirectIngestionMatrixQuarkusPekkoSmokeTest} uses.
   */
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

  /**
   * Real, live K3s-pointed override of the production {@code kubernetesClient()} bean - see this
   * class's own javadoc for why no ambient-cluster safety risk exists here even though the real
   * production bean is still eagerly constructed alongside this one.
   */
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
