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
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The first real, no-mocks proof of Phase G's own core new mechanism (see the repo's own
 * gap-bridging plan) - a real HTTP call against a real, booted Quarkus launcher app dispatches a
 * real Kubernetes Job into a real (Testcontainers-managed) K3s cluster, and that Job's own pod
 * reaches a real, sibling Testcontainers-managed service (OpenSearch) it has no cluster-DNS route
 * to, via the new {@code hostAliases} plumbing this session added to {@code KubernetesJobSpec}/
 * {@code KubernetesJobLifecycle} (fowf) and {@code DirectIngestionLauncher}/{@code
 * EnvConfiguredJobLauncher}/{@code LauncherQuarkusBinding} (this repo).
 *
 * <p><b>Real business-logic proof when Docker Hub credentials are available (2026-09-21)</b>: if
 * {@code DOCKER_HUB_USERNAME}/{@code DOCKER_HUB_TOKEN} are set (same gate {@code
 * DirectIngestionLauncherRealImageIntegrationTest} already uses), this test dispatches the real,
 * private, pushed Pekko executor image and seeds a real (ASCII-only, shell-safe) WorldCheck row -
 * {@code wc-3}, "Acme Holdings" - proving the real {@code FieldMappingEngine} mapping into a real
 * OpenSearch document, not just the plumbing around it. Falls back to a real, public stand-in image
 * ({@code curlimages/curl@sha256:83a505ba2ba62f208ed6e410c268b7b9aa48f0f7b403c8108b9773b44199dbba})
 * when credentials aren't available, so this test still runs in any environment - only the
 * dispatched container's own command/image differs; the real REST -&gt; real Keycloak auth -&gt;
 * real K8s dispatch -&gt; real cross-container reachability plumbing is identical either way. The
 * {@code IngestionSpec} in the request body is always a real, valid WorldCheck spec (from {@link
 * WorldCheckFixtures}) - {@code ExecutionPlanCompiler} still has to really resolve it to {@code
 * PEKKO_STREAMS} for this test to dispatch the right image at all.
 *
 * <p>The dispatched command ends in a real shell {@code #} comment so the extra {@code
 * /tmp/ingestion-spec.yaml} argument {@link
 * com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher}'s own {@code
 * reconstructSpecAndRunCommand} always appends is silently ignored - this stand-in never reads the
 * spec file at all, unlike a real runner. The document id it writes is a fixed constant, not
 * generated per test run - this class has exactly one real test method, so there is no collision
 * risk, and a fixed id keeps the dispatched command a plain string baked once at resource-startup
 * time rather than needing per-test config injection (Quarkus's own {@code
 * QuarkusTestResourceLifecycleManager#start} runs once before any {@code @Test} method).
 */
@QuarkusTest
@QuarkusTestResource(
    value = DirectIngestionMatrixQuarkusPekkoSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class DirectIngestionMatrixQuarkusPekkoSmokeTest {

  private static final String DOCUMENT_ID = "smoke-doc-1";

  @Test
  @Timeout(180)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = SmokeResource.fixture.mintUserToken();

    var spec =
        SmokeResource.realImageMode
            ? WorldCheckFixtures.boundedFileSpec(
                "file:/tmp?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                SmokeResource.openSearchUrlForPod,
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

    // Real polling loop against the real GET endpoint - not a direct KubernetesClient call - this
    // proves the observe() path through the real REST surface too, not just launch().
    String phase = pollUntilTerminal(token, correlationId);
    assertEquals(
        "SUCCEEDED", phase, "the real dispatched Job must reach a real terminal SUCCEEDED phase");

    // Independent verification: a real HTTP GET against the real OpenSearch container (from the
    // test JVM's own side) confirms the dispatched pod's own write genuinely happened, not just
    // that the Job phase reported success.
    String documentUri =
        SmokeResource.realImageMode
            ? SmokeResource.opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-3"
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
    // wc-3 has a blank FIRST NAME and no ALIASES, so its own real, already-documented mapping
    // behavior (see PekkoWorldCheckToOpenSearchIntegrationTest's identical assertion) produces no
    // "names" field at all - "entity_kind":"organization" (via classify_party_kind) is the real,
    // always-present field this row's own mapping actually populates.
    assertTrue(
        document
            .body()
            .contains(
                SmokeResource.realImageMode ? "\"entity_kind\":\"organization\"" : DOCUMENT_ID),
        "unexpected document body: " + document.body());
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
    static final String ROLE_NAME = "quarkus-direct-smoke-role";
    static final String NAMESPACE = "fds-quarkus-smoke";
    static final String CURL_IMAGE =
        "curlimages/curl@sha256:83a505ba2ba62f208ed6e410c268b7b9aa48f0f7b403c8108b9773b44199dbba";

    /**
     * Re-pinned 2026-09-21 alongside {@code DirectIngestionLauncherRealImageIntegrationTest} - see
     * that class's own javadoc for why the digest changed (the real SLF4J-provider fix).
     */
    static final String PEKKO_IMAGE =
        "docker.io/forwardmeasure/data-streaming-executor-pekko@sha256:"
            + "a1eab0f12073b4b2f0215fd350061522ffa01aca08ab259a81e8053ee04a90e3";

    static final String PULL_SECRET_NAME = "dockerhub-pull-secret";

    /**
     * Real WorldCheck row wc-3 ("Acme Holdings") plus the real header line, both extracted directly
     * from {@link WorldCheckFixtures#SAMPLE_TSV} (never hand-retyped - a manually-escaped TSV row
     * embedded character-by-character into a shell command is exactly the kind of thing that's easy
     * to get subtly wrong, e.g. a miscounted tab). Base64-transported into the pod the same way
     * {@link com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher} already
     * transports the {@code IngestionSpec} itself, rather than embedded literally in a shell {@code
     * printf} - avoids all escaping risk entirely.
     */
    static String realRowSeedAndRunCommand() {
      String[] lines = WorldCheckFixtures.SAMPLE_TSV.split("\n");
      String seedTsv = lines[0] + "\n" + lines[3] + "\n";
      String seedTsvBase64 =
          Base64.getEncoder().encodeToString(seedTsv.getBytes(StandardCharsets.UTF_8));
      return "sh -c 'echo "
          + seedTsvBase64
          + " | base64 -d > /tmp/source.csv && "
          + "exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml'";
    }

    static volatile AuthzenKeycloakFixture fixture;
    static volatile KubernetesTestContainer kubernetes;
    static volatile OpenSearchTestContainer opensearch;
    static volatile boolean realImageMode;
    static volatile String openSearchUrlForPod;

    @Override
    public Map<String, String> start() {
      fixture = AuthzenKeycloakFixture.start();
      var tenantDid =
          com.forwardmeasure.jpa.tenancy.Did.parse("did:fwmtest:tenant:" + UUID.randomUUID());
      UUID tenantId = com.forwardmeasure.jpa.tenancy.TenantId.forDid(tenantDid).value();
      String organizationId =
          fixture.provisionTenant("quarkus-direct-smoke-org", tenantDid, ROLE_NAME);
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
      try (var client = kubernetes.createClient()) {
        client
            .namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
        String username = System.getenv("DOCKER_HUB_USERNAME");
        String token = System.getenv("DOCKER_HUB_TOKEN");
        realImageMode = username != null && token != null;
        if (realImageMode) {
          client
              .secrets()
              .inNamespace(NAMESPACE)
              .resource(dockerConfigSecret(username, token))
              .create();
        }
      }

      opensearch = new OpenSearchTestContainer().start();
      int opensearchPort = opensearch.hostEndpoint().getPort();
      String gatewayIp = dockerBridgeGatewayIp();
      openSearchUrlForPod = "http://host.docker.internal:" + opensearchPort;

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
          Map.entry("datastreaming.launcher.k8s.images", pekkoImage),
          // Quarkus validates every @ConfigProperty-annotated producer parameter at startup,
          // before any test method runs and regardless of whether the bean is ever actually
          // injected - a blank "" value (the checked-in application.yml's own real default) fails
          // that validation outright, the same real gap forwardmeasure-entity-intelligence's own
          // AuthorizationSmokeResourceTest already documents for an identical reason. The curl
          // stand-in image is public and needs no real pull secret - "unused" is functionally
          // inert, only non-blank for Quarkus's own sake; real-image mode uses the real secret.
          Map.entry(
              "datastreaming.launcher.k8s.image-pull-secrets",
              realImageMode ? PULL_SECRET_NAME : "unused"),
          Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
          // Same blank-default-fails-startup-validation reason - datastreaming.launcher.fowf.* is
          // never actually called by this Direct-REST-only test (WorkflowIngestionLauncher/
          // ExecutionsApi are produced eagerly regardless), so any non-blank value is sufficient.
          Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
          Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
          Map.entry("datastreaming.launcher.pekko.image", pekkoImage),
          Map.entry("datastreaming.launcher.pekko.command", pekkoCommand),
          Map.entry("datastreaming.launcher.kafka-streams.image", CURL_IMAGE),
          Map.entry("datastreaming.launcher.kafka-streams.command", "true #"),
          // Same blank-default-fails-startup-validation reason - this cell never dispatches a
          // Spark-staged plan, so any non-blank value is sufficient.
          Map.entry("datastreaming.launcher.spark.image", "unused"),
          Map.entry("datastreaming.launcher.spark.command", "unused"),
          Map.entry("datastreaming.launcher.kafka.bootstrap-servers", "unused"));
    }

    @Override
    public void stop() {
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
