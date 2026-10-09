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
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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

/** Real REST admission, selected current executor, and persisted provider output. */
@QuarkusTest
@QuarkusTestResource(
    value = DirectIngestionMatrixQuarkusPekkoTestCustomerMasterSmokeTest.SmokeResource.class,
    restrictToAnnotatedClass = true)
class DirectIngestionMatrixQuarkusPekkoTestCustomerMasterSmokeTest {

  @Test
  @Timeout(180)
  void dispatchesARealJobThatReachesRealOpenSearchThroughHostAliases() throws Exception {
    String correlationId = "smoke-" + UUID.randomUUID();
    String token = SmokeResource.fixture.mintUserToken();

    var spec =
        TestCustomerMasterFixtures.boundedFileSpec(
            "file:/tmp?fileName=source.csv&noop=true&initialDelay=0&delay=100",
            SmokeResource.openSearchUrlForPod,
            Path.of(""));

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
        SmokeResource.opensearch.hostEndpoint()
            + "/test-customer-master-screening-records/_doc/"
            + "KYC22438AML";
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
    static final String ROLE_NAME = "quarkus-direct-cm-smoke-role";
    static final String NAMESPACE = "fds-quarkus-cm-smoke";

    private static String PEKKO_IMAGE;

    /**
     * Real Customer Master header + the real, well-populated organisation row (ss-3, "iShares
     * V..."), both extracted directly from {@link TestCustomerMasterFixtures#SAMPLE_CSV} - never
     * hand-retyped.
     */
    static String realRowSeedAndRunCommand() {
      String[] lines = TestCustomerMasterFixtures.SAMPLE_CSV.split("\n");
      String seedCsv = lines[0] + "\n" + lines[3] + "\n";
      String seedCsvBase64 =
          Base64.getEncoder().encodeToString(seedCsv.getBytes(StandardCharsets.UTF_8));
      return "sh -c 'echo "
          + seedCsvBase64
          + " | base64 -d > /tmp/source.csv && "
          + "exec java -jar /deployments/application.jar /tmp/ingestion-spec.yaml'";
    }

    static volatile AuthzenKeycloakFixture fixture;
    static volatile KubernetesTestContainer kubernetes;
    static volatile OpenSearchTestContainer opensearch;

    static volatile String openSearchUrlForPod;

    @Override
    public Map<String, String> start() {
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
          fixture.provisionTenant("quarkus-direct-cm-smoke-org", tenantDid, ROLE_NAME);
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
      PEKKO_IMAGE = kubernetes.loadImageAndPinDigest(executorImage);
      try (var client = kubernetes.createClient()) {
        client
            .namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }

      opensearch = new OpenSearchTestContainer().start();
      int opensearchPort = opensearch.hostEndpoint().getPort();
      String gatewayIp = dockerBridgeGatewayIp();
      openSearchUrlForPod = "http://host.docker.internal:" + opensearchPort;

      String pekkoImage = PEKKO_IMAGE;
      String pekkoCommand = realRowSeedAndRunCommand();

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
          Map.entry("datastreaming.launcher.k8s.image-pull-secrets", "unused"),
          Map.entry("datastreaming.launcher.k8s.host-aliases", "host.docker.internal=" + gatewayIp),
          Map.entry("datastreaming.launcher.fowf.keycloak.client-id", "unused"),
          Map.entry("datastreaming.launcher.fowf.keycloak.client-secret", "unused"),
          Map.entry("datastreaming.launcher.pekko.image", pekkoImage),
          Map.entry("datastreaming.launcher.pekko.command", pekkoCommand),
          Map.entry("datastreaming.launcher.kafka-streams.image", "unused"),
          Map.entry("datastreaming.launcher.kafka-streams.command", "false"),
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
