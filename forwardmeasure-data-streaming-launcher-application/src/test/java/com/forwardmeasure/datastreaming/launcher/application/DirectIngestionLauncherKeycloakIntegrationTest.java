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
package com.forwardmeasure.datastreaming.launcher.application;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationDecision;
import com.forwardmeasure.authzen.AuthorizationDeniedException;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.KeycloakOrganizationClaims;
import com.forwardmeasure.authzen.client.AuthzenAuthorizationFactory;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocks proof that {@link DirectIngestionLauncher#launch} actually reaches a genuine
 * Keycloak 26.7 AuthZEN PDP through {@link DataStreamingAuthorizationResources}/{@link
 * AuthorizationAction} - not a fake HTTP server or a hand-built claims map - added 2026-09-14 to
 * close a confirmed real gap (see docs/fds-authorization-remediation-guide.md, §4 verification
 * checklist item "A live Keycloak testcontainer round trip"). Mirrors
 * forwardmeasure-authzen-client's own {@code AuthzenAuthorizationServiceKeycloakIntegrationTest}
 * exactly, scoped to this launcher's own real action/resource vocabulary.
 *
 * <p>Deliberately does not exercise a real Kubernetes cluster: {@link DirectIngestionLauncherTest}
 * already proves the real launch/observe/cancel Job mechanics end-to-end with a permissive {@code
 * StubAuthorizationService} - running that same mechanic again against a second, real Keycloak
 * container would be redundant, not more rigorous. What's new and worth proving here is
 * specifically that this launcher's own {@code authorize(...)} call, wired to a real PDP, both
 * denies before ever touching Kubernetes (the {@code launch} deny case below passes a {@code null}
 * {@link io.fabric8.kubernetes.client.KubernetesClient} - if authorization ran second instead of
 * first, this test would NullPointerException instead of denying) and genuinely grants for a real,
 * provisioned role.
 */
class DirectIngestionLauncherKeycloakIntegrationTest {

  private static final String GRANTED_RESOURCE_ID = "keycloak-integration-granted-run";
  private static final String GRANTED_ROLE = "ingestion-run-launcher";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static AuthzenKeycloakFixture fixture;
  private static AuthorizationService authorization;
  private static ActiveOrganization actor;

  @BeforeAll
  static void startKeycloakAndProvisionRealAuthorization() {
    fixture = AuthzenKeycloakFixture.start();
    fixture.grantResourceAuthorization(
        "datastreaming-ingestion-run",
        GRANTED_RESOURCE_ID,
        "ingestion-run-launch-permission",
        GRANTED_ROLE,
        Set.of(AuthorizationAction.INGESTION_RUN_LAUNCH.scope()));
    UUID tenantId = UUID.randomUUID();
    fixture.provisionTenant("datastreaming-org", tenantId, GRANTED_ROLE);

    String accessToken = fixture.mintUserToken();
    Map<String, Object> claims = decodeClaims(accessToken);
    actor = KeycloakOrganizationClaims.extract(claims, AuthzenKeycloakFixture.CLIENT_ID);

    authorization =
        AuthzenAuthorizationFactory.create(
            MAPPER,
            fixture.issuer(),
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID,
            AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET,
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            100,
            "fds-integration-test-v1");
  }

  @AfterAll
  static void stopKeycloak() {
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  void deniesAnUnprovisionedLaunchBeforeEverTouchingKubernetes() {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.rejecting(), authorization, "unused-image", "unused-command");
    // No grantResourceAuthorization call exists for this correlationId - a real, unprovisioned
    // resource, denied by the real PDP exactly as fail-closed requires.
    DirectLaunchRequest request =
        new DirectLaunchRequest(
            "keycloak-integration-denied-run",
            "keycloak-integration-test",
            ingestionSpec(),
            Map.of(),
            Map.of(),
            null);

    assertThrows(AuthorizationDeniedException.class, () -> launcher.launch(null, request, actor));
  }

  @Test
  void grantsTheRealAuthzenScopeForAProvisionedRole() {
    AuthorizationDecision decision =
        authorization.evaluate(
            new AuthorizationRequest(
                actor,
                DataStreamingAuthorizationResources.ingestionRun(GRANTED_RESOURCE_ID),
                AuthorizationAction.INGESTION_RUN_LAUNCH,
                "keycloak-integration-permit",
                Map.of()));
    assertTrue(decision.permitted(), "a role holding the granted scope must be permitted");
  }

  private static IngestionSpec ingestionSpec() {
    return new IngestionSpec(
        new SourceSpec("file", "file:///tmp/does-not-matter.csv", null, null),
        new TransformSpec("party", List.of()),
        new SinkSpec("opensearch", "test-index", null, null),
        new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(1, 1), null, null));
  }

  private static Map<String, Object> decodeClaims(String jwt) {
    String[] segments = jwt.split("\\.");
    byte[] payload = Base64.getUrlDecoder().decode(segments[1]);
    try {
      return MAPPER.readValue(payload, new TypeReference<Map<String, Object>>() {});
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
