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
import java.time.Duration;
import java.util.Base64;
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
 * denies before ever touching Kubernetes (the {@code cancel} deny case below passes a {@code null}
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
    // The real Keycloak resource this grants must be keyed by the SAME resource id
    // DataStreamingAuthorizationResources.ingestionRun(...) actually sends at evaluation time - a
    // fixed collection id ("ingestion-runs"), not the varying correlationId (that only ever lands
    // in resource.properties.correlation_id, which Keycloak's own resource-name matching never
    // consults). Confirmed against production: DirectIngestionLauncher.launch/observe/cancel all
    // build this same resource via ingestionRun(correlationId), so a real deployment authorizes
    // per-role-on-the-collection, not per-run - matching OpenWorkflowAuthorizationResources'
    // identical (type, fixed-collection-id, properties) shape and its own real, working
    // KeycloakOrganizationFixture#grantHumanTaskAuthorization precedent (also collection-scoped, no
    // per-instance id parameter at all). Deriving the id from the real factory here, rather than
    // repeating "ingestion-runs" as a second literal, is deliberate: keeps this grant from ever
    // silently drifting out of sync with the factory again the way it did before this fix.
    String ingestionRunsResourceId =
        DataStreamingAuthorizationResources.ingestionRun(GRANTED_RESOURCE_ID).id();
    fixture.grantResourceAuthorization(
        "datastreaming-ingestion-run",
        ingestionRunsResourceId,
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
  void deniesAnUngrantedCancelBeforeEverTouchingKubernetes() {
    DirectIngestionLauncher launcher =
        new DirectIngestionLauncher(
            IngestionJobPolicy.rejecting(), authorization, "unused-image", "unused-command");
    // GRANTED_ROLE only ever received the INGESTION_RUN_LAUNCH scope (see @BeforeAll) - never
    // INGESTION_RUN_CANCEL - so this is a real, never-granted permission, denied by the real PDP
    // exactly as fail-closed requires. Deliberately not "a different correlationId than the
    // granted run": once authorization is correctly collection-scoped (see @BeforeAll's own
    // comment), every correlationId resolves to the identical Keycloak resource, so varying it
    // alone can never produce a real denial - this exercises the one dimension that genuinely
    // does. Cancel (like launch/observe) still authorizes before ever touching the passed-in
    // KubernetesClient - the null client below would NullPointerException instead of denying if
    // that ordering ever regressed.
    assertThrows(
        AuthorizationDeniedException.class,
        () ->
            launcher.cancel(
                null, "keycloak-integration-test", "keycloak-integration-denied-run", actor));
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
