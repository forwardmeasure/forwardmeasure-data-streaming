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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.KeycloakOrganizationClaims;
import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Stage 1+2 only: real Postgres + the real {@code openworkflow-migrations} image (Stage 1) plus a
 * real Keycloak Organization carrying the identical, independently-derived {@code TenantId} (Stage
 * 2). Verifies both sides agree on the real tenant identity - the load-bearing precondition for
 * every later stage (execution-management/the engine/the operation-adapter all resolve {@code
 * TenantId} from the minted JWT, and must land on the exact same tenant migrations onboarded).
 */
class RealFowfWorkflowFixtureStage1Test {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ROLE = "workflow-run-launcher";

  @Test
  @Timeout(180)
  void migrationsAndKeycloakAgreeOnTheSameRealTenantIdentity() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage1", ROLE)) {
      try (Connection connection = fixture.postgres().dataSource().getConnection();
          Statement statement = connection.createStatement();
          ResultSet rows =
              statement.executeQuery(
                  "select tenant_id, tenant_did, alias, database_name, status"
                      + " from tenant_registry where alias = 'fds-stage1'")) {
        assertTrue(rows.next(), "expected a tenant_registry row for alias 'fds-stage1'");
        assertEquals(
            "did:web:fds-stage1." + RealFowfWorkflowFixture.TENANT_DOMAIN,
            rows.getString("tenant_did"));
        assertEquals("ACTIVE", rows.getString("status"));
        assertEquals(
            fixture.tenantId().value().toString(),
            rows.getString("tenant_id"),
            "the fixture's own independently-derived TenantId must match the real"
                + " openworkflow-migrations-assigned one - proof the DID derivation is genuinely"
                + " identical, not coincidentally close");
      }

      String accessToken = fixture.keycloak().mintUserToken();
      Map<String, Object> claims = decodeClaims(accessToken);
      ActiveOrganization actor =
          KeycloakOrganizationClaims.extract(claims, AuthzenKeycloakFixture.CLIENT_ID);
      assertEquals(
          fixture.tenantId(),
          actor.tenantId(),
          "a real minted JWT must resolve to the exact same TenantId the real tenant registry"
              + " row has - this is what lets fowf's own QuarkusActiveOrganizationProvider (same"
              + " KeycloakOrganizationClaims shape) resolve requests to the right tenant");
      assertEquals(fixture.organizationId(), actor.organizationId());
    }
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
