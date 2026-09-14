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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.ActiveOrganizationProvider;
import com.forwardmeasure.authzen.AuthenticationRequiredException;
import com.forwardmeasure.authzen.KeycloakOrganizationClaims;
import jakarta.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * Reads only the nested active-Organization claims from Quarkus's verified JWT. Ported from
 * forwardmeasure-entity-intelligence's own real {@code QuarkusActiveOrganizationProvider} (itself
 * ported from forwardmeasure-openworkflow's), added 2026-09-14 to close a confirmed gap: this repo
 * had zero inbound authentication before (see docs/fds-authorization-remediation-guide.md).
 *
 * <p>One deliberate change from both siblings: they throw {@link SecurityException} for a
 * missing/malformed token, since their own {@code SecurityExceptionMapper} is the 401 mapper for
 * that repo. Here, {@code SecurityException} is already reserved for {@code IngestionJobPolicy}'s
 * own, distinct 403 case (see this module's own {@code SecurityExceptionMapper} in {@code
 * launcher-jaxrs}), so every guard in this class throws {@link AuthenticationRequiredException}
 * instead - the same type {@link KeycloakOrganizationClaims#extract} already throws, mapped to 401
 * by this module's own new {@code AuthenticationRequiredExceptionMapper}.
 */
@RequestScoped
public class QuarkusActiveOrganizationProvider implements ActiveOrganizationProvider {
  private final JsonWebToken token;
  private final ObjectMapper mapper;
  private final String clientId;

  // Deliberately NOT datastreaming.launcher.fowf.keycloak.client-id (this service's own identity
  // for its outbound client-credentials call to fowf) - this is which client's roles to read out
  // of the INCOMING browser-issued JWT's organization claim. Mirrors fei's own
  // organization-client-id vs. client-id distinction exactly.
  public QuarkusActiveOrganizationProvider(
      JsonWebToken token,
      ObjectMapper mapper,
      @ConfigProperty(name = "datastreaming.launcher.authorization.organization-client-id")
          String organizationClientId) {
    this.token = token;
    this.mapper = mapper;
    this.clientId = organizationClientId;
  }

  @Override
  public ActiveOrganization current() {
    String rawToken = token.getRawToken();
    if (rawToken == null || rawToken.isBlank()) {
      throw new AuthenticationRequiredException("An authenticated JWT is required");
    }
    try {
      String[] segments = rawToken.split("\\.");
      if (segments.length != 3) {
        throw new AuthenticationRequiredException(
            "Verified JWT has an invalid compact representation");
      }
      byte[] payload = Base64.getUrlDecoder().decode(segments[1]);
      Map<String, Object> claims = mapper.readValue(payload, new TypeReference<>() {});
      return KeycloakOrganizationClaims.extract(claims, clientId);
    } catch (IllegalArgumentException | IOException exception) {
      throw new AuthenticationRequiredException(
          "Verified JWT claims could not be decoded", exception);
    }
  }
}
