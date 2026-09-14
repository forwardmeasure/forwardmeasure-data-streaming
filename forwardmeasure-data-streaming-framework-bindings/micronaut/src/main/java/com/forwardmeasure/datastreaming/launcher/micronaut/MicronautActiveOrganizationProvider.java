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
package com.forwardmeasure.datastreaming.launcher.micronaut;

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.ActiveOrganizationProvider;
import com.forwardmeasure.authzen.AuthenticationRequiredException;
import com.forwardmeasure.authzen.KeycloakOrganizationClaims;
import io.micronaut.context.annotation.Value;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;

/**
 * Reads only the nested active-Organization claims from Micronaut's verified JWT. Ported from
 * forwardmeasure-entity-intelligence's own real {@code MicronautActiveOrganizationProvider}, added
 * 2026-09-14 to close a confirmed gap - this repo had zero inbound authentication before (see
 * docs/fds-authorization-remediation-guide.md). That sibling also declares a thread-local
 * authentication-scoping escape hatch for Micronaut's reactive dispatch - carried over from fowf's
 * own real source but with no current caller in either sibling; not ported here, matching this
 * session's own "no abstraction beyond what the task requires" discipline.
 *
 * <p>One deliberate change from both siblings: they throw {@link SecurityException} for a missing
 * authentication, since their own {@code SecurityExceptionMapper} is the 401 mapper for that repo.
 * Here, {@code SecurityException} is already reserved for {@code IngestionJobPolicy}'s own,
 * distinct 403 case (see this module's own {@code SecurityExceptionMapper} in {@code
 * launcher-jaxrs}), so this class throws {@link AuthenticationRequiredException} instead - the same
 * type {@link KeycloakOrganizationClaims#extract} already throws, mapped to 401 by this module's
 * own new {@code MicronautAuthenticationRequiredExceptionMapper}.
 */
@Singleton
public class MicronautActiveOrganizationProvider implements ActiveOrganizationProvider {
  private final SecurityService security;
  private final String clientId;

  // Deliberately NOT datastreaming.launcher.fowf.keycloak.client-id (this service's own identity
  // for its outbound client-credentials call to fowf) - this is which client's roles to read out
  // of the INCOMING browser-issued JWT's organization claim. Mirrors fei's own
  // organization-client-id vs. client-id distinction exactly.
  public MicronautActiveOrganizationProvider(
      SecurityService security,
      @Value("${datastreaming.launcher.authorization.organization-client-id}")
          String organizationClientId) {
    this.security = security;
    this.clientId = organizationClientId;
  }

  @Override
  public ActiveOrganization current() {
    var authentication =
        security
            .getAuthentication()
            .orElseThrow(
                () -> new AuthenticationRequiredException("An authenticated JWT is required"));
    return KeycloakOrganizationClaims.extract(authentication.getAttributes(), clientId);
  }
}
