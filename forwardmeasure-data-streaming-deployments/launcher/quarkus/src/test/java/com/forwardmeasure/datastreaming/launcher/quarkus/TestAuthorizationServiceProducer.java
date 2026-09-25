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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.client.AuthzenAuthorizationFactory;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Real, Keycloak-backed {@link AuthorizationService} for the {@code test} build profile - {@link
 * LauncherQuarkusBinding}'s own real producer is {@code @UnlessBuildProfile("test")} (a shared
 * framework-bindings class, not this module's to change), so under {@code @QuarkusTest} (which
 * always activates the {@code test} profile) there is otherwise no {@link AuthorizationService}
 * producer at all, and {@link
 * com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher}'s own CDI
 * injection point fails to resolve. Mirrors forwardmeasure-entity-intelligence's own real {@code
 * TestAuthorizationServiceProducer} precedent exactly - the same real {@link
 * AuthzenAuthorizationFactory#create} call, reading config a per-test-class {@code
 * QuarkusTestResourceLifecycleManager} injects (a real {@code AuthzenKeycloakFixture} issuer/
 * client-id/client-secret) rather than the checked-in application.yml defaults (a dummy localhost
 * issuer, a blank client-secret).
 */
@ApplicationScoped
public class TestAuthorizationServiceProducer {

  @Produces
  @ApplicationScoped
  @IfBuildProfile("test")
  AuthorizationService authorizationService(
      ObjectMapper mapper,
      @ConfigProperty(name = "datastreaming.launcher.authorization.issuer") URI issuer,
      @ConfigProperty(name = "datastreaming.launcher.authorization.client-id") String clientId,
      @ConfigProperty(name = "datastreaming.launcher.authorization.client-secret")
          String clientSecret,
      @ConfigProperty(name = "datastreaming.launcher.authorization.request-timeout")
          Duration requestTimeout,
      @ConfigProperty(name = "datastreaming.launcher.authorization.decision-ttl")
          Duration decisionTtl,
      @ConfigProperty(name = "datastreaming.launcher.authorization.maximum-cache-entries")
          int cacheEntries,
      @ConfigProperty(name = "datastreaming.launcher.authorization.policy-version")
          String policyVersion) {
    return AuthzenAuthorizationFactory.create(
        mapper,
        issuer,
        clientId,
        clientSecret,
        requestTimeout,
        decisionTtl,
        cacheEntries,
        policyVersion);
  }
}
