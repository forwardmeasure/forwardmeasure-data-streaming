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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.client.AuthzenAuthorizationFactory;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.auth.KeycloakClientCredentialsTokenSupplier;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Micronaut composition for the ingestion launcher's own non-REST beans - real {@link
 * KubernetesClient}/{@link ExecutionsApi} construction and real config, matching {@link
 * com.forwardmeasure.datastreaming.launcher.quarkus.LauncherQuarkusBinding}/{@link
 * com.forwardmeasure.datastreaming.launcher.spring.LauncherSpringBinding} exactly. The JAX-RS
 * resources and exception mappers themselves are NOT produced here - a {@code @Factory} method
 * alone doesn't give Micronaut's compile-time route/provider discovery what it needs for those; see
 * this package's own thin {@code @Singleton} subclasses instead, mirroring {@code
 * MicronautReferencePopulationResource} (forwardmeasure-entity-intelligence).
 */
@Factory
public class LauncherMicronautBinding {

  @Singleton
  KubernetesClient kubernetesClient() {
    return new KubernetesClientBuilder().build();
  }

  @Singleton
  ExecutionsApi executionsApi(
      @Value("${datastreaming.launcher.fowf.base-url}") String baseUrl,
      @Value("${datastreaming.launcher.fowf.keycloak.token-url}") String tokenUrl,
      @Value("${datastreaming.launcher.fowf.keycloak.client-id}") String clientId,
      @Value("${datastreaming.launcher.fowf.keycloak.client-secret}") String clientSecret) {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(baseUrl);
    apiClient.setBearerToken(
        new KeycloakClientCredentialsTokenSupplier(URI.create(tokenUrl), clientId, clientSecret));
    return new ExecutionsApi(apiClient);
  }

  /**
   * This module's own Jackson dependency is a different namespace than {@code
   * com.fasterxml.jackson.databind} and doesn't provide that classic-namespace {@code ObjectMapper}
   * bean by default - the same real gap forwardmeasure-entity-intelligence's own {@code
   * MicronautAuthorizationServiceProducer} already works around the same way.
   */
  @Singleton
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  /**
   * The one shared, product-wide real {@link AuthorizationService} - real, fail-closed caller
   * authorization against a real Keycloak AuthZEN PDP, added 2026-09-14 to close a confirmed real
   * gap (see docs/fds-authorization-remediation-guide.md). Micronaut's own {@code
   * micronaut.security.enabled}/{@code intercept-url-map} config (see each deployment leaf's own
   * application.yml) is this framework's real "authenticated by default" enforcement - no
   * SecurityFilterChain-equivalent bean needed here, unlike Spring.
   */
  @Singleton
  AuthorizationService authorizationService(
      ObjectMapper mapper,
      @Value("${datastreaming.launcher.authorization.issuer}") URI issuer,
      @Value("${datastreaming.launcher.authorization.client-id}") String clientId,
      @Value("${datastreaming.launcher.authorization.client-secret}") String clientSecret,
      @Value("${datastreaming.launcher.authorization.request-timeout}") Duration requestTimeout,
      @Value("${datastreaming.launcher.authorization.decision-ttl}") Duration decisionTtl,
      @Value("${datastreaming.launcher.authorization.maximum-cache-entries}") int cacheEntries,
      @Value("${datastreaming.launcher.authorization.policy-version}") String policyVersion) {
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

  @Singleton
  IngestionJobPolicy ingestionJobPolicy(
      @Value("${datastreaming.launcher.k8s.namespaces}") String namespaces,
      @Value("${datastreaming.launcher.k8s.images}") String images) {
    return IngestionJobPolicy.configured(commaSeparated(namespaces), commaSeparated(images));
  }

  @Singleton
  DirectIngestionLauncher directIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      @Value("${datastreaming.launcher.pekko.image}") String image,
      @Value("${datastreaming.launcher.pekko.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectIngestionLauncher(
        policy, authorization, image, command, commaSeparatedList(pullSecrets));
  }

  @Singleton
  DirectCorrelationLauncher directCorrelationLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      @Value("${datastreaming.launcher.spark.image}") String image,
      @Value("${datastreaming.launcher.spark.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectCorrelationLauncher(
        policy, authorization, image, command, commaSeparatedList(pullSecrets));
  }

  @Singleton
  WorkflowIngestionLauncher workflowIngestionLauncher(
      ExecutionsApi executionsApi, AuthorizationService authorization) {
    return new WorkflowIngestionLauncher(executionsApi, authorization);
  }

  static Set<String> commaSeparated(String value) {
    return Set.copyOf(commaSeparatedList(value));
  }

  static List<String> commaSeparatedList(String value) {
    if (value == null || value.isBlank()) {
      return List.of();
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(entry -> !entry.isEmpty())
        .collect(Collectors.toList());
  }
}
