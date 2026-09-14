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
import com.forwardmeasure.authzen.ActiveOrganizationProvider;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.authzen.client.AuthzenAuthorizationFactory;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.auth.KeycloakClientCredentialsTokenSupplier;
import com.forwardmeasure.datastreaming.launcher.jaxrs.CorrelationRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.IngestionRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.WorkflowRunResource;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.quarkus.arc.profile.UnlessBuildProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Quarkus CDI composition for the ingestion launcher - real {@link KubernetesClient}/{@link
 * ExecutionsApi} construction and real config, the environment-specific decisions {@code
 * launcher-application}'s own javadoc explicitly leaves to whichever framework binding assembles
 * it. Mirrors {@code IngestionServiceQuarkusBinding} (forwardmeasure-entity-intelligence) exactly:
 * plain {@code @Produces} factory methods, no REST resource classes of its own (those live in
 * {@code launcher-jaxrs} and are produced here, not defined here) - {@code @Path}-annotated CDI
 * beans and {@code @Provider} exception mappers are both auto-discovered by Quarkus's build-time
 * Jandex index, so unlike Spring/Micronaut neither needs any explicit registration here.
 */
@ApplicationScoped
public class LauncherQuarkusBinding {

  @Produces
  @ApplicationScoped
  KubernetesClient kubernetesClient() {
    return new KubernetesClientBuilder().build();
  }

  @Produces
  @ApplicationScoped
  ExecutionsApi executionsApi(
      @ConfigProperty(name = "datastreaming.launcher.fowf.base-url") String baseUrl,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.token-url") String tokenUrl,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.client-id") String clientId,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.client-secret")
          String clientSecret) {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(baseUrl);
    apiClient.setBearerToken(
        new KeycloakClientCredentialsTokenSupplier(URI.create(tokenUrl), clientId, clientSecret));
    return new ExecutionsApi(apiClient);
  }

  /**
   * The one shared, product-wide real {@link AuthorizationService} - real, fail-closed caller
   * authorization against a real Keycloak AuthZEN PDP, added 2026-09-14 to close a confirmed real
   * gap (see docs/fds-authorization-remediation-guide.md). Excluded from the {@code test} build
   * profile, matching forwardmeasure-entity-intelligence's own {@code
   * QuarkusAuthorizationServiceProducer} exactly - real tests get {@code
   * StubAuthorizationService.permitAll()} instead (see each deployment leaf's own test-scoped
   * producer).
   */
  @Produces
  @ApplicationScoped
  @UnlessBuildProfile("test")
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

  @Produces
  @ApplicationScoped
  IngestionJobPolicy ingestionJobPolicy(
      @ConfigProperty(name = "datastreaming.launcher.k8s.namespaces") String namespaces,
      @ConfigProperty(name = "datastreaming.launcher.k8s.images") String images) {
    return IngestionJobPolicy.configured(commaSeparated(namespaces), commaSeparated(images));
  }

  @Produces
  @ApplicationScoped
  DirectIngestionLauncher directIngestionLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      @ConfigProperty(name = "datastreaming.launcher.pekko.image") String image,
      @ConfigProperty(name = "datastreaming.launcher.pekko.command") String command,
      @ConfigProperty(name = "datastreaming.launcher.k8s.image-pull-secrets") String pullSecrets) {
    return new DirectIngestionLauncher(
        policy, authorization, image, command, commaSeparatedList(pullSecrets));
  }

  @Produces
  @ApplicationScoped
  DirectCorrelationLauncher directCorrelationLauncher(
      IngestionJobPolicy policy,
      AuthorizationService authorization,
      @ConfigProperty(name = "datastreaming.launcher.spark.image") String image,
      @ConfigProperty(name = "datastreaming.launcher.spark.command") String command,
      @ConfigProperty(name = "datastreaming.launcher.k8s.image-pull-secrets") String pullSecrets) {
    return new DirectCorrelationLauncher(
        policy, authorization, image, command, commaSeparatedList(pullSecrets));
  }

  @Produces
  @ApplicationScoped
  WorkflowIngestionLauncher workflowIngestionLauncher(
      ExecutionsApi executionsApi, AuthorizationService authorization) {
    return new WorkflowIngestionLauncher(executionsApi, authorization);
  }

  @Produces
  @ApplicationScoped
  IngestionRunResource ingestionRunResource(
      DirectIngestionLauncher launcher,
      KubernetesClient client,
      ActiveOrganizationProvider organizations) {
    return new IngestionRunResource(launcher, client, organizations);
  }

  @Produces
  @ApplicationScoped
  CorrelationRunResource correlationRunResource(
      DirectCorrelationLauncher launcher,
      KubernetesClient client,
      ActiveOrganizationProvider organizations) {
    return new CorrelationRunResource(launcher, client, organizations);
  }

  @Produces
  @ApplicationScoped
  WorkflowRunResource workflowRunResource(
      WorkflowIngestionLauncher launcher, ActiveOrganizationProvider organizations) {
    return new WorkflowRunResource(launcher, organizations);
  }

  private static Set<String> commaSeparated(String value) {
    return Set.copyOf(commaSeparatedList(value));
  }

  private static List<String> commaSeparatedList(String value) {
    if (value == null || value.isBlank()) {
      return List.of();
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(entry -> !entry.isEmpty())
        .collect(Collectors.toList());
  }
}
