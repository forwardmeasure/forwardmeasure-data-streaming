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
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.auth.KeycloakClientCredentialsTokenSupplier;
import com.forwardmeasure.datastreaming.launcher.jaxrs.IngestionRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.WorkflowRunResource;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
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
 * WorkflowExecutionsApi} construction and real config, the environment-specific decisions {@code
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
  WorkflowExecutionsApi executionsApi(
      @ConfigProperty(name = "datastreaming.launcher.fowf.base-url") String baseUrl,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.token-url") String tokenUrl,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.client-id") String clientId,
      @ConfigProperty(name = "datastreaming.launcher.fowf.keycloak.client-secret")
          String clientSecret) {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(baseUrl);
    apiClient.setBearerToken(
        new KeycloakClientCredentialsTokenSupplier(URI.create(tokenUrl), clientId, clientSecret));
    return new WorkflowExecutionsApi(apiClient);
  }

  /**
   * The one shared, product-wide real {@link AuthorizationService} - real, fail-closed caller
   * authorization against a real Keycloak AuthZEN PDP, added 2026-09-14 to close a confirmed real
   * gap (see docs/fds-authorization-remediation-guide.md). Excluded from the {@code test} build
   * profile, mirroring forwardmeasure-entity-intelligence's own {@code
   * QuarkusAuthorizationServiceProducer}.
   *
   * <p>As of 2026-09-20 this repo has no
   * {@code @QuarkusTest}/{@code @SpringBootTest}/{@code @MicronautTest} for the launcher (no test
   * here boots a framework deployment context at all, so this producer never actually fires under
   * test), and the shared {@code StubAuthorizationService} this javadoc used to point at has been
   * removed repo-wide - it masked a real Keycloak Organizations-group authorization bug (native
   * Role policies don't see roles granted only via Organization membership; see
   * forwardmeasure-authzen's {@code OrganizationRolePolicyProvider}). The one real Keycloak-backed
   * proof for this launcher ({@code DirectIngestionLauncherKeycloakIntegrationTest}) builds its own
   * {@link AuthzenAuthorizationFactory}-based service directly in plain JUnit against {@code
   * AuthzenKeycloakFixture} and never goes through this CDI producer at all; every other test that
   * needs an {@link AuthorizationService} but isn't testing authorization itself now uses a small,
   * file-local fake declared in that test class, not a shared stub. If a real
   * {@code @QuarkusTest}/{@code @SpringBootTest}/{@code @MicronautTest} is added for this launcher
   * later, follow forwardmeasure-entity-intelligence's and forwardmeasure-openworkflow's own {@code
   * AuthorizationSmokeResourceTest} precedent (a
   * `QuarkusTestResourceLifecycleManager`/{@code @DynamicPropertySource}/{@code
   * TestPropertyProvider} that starts a Keycloak fixture and injects its issuer/client config as
   * real properties) rather than reintroducing a stub producer.
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
      @ConfigProperty(name = "datastreaming.launcher.pekko.image") String pekkoImage,
      @ConfigProperty(name = "datastreaming.launcher.pekko.command") String pekkoCommand,
      @ConfigProperty(name = "datastreaming.launcher.kafka-streams.image") String kafkaStreamsImage,
      @ConfigProperty(name = "datastreaming.launcher.kafka-streams.command")
          String kafkaStreamsCommand,
      @ConfigProperty(name = "datastreaming.launcher.k8s.image-pull-secrets") String pullSecrets,
      @ConfigProperty(name = "datastreaming.launcher.k8s.host-aliases", defaultValue = "")
          String hostAliases,
      @ConfigProperty(name = "datastreaming.launcher.spark.image", defaultValue = "")
          String sparkImage,
      @ConfigProperty(name = "datastreaming.launcher.spark.command", defaultValue = "")
          String sparkCommand,
      @ConfigProperty(name = "datastreaming.launcher.kafka.bootstrap-servers", defaultValue = "")
          String kafkaBootstrapServers) {
    return new DirectIngestionLauncher(
        policy,
        authorization,
        pekkoImage,
        pekkoCommand,
        kafkaStreamsImage,
        kafkaStreamsCommand,
        commaSeparatedList(pullSecrets),
        commaSeparatedMap(hostAliases),
        blankToNull(sparkImage),
        blankToNull(sparkCommand),
        blankToNull(kafkaBootstrapServers));
  }

  @Produces
  @ApplicationScoped
  WorkflowIngestionLauncher workflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi, AuthorizationService authorization) {
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

  /**
   * {@code hostname=ip,hostname2=ip2} - real, only in a test environment whose dispatch target (a
   * Testcontainers-managed K3s node) can't resolve a sibling Testcontainers-managed service through
   * cluster DNS (see {@link DirectIngestionLauncher}'s own {@code hostAliases} constructor param
   * javadoc); empty in every real deployment.
   */
  private static java.util.Map<String, String> commaSeparatedMap(String value) {
    java.util.Map<String, String> result = new java.util.LinkedHashMap<>();
    for (String entry : commaSeparatedList(value)) {
      int equals = entry.indexOf('=');
      if (equals > 0) {
        result.put(entry.substring(0, equals).trim(), entry.substring(equals + 1).trim());
      }
    }
    return result;
  }

  /**
   * A real deployment that never needs the Spark two-Job pipeline leaves {@code
   * datastreaming.launcher.spark.*}/{@code datastreaming.launcher.kafka.bootstrap-servers} unset -
   * {@link DirectIngestionLauncher}'s own fullest constructor requires {@code null}, not an empty
   * string, to correctly report "not configured for Spark" (see its own javadoc).
   */
  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
