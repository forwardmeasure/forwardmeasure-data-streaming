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
import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.api.FlowControlSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformGraph;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.application.auth.KeycloakClientCredentialsTokenSupplier;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.RunAccepted;
import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.openworkflow.common.model.Violation;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecution;
import com.forwardmeasure.openworkflow.execution.api.model.WorkflowExecutionState;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.SerdeImport;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Micronaut composition for the ingestion launcher's own non-REST beans - real {@link
 * KubernetesClient}/{@link WorkflowExecutionsApi} construction and real config, matching {@link
 * com.forwardmeasure.datastreaming.launcher.quarkus.LauncherQuarkusBinding}/{@link
 * com.forwardmeasure.datastreaming.launcher.spring.LauncherSpringBinding} exactly. The JAX-RS
 * resources and exception mappers themselves are NOT produced here - a {@code @Factory} method
 * alone doesn't give Micronaut's compile-time route/provider discovery what it needs for those; see
 * this package's own thin {@code @Singleton} subclasses instead, mirroring {@code
 * MicronautReferencePopulationResource} (forwardmeasure-entity-intelligence).
 *
 * <p>Real, previously-undiscovered functional gap found and fixed 2026-09-23, building this
 * module's first-ever real boot test: {@code micronaut-jaxrs-server}'s own request-body binding
 * requires the target type to have compile-time bean-introspection metadata ({@code
 * io.micronaut.core.beans.exceptions.IntrospectionException: No bean introspection available for
 * type [class DirectLaunchRequest]}), and {@link DirectLaunchRequest}/{@link WorkflowLaunchRequest}
 * are plain records in the framework-agnostic {@code forwardmeasure-data-streaming-launcher-
 * application} module, which deliberately has no Micronaut dependency to annotate them directly
 * with. {@code @Introspected(classes = ...)} on any class in a module Micronaut's own annotation
 * processor compiles (this one) generates that metadata for an external type without needing to
 * modify its own source - the same real mechanism {@code @SerdeImport} uses for JSON encoding one
 * layer up. Every real request DTO {@code IngestionRunResource}/{@code WorkflowRunResource} accepts
 * needs an entry here; this class was the natural home since it already assembles this framework's
 * launcher wiring.
 *
 * <p>{@code @Introspected} alone wasn't enough - the follow-up error ({@code No deserializable
 * introspection present for type: DirectLaunchRequest ... Consider adding Serdeable.Deserializable
 * ... Alternatively ... use @SerdeImport}) confirmed {@code micronaut-jaxrs-server}'s own body
 * reader deserializes through {@code micronaut-serde}, not raw reflection - {@code @SerdeImport} is
 * the same external-type mechanism, one layer more specific.
 *
 * <p>{@code @SerdeImport} on the outer request types alone did not cascade to {@link IngestionSpec}
 * (referenced by {@link DirectLaunchRequest}) or its own nested record graph - each reachable type
 * needs its own explicit entry; confirmed live by iterating one {@code IntrospectionException} at a
 * time until every type {@code WorldCheckFixtures}' own real spec fixtures actually construct was
 * covered.
 */
@Factory
@Introspected(
    classes = {
      DirectLaunchRequest.class,
      WorkflowLaunchRequest.class,
      IngestionSpec.class,
      SourcePlan.class,
      SourceSpec.class,
      SourceSpec.FormatSpec.class,
      SourceSpec.SchemaRef.class,
      TransformSpec.class,
      TransformSpec.FieldRule.class,
      TransformGraph.class,
      TransformGraph.TransformNode.class,
      TransformGraph.TransformEdge.class,
      SinkSpec.class,
      SinkSpec.BatchingSpec.class,
      DeliverySemantics.class,
      ConcurrencySpec.class,
      FlowControlSpec.class,
      ErrorPolicy.class,
      RunAccepted.class,
      Problem.class,
      Violation.class,
      KubernetesJobObservation.class,
      WorkflowExecution.class,
      WorkflowExecutionState.class
    })
@SerdeImport(DirectLaunchRequest.class)
@SerdeImport(WorkflowLaunchRequest.class)
@SerdeImport(IngestionSpec.class)
@SerdeImport(SourcePlan.class)
@SerdeImport(SourceSpec.class)
@SerdeImport(SourceSpec.FormatSpec.class)
@SerdeImport(SourceSpec.SchemaRef.class)
@SerdeImport(TransformSpec.class)
@SerdeImport(TransformSpec.FieldRule.class)
@SerdeImport(TransformGraph.class)
@SerdeImport(TransformGraph.TransformNode.class)
@SerdeImport(TransformGraph.TransformEdge.class)
@SerdeImport(SinkSpec.class)
@SerdeImport(SinkSpec.BatchingSpec.class)
@SerdeImport(DeliverySemantics.class)
@SerdeImport(ConcurrencySpec.class)
@SerdeImport(FlowControlSpec.class)
@SerdeImport(ErrorPolicy.class)
@SerdeImport(RunAccepted.class)
// Every error body: forwardmeasure-platform's RFC 9457 problem (openworkflow-common-models).
@SerdeImport(Problem.class)
@SerdeImport(Violation.class)
@SerdeImport(KubernetesJobObservation.class)
// Real, live-caught gap (2026-09-24, WorkflowBoundedMatrixMicronautPekkoSmokeTest): fowf's own
// generated WorkflowExecution model - returned directly by WorkflowRunResource's create()/get() -
// had no
// Micronaut Serde introspection registered at all, so encoding the real response body failed with
// a real 500 ("No serializable introspection present for type WorkflowExecution") the moment a
// workflow-
// mode test tried to dispatch through this binding for the first time. Every field on
// WorkflowExecution
// itself is a plain UUID/String/Long/Date/Object (confirmed via javap on the real installed jar -
// error/output/effects/timers are all just Object/List<Object>, no further concrete nested type to
// register), so this and WorkflowExecutionState are the only two additions needed.
@SerdeImport(WorkflowExecution.class)
@SerdeImport(WorkflowExecutionState.class)
public class LauncherMicronautBinding {

  @Singleton
  KubernetesClient kubernetesClient() {
    return new KubernetesClientBuilder().build();
  }

  @Singleton
  WorkflowExecutionsApi executionsApi(
      @Value("${datastreaming.launcher.fowf.base-url}") String baseUrl,
      @Value("${datastreaming.launcher.fowf.keycloak.token-url}") String tokenUrl,
      @Value("${datastreaming.launcher.fowf.keycloak.client-id}") String clientId,
      @Value("${datastreaming.launcher.fowf.keycloak.client-secret}") String clientSecret) {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(baseUrl);
    apiClient.setBearerToken(
        new KeycloakClientCredentialsTokenSupplier(URI.create(tokenUrl), clientId, clientSecret));
    return new WorkflowExecutionsApi(apiClient);
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
      @Value("${datastreaming.launcher.pekko.image}") String pekkoImage,
      @Value("${datastreaming.launcher.pekko.command}") String pekkoCommand,
      @Value("${datastreaming.launcher.kafka-streams.image}") String kafkaStreamsImage,
      @Value("${datastreaming.launcher.kafka-streams.command}") String kafkaStreamsCommand,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets,
      @Value("${datastreaming.launcher.k8s.host-aliases}") String hostAliases,
      @Value("${datastreaming.launcher.spark.image:}") String sparkImage,
      @Value("${datastreaming.launcher.spark.command:}") String sparkCommand,
      @Value("${datastreaming.launcher.kafka.bootstrap-servers:}") String kafkaBootstrapServers) {
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

  @Singleton
  WorkflowIngestionLauncher workflowIngestionLauncher(
      WorkflowExecutionsApi executionsApi,
      AuthorizationService authorization,
      DirectIngestionLauncher direct) {
    return new WorkflowIngestionLauncher(executionsApi, authorization, false, direct.planner());
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

  /**
   * {@code hostname=ip,hostname2=ip2} - real, only in a test environment whose dispatch target (a
   * Testcontainers-managed K3s node) can't resolve a sibling Testcontainers-managed service through
   * cluster DNS (see {@link DirectIngestionLauncher}'s own {@code hostAliases} constructor param
   * javadoc); empty in every real deployment. Ported from {@code LauncherQuarkusBinding}'s own
   * identical helper - this binding was missing it entirely until now, the same real gap found and
   * fixed in {@code LauncherSpringBinding} while building this org's own boot-test infrastructure.
   */
  static java.util.Map<String, String> commaSeparatedMap(String value) {
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
