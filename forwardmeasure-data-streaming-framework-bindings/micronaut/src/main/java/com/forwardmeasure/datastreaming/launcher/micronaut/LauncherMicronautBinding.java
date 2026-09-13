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

  @Singleton
  IngestionJobPolicy ingestionJobPolicy(
      @Value("${datastreaming.launcher.k8s.namespaces}") String namespaces,
      @Value("${datastreaming.launcher.k8s.images}") String images) {
    return IngestionJobPolicy.configured(commaSeparated(namespaces), commaSeparated(images));
  }

  @Singleton
  DirectIngestionLauncher directIngestionLauncher(
      IngestionJobPolicy policy,
      @Value("${datastreaming.launcher.pekko.image}") String image,
      @Value("${datastreaming.launcher.pekko.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectIngestionLauncher(policy, image, command, commaSeparatedList(pullSecrets));
  }

  @Singleton
  DirectCorrelationLauncher directCorrelationLauncher(
      IngestionJobPolicy policy,
      @Value("${datastreaming.launcher.spark.image}") String image,
      @Value("${datastreaming.launcher.spark.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectCorrelationLauncher(policy, image, command, commaSeparatedList(pullSecrets));
  }

  @Singleton
  WorkflowIngestionLauncher workflowIngestionLauncher(ExecutionsApi executionsApi) {
    return new WorkflowIngestionLauncher(executionsApi);
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
