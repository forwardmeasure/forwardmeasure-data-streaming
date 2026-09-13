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
package com.forwardmeasure.datastreaming.launcher.spring;

import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.IngestionJobPolicy;
import com.forwardmeasure.datastreaming.launcher.application.WorkflowIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.auth.KeycloakClientCredentialsTokenSupplier;
import com.forwardmeasure.datastreaming.launcher.jaxrs.CorrelationRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.IngestionRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.WorkflowRunResource;
import com.forwardmeasure.datastreaming.launcher.jaxrs.mapper.ApiExceptionMapper;
import com.forwardmeasure.datastreaming.launcher.jaxrs.mapper.NullPointerExceptionMapper;
import com.forwardmeasure.datastreaming.launcher.jaxrs.mapper.SecurityExceptionMapper;
import com.forwardmeasure.datastreaming.launcher.jaxrs.mapper.UnsupportedOperationExceptionMapper;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.ExecutionsApi;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring composition for the ingestion launcher - real {@link KubernetesClient}/{@link
 * ExecutionsApi} construction and real config. Registers the shared JAX-RS resources and mappers
 * into the single Jersey {@code ResourceConfig} via {@link ResourceConfigCustomizer}, ported from
 * forwardmeasure-entity-intelligence's own {@code IngestionServiceSpringBinding} (itself ported
 * from fowf's real {@code OpenWorkflowDefinitionManagementSpringBinding}) - Jersey needs every
 * {@code @Provider} exception mapper registered explicitly here, unlike Quarkus's build-time Jandex
 * auto-discovery.
 */
@Configuration(proxyBeanMethods = false)
public class LauncherSpringBinding {

  @Bean
  KubernetesClient kubernetesClient() {
    return new KubernetesClientBuilder().build();
  }

  @Bean
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

  @Bean
  IngestionJobPolicy ingestionJobPolicy(
      @Value("${datastreaming.launcher.k8s.namespaces}") String namespaces,
      @Value("${datastreaming.launcher.k8s.images}") String images) {
    return IngestionJobPolicy.configured(commaSeparated(namespaces), commaSeparated(images));
  }

  @Bean
  DirectIngestionLauncher directIngestionLauncher(
      IngestionJobPolicy policy,
      @Value("${datastreaming.launcher.pekko.image}") String image,
      @Value("${datastreaming.launcher.pekko.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectIngestionLauncher(policy, image, command, commaSeparatedList(pullSecrets));
  }

  @Bean
  DirectCorrelationLauncher directCorrelationLauncher(
      IngestionJobPolicy policy,
      @Value("${datastreaming.launcher.spark.image}") String image,
      @Value("${datastreaming.launcher.spark.command}") String command,
      @Value("${datastreaming.launcher.k8s.image-pull-secrets}") String pullSecrets) {
    return new DirectCorrelationLauncher(policy, image, command, commaSeparatedList(pullSecrets));
  }

  @Bean
  WorkflowIngestionLauncher workflowIngestionLauncher(ExecutionsApi executionsApi) {
    return new WorkflowIngestionLauncher(executionsApi);
  }

  @Bean
  IngestionRunResource ingestionRunResource(
      DirectIngestionLauncher launcher, KubernetesClient client) {
    return new IngestionRunResource(launcher, client);
  }

  @Bean
  CorrelationRunResource correlationRunResource(
      DirectCorrelationLauncher launcher, KubernetesClient client) {
    return new CorrelationRunResource(launcher, client);
  }

  @Bean
  WorkflowRunResource workflowRunResource(WorkflowIngestionLauncher launcher) {
    return new WorkflowRunResource(launcher);
  }

  @Bean
  ResourceConfigCustomizer launcherResourceConfigCustomizer(
      IngestionRunResource ingestionRuns,
      CorrelationRunResource correlationRuns,
      WorkflowRunResource workflowRuns) {
    return resourceConfig ->
        resourceConfig
            .register(ingestionRuns)
            .register(correlationRuns)
            .register(workflowRuns)
            .register(SecurityExceptionMapper.class)
            .register(UnsupportedOperationExceptionMapper.class)
            .register(NullPointerExceptionMapper.class)
            .register(ApiExceptionMapper.class);
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
