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
package com.forwardmeasure.datastreaming.launcher.micronaut.deployment;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

/**
 * Real, compile-time bean-graph replacement of the production {@code kubernetesClient()} bean - see
 * the Quarkus sibling {@code TestKubernetesClientProducer}'s own javadoc for the real, live-caught
 * safety near-miss this pattern fixes (an un-isolated {@code @MicronautTest} can reach a real
 * cluster via this machine's own ambient kubeconfig). One shared class, not nested per test class -
 * see {@link CurrentKubernetesTestContainer}'s own javadoc for the real, live {@code
 * DependencyInjectionException} that a per-test-class nested factory caused the moment a second
 * Micronaut smoke test existed on the same compiled test classpath.
 *
 * <p>Micronaut's {@code @Replaces} is resolved at compile-time bean-definition-graph construction,
 * not runtime tie-breaking - the replaced production bean definition is never even registered, so
 * there is no eager-construction risk here by construction, not by luck (unlike Spring's
 * {@code @Primary}).
 */
@Factory
class TestKubernetesClientFactory {

  @Singleton
  @Replaces(
      bean = KubernetesClient.class,
      factory = com.forwardmeasure.datastreaming.launcher.micronaut.LauncherMicronautBinding.class)
  KubernetesClient testKubernetesClient() {
    return new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(CurrentKubernetesTestContainer.get().kubeConfigYaml()))
        .build();
  }
}
