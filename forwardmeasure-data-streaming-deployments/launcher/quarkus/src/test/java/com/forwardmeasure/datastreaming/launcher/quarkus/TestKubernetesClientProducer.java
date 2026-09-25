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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;

/**
 * Real, live-verified near-miss fixed 2026-09-21: {@link LauncherQuarkusBinding}'s own real
 * production {@code kubernetesClient()} producer (correctly, for a real deployment) calls the
 * no-arg {@code new KubernetesClientBuilder().build()}, which auto-configures from whatever ambient
 * kubeconfig this JVM's own environment provides. Under a real deployment (running inside the
 * target cluster) that's exactly right - in-cluster config. Under {@code @QuarkusTest} on a
 * developer's own machine, it instead picked up that machine's own real {@code ~/.kube/config},
 * which - confirmed live - pointed at a real, existing GKE production cluster ({@code
 * gke_genai-llm-393115_us-central1_openworkflow-prod}), and this repo's own real {@code
 * DirectIngestionLauncher} genuinely attempted a real {@code POST .../namespaces/.../jobs} against
 * it (only failed because that machine's own gcloud auth token had separately expired - a real,
 * live 401, not a safety mechanism this test can rely on). A `@QuarkusTest` must never be able to
 * reach real infrastructure regardless of ambient credential state - this
 * {@code @Alternative @Priority} test-scoped producer (Quarkus's own real, documented mechanism for
 * a test to override a production CDI bean, no {@code beans.xml} needed) replaces the client with
 * one built explicitly from the real, disposable K3s testcontainer's own kubeconfig, so there is no
 * ambient fallback path at all.
 *
 * <p>Generalized 2026-09-21 (same day) onto {@link CurrentKubernetesTestContainer} - the first
 * version hardcoded a reference to {@code DirectIngestionMatrixQuarkusPekkoSmokeTest}'s own static
 * field, confirmed broken live the moment a second smoke test tried to reuse it (a {@code
 * NullPointerException} surfaced as a real HTTP 400 from the launcher's own generic exception
 * mapper). Every {@code @QuarkusTestResource} using this producer must call {@code
 * CurrentKubernetesTestContainer#set} in its own {@code start()}.
 */
@ApplicationScoped
public class TestKubernetesClientProducer {

  @Produces
  @ApplicationScoped
  @Alternative
  @Priority(1)
  KubernetesClient kubernetesClient() {
    String kubeConfigYaml = CurrentKubernetesTestContainer.get().kubeConfigYaml();
    return new KubernetesClientBuilder().withConfig(Config.fromKubeconfig(kubeConfigYaml)).build();
  }
}
