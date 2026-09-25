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

import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import java.util.Objects;

/**
 * A shared, test-class-agnostic holder for whichever real K3s {@link KubernetesTestContainer} the
 * currently-running Micronaut smoke test started - {@code TestKubernetesClientFactory} reads it
 * generically rather than each test class nesting its own {@code @Factory}/{@code @Replaces} bean.
 *
 * <p>Mirrors the Quarkus deployment module's own {@code CurrentKubernetesTestContainer} exactly,
 * for the identical real reason (2026-09-23): {@code
 * DirectIngestionMatrixMicronautPekkoSmokeTest}'s first version of this fix nested its own {@code
 * TestKubernetesClientFactory} inside itself - both that test class and this module's own {@code
 * DirectIngestionMatrixMicronautKafkaStreamsSmokeTest} are compiled onto the same test classpath,
 * so Micronaut's compile-time bean scanning discovered *both* nested factories simultaneously
 * regardless of which class Surefire actually ran, each independently {@code @Replaces}-ing the
 * same production {@code KubernetesClient} bean - a live, real {@code DependencyInjectionException:
 * Multiple possible bean candidates found: [KubernetesClient, KubernetesClient]} the moment a
 * second Micronaut smoke test existed. Every test's own {@code getProperties()} must call {@link
 * #set} right after starting its own container.
 */
final class CurrentKubernetesTestContainer {

  private static volatile KubernetesTestContainer current;

  private CurrentKubernetesTestContainer() {}

  static void set(KubernetesTestContainer container) {
    current = Objects.requireNonNull(container, "container");
  }

  static void clear() {
    current = null;
  }

  static KubernetesTestContainer get() {
    KubernetesTestContainer container = current;
    if (container == null) {
      throw new IllegalStateException(
          "No KubernetesTestContainer is currently registered - the running test's own"
              + " getProperties() must call CurrentKubernetesTestContainer#set");
    }
    return container;
  }
}
