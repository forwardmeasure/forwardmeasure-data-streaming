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

import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import java.util.Objects;

/**
 * A shared, test-class-agnostic holder for whichever real K3s {@link KubernetesTestContainer} the
 * currently-running {@code @QuarkusTestResource} started - {@link TestKubernetesClientProducer}
 * reads it generically rather than hardcoding one specific test class's own static field.
 *
 * <p>Generalizes the real safety fix from {@code DirectIngestionMatrixQuarkusPekkoSmokeTest}
 * (2026-09-21): its own first version of this fix pointed {@code TestKubernetesClientProducer}
 * directly at {@code DirectIngestionMatrixQuarkusPekkoSmokeTest.SmokeResource.kubernetes}, which
 * only worked for that one test class - confirmed broken live the moment a second smoke test
 * ({@code DirectIngestionMatrixQuarkusKafkaStreamsSmokeTest}) tried to reuse the same producer.
 * Every {@code SmokeResource#start()} must call {@link #set} right after starting its own
 * container, and {@link #clear()} in {@code stop()} - a stale reference from a torn-down container
 * is worse than a clear "not set" failure.
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
              + " QuarkusTestResourceLifecycleManager#start() must call"
              + " CurrentKubernetesTestContainer#set");
    }
    return container;
  }
}
