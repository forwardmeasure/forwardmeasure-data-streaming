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
package com.forwardmeasure.datastreaming.launcher.application;

import java.util.Objects;
import java.util.Set;

/**
 * Allowlists for direct-mode Kubernetes Job dispatch - mirrors fowf's own {@code
 * KubernetesJobPolicy} shape ({@code openworkflow-operation-adapter-kubernetes-job}), minus a
 * tenant parameter: this launcher has no multi-tenant model of its own today, unlike fowf's, so
 * there is nothing real to key an allowlist on beyond the two things that actually matter for a
 * single-purpose ingestion launcher - which namespace it may write into, and which image reference
 * it may run. {@code parallelism}/{@code completions} aren't part of this policy at all (unlike
 * fowf's own, which also authorizes scale): every direct-mode ingestion Job is genuinely one pod
 * running one {@code IngestionSpec} - there is no scale dimension to authorize.
 */
public interface IngestionJobPolicy {
  void authorizeNamespace(String namespace);

  void authorizeImage(String image);

  static IngestionJobPolicy rejecting() {
    return configured(Set.of(), Set.of());
  }

  static IngestionJobPolicy configured(Set<String> namespaces, Set<String> images) {
    Set<String> namespaceCopy = Set.copyOf(namespaces);
    Set<String> imageCopy = Set.copyOf(images);
    return new IngestionJobPolicy() {
      @Override
      public void authorizeNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (!namespaceCopy.contains(namespace)) {
          throw new SecurityException("ingestion Job namespace is not allowed: " + namespace);
        }
      }

      @Override
      public void authorizeImage(String image) {
        Objects.requireNonNull(image, "image");
        if (!image.contains("@sha256:")) {
          throw new SecurityException("ingestion Job image must be pinned by sha256 digest");
        }
        if (!imageCopy.contains(image)) {
          throw new SecurityException("ingestion Job image is not allowed: " + image);
        }
      }
    };
  }
}
