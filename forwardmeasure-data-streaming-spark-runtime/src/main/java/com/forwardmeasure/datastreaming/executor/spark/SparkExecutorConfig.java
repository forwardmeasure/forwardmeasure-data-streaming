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
package com.forwardmeasure.datastreaming.executor.spark;

import java.util.Objects;

/**
 * Everything {@link SparkSessionFactory} needs to build a {@code SparkSession} against either a
 * single embedded driver ({@code local[*]}) or a real distributed Kubernetes cluster ({@code
 * master} starting with {@code k8s://}) - deliberately just a plain value object, not an env-var
 * reader: each caller (this module's own {@code SparkIngestionRunner}/{@code
 * SparkCorrelationRunner}, fei's {@code CorrelatedSourceIngestionWorker}, or anyone else) reads
 * *its own* environment variables under whatever names its own deployment already uses (real
 * production infrastructure - a Helm release, an already-provisioned executor image - may already
 * depend on a specific name; this type doesn't force a rename) and constructs one of these.
 *
 * <p>{@code containerImage}/{@code namespace} are required by {@link SparkSessionFactory} only when
 * {@code master} starts with {@code k8s://} - {@code local[*]} mode needs neither. {@code
 * serviceAccountName}/{@code podTemplateFile} are always optional: Spark's own defaults (the
 * namespace's default service account, no executor pod template) are correct for a caller that
 * doesn't need to override them.
 */
public record SparkExecutorConfig(
    String master,
    String containerImage,
    String namespace,
    String serviceAccountName,
    String podTemplateFile) {

  public SparkExecutorConfig {
    Objects.requireNonNull(master, "master");
  }

  /** The single-pod, embedded-driver mode every FDS/fei Spark runner defaulted to before today. */
  public static SparkExecutorConfig localMode() {
    return new SparkExecutorConfig("local[*]", null, null, null, null);
  }
}
