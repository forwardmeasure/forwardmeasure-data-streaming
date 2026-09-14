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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;

/**
 * Builds a real {@code SparkSession} for either mode a plain {@code java -jar} Spark process (never
 * {@code spark-submit}) can run in - single-pod embedded ({@code local[*]}) or real distributed
 * Kubernetes ({@code k8s://...}, separate driver/executor pods).
 *
 * <p>Extracted 2026-09-13 from what was then a single {@code SparkIngestionRunner}'s own {@code
 * main()} (which had only ever set {@code master("local[*]")} - a real bug, since {@code
 * SparkSession.getOrCreate()} throws immediately with no master set at all when run this way) once
 * fei's {@code CorrelatedSourceIngestionWorker} independently solved the identical problem for its
 * own production {@code k8s://} deployment (real distributed Spark is not hypothetical there - the
 * design plan's own D4 decision already documented a real, already-provisioned Helm release using
 * {@code k8s://https://kubernetes.default.svc} with a dedicated executor image and real executor
 * sizing). Rather than a second independent implementation, this is the one, shared place that
 * logic lives now - this module's own {@code SparkIngestionRunner}/{@code SparkCorrelationRunner}
 * (split into two classes 2026-09-13, after this factory already existed) and fei's worker build a
 * {@link SparkExecutorConfig} from whatever environment variables their own deployment already
 * uses, then call this.
 */
public final class SparkSessionFactory {

  private SparkSessionFactory() {}

  public static SparkSession create(String appName, SparkExecutorConfig config) {
    SparkSession.Builder builder = SparkSession.builder().appName(appName).master(config.master());
    sparkConfigProperties(config).forEach(builder::config);
    return builder.getOrCreate();
  }

  /**
   * The actual {@code k8s://}-mode config-key wiring, separated out so it's testable without a real
   * Kubernetes-backed Spark cluster to connect to (unlike {@link #create}, which does need one for
   * the {@code k8s://} branch to actually succeed).
   *
   * @throws NullPointerException if {@code master} starts with {@code k8s://} and {@code
   *     containerImage}/{@code namespace} aren't both present - Spark's own distributed-mode
   *     scheduler backend cannot start without them, so failing fast here beats a confusing failure
   *     deep inside Spark's own cluster-manager bootstrap.
   */
  static Map<String, String> sparkConfigProperties(SparkExecutorConfig config) {
    if (!config.master().startsWith("k8s://")) {
      return Map.of();
    }
    Objects.requireNonNull(
        config.containerImage(), "containerImage is required when master starts with k8s://");
    Objects.requireNonNull(
        config.namespace(), "namespace is required when master starts with k8s://");
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("spark.kubernetes.container.image", config.containerImage());
    properties.put("spark.kubernetes.namespace", config.namespace());
    if (config.serviceAccountName() != null && !config.serviceAccountName().isBlank()) {
      properties.put(
          "spark.kubernetes.authenticate.driver.serviceAccountName", config.serviceAccountName());
    }
    if (config.podTemplateFile() != null && !config.podTemplateFile().isBlank()) {
      properties.put("spark.kubernetes.executor.podTemplateFile", config.podTemplateFile());
    }
    return properties;
  }
}
