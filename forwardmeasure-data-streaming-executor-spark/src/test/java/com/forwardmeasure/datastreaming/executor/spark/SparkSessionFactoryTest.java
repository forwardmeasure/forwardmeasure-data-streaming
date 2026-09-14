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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Real proof for the local-mode path (an actual {@code SparkSession} that actually runs a job - the
 * exact class of bug this factory was extracted to fix, a missing {@code master} throwing on {@code
 * getOrCreate()}, would fail right here). The {@code k8s://} branch's config-key wiring is proven
 * directly against {@link SparkSessionFactory#sparkConfigProperties} instead of a real {@code
 * create()} call - a real distributed Spark-on-Kubernetes cluster to connect to is a materially
 * bigger ask than anything else in this module's own test suite, and the config-key wiring is the
 * only part of {@code k8s://} mode this module's own code controls; the rest is Spark's own
 * cluster-manager bootstrap.
 */
class SparkSessionFactoryTest {

  @Test
  void createBuildsARealWorkingLocalSparkSession() {
    SparkSession spark =
        SparkSessionFactory.create("spark-session-factory-test", SparkExecutorConfig.localMode());
    try {
      long count =
          spark.createDataset(List.of(1, 2, 3), org.apache.spark.sql.Encoders.INT()).count();
      assertEquals(3, count);
    } finally {
      spark.stop();
    }
  }

  @Test
  void localModeNeedsNoKubernetesConfig() {
    Map<String, String> properties =
        SparkSessionFactory.sparkConfigProperties(SparkExecutorConfig.localMode());

    assertTrue(properties.isEmpty());
  }

  @Test
  void k8sModeWiresAllFourConfigKeysWhenAllArePresent() {
    SparkExecutorConfig config =
        new SparkExecutorConfig(
            "k8s://https://kubernetes.default.svc",
            "registry.test/spark-executor@sha256:" + "a".repeat(64),
            "entity-intelligence",
            "spark-runner",
            "/etc/spark/executor-pod-template.yaml");

    Map<String, String> properties = SparkSessionFactory.sparkConfigProperties(config);

    assertEquals(config.containerImage(), properties.get("spark.kubernetes.container.image"));
    assertEquals(config.namespace(), properties.get("spark.kubernetes.namespace"));
    assertEquals(
        config.serviceAccountName(),
        properties.get("spark.kubernetes.authenticate.driver.serviceAccountName"));
    assertEquals(
        config.podTemplateFile(), properties.get("spark.kubernetes.executor.podTemplateFile"));
    assertEquals(4, properties.size());
  }

  @Test
  void k8sModeOmitsOptionalKeysWhenAbsent() {
    SparkExecutorConfig config =
        new SparkExecutorConfig(
            "k8s://https://kubernetes.default.svc",
            "registry.test/spark-executor@sha256:" + "a".repeat(64),
            "entity-intelligence",
            null,
            null);

    Map<String, String> properties = SparkSessionFactory.sparkConfigProperties(config);

    assertEquals(2, properties.size());
    assertTrue(properties.containsKey("spark.kubernetes.container.image"));
    assertTrue(properties.containsKey("spark.kubernetes.namespace"));
  }

  @Test
  void k8sModeRequiresContainerImage() {
    SparkExecutorConfig config =
        new SparkExecutorConfig(
            "k8s://https://kubernetes.default.svc", null, "entity-intelligence", null, null);

    assertThrows(
        NullPointerException.class, () -> SparkSessionFactory.sparkConfigProperties(config));
  }

  @Test
  void k8sModeRequiresNamespace() {
    SparkExecutorConfig config =
        new SparkExecutorConfig(
            "k8s://https://kubernetes.default.svc",
            "registry.test/spark-executor@sha256:" + "a".repeat(64),
            null,
            null,
            null);

    assertThrows(
        NullPointerException.class, () -> SparkSessionFactory.sparkConfigProperties(config));
  }
}
