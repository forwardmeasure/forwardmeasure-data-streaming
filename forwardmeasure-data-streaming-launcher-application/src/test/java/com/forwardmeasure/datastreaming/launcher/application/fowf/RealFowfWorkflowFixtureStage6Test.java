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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Stage 6 only: the real {@code openworkflow-operation-adapter-quarkus} image (built locally with
 * the 2026-09-21 {@code AsyncApiKubernetesDeploymentOperationExecutor} schema fix - see {@link
 * RealFowfWorkflowFixture#OPERATION_ADAPTER_KAFKA_IMAGE}'s own javadoc), booted against this
 * fixture's real Postgres/Kafka/Keycloak plus a real, disposable K3s cluster - proves the whole
 * wiring this class's own javadoc documents (the {@code host.docker.internal} kubeconfig rewrite,
 * the deny-by- default policy allowlist, the {@code OPENWORKFLOW_AUTHORIZATION_*} env var
 * convention) actually boots a real process, not just that it compiles. Deliberately narrow,
 * mirroring Stage 3/4's own scope: proves the service can start and reach every real dependency,
 * via a real HTTP call (its health endpoint) rather than only trusting the container's own log-line
 * wait strategy. The full apply+watch dispatch through a real workflow execution is Phase D's own
 * separate, larger final proof (submit via {@code WorkflowIngestionLauncher}, watch a real K8s
 * {@code Deployment} become {@code Available}), not this stage.
 */
class RealFowfWorkflowFixtureStage6Test {

  private static final String ROLE = "workflow-run-launcher";
  private static final String NAMESPACE = "streaming";
  private static final String IMAGE =
      "docker.io/library/busybox@sha256:"
          + "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";

  @Test
  @Timeout(300)
  void operationAdapterBootsAgainstRealKafkaPostgresKeycloakAndK3s() throws Exception {
    try (RealFowfWorkflowFixture fixture = RealFowfWorkflowFixture.start("fds-stage6", ROLE)) {
      try (var client = fixture.kubernetes().createClient()) {
        client
            .namespaces()
            .resource(
                new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
            .create();
      }

      GenericContainer<?> operationAdapter = fixture.startOperationAdapter(NAMESPACE, IMAGE, 5);

      String healthUrl =
          "http://"
              + operationAdapter.getHost()
              + ":"
              + operationAdapter.getMappedPort(8080)
              + "/q/health";
      HttpClient http = HttpClient.newHttpClient();
      HttpResponse<String> response =
          http.send(
              HttpRequest.newBuilder(URI.create(healthUrl)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(
          200,
          response.statusCode(),
          "operation-adapter health check must report the whole real dependency chain healthy: "
              + response.body());
    }
  }
}
