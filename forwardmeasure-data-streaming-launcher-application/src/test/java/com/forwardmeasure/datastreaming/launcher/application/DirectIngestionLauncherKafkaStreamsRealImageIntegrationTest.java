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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationDecision;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.testfixtures.WorldCheckFixtures;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.junit.kubernetes.WithKubernetesContainer;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The real thing, not a stand-in - the Kafka Streams sibling of {@link
 * DirectIngestionLauncherRealImageIntegrationTest}, closing the parity gap that test's own javadoc
 * left open ("a real second image proof belongs here once a real, pushed Kafka Streams executor
 * image exists"). Pulls the actual private {@code
 * docker.io/forwardmeasure/data-streaming-executor-kafka-streams} image into a real K3s cluster via
 * a real {@code imagePullSecret}, runs its real {@code KafkaStreamsDeliveryEngine.main()} in {@code
 * BOUNDED} mode, and proves a real WorldCheck row - the identical fixture {@code
 * PekkoWorldCheckToOpenSearchIntegrationTest} already proves for the Pekko engine - flows through a
 * real Kafka topic, the real {@code FieldMappingEngine}, and a real OpenSearch sink.
 *
 * <p>Unlike the Pekko sibling's trivial two-field spec, this uses the real {@link
 * WorldCheckFixtures#boundedKafkaSpec} - {@code BoundedKafkaStreamsConsumerRunner}'s only sink is
 * OpenSearch (no {@code file} option exists for the Kafka Streams engine, see that class's own
 * javadoc), and a Kafka-sourced spec is the one real, non-arbitrary trigger {@code
 * ExecutionPlanCompiler} has for resolving {@code KAFKA_STREAMS} in {@code BOUNDED} mode - so this
 * test's own dispatch is a real, unmodified planner decision, not a forced/faked engine choice.
 * {@code indexSettingsFile} is left blank (an empty {@link Path}) - {@code
 * OpenSearchIndexInitializer#ensureIndex} no-ops on a blank value, so the index is created by
 * OpenSearch's own default dynamic mapping on first write, matching this class's own scope (proving
 * real dispatch/business-logic plumbing, not re-proving the strict-mapping fidelity {@code
 * PekkoWorldCheckToOpenSearchIntegrationTest} already covers).
 *
 * <p>Needs real Docker Hub credentials the test never hardcodes - skipped if {@code
 * DOCKER_HUB_USERNAME}/{@code DOCKER_HUB_TOKEN} aren't set, same as the Pekko sibling.
 */
@WithKubernetesContainer
@WithKafkaContainer(hostDockerInternalListener = true)
@WithOpenSearchContainer
final class DirectIngestionLauncherKafkaStreamsRealImageIntegrationTest {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(
          DirectIngestionLauncherKafkaStreamsRealImageIntegrationTest.class);
  private static final String NAMESPACE = "real-kafka-streams-image-test";
  private static final String KAFKA_TOPIC = "worldcheck-rows";
  private static final String KAFKA_STREAMS_IMAGE =
      "docker.io/forwardmeasure/data-streaming-executor-kafka-streams@sha256:"
          + "fa7235432b6991a2d717d0e8cb5f6b5a39786023782d9834e681afb41ea001fd";
  private static final String PULL_SECRET_NAME = "dockerhub-pull-secret";
  private static final String RUN_COMMAND = "java -jar /deployments/application.jar";
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          TenantDatabase.forAlias("test-tenant"),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  @Test
  @Timeout(300)
  void launchesTheRealKafkaStreamsImageAndProcessesARealWorldCheckRowEndToEnd(
      KubernetesTestContainer kubernetes,
      KafkaTestContainer kafka,
      OpenSearchTestContainer opensearch)
      throws Exception {
    String username = System.getenv("DOCKER_HUB_USERNAME");
    String token = System.getenv("DOCKER_HUB_TOKEN");
    Assumptions.assumeTrue(
        username != null && token != null,
        "DOCKER_HUB_USERNAME/DOCKER_HUB_TOKEN not set - skipping the real private-image proof");

    seedOneRealWorldCheckRow(kafka.bootstrapServers());

    String gatewayIp = dockerBridgeGatewayIp();
    int opensearchPort = opensearch.hostEndpoint().getPort();
    IngestionSpec spec =
        WorldCheckFixtures.boundedKafkaSpec(
            "kafka:" + KAFKA_TOPIC + "?brokers=" + kafka.hostDockerInternalBootstrapServers(),
            "http://host.docker.internal:" + opensearchPort,
            Path.of(""));

    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();
      client
          .secrets()
          .inNamespace(NAMESPACE)
          .resource(dockerConfigSecret(username, token))
          .create();

      DirectIngestionLauncher launcher =
          new DirectIngestionLauncher(
              IngestionJobPolicy.configured(Set.of(NAMESPACE), Set.of(KAFKA_STREAMS_IMAGE)),
              new PermitAllAuthorizationService(),
              "unused",
              "true",
              KAFKA_STREAMS_IMAGE,
              RUN_COMMAND,
              List.of(PULL_SECRET_NAME),
              Map.of("host.docker.internal", gatewayIp));
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(), NAMESPACE, spec, Map.of(), Map.of(), null);

      String jobName = launcher.launch(client, request, ACTOR);

      KubernetesJobObservation observation =
          pollUntilTerminal(launcher, client, request.correlationId());

      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          observation.phase(),
          () ->
              "job did not succeed (" + observation + ") - pod logs:\n" + podLogs(client, jobName));
    }

    HttpResponse<String> document =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create(
                            opensearch.hostEndpoint() + "/worldcheck-screening-records/_doc/wc-1"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document the dispatched pod wrote: " + document.body());
    assertTrue(document.body().contains("Smith"));
  }

  /**
   * Real WorldCheck row wc-1 from {@link WorldCheckFixtures#SAMPLE_TSV}, encoded as the flat JSON
   * object {@code BoundedKafkaStreamsConsumerRunner} decodes - keys are the identical real column
   * names {@code worldcheck-to-opensearch-kafka.yaml}'s own {@code mapper.fields} reference.
   */
  private static void seedOneRealWorldCheckRow(String bootstrapServers) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("UID", "wc-1");
    row.put("LAST NAME", "Smith");
    row.put("FIRST NAME", "José");
    row.put("CATEGORY", "INDIVIDUAL");
    row.put("E/I", "M");
    row.put("DOB", "1975/03/15");
    row.put("CITIZENSHIP", "UNITED STATES;RUSSIA");
    row.put("COUNTRIES", "");
    row.put("ALIASES", "Johnny Smith;J. Smith");
    row.put("SSN", "123-45-6789");
    row.put("POSITION", "Businessman");
    row.put("PEP ROLES", "Senator~Governor");
    row.put("PEP STATUS", "Former PEP");
    row.put("KEYWORDS", "Fraud~Bribery");
    row.put("EXTERNAL SOURCES", "https://example.com/report1");
    row.put("FURTHER INFORMATION", "Subject of an investigation.");
    row.put("SPECIAL INTEREST CATEGORIES", "Sanctions Related;PEP");
    row.put("LOCATIONS", "~ Moscow, Moscow Oblast ~ RUSSIA");

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(row);
      producer.send(new ProducerRecord<>(KAFKA_TOPIC, "wc-1", json)).get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed the real WorldCheck row onto Kafka", e);
    }
  }

  private static Secret dockerConfigSecret(String username, String token) {
    String auth =
        Base64.getEncoder()
            .encodeToString((username + ":" + token).getBytes(StandardCharsets.UTF_8));
    String dockerConfigJson =
        "{\"auths\":{\"https://index.docker.io/v1/\":{\"username\":\""
            + username
            + "\",\"password\":\""
            + token
            + "\",\"auth\":\""
            + auth
            + "\"}}}";
    return new SecretBuilder()
        .withNewMetadata()
        .withName(PULL_SECRET_NAME)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withType("kubernetes.io/dockerconfigjson")
        .addToData(
            ".dockerconfigjson",
            Base64.getEncoder().encodeToString(dockerConfigJson.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

  /**
   * Real diagnostic, not a guess: the actual stdout/stderr of the pod the Job ran, fetched while
   * the cluster still exists - {@code @WithKubernetesContainer} tears the cluster down once the
   * test method returns, so a failure with no captured logs is a dead end after the fact.
   */
  private static String podLogs(KubernetesClient client, String jobName) {
    List<Pod> pods =
        client.pods().inNamespace(NAMESPACE).withLabel("job-name", jobName).list().getItems();
    StringBuilder logs = new StringBuilder();
    for (Pod pod : pods) {
      logs.append("--- ").append(pod.getMetadata().getName()).append(" ---\n");
      try {
        logs.append(
            client.pods().inNamespace(NAMESPACE).withName(pod.getMetadata().getName()).getLog());
      } catch (RuntimeException e) {
        logs.append("(failed to fetch logs: ").append(e).append(")\n");
      }
    }
    return logs.toString();
  }

  private static KubernetesJobObservation pollUntilTerminal(
      DirectIngestionLauncher launcher, KubernetesClient client, String correlationId)
      throws InterruptedException {
    KubernetesJobObservation.Phase lastLoggedPhase = null;
    while (true) {
      Optional<KubernetesJobObservation> observation =
          launcher.observe(client, NAMESPACE, correlationId, ACTOR);
      if (observation.isPresent() && observation.get().phase() != lastLoggedPhase) {
        lastLoggedPhase = observation.get().phase();
        LOGGER.info("observed {}", observation.get());
      }
      if (observation.isPresent() && isTerminal(observation.get().phase())) {
        return observation.get();
      }
      Thread.sleep(500);
    }
  }

  private static boolean isTerminal(KubernetesJobObservation.Phase phase) {
    return phase == KubernetesJobObservation.Phase.SUCCEEDED
        || phase == KubernetesJobObservation.Phase.FAILED;
  }

  /**
   * A local, file-scoped stand-in for {@link AuthorizationService} - see {@link
   * DirectIngestionLauncherRealImageIntegrationTest}'s own identical class for why this isn't a
   * shared, importable-from-anywhere stub.
   */
  private static final class PermitAllAuthorizationService implements AuthorizationService {
    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      return new AuthorizationDecision(true, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }

  private static String dockerBridgeGatewayIp() {
    try {
      Process process =
          new ProcessBuilder(
                  "docker",
                  "network",
                  "inspect",
                  "bridge",
                  "--format",
                  "{{(index .IPAM.Config 0).Gateway}}")
              .redirectErrorStream(true)
              .start();
      String output =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
      process.waitFor();
      return output;
    } catch (Exception e) {
      throw new IllegalStateException("failed to discover the real Docker bridge gateway IP", e);
    }
  }
}
