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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.authzen.ActiveOrganization;
import com.forwardmeasure.authzen.AuthorizationDecision;
import com.forwardmeasure.authzen.AuthorizationRequest;
import com.forwardmeasure.authzen.AuthorizationService;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.testfixtures.TestCustomerMasterFixtures;
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
import io.fabric8.kubernetes.client.KubernetesClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The real, no-mocks proof of this repo's own two-Job Spark-then-delivery pipeline (see {@link
 * DirectIngestionLauncher}'s own javadoc) - the one capability this launcher's other real-image
 * tests never exercise, since none of their specs ever use a {@code HEAVY} transform. Uses this
 * repo's own real, locally-built {@code data-streaming-executor-spark}/{@code
 * data-streaming-executor-kafka-streams} images (no Docker Hub credentials needed - {@link
 * KubernetesTestContainer#loadImageAndPinDigest} loads straight from the host's local image cache,
 * same mechanism {@code RealFowfWorkflowBoundedIngestionTest} already proved live), dispatches
 * {@link TestCustomerMasterFixtures#boundedKafkaSpecWithScreening} (whose one {@code
 * screening_hits} field rule is the real {@code HEAVY screen_against_worldcheck_reference}
 * transform), and verifies the real, end-to-end result: a real Spark Job reads real seeded rows off
 * a real Kafka topic, screens each one against the transform's own bundled reference population,
 * writes every mapped row (including a genuine, non-empty {@code screening_hits} list for the one
 * row engineered to match) to a real handoff topic; {@link DirectIngestionLauncher#observe} then
 * reconciles a second, real delivery Job (always {@code KafkaStreamsDeliveryEngine} for a
 * Spark-staged plan - {@link com.forwardmeasure.datastreaming.core.SparkHandoffSpecs}'s own derived
 * spec is always {@code kafka}-sourced, so {@code ExecutionPlanCompiler} always resolves {@code
 * KAFKA_STREAMS} for it, regardless of the original spec's own engine - a real, non-obvious
 * consequence of the architecture, not something this test forces), which reads that topic and
 * writes the final, screened rows to a real OpenSearch index.
 */
@WithKubernetesContainer
@WithKafkaContainer(hostDockerInternalListener = true)
@WithOpenSearchContainer
final class DirectIngestionLauncherSparkTwoJobRealImageIntegrationTest {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(
          DirectIngestionLauncherSparkTwoJobRealImageIntegrationTest.class);
  private static final String NAMESPACE = "spark-two-job-test";
  private static final String SOURCE_TOPIC = "test-customer-master-rows";

  /**
   * This repo's own real, locally-built images ({@code ${project.version}}) - no Docker Hub
   * reference needed at all, same reasoning as {@code RealFowfWorkflowBoundedIngestionTest}'s own
   * {@code PEKKO_LOCAL_IMAGE}. The Spark image was rebuilt 2026-09-25 specifically to bake in the
   * new {@code screen_against_worldcheck_reference} transform + its real {@code
   * forwardmeasure-entity-matching-core} dependency - the Kafka Streams image needed no rebuild
   * (the delivery stage's own field rules are always {@code raw: true} identity passthroughs, never
   * invoking any transform - see {@code SparkHandoffSpecs#identityFieldRules}).
   */
  private static final String SPARK_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-spark:1.1.0";

  private static final String KAFKA_STREAMS_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-kafka-streams:1.1.0";

  private static final String RUN_COMMAND = "java -jar /deployments/application.jar";
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  @Test
  @Timeout(600)
  void launchDispatchesARealSparkStageThenARealDeliveryJobAndScreeningHitsLandInOpenSearch(
      KubernetesTestContainer kubernetes,
      KafkaTestContainer kafka,
      OpenSearchTestContainer opensearch)
      throws Exception {
    seedRealCustomerMasterRows(kafka.bootstrapServers());

    String gatewayIp = dockerBridgeGatewayIp();
    int opensearchPort = opensearch.hostEndpoint().getPort();
    IngestionSpec spec =
        TestCustomerMasterFixtures.boundedKafkaSpecWithScreening(
            kafka.hostDockerInternalBootstrapServers(),
            SOURCE_TOPIC,
            "http://host.docker.internal:" + opensearchPort,
            Path.of(""));

    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();

      // See SPARK_LOCAL_IMAGE/KAFKA_STREAMS_LOCAL_IMAGE's own javadoc: loaded straight from the
      // host's local image cache, digest-pinned - AsyncApiKubernetesJobOperationExecutor's own
      // sibling KubernetesJobLifecycle enforces this org's standing digest-pinning policy on every
      // dispatched Job image.
      String sparkImage = kubernetes.loadImageAndPinDigest(SPARK_LOCAL_IMAGE);
      String kafkaStreamsImage = kubernetes.loadImageAndPinDigest(KAFKA_STREAMS_LOCAL_IMAGE);

      DirectIngestionLauncher launcher =
          new DirectIngestionLauncher(
              IngestionJobPolicy.configured(
                  Set.of(NAMESPACE), Set.of(sparkImage, kafkaStreamsImage)),
              new PermitAllAuthorizationService(),
              "unused",
              "true",
              kafkaStreamsImage,
              RUN_COMMAND,
              List.of(),
              Map.of("host.docker.internal", gatewayIp),
              sparkImage,
              RUN_COMMAND,
              kafka.hostDockerInternalBootstrapServers());
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(), NAMESPACE, spec, Map.of(), Map.of(), null);

      String sparkJobName = launcher.launch(client, request, ACTOR);
      assertEquals(
          DirectIngestionLauncher.deterministicJobName(ACTOR, request.correlationId() + ":spark"),
          sparkJobName,
          "launch() must return the Spark Job's own name for a staged plan, not the delivery"
              + " Job's");

      KubernetesJobObservation finalObservation =
          pollUntilTerminal(launcher, client, request.correlationId());
      LOGGER.info(
          "=== spark pod logs ===\n{}",
          podLogs(
              client,
              DirectIngestionLauncher.deterministicJobName(
                  ACTOR, request.correlationId() + ":spark")));
      LOGGER.info(
          "=== delivery pod logs ===\n{}",
          podLogs(
              client,
              DirectIngestionLauncher.deterministicJobName(
                  ACTOR, request.correlationId() + ":delivery")));
      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          finalObservation.phase(),
          () -> "" + finalObservation);
    }

    String documentUri =
        opensearch.hostEndpoint()
            + "/test-customer-master-screening-records-screened/_doc/KYC-SCREEN-1";
    HttpResponse<String> document =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(documentUri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200,
        document.statusCode(),
        "expected the real document the two-Job pipeline wrote: " + document.body());
    assertTrue(
        document.body().contains("\"reference_uid\":\"wc-1\""),
        "expected a real screening_hits match against reference entity wc-1: " + document.body());
  }

  /**
   * {@link TestCustomerMasterFixtures#sampleCsvWithScreeningMatch()}'s 5 rows, seeded as flat JSON
   * objects onto {@link #SOURCE_TOPIC} - the same seeding contract {@code
   * DirectIngestionLauncherKafkaStreamsRealImageIntegrationTest#seedOneRealWorldCheckRow} already
   * documents (one JSON object per row, keys matching the real column names {@code
   * SparkCorrelationEngine#sourceRowFor}/{@code BoundedKafkaStreamsConsumerRunner} both expect).
   */
  private static void seedRealCustomerMasterRows(String bootstrapServers) {
    String[] lines = TestCustomerMasterFixtures.sampleCsvWithScreeningMatch().split("\n");
    String[] header = lines[0].split("\\|");
    ObjectMapper mapper = new ObjectMapper();

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      for (int lineIndex = 1; lineIndex < lines.length; lineIndex++) {
        String[] values = lines[lineIndex].split("\\|");
        Map<String, Object> row = new LinkedHashMap<>();
        for (int column = 0; column < header.length; column++) {
          row.put(header[column], column < values.length ? values[column] : "");
        }
        String json = mapper.writeValueAsString(row);
        producer
            .send(new ProducerRecord<>(SOURCE_TOPIC, (String) row.get("ENTITYS_UNIQUE_ID"), json))
            .get();
      }
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed real Customer Master rows onto Kafka", e);
    }
  }

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
          new String(
                  process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
              .strip();
      process.waitFor();
      return output;
    } catch (Exception e) {
      throw new IllegalStateException("failed to discover the real Docker bridge gateway IP", e);
    }
  }
}
