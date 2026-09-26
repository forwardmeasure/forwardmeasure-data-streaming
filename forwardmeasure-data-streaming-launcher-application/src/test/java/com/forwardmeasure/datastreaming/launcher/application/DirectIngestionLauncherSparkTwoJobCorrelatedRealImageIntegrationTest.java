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
import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec.FieldRule;
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
import io.fabric8.kubernetes.client.KubernetesClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
 * The correlated (multi-source) sibling of {@link
 * DirectIngestionLauncherSparkTwoJobRealImageIntegrationTest} - same real two-Job Spark-then-
 * delivery pipeline, this time with {@code sources.size() == 2}. Closes a real gap found while
 * building this: the packaged Spark image's entrypoint used to be hardcoded to {@link
 * com.forwardmeasure.datastreaming.executor.spark.SparkIngestionRunner}, which explicitly rejects
 * any correlated plan - a correlated {@code HEAVY}-transform spec would have reached the pod and
 * crashed. Fixed via {@link com.forwardmeasure.datastreaming.executor.spark.SparkStageRunner}, the
 * new cardinality-aware entrypoint (mirrors {@code PekkoStreamsDeliveryEngine}'s own dispatch).
 *
 * <p>Two Kafka-sourced sources, correlated on {@code uid}: {@code core} carries the full screening
 * mapper (name/entity-type/country/nationality/dob -&gt; {@code screening_hits} via the real {@code
 * HEAVY screen_against_worldcheck_reference} transform, entirely from its own columns - the current
 * architecture has no post-merge transform stage, so a transform needing fields from *both* sources
 * isn't expressible yet, a real, separate, out-of-scope gap, not something this test pretends to
 * cover); {@code alias} contributes one extra {@code position} field per {@code uid}, merged in by
 * {@code SparkCorrelationEngine#correlate}. Deliberately a field {@code core} never populates, not
 * {@code names} - confirmed live that two sources both writing the same target field key (both
 * tried {@code names} originally) doesn't union their lists, it picks the higher-trust-weight
 * source's own value outright ({@code mergeGroup}'s real {@code putIfAbsent} semantics); a disjoint
 * field is the correct way to prove cross-source merge. Two real correlation groups seeded (one
 * matching a reference entity, one not), proving real cross-source merge on the *right* key, not
 * just "a correlated plan happens not to crash."
 */
@WithKubernetesContainer
@WithKafkaContainer(hostDockerInternalListener = true)
@WithOpenSearchContainer
final class DirectIngestionLauncherSparkTwoJobCorrelatedRealImageIntegrationTest {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(
          DirectIngestionLauncherSparkTwoJobCorrelatedRealImageIntegrationTest.class);
  private static final String NAMESPACE = "spark-two-job-correlated-test";
  private static final String CORE_TOPIC = "customer-master-core-rows";
  private static final String ALIAS_TOPIC = "customer-master-alias-rows";
  private static final String INDEX = "test-customer-master-correlated-screening-records";

  private static final String SPARK_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-spark:1.1.0";
  private static final String KAFKA_STREAMS_LOCAL_IMAGE =
      "forwardmeasure/data-streaming-executor-kafka-streams:1.1.0";
  private static final String RUN_COMMAND = "java -jar /deployments/application.jar";
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          TenantDatabase.forAlias("test-tenant"),
          "org-1",
          "actor-1",
          Set.of("reviewer"));

  @Test
  @Timeout(600)
  void launchDispatchesARealCorrelatedSparkStageThenARealDeliveryJob(
      KubernetesTestContainer kubernetes,
      KafkaTestContainer kafka,
      OpenSearchTestContainer opensearch)
      throws Exception {
    seedRows(kafka.bootstrapServers());

    String gatewayIp = dockerBridgeGatewayIp();
    int opensearchPort = opensearch.hostEndpoint().getPort();
    String kafkaUriForPod = kafka.hostDockerInternalBootstrapServers();
    IngestionSpec spec = correlatedScreeningSpec(kafkaUriForPod, opensearchPort);

    try (KubernetesClient client = kubernetes.createClient()) {
      client
          .namespaces()
          .resource(
              new NamespaceBuilder().withNewMetadata().withName(NAMESPACE).endMetadata().build())
          .create();

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
              kafkaUriForPod);
      DirectLaunchRequest request =
          new DirectLaunchRequest(
              UUID.randomUUID().toString(), NAMESPACE, spec, Map.of(), Map.of(), null);

      launcher.launch(client, request, ACTOR);

      KubernetesJobObservation finalObservation =
          pollUntilTerminal(launcher, client, request.correlationId());
      LOGGER.info(
          "=== spark pod logs ===\n{}",
          podLogs(
              client,
              DirectIngestionLauncher.deterministicJobName(request.correlationId() + ":spark")));
      LOGGER.info(
          "=== delivery pod logs ===\n{}",
          podLogs(
              client,
              DirectIngestionLauncher.deterministicJobName(request.correlationId() + ":delivery")));
      assertEquals(
          KubernetesJobObservation.Phase.SUCCEEDED,
          finalObservation.phase(),
          () -> "" + finalObservation);
    }

    HttpClient http = HttpClient.newHttpClient();
    HttpResponse<String> matched =
        http.send(
            HttpRequest.newBuilder(
                    URI.create(opensearch.hostEndpoint() + "/" + INDEX + "/_doc/KYC-SCREEN-1"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200, matched.statusCode(), "expected the merged, screened document: " + matched.body());
    assertTrue(
        matched.body().contains("\"reference_uid\":\"wc-1\""),
        "expected a real screening match merged from the core source: " + matched.body());
    assertTrue(
        matched.body().contains("\"position\":\"Senior Relationship Manager\""),
        "expected the alias source's own contributed field to have been merged in: "
            + matched.body());

    HttpResponse<String> unmatched =
        http.send(
            HttpRequest.newBuilder(
                    URI.create(opensearch.hostEndpoint() + "/" + INDEX + "/_doc/KYC-OTHER-1"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(
        200, unmatched.statusCode(), "expected the second, unrelated correlation group too");
    assertTrue(
        !unmatched.body().contains("\"screening_hits\":[{"),
        "expected no screening hit for an unrelated name: " + unmatched.body());
  }

  /**
   * Two sources correlated on {@code uid}. {@code core}'s own mapper is self-contained (every
   * {@code screen_against_worldcheck_reference} input comes from {@code core}'s own columns) since
   * there is no post-merge transform stage today - see this class's own javadoc.
   */
  private static IngestionSpec correlatedScreeningSpec(String kafkaBrokers, int opensearchPort) {
    SourceSpec coreSource =
        new SourceSpec(
            "kafka",
            "n/a",
            null,
            null,
            null,
            Map.of(
                "kafka.bootstrap.servers",
                kafkaBrokers,
                "subscribe",
                CORE_TOPIC,
                "startingOffsets",
                "earliest",
                "endingOffsets",
                "latest"));
    TransformSpec coreMapper =
        new TransformSpec(
            "party",
            List.of(
                new FieldRule("uid", "UID", null, null, null, null, null, Map.of(), null),
                new FieldRule(
                    "names",
                    "FULL_NAME",
                    null,
                    null,
                    null,
                    null,
                    true,
                    Map.of("name_type", "PRIMARY"),
                    null),
                new FieldRule(
                    "screening_hits",
                    null,
                    null,
                    Map.of(
                        "full_name", "FULL_NAME",
                        "entity_type", "ENTITY_TYPE",
                        "country", "COUNTRY",
                        "nationality", "NATIONALITY",
                        "dob", "DOB"),
                    "screen_against_worldcheck_reference",
                    true,
                    null,
                    Map.of(),
                    null)));

    SourceSpec aliasSource =
        new SourceSpec(
            "kafka",
            "n/a",
            null,
            null,
            null,
            Map.of(
                "kafka.bootstrap.servers",
                kafkaBrokers,
                "subscribe",
                ALIAS_TOPIC,
                "startingOffsets",
                "earliest",
                "endingOffsets",
                "latest"));
    // Deliberately a target field "core" never populates (position) - SparkCorrelationEngine
    // #mergeGroup merges by putIfAbsent per target-field key, sorted by trust weight: two sources
    // both writing "names" would have the higher-trust-weight source's own list win outright, not
    // union with the other's (confirmed live, not assumed - this test used "names" for the alias
    // contribution originally and the alias entry was silently dropped). A field only "alias"
    // populates is the real, correct way to prove cross-source merge without hitting that.
    TransformSpec aliasMapper =
        new TransformSpec(
            "party",
            List.of(
                new FieldRule("uid", "UID", null, null, null, null, null, Map.of(), null),
                new FieldRule("position", "ALIAS", null, null, null, true, null, Map.of(), null)));

    SinkSpec sink =
        new SinkSpec(
            "opensearch",
            "http://host.docker.internal:" + opensearchPort,
            INDEX,
            null,
            null,
            Map.of("idField", "uid"));

    return new IngestionSpec(
        List.of(
            new SourcePlan("core", coreSource, coreMapper, 1.0),
            new SourcePlan("alias", aliasSource, aliasMapper, 0.5)),
        "uid",
        null,
        sink,
        ExecutionMode.BOUNDED,
        DeliverySemantics.defaults(),
        null);
  }

  private static void seedRows(String bootstrapServers) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    ObjectMapper mapper = new ObjectMapper();
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      send(
          producer,
          mapper,
          CORE_TOPIC,
          coreRow("KYC-SCREEN-1", "Jose Smith", "Individual", "RUSSIA", "Russia", "19750315"));
      send(producer, mapper, ALIAS_TOPIC, aliasRow("KYC-SCREEN-1", "Senior Relationship Manager"));
      send(
          producer,
          mapper,
          CORE_TOPIC,
          coreRow(
              "KYC-OTHER-1",
              "Completely Unrelated Person",
              "Individual",
              "JAPAN",
              "Japan",
              "19900101"));
      send(producer, mapper, ALIAS_TOPIC, aliasRow("KYC-OTHER-1", "Junior Analyst"));
    } catch (Exception e) {
      throw new IllegalStateException("failed to seed correlated rows onto Kafka", e);
    }
  }

  private static void send(
      KafkaProducer<String, String> producer,
      ObjectMapper mapper,
      String topic,
      Map<String, Object> row)
      throws Exception {
    producer
        .send(new ProducerRecord<>(topic, (String) row.get("UID"), mapper.writeValueAsString(row)))
        .get();
  }

  private static Map<String, Object> coreRow(
      String uid,
      String fullName,
      String entityType,
      String country,
      String nationality,
      String dob) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("UID", uid);
    row.put("FULL_NAME", fullName);
    row.put("ENTITY_TYPE", entityType);
    row.put("COUNTRY", country);
    row.put("NATIONALITY", nationality);
    row.put("DOB", dob);
    return row;
  }

  private static Map<String, Object> aliasRow(String uid, String alias) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("UID", uid);
    row.put("ALIAS", alias);
    return row;
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
