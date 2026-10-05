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
package com.forwardmeasure.datastreaming.executor.pekko;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.camel.ProducerTemplate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerMessage.Committable;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Committer;
import org.apache.pekko.kafka.javadsl.Consumer;

/**
 * Runs one {@code CONTINUOUS} ingestion via Pekko - named the same way {@link
 * ContinuousKafkaStreamsConsumerRunner} already is on the Kafka Streams side (extracted 2026-09-25
 * out of {@link PekkoStreamsDeliveryEngine}, a pure relocation - same real Kafka commit-after-write
 * behavior, no change intended).
 *
 * <p>Real Kafka commit-after-write: {@code Consumer.committableSource} reads, {@link
 * PekkoIngestionRunner#sendOneRow} writes through the same real Camel producer endpoint the bounded
 * path uses (so every connector {@code sink.connector()} resolves to for bounded mode also works
 * here, not just {@code opensearch}), and {@code Committer.sink} commits the consumed offset only
 * once that write genuinely succeeds - real at-least-once delivery, safe in practice for an
 * idempotent sink the same way {@link BoundedKafkaStreamsConsumerRunner}'s own reasoning already
 * establishes. Deliberately not {@code Transactional.source}/{@code Transactional.sink}: a Kafka
 * transaction ties the consumed offset to a *produced Kafka record*, not to an arbitrary external
 * sink write, so it cannot express "commit only after this OpenSearch/JDBC/file write succeeded" at
 * all - committable offsets are the correct real mechanism for that, not a downgrade from it.
 */
final class ContinuousPekkoStreamsConsumerRunner {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RECORD_TYPE = new TypeReference<>() {};

  private ContinuousPekkoStreamsConsumerRunner() {}

  static PekkoStreamsExecutionHandle run(ExecutionPlan plan, ActorSystem system) {
    SourcePlan source = plan.sources().get(0);
    KafkaConnectorUri sourceUri = KafkaConnectorUri.parse(source.source().uri());
    FieldMappingEngine mapper = new FieldMappingEngine();
    IngestionPipeline.SinkFailurePolicy sinkFailurePolicy =
        IngestionPipeline.SinkFailurePolicy.from(plan.errors());

    CamelBridge bridge = new CamelBridge();
    ProducerTemplate producerTemplate = bridge.camelContext().createProducerTemplate();

    ConsumerSettings<String, String> consumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withBootstrapServers(sourceUri.bootstrapServers())
            .withGroupId(
                com.forwardmeasure.datastreaming.api.ExecutionIdentity.of(
                    plan, "fds-pekko-continuous-", System.getenv("FDS_EXECUTION_ID")))
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            // Real, live-found bug fix (2026-09-25): pekko.kafka.consumer.stop-timeout defaults
            // to 30s (see its own reference.conf comment, confirmed by decompiling the real
            // pekko-connectors-kafka jar, not assumed) - a delay that exists so a plain consumer
            // stream gets a chance to finish committing in-flight messages before the underlying
            // actor stops. That comment says explicitly "this can be set to 0 for streams using
            // DrainingControl" - which is exactly what PekkoStreamsExecutionHandle#stop() already
            // is (drainAndShutdown()), making the extra 30s wholly redundant. Confirmed live: a
            // real SIGTERM against a real subprocess previously took exactly 30.02s to exit
            // (PekkoStreamsDeliveryEngineSigtermIntegrationTest) - this setting was the entire
            // cause, not drainAndShutdown() itself hanging.
            .withStopTimeout(java.time.Duration.ZERO);
    CommitterSettings committerSettings = CommitterSettings.create(system);

    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(sourceUri.topic()))
            .mapAsync(
                1,
                msg ->
                    IngestionPipeline.withSinkFailureHandling(
                            sinkFailurePolicy,
                            () ->
                                sendMappedRow(
                                    msg.record().value(), source, plan, mapper, producerTemplate))
                        .thenApply(ignored -> (Committable) msg.committableOffset()))
            .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
            .run(system);

    return new PekkoStreamsExecutionHandle("fds-pekko-continuous", control, bridge);
  }

  private static CompletionStage<Void> sendMappedRow(
      String json,
      SourcePlan source,
      ExecutionPlan plan,
      FieldMappingEngine mapper,
      ProducerTemplate producerTemplate) {
    Map<String, Object> raw;
    try {
      raw = JSON.readValue(json, RECORD_TYPE);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to parse continuous record", e);
    }
    SourceRow row = new MapSourceRow(raw);
    Map<String, Object> mapped = mapper.map(row, source.mapper());
    return PekkoIngestionRunner.sendOneRow(plan.destination(), producerTemplate, JSON, mapped);
  }
}
