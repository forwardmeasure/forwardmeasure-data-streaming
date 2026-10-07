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
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.camel.ProducerTemplate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerMessage.Committable;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Committer;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Restores a compacted changelog before starting commit-after-state-and-sink source consumers. */
final class ContinuousPekkoStreamsCorrelationRunner {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContinuousPekkoStreamsCorrelationRunner.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RECORD_TYPE = new TypeReference<>() {};

  private ContinuousPekkoStreamsCorrelationRunner() {}

  static ExecutionHandle run(ExecutionPlan plan, ActorSystem system) {
    if (plan.sources().size() <= 1)
      throw new IllegalArgumentException("Correlation needs multiple sources");
    String brokers =
        KafkaConnectorUri.parse(plan.sources().getFirst().source().uri()).bootstrapServers();
    for (SourcePlan source : plan.sources()) {
      if (!brokers.equals(KafkaConnectorUri.parse(source.source().uri()).bootstrapServers())) {
        throw new IllegalArgumentException(
            "Continuous correlation sources must use the same Kafka cluster");
      }
    }
    String identity = KafkaCorrelationState.identity(plan);
    KafkaCorrelationState state = new KafkaCorrelationState(plan, brokers, identity);
    CamelBridge bridge;
    try {
      bridge = new CamelBridge();
    } catch (RuntimeException failure) {
      state.close();
      throw failure;
    }
    ExecutorService writes = Executors.newSingleThreadExecutor();
    List<Consumer.DrainingControl<Done>> controls = new ArrayList<>();
    CompositeExecutionHandle handle =
        new CompositeExecutionHandle(identity, controls, bridge, state, writes);
    try {
      ProducerTemplate producer = bridge.camelContext().createProducerTemplate();
      for (SourcePlan source : plan.sources()) {
        KafkaConnectorUri uri = KafkaConnectorUri.parse(source.source().uri());
        ConsumerSettings<String, String> settings =
            ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers(brokers)
                .withGroupId(identity + "-" + source.sourceKey())
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .withStopTimeout(Duration.ZERO);
        FieldMappingEngine mapper = new FieldMappingEngine();
        Consumer.DrainingControl<Done> control =
            Consumer.committableSource(settings, Subscriptions.topics(uri.topic()))
                .mapAsync(
                    1,
                    message ->
                        IngestionPipeline.withSinkFailureHandling(
                                IngestionPipeline.SinkFailurePolicy.from(plan.errors()),
                                () ->
                                    CompletableFuture.runAsync(
                                        () ->
                                            process(
                                                message.record(),
                                                source,
                                                plan,
                                                mapper,
                                                state,
                                                producer),
                                        writes))
                            .thenApply(ignored -> (Committable) message.committableOffset()))
                .toMat(
                    Committer.sink(CommitterSettings.create(system)),
                    Consumer::createDrainingControl)
                .run(system);
        controls.add(control);
        control
            .streamCompletion()
            .whenComplete(
                (done, failure) -> {
                  if (failure != null) handle.failure.compareAndSet(null, failure);
                });
      }
      return handle;
    } catch (RuntimeException failure) {
      handle.stop();
      throw failure;
    }
  }

  private static void process(
      ConsumerRecord<String, String> record,
      SourcePlan source,
      ExecutionPlan plan,
      FieldMappingEngine mapper,
      KafkaCorrelationState state,
      ProducerTemplate producer) {
    Map<String, Object> mapped;
    String key;
    try {
      mapped =
          mapper.map(
              new MapSourceRow(JSON.readValue(record.value(), RECORD_TYPE)), source.mapper());
      Object value = mapped.get(plan.blockingField());
      if (value == null) throw new IllegalArgumentException("Missing correlation key");
      key = String.valueOf(value).strip().toUpperCase(Locale.ROOT);
      if (key.isBlank()) throw new IllegalArgumentException("Blank correlation key");
    } catch (Exception failure) {
      if (IngestionPipeline.MalformedRecordPolicy.from(plan.errors())
          == IngestionPipeline.MalformedRecordPolicy.FAIL) {
        throw new IllegalArgumentException("Unable to map source " + source.sourceKey(), failure);
      }
      LOGGER.warn(
          "Skipping malformed record source={} partition={} offset={}",
          source.sourceKey(),
          record.partition(),
          record.offset(),
          failure);
      return;
    }
    state.updateAndWrite(
        new KafkaCorrelationState.Contribution(
            key, source.sourceKey(), record.topic(), record.partition(), record.offset(), mapped),
        merged -> PekkoIngestionRunner.sendOneRow(plan.destination(), producer, JSON, merged));
  }

  private static final class CompositeExecutionHandle implements ExecutionHandle {
    private final String id;
    private final List<Consumer.DrainingControl<Done>> controls;
    private final CamelBridge bridge;
    private final KafkaCorrelationState state;
    private final ExecutorService writes;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    CompositeExecutionHandle(
        String id,
        List<Consumer.DrainingControl<Done>> controls,
        CamelBridge bridge,
        KafkaCorrelationState state,
        ExecutorService writes) {
      this.id = id;
      this.controls = controls;
      this.bridge = bridge;
      this.state = state;
      this.writes = writes;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public boolean isRunning() {
      return !stopped.get()
          && failure.get() == null
          && controls.stream()
              .allMatch(control -> !control.streamCompletion().toCompletableFuture().isDone());
    }

    @Override
    public Optional<Throwable> failure() {
      return Optional.ofNullable(failure.get());
    }

    @Override
    public void stop() {
      if (!stopped.compareAndSet(false, true)) return;
      try {
        List<CompletionStage<Done>> drains =
            controls.stream()
                .map(
                    control ->
                        control.drainAndShutdown(java.util.concurrent.ForkJoinPool.commonPool()))
                .toList();
        for (CompletionStage<Done> drain : drains) {
          try {
            drain.toCompletableFuture().get(30, TimeUnit.SECONDS);
          } catch (Exception problem) {
            LOGGER.warn("Correlation consumer shutdown failed", problem);
          }
        }
      } finally {
        writes.shutdownNow();
        state.close();
        try {
          bridge.close();
        } catch (Exception problem) {
          LOGGER.warn("Correlation sink shutdown failed", problem);
        }
      }
    }
  }
}
