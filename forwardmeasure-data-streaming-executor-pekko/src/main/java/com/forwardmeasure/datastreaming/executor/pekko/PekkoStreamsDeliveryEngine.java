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
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.executor.streaming.CompletedExecutionHandle;
import com.forwardmeasure.datastreaming.executor.streaming.DeliveryEngine;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real Pekko Streams {@link DeliveryEngine} - branches on {@link ExecutionMode} rather than
 * being split into separate bounded/continuous classes (see the repo's own gap-bridging plan, "the
 * 2x2x2 cube").
 *
 * <p>{@code BOUNDED} reconstructs an equivalent {@link IngestionSpec} from {@code plan} and
 * delegates straight to the existing, proven {@link PekkoIngestionRunner#run(IngestionSpec,
 * ActorSystem)} (single-source) or {@link PekkoCorrelationRunner#run(IngestionSpec, ActorSystem)}
 * (correlated) - a legitimate reuse, not a hack: {@link ExecutionPlan} carries every field an
 * {@code IngestionSpec} does (see its own javadoc), so the reconstruction is lossless, and
 * rewriting already-tested Camel/backpressure logic a second time against a different input type
 * would add real risk for zero real benefit.
 *
 * <p>{@code CONTINUOUS} is genuinely new: real Kafka commit-after-write, not the old {@code
 * PekkoStreamingStageRunnerProvider}'s topic-in/topic-out shape (which never wrote to a real sink)
 * - {@code Consumer.committableSource} reads, {@link PekkoIngestionRunner#sendOneRow} writes
 * through the same real Camel producer endpoint the bounded path uses (so every connector {@code
 * sink.connector()} resolves to for bounded mode also works here, not just {@code opensearch}), and
 * {@code Committer.sink} commits the consumed offset only once that write genuinely succeeds - real
 * at-least-once delivery, safe in practice for an idempotent sink the same way {@code
 * BoundedKafkaConsumerRunner}'s own reasoning already establishes (see the repo's own gap-bridging
 * plan). Deliberately not {@code Transactional.source}/{@code Transactional.sink} (what the old
 * provider used): a Kafka transaction ties the consumed offset to a *produced Kafka record*, not to
 * an arbitrary external sink write, so it cannot express "commit only after this OpenSearch/JDBC/
 * file write succeeded" at all - committable offsets are the correct real mechanism for that, not a
 * downgrade from it.
 */
public final class PekkoStreamsDeliveryEngine implements DeliveryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoStreamsDeliveryEngine.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> RECORD_TYPE = new TypeReference<>() {};

  private final ActorSystem system;

  public PekkoStreamsDeliveryEngine(ActorSystem system) {
    this.system = Objects.requireNonNull(system, "system");
  }

  @Override
  public ExecutionHandle execute(ExecutionPlan plan, ExecutionMode mode) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(mode, "mode");
    return mode == ExecutionMode.CONTINUOUS ? executeContinuous(plan) : executeBounded(plan, mode);
  }

  private ExecutionHandle executeBounded(ExecutionPlan plan, ExecutionMode mode) {
    IngestionSpec spec =
        new IngestionSpec(
            plan.sources(),
            plan.blockingField(),
            plan.transforms(),
            plan.destination(),
            mode,
            plan.delivery(),
            plan.errors());
    String id = "fds-pekko-bounded-" + UUID.randomUUID();
    try {
      if (spec.sources().size() == 1) {
        new PekkoIngestionRunner().run(spec, system);
      } else {
        PekkoCorrelationRunner.run(spec, system);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("PekkoStreamsDeliveryEngine: bounded run failed", e);
    }
    return new CompletedExecutionHandle(id);
  }

  private ExecutionHandle executeContinuous(ExecutionPlan plan) {
    if (plan.sources().size() != 1) {
      throw new UnsupportedOperationException(
          "PekkoStreamsDeliveryEngine: correlated plans (sources.size() > 1) are not dispatched"
              + " in CONTINUOUS mode yet");
    }
    if (plan.sparkStage().isPresent()) {
      throw new UnsupportedOperationException(
          "PekkoStreamsDeliveryEngine: a plan with a Spark compute stage is not dispatched here"
              + " yet - see this engine's own javadoc");
    }
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
            .withGroupId("fds-pekko-continuous-" + UUID.randomUUID())
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

  private java.util.concurrent.CompletionStage<Void> sendMappedRow(
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

  /**
   * The real, deployable entrypoint for this engine's own container image - reads the same {@code
   * INGESTION_SPEC_PATH} convention {@link PekkoIngestionRunner#main} uses, compiles it, and
   * dispatches. For {@code BOUNDED} mode, {@link #execute} already blocks until the run completes
   * (see {@link #executeBounded}), so {@code main()} exits as soon as it returns - correct for a
   * one-shot Kubernetes {@code Job}. For {@code CONTINUOUS} mode, {@link #execute} returns
   * immediately with a live handle (see {@link #executeContinuous}) - {@code main()} blocks on a
   * shutdown hook instead, so the process stays up for a Kubernetes {@code Deployment} and stops
   * the running stream cleanly on {@code SIGTERM} rather than the JVM just exiting underneath it.
   */
  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    ActorSystem system = ActorSystem.create("fds-pekko-streams-delivery-engine");
    int exitCode = 0;
    try {
      ExecutionHandle handle =
          new PekkoStreamsDeliveryEngine(system).execute(plan, spec.executionMode());
      if (handle.isRunning()) {
        awaitShutdown(handle);
      }
      LOGGER.info("PekkoStreamsDeliveryEngine: run completed, id={}", handle.id());
    } catch (Exception e) {
      LOGGER.error("PekkoStreamsDeliveryEngine: run failed", e);
      exitCode = 1;
    } finally {
      system.terminate();
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /**
   * Blocks {@code main()} until a {@code SIGTERM} (Kubernetes' own {@code Deployment} pod-eviction
   * signal) arrives, then stops {@code handle} before letting the JVM actually exit - the shutdown
   * hook is the only reliable place to do this cleanly, since a plain infinite sleep would leave no
   * chance to call {@link ExecutionHandle#stop()} at all.
   */
  private static void awaitShutdown(ExecutionHandle handle) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  handle.stop();
                  latch.countDown();
                }));
    latch.await();
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
