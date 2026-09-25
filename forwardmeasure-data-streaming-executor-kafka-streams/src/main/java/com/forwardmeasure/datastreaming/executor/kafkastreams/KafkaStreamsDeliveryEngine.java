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
package com.forwardmeasure.datastreaming.executor.kafkastreams;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.executor.streaming.CompletedExecutionHandle;
import com.forwardmeasure.datastreaming.executor.streaming.DeliveryEngine;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.OpenSearchSinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real Kafka Streams {@link DeliveryEngine} - collapses the old {@code
 * KafkaStreamsStageRunnerProvider} (continuous-only, topic-in/topic-out) into an engine that
 * branches on {@link ExecutionMode}: {@code CONTINUOUS} keeps the same real Kafka Streams topology
 * (same {@link FieldMappingEngine} mapping, {@code EXACTLY_ONCE_V2} default) but now terminates in
 * a real sink write via {@link SinkRowWriter}, not just another topic; {@code BOUNDED} is genuinely
 * new - a plain consumer/producer poll loop against a real {@link InputFrontier} (see {@link
 * BoundedKafkaConsumerRunner}), since the Kafka Streams DSL runtime itself has no "stop at these
 * offsets" concept.
 *
 * <p>Deliberately scoped to single-source, no-Spark-stage plans for now - matches every real
 * WorldCheck/State-Street spec (see the repo's own gap-bridging plan), and correlated/Spark-staged
 * dispatch through this engine is real future work, not silently unsupported.
 */
public final class KafkaStreamsDeliveryEngine implements DeliveryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsDeliveryEngine.class);

  private final Map<String, Object> streamsConfigOverrides;
  private final FieldMappingEngine mapper = new FieldMappingEngine();

  public KafkaStreamsDeliveryEngine(Map<String, Object> streamsConfigOverrides) {
    this.streamsConfigOverrides =
        Map.copyOf(Objects.requireNonNull(streamsConfigOverrides, "streamsConfigOverrides"));
  }

  @Override
  public ExecutionHandle execute(ExecutionPlan plan, ExecutionMode mode) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(mode, "mode");
    if (plan.sources().size() != 1) {
      throw new UnsupportedOperationException(
          "KafkaStreamsDeliveryEngine: correlated plans (sources.size() > 1) are not dispatched"
              + " here yet");
    }
    if (plan.sparkStage().isPresent()) {
      throw new UnsupportedOperationException(
          "KafkaStreamsDeliveryEngine: a plan with a Spark compute stage is not dispatched here"
              + " yet - see this engine's own javadoc");
    }
    return mode == ExecutionMode.CONTINUOUS ? executeContinuous(plan) : executeBounded(plan);
  }

  private ExecutionHandle executeBounded(ExecutionPlan plan) {
    SourcePlan source = plan.sources().get(0);
    String id = "fds-kafka-streams-bounded-" + UUID.randomUUID();
    BoundedKafkaConsumerRunner.run(source, plan.destination(), plan.errors(), mapper);
    return new CompletedExecutionHandle(id);
  }

  private ExecutionHandle executeContinuous(ExecutionPlan plan) {
    SourcePlan source = plan.sources().get(0);
    KafkaConnectorUri sourceUri = KafkaConnectorUri.parse(source.source().uri());
    String applicationId = "fds-kafka-streams-" + UUID.randomUUID();

    Topology topology = buildContinuousTopology(source.mapper(), sourceUri.topic(), plan);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, sourceUri.bootstrapServers());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    streamsConfigOverrides.forEach(props::put);

    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
    return new KafkaStreamsExecutionHandle(applicationId, streams);
  }

  private Topology buildContinuousTopology(
      TransformSpec mapperSpec, String inputTopic, ExecutionPlan plan) {
    StreamsBuilder builder = new StreamsBuilder();
    StageRecordSerde recordSerde = new StageRecordSerde();

    KStream<String, Map<String, Object>> input =
        builder.stream(inputTopic, Consumed.with(Serdes.String(), recordSerde));

    KStream<String, Map<String, Object>> mapped =
        input.mapValues(record -> mapper.map(toSourceRow(record), mapperSpec));

    // One SinkRowWriter per stream-thread partition group, not per record - KafkaStreams#foreach
    // runs on the processing thread, so a single shared writer (real HTTP client, real per-call
    // PUT) is safe the same way OpenSearchSinkRowWriter's own single-threaded-per-partition use is
    // in BoundedKafkaConsumerRunner.
    SinkRowWriter sinkRowWriter = new OpenSearchSinkRowWriter(plan.destination());
    mapped.foreach((key, record) -> sinkRowWriter.write(record));
    return builder.build();
  }

  private static SourceRow toSourceRow(Map<String, Object> record) {
    return field -> {
      Object value = record.get(field);
      return value == null ? null : value.toString();
    };
  }

  /**
   * The real, deployable entrypoint for this engine's own container image - no such image has ever
   * existed before this class (unlike the Pekko side, which reused {@code PekkoIngestionRunner}'s
   * own long-standing entrypoint). Same {@code INGESTION_SPEC_PATH} convention, same
   * bounded-exits/continuous-blocks shape as {@code PekkoStreamsDeliveryEngine#main} - see that
   * class's own javadoc for why.
   */
  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);

    int exitCode = 0;
    try {
      ExecutionHandle handle =
          new KafkaStreamsDeliveryEngine(Map.of()).execute(plan, spec.executionMode());
      if (handle.isRunning()) {
        awaitShutdown(handle);
      }
      LOGGER.info("KafkaStreamsDeliveryEngine: run completed, id={}", handle.id());
    } catch (Exception e) {
      LOGGER.error("KafkaStreamsDeliveryEngine: run failed", e);
      exitCode = 1;
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

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
