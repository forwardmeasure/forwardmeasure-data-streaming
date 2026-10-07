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

import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.OpenSearchSinkRowWriter;
import com.forwardmeasure.datastreaming.mappers.SinkRowWriter;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Runs one {@code CONTINUOUS} Kafka-sourced ingestion as a real, long-running {@link KafkaStreams}
 * topology - the named sibling {@link BoundedKafkaStreamsConsumerRunner} already had, extracted out
 * of {@link KafkaStreamsDeliveryEngine} (2026-09-25, a pure relocation - same real topology/{@code
 * EXACTLY_ONCE_V2} default, no behavior change intended beyond the fix below) so both engines'
 * bounded/continuous logic reads as parallel, equally-discoverable classes rather than one engine's
 * continuous path living inline while its bounded path has its own name.
 *
 * <p>Real, live-found bug fixed during this extraction: {@code toSourceRow} used to build its own
 * {@code SourceRow} via a bare lambda (only the abstract {@code get} method), which silently
 * defeats {@code raw: true} field rules the exact same way {@link
 * BoundedKafkaStreamsConsumerRunner}'s own identical bug did (see its own javadoc) - a lambda can't
 * override {@code SourceRow#getRaw}'s default method, so it always fell back to {@code get}'s lossy
 * {@code String.valueOf(...)}. Fixed the same way: {@link MapSourceRow} already correctly overrides
 * {@code getRaw}.
 */
final class ContinuousKafkaStreamsConsumerRunner {

  private final Map<String, Object> streamsConfigOverrides;
  private final FieldMappingEngine mapper = new FieldMappingEngine();

  ContinuousKafkaStreamsConsumerRunner(Map<String, Object> streamsConfigOverrides) {
    this.streamsConfigOverrides = streamsConfigOverrides;
  }

  KafkaStreamsExecutionHandle run(ExecutionPlan plan) {
    SourcePlan source = plan.sources().get(0);
    KafkaConnectorUri sourceUri = KafkaConnectorUri.parse(source.source().uri());
    String applicationId =
        com.forwardmeasure.datastreaming.api.ExecutionIdentity.of(
            plan, "fds-kafka-streams-", System.getenv("FDS_EXECUTION_ID"));

    SinkRowWriter sinkRowWriter = new OpenSearchSinkRowWriter(plan.destination());
    try {
      Topology topology = buildTopology(source.mapper(), sourceUri.topic(), plan, sinkRowWriter);

      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, sourceUri.bootstrapServers());
      props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      streamsConfigOverrides.forEach(props::put);

      KafkaStreams streams = new KafkaStreams(topology, props);
      var handle = new KafkaStreamsExecutionHandle(applicationId, streams, sinkRowWriter);
      try {
        streams.start();
      } catch (RuntimeException failure) {
        handle.stop();
        throw failure;
      }
      return handle;
    } catch (RuntimeException failure) {
      sinkRowWriter.close();
      throw failure;
    }
  }

  private Topology buildTopology(
      TransformSpec mapperSpec,
      String inputTopic,
      ExecutionPlan plan,
      SinkRowWriter sinkRowWriter) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> input =
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Map<String, Object>> mapped =
        input
            .mapValues(
                record ->
                    KafkaRecordMapping.map(
                        record,
                        mapperSpec,
                        mapper,
                        IngestionPipeline.MalformedRecordPolicy.from(plan.errors()),
                        null))
            .filter((key, record) -> record != null);

    // One SinkRowWriter per stream-thread partition group, not per record - KafkaStreams#foreach
    // runs on the processing thread, so a single shared writer (real HTTP client, real per-call
    // PUT) is safe the same way OpenSearchSinkRowWriter's own single-threaded-per-partition use is
    // in BoundedKafkaStreamsConsumerRunner.
    mapped.foreach(
        (key, record) ->
            IngestionPipeline.withSinkFailureHandlingBlocking(
                IngestionPipeline.SinkFailurePolicy.from(plan.errors()),
                () -> sinkRowWriter.write(record)));
    return builder.build();
  }
}
