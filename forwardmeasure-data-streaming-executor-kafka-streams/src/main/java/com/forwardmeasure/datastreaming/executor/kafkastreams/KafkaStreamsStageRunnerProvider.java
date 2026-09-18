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

import com.forwardmeasure.datastreaming.api.StreamingStageSpec;
import com.forwardmeasure.datastreaming.api.StreamingStageSpec.FilterSpec;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingEngineId;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageHandle;
import com.forwardmeasure.datastreaming.executor.streaming.StreamingStageRunnerProvider;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Kafka Streams implementation of {@link StreamingStageRunnerProvider} - the first real provider
 * for this org's continuous-streaming capability (see this repo's own gap-bridging plan). Each
 * record on {@code inputTopic} is a flat JSON object; this stage applies the spec's {@code
 * TransformSpec} via the same {@link FieldMappingEngine} the bounded Pekko/Spark executors already
 * use (the field-resolution/named-transform logic is shared, not reimplemented per engine), then
 * the optional {@link FilterSpec} (a consumer-side predicate, not a producer-side branch - see
 * {@link StreamingStageSpec}'s own javadoc), then writes the mapped record as flat JSON to {@code
 * outputTopic}.
 *
 * <p>Deliberately not built on fowf's {@code openworkflow-durable-processing} command/aggregate
 * kernel - verified by reading its real classes: {@code DurableProcess}/{@code DurableAggregate}/
 * {@code DurableCommandKey} model "apply a command to a durable aggregate, get a decision back",
 * which fits fowf's own workflow-execution state machine, not a stateless per-record map+filter
 * stage - there is no aggregate or command here. That library's one genuinely reusable piece for a
 * use case like this, a generic Jackson {@code Serde}, is small enough ({@link StageRecordSerde})
 * that duplicating it here beats depending on a whole command-sourcing kernel for one utility
 * class.
 */
public final class KafkaStreamsStageRunnerProvider implements StreamingStageRunnerProvider {
  private final Map<String, Object> streamsConfigOverrides;
  private final FieldMappingEngine mapper = new FieldMappingEngine();

  public KafkaStreamsStageRunnerProvider(Map<String, Object> streamsConfigOverrides) {
    this.streamsConfigOverrides =
        Map.copyOf(Objects.requireNonNull(streamsConfigOverrides, "streamsConfigOverrides"));
  }

  @Override
  public StreamingEngineId engineId() {
    return StreamingEngineId.KAFKA_STREAMS;
  }

  @Override
  public StreamingStageHandle start(StreamingStageSpec spec) {
    Objects.requireNonNull(spec, "spec");
    Objects.requireNonNull(spec.name(), "spec.name");
    Objects.requireNonNull(spec.inputTopic(), "spec.inputTopic");
    Objects.requireNonNull(spec.outputTopic(), "spec.outputTopic");
    Objects.requireNonNull(spec.mapper(), "spec.mapper");

    Topology topology = buildTopology(spec);
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, spec.name());
    streamsConfigOverrides.forEach(props::put);

    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
    return new KafkaStreamsStageHandle(spec.name(), streams);
  }

  private Topology buildTopology(StreamingStageSpec spec) {
    StreamsBuilder builder = new StreamsBuilder();
    StageRecordSerde recordSerde = new StageRecordSerde();

    KStream<String, Map<String, Object>> input =
        builder.stream(spec.inputTopic(), Consumed.with(Serdes.String(), recordSerde));

    KStream<String, Map<String, Object>> mapped =
        input.mapValues(record -> mapper.map(toSourceRow(record), spec.mapper()));

    FilterSpec filter = spec.filter();
    if (filter != null) {
      mapped = mapped.filter((key, record) -> matches(record, filter));
    }

    mapped.to(spec.outputTopic(), Produced.with(Serdes.String(), recordSerde));
    return builder.build();
  }

  private static SourceRow toSourceRow(Map<String, Object> record) {
    return field -> {
      Object value = record.get(field);
      return value == null ? null : value.toString();
    };
  }

  private static boolean matches(Map<String, Object> record, FilterSpec filter) {
    Object value = record.get(filter.field());
    return value != null && value.toString().equals(filter.equals());
  }
}
