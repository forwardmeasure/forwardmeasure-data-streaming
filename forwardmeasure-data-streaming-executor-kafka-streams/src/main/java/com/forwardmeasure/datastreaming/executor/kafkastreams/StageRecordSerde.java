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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Flat-JSON {@code Map<String,Object>} serde for stage records. Deliberately not fowf's own {@code
 * JacksonSerde} ({@code openworkflow-durable-processing-kafka-streams}) - see {@link
 * KafkaStreamsStageRunnerProvider}'s own javadoc for why that library's real, non-generic surface
 * (a command/aggregate kernel) doesn't fit a stateless map/filter stage. This one class is small
 * enough to own directly rather than pull in that dependency for it alone.
 */
final class StageRecordSerde implements Serde<Map<String, Object>> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};

  @Override
  public Serializer<Map<String, Object>> serializer() {
    return (topic, value) -> {
      if (value == null) {
        return null;
      }
      try {
        return MAPPER.writeValueAsBytes(value);
      } catch (Exception failure) {
        throw new SerializationException("Unable to serialize stage record", failure);
      }
    };
  }

  @Override
  public Deserializer<Map<String, Object>> deserializer() {
    return (topic, value) -> {
      if (value == null) {
        return null;
      }
      try {
        return MAPPER.readValue(value, TYPE);
      } catch (Exception failure) {
        throw new SerializationException("Unable to deserialize stage record", failure);
      }
    };
  }
}
