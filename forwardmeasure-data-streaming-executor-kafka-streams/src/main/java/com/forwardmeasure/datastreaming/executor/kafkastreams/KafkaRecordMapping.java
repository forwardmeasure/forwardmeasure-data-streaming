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
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Applies the ingestion error contract before Kafka Streams can acknowledge a source record. */
final class KafkaRecordMapping {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordMapping.class);

  private KafkaRecordMapping() {}

  static Map<String, Object> map(
      String value,
      TransformSpec specification,
      FieldMappingEngine mapper,
      MalformedRecordPolicy policy,
      String blockingField) {
    try {
      Map<String, Object> raw = JSON.readValue(value, TYPE);
      var mapped = mapper.map(new MapSourceRow(raw), specification);
      if (blockingField != null) {
        Object key = mapped.get(blockingField);
        if (key == null || key.toString().isBlank())
          throw new IllegalArgumentException("Missing or blank correlation key");
      }
      return mapped;
    } catch (Exception failure) {
      return switch (policy) {
        case FAIL -> {
          if (failure instanceof RuntimeException runtime) throw runtime;
          throw new IllegalArgumentException("Unable to parse or map record", failure);
        }
        case SKIP -> {
          LOGGER.warn("Kafka source record skipped: {}", failure.getClass().getSimpleName());
          yield null;
        }
        case DEAD_LETTER -> {
          LOGGER.error("Kafka source record dead-lettered: {}", failure.getClass().getSimpleName());
          yield null;
        }
      };
    }
  }
}
