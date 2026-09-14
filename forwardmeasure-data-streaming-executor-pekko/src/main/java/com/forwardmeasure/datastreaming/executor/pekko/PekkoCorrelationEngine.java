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

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pekko-backed counterpart to {@code SparkCorrelationEngine} (added 2026-09-13 for multi-source
 * correlation with {@code execution.engine() == "pekko"}, alongside Spark) - reads every source,
 * maps each row via {@link FieldMappingEngine}, groups by {@code blockingField}, and merges each
 * group by trust weight, with the exact same merge semantics (highest-trust record in a group wins
 * every field it resolved; a lower-trust record only fills what the winner left unset) - verified
 * by mirroring {@code SparkCorrelationEngine}'s own {@code mergeGroup} logic directly, so a
 * correlation run produces the same result on either engine.
 *
 * <p><b>Real, deliberate scale boundary, not a silent gap</b>: correlation genuinely needs every
 * source's rows before it can group/merge by blocking key (a row from source A might match a
 * later-arriving row from source B) - there is no way to bound memory below "every mapped row
 * across every source" the way a single-source pipeline can. This is exactly the case D4 reserves
 * for Spark's own real distributed shuffle at real scale; this engine is for the case where that's
 * genuine overkill (a bounded correlation run small enough to hold in one JVM's heap).
 */
public final class PekkoCorrelationEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoCorrelationEngine.class);

  private PekkoCorrelationEngine() {}

  /** One mapped source record, ready to group by blocking key and merge by trust weight. */
  public record PekkoCorrelationRecord(
      String blockingKey, String sourceKey, double trustWeight, Map<String, Object> mappedFields) {}

  /**
   * Reads and maps one source, blocking the calling thread until every row is read (the same "run
   * to completion" style {@code PekkoIngestionRunner#run} already uses). A row that doesn't resolve
   * {@code blockingField} is skipped unconditionally (there's no key to correlate on, not a
   * malformed-record situation); a row that fails to *map* at all is handled per {@code
   * malformedRecordPolicy} (see {@code IngestionPipeline}'s own javadoc for what each value means -
   * wired in 2026-09-13, replacing what used to be an unconditional skip-and-log here regardless of
   * {@code execution.failure().malformedRecord()}).
   */
  public static CompletionStage<List<PekkoCorrelationRecord>> readAndMap(
      CamelBridge bridge,
      ActorSystem system,
      SourceSpec source,
      TransformSpec mapper,
      String blockingField,
      String sourceKey,
      double trustWeight,
      Map<String, NamedTransform> supplementalTransforms,
      MalformedRecordPolicy malformedRecordPolicy) {
    FieldMappingEngine engine = new FieldMappingEngine();
    Source<SourceRow, ?> rows = PekkoIngestionRunner.rowSource(bridge, source);
    return rows.mapConcat(
            row -> {
              Map<String, Object> mapped;
              try {
                mapped = engine.map(row, mapper, supplementalTransforms);
              } catch (RuntimeException mappingFailure) {
                return handleMalformedRow(sourceKey, mappingFailure, malformedRecordPolicy);
              }
              Object blockingValue = mapped.get(blockingField);
              if (blockingValue == null) {
                return List.of();
              }
              String blockingKey = normalizeBlockingKey(String.valueOf(blockingValue));
              if (blockingKey.isBlank()) {
                return List.of();
              }
              return List.of(
                  new PekkoCorrelationRecord(blockingKey, sourceKey, trustWeight, mapped));
            })
        .runWith(Sink.seq(), system);
  }

  private static List<PekkoCorrelationRecord> handleMalformedRow(
      String sourceKey, RuntimeException mappingFailure, MalformedRecordPolicy policy) {
    switch (policy) {
      case FAIL -> throw mappingFailure;
      case DEAD_LETTER ->
          LOGGER.error(
              "PekkoCorrelationEngine: source '{}' row dead-lettered", sourceKey, mappingFailure);
      case SKIP ->
          LOGGER.warn(
              "PekkoCorrelationEngine: source '{}' row failed to map - skipping",
              sourceKey,
              mappingFailure);
    }
    return List.of();
  }

  /**
   * Unions every source's mapped records, groups by blocking key, and merges each group by trust
   * weight. {@code sources} must not be empty.
   */
  public static List<Map<String, Object>> correlate(List<List<PekkoCorrelationRecord>> sources) {
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("sources must not be empty");
    }
    Map<String, List<PekkoCorrelationRecord>> grouped = new LinkedHashMap<>();
    for (List<PekkoCorrelationRecord> source : sources) {
      for (PekkoCorrelationRecord record : source) {
        grouped.computeIfAbsent(record.blockingKey(), key -> new ArrayList<>()).add(record);
      }
    }
    List<Map<String, Object>> merged = new ArrayList<>();
    for (List<PekkoCorrelationRecord> group : grouped.values()) {
      merged.add(mergeGroup(group));
    }
    return merged;
  }

  private static Map<String, Object> mergeGroup(List<PekkoCorrelationRecord> group) {
    List<PekkoCorrelationRecord> sorted = new ArrayList<>(group);
    sorted.sort(Comparator.comparingDouble(PekkoCorrelationRecord::trustWeight).reversed());

    Map<String, Object> merged = new LinkedHashMap<>();
    for (PekkoCorrelationRecord record : sorted) {
      for (Map.Entry<String, Object> field : record.mappedFields().entrySet()) {
        merged.putIfAbsent(field.getKey(), field.getValue());
      }
    }
    return merged;
  }

  private static String normalizeBlockingKey(String value) {
    return value == null ? "" : value.strip().toUpperCase(Locale.ROOT);
  }
}
