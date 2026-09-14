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
package com.forwardmeasure.datastreaming.executor.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Generalizes what fei's own {@code CorrelatedSourceIngestionWorker} already proves in production
 * (D4): reads each of several sources, maps every row via FDS's {@link FieldMappingEngine} instead
 * of hand-wired per-source Spark code, blocks (groups) mapped records across all sources on one
 * mapped field, and merges each blocked group by trust weight - the highest-trust record in a group
 * wins every field it actually resolved; a lower-trust record only fills a field the winner left
 * genuinely unset.
 *
 * <p>Deliberately stops at the merged {@code Map<String,Object>} - target-model coercion and sink
 * writing are the caller's own concern (D1: the merged map still needs binding into whatever real
 * typed target the caller uses - a Protobuf {@code DynamicMessage} today for fei's still-unmigrated
 * correlation path, or an openapi-generator-produced model class once that migration happens - the
 * same separation {@code PekkoIngestionRunner} already keeps on the Pekko side). {@link
 * FieldMappingEngine} and any supplemental transforms are constructed fresh inside each {@code
 * mapPartitions} closure, never captured from the driver - the same "no live object crosses the
 * wire" discipline this org's own real Spark code already established, extended here since {@code
 * FieldMappingEngine} itself isn't (and doesn't need to be) {@code Serializable}.
 */
public final class SparkCorrelationEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkCorrelationEngine.class);

  private SparkCorrelationEngine() {}

  /**
   * Reads and maps one source. {@code blockingField} names the mapped (target-side) field whose
   * value becomes the correlation key - a row that doesn't resolve it is skipped, matching fei's
   * own "no stable key, no correlation" rule.
   */
  public static JavaRDD<SparkCorrelationRecord> readAndMap(
      SparkSession spark,
      SparkSourceConfig sourceConfig,
      String blockingField,
      Map<String, NamedTransform> supplementalTransforms,
      MalformedRecordPolicy malformedRecordPolicy) {
    Dataset<Row> rawRows = readSource(spark, sourceConfig.source());
    var mapper = sourceConfig.mapper();
    String sourceKey = sourceConfig.sourceKey();
    double trustWeight = sourceConfig.effectiveTrustWeight();
    String connector = sourceConfig.source().connector();

    return rawRows
        .javaRDD()
        .mapPartitions(
            (FlatMapFunction<Iterator<Row>, SparkCorrelationRecord>)
                rowIterator -> {
                  FieldMappingEngine engine = new FieldMappingEngine();
                  ObjectMapper objectMapper = new ObjectMapper();
                  List<SparkCorrelationRecord> mappedRecords = new ArrayList<>();
                  while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Map<String, Object> mapped;
                    try {
                      mapped =
                          engine.map(
                              sourceRowFor(connector, row, objectMapper),
                              mapper,
                              supplementalTransforms);
                    } catch (RuntimeException mappingFailure) {
                      handleMalformedRow(sourceKey, mappingFailure, malformedRecordPolicy);
                      continue;
                    }
                    Object blockingValue = mapped.get(blockingField);
                    if (blockingValue == null) {
                      continue;
                    }
                    String blockingKey = normalizeBlockingKey(String.valueOf(blockingValue));
                    if (blockingKey.isBlank()) {
                      continue;
                    }
                    mappedRecords.add(
                        new SparkCorrelationRecord(blockingKey, sourceKey, trustWeight, mapped));
                  }
                  return mappedRecords.iterator();
                });
  }

  private static void handleMalformedRow(
      String sourceKey, RuntimeException mappingFailure, MalformedRecordPolicy policy) {
    switch (policy) {
      case FAIL -> throw mappingFailure;
      case DEAD_LETTER ->
          LOGGER.error(
              "SparkCorrelationEngine: source '{}' row dead-lettered", sourceKey, mappingFailure);
      case SKIP ->
          LOGGER.warn(
              "SparkCorrelationEngine: source '{}' row failed to map - skipping",
              sourceKey,
              mappingFailure);
    }
  }

  /**
   * Reads and maps one source with **no correlation step at all** - every mapped row is emitted
   * independently, never grouped or merged with any other row, even one that happens to share a
   * field value. This is the right tool for a genuinely single-source Spark run (no {@code
   * blockingField} parameter here at all, since there's nothing to correlate against) - {@link
   * #readAndMap}/{@link #correlate} are for matching the *same real-world entity across different
   * sources*, not for deduplicating rows within one. Using {@link #correlate} for a single source
   * would silently merge two unrelated rows that happen to share a blocking-key value into one
   * output record - correct entity-matching behavior across sources, a real correctness bug for
   * plain ingestion (confirmed by reading {@link #correlate}'s own {@code groupBy}, not assumed) -
   * exactly what {@code PekkoIngestionRunner} (the Pekko equivalent) never does, since it has no
   * grouping step of any kind.
   */
  public static JavaRDD<Map<String, Object>> readAndMapSingleSource(
      SparkSession spark,
      SourceSpec source,
      TransformSpec mapper,
      Map<String, NamedTransform> supplementalTransforms,
      MalformedRecordPolicy malformedRecordPolicy) {
    Dataset<Row> rawRows = readSource(spark, source);
    String connector = source.connector();
    return rawRows
        .javaRDD()
        .mapPartitions(
            (FlatMapFunction<Iterator<Row>, Map<String, Object>>)
                rowIterator -> {
                  FieldMappingEngine engine = new FieldMappingEngine();
                  ObjectMapper objectMapper = new ObjectMapper();
                  List<Map<String, Object>> mappedRows = new ArrayList<>();
                  while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    try {
                      mappedRows.add(
                          engine.map(
                              sourceRowFor(connector, row, objectMapper),
                              mapper,
                              supplementalTransforms));
                    } catch (RuntimeException mappingFailure) {
                      handleMalformedRow("(single source)", mappingFailure, malformedRecordPolicy);
                    }
                  }
                  return mappedRows.iterator();
                });
  }

  /**
   * Unions every source's mapped records, groups by blocking key, and merges each group by trust
   * weight. {@code sources} must not be empty.
   */
  public static JavaRDD<Map<String, Object>> correlate(
      List<JavaRDD<SparkCorrelationRecord>> sources) {
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("sources must not be empty");
    }
    JavaRDD<SparkCorrelationRecord> unioned = null;
    for (JavaRDD<SparkCorrelationRecord> mapped : sources) {
      unioned = unioned == null ? mapped : unioned.union(mapped);
    }

    JavaPairRDD<String, Iterable<SparkCorrelationRecord>> grouped =
        unioned.groupBy(SparkCorrelationRecord::blockingKey);

    return grouped.map(
        (Function<Tuple2<String, Iterable<SparkCorrelationRecord>>, Map<String, Object>>)
            pair -> mergeGroup(pair._2()));
  }

  private static Map<String, Object> mergeGroup(Iterable<SparkCorrelationRecord> group) {
    List<SparkCorrelationRecord> sorted = new ArrayList<>();
    group.forEach(sorted::add);
    sorted.sort(Comparator.comparingDouble(SparkCorrelationRecord::trustWeight).reversed());

    Map<String, Object> merged = new LinkedHashMap<>();
    for (SparkCorrelationRecord record : sorted) {
      for (Map.Entry<String, Object> field : record.mappedFields().entrySet()) {
        merged.putIfAbsent(field.getKey(), field.getValue());
      }
    }
    return merged;
  }

  /**
   * Reads {@code source} via Spark's own native, pluggable {@code DataFrameReader} format mechanism
   * - {@code source.connector()} (or, for {@code file}, {@code source.format().type()}) becomes the
   * Spark format name directly, and {@code source.options()} passes through with only {@code
   * user}/{@code password} resolved first via {@link SparkSecretOptions} (added 2026-09-13 - a
   * secret-reference value like {@code env:DB_PASSWORD} would otherwise reach Spark's own reader as
   * that literal string, which has no idea what to do with it) - not a hardcoded per-connector Java
   * branch (rewritten 2026-09-13; the previous version was a two-case {@code switch} that could
   * only ever support what it happened to have a case for, the exact anti-pattern {@code
   * CamelBridge}'s own URI-scheme dispatch on the Pekko side never had). Adding support for another
   * Spark-native format (parquet, orc, avro, delta...) needs no new Java here at all - only the
   * matching Spark data source artifact on this module's own classpath (already true for {@code
   * kafka} via {@code spark-sql-kafka-0-10}) and the right {@code options} in the spec document.
   *
   * <p>{@code file} and {@code jdbc} keep a small amount of glue beyond pure pass-through - not
   * because they need bespoke per-connector logic, but for two narrow, structural reasons: {@code
   * file} is the one connector where {@code source.uri()} is a *path* argument to {@code load(...)}
   * rather than an option (matching every other Spark file-based format), and {@code csv} defaults
   * {@code header=true} for backward compatibility with every existing spec that never set {@code
   * options} at all; {@code jdbc} defaults {@code driver}/{@code url}/{@code query} from this
   * record's own long-standing dedicated fields for the same reason, overridable via {@code
   * options}. Every other connector is pure metadata pass-through - zero glue.
   */
  private static Dataset<Row> readSource(SparkSession spark, SourceSpec source) {
    String connector = source.connector();
    Map<String, String> options = SparkSecretOptions.resolve(source.options());

    if ("file".equals(connector)) {
      String format = source.format() != null ? source.format().type() : "csv";
      if ("csv".equals(format)) {
        options.putIfAbsent("header", "true");
      }
      return spark.read().format(format).options(options).load(source.uri());
    }
    if ("jdbc".equals(connector)) {
      options.putIfAbsent("url", source.uri());
      options.putIfAbsent("driver", "org.postgresql.Driver");
      if (source.query() != null) {
        options.putIfAbsent("query", source.query());
      }
      return spark.read().format("jdbc").options(options).load();
    }
    return spark.read().format(connector).options(options).load();
  }

  /**
   * Wraps one raw Spark {@link Row} as a {@link SourceRow} - {@link SparkSourceRow} (plain
   * column-by-name lookup) for every connector except {@code kafka}, whose own raw DataFrame
   * columns are the Kafka envelope itself ({@code key}/{@code value}/{@code topic}/{@code
   * partition}/{@code offset}/{@code timestamp}), not the message's own business fields - {@code
   * value} (the message body, a JSON object per message, matching the Pekko kafka source's own body
   * shape) needs parsing into a {@link MapSourceRow} first. {@code objectMapper} is passed in
   * (constructed fresh once per partition by the caller), not created per row.
   */
  private static SourceRow sourceRowFor(String connector, Row row, ObjectMapper objectMapper) {
    if (!"kafka".equals(connector)) {
      return new SparkSourceRow(row);
    }
    byte[] value = (byte[]) row.getAs("value");
    try {
      Map<String, Object> parsed =
          objectMapper.readValue(value, new TypeReference<Map<String, Object>>() {});
      return new MapSourceRow(parsed);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String normalizeBlockingKey(String value) {
    return value == null ? "" : value.strip().toUpperCase(Locale.ROOT);
  }
}
