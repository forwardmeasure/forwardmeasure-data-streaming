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

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.transforms.NamedTransform;
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
 * same separation {@code IngestionPipelineRunner} already keeps on the Pekko side). {@link
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
      Map<String, NamedTransform> supplementalTransforms) {
    Dataset<Row> rawRows = readSource(spark, sourceConfig.source());
    var mapper = sourceConfig.mapper();
    String sourceKey = sourceConfig.sourceKey();
    double trustWeight = sourceConfig.effectiveTrustWeight();

    return rawRows
        .javaRDD()
        .mapPartitions(
            (FlatMapFunction<Iterator<Row>, SparkCorrelationRecord>)
                rowIterator -> {
                  FieldMappingEngine engine = new FieldMappingEngine();
                  List<SparkCorrelationRecord> mappedRecords = new ArrayList<>();
                  while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    try {
                      Map<String, Object> mapped =
                          engine.map(new SparkSourceRow(row), mapper, supplementalTransforms);
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
                    } catch (RuntimeException mappingFailure) {
                      LOGGER.warn(
                          "SparkCorrelationEngine: source '{}' row failed to map - skipping",
                          sourceKey,
                          mappingFailure);
                    }
                  }
                  return mappedRecords.iterator();
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

  private static Dataset<Row> readSource(SparkSession spark, SourceSpec source) {
    return switch (source.connector()) {
      case "file" -> spark.read().option("header", "true").csv(source.uri());
      case "jdbc" ->
          spark
              .read()
              .format("jdbc")
              .option("url", source.uri())
              .option("query", source.query())
              .option("driver", "org.postgresql.Driver")
              .load();
      default ->
          throw new IllegalArgumentException(
              "unsupported source connector for the Spark executor: '"
                  + source.connector()
                  + "' (only file/jdbc - see D5)");
    };
  }

  private static String normalizeBlockingKey(String value) {
    return value == null ? "" : value.strip().toUpperCase(Locale.ROOT);
  }
}
