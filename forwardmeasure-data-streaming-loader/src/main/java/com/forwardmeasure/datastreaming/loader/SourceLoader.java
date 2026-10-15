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
package com.forwardmeasure.datastreaming.loader;

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.LoaderHandoffSpecs;
import com.forwardmeasure.datastreaming.sourcereaders.BoundedRowReader;
import com.forwardmeasure.datastreaming.sourcereaders.TextContent;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The bounded-load first stage on a Kafka Streams installation: reads each non-Kafka source of a
 * spec and writes its rows, unmapped, to {@link LoaderHandoffSpecs#sourceTopic}. The Kafka Streams
 * delivery stage ({@link LoaderHandoffSpecs#deliveryStageSpec}) maps, correlates and delivers.
 *
 * <p>As a Job: {@code java -jar loader.jar <spec path>} with {@code KAFKA_BOOTSTRAP_SERVERS} and
 * {@value #LOAD_TOPIC_ENV} set by the launcher.
 */
public final class SourceLoader {

  public static final String LOAD_TOPIC_ENV = "FDS_LOAD_TOPIC";
  public static final String KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);

  private SourceLoader() {}

  /** Loads every non-Kafka source of {@code spec}, returning rows written per source key. */
  public static Map<String, Long> load(
      IngestionSpec spec, String bootstrapServers, String loadTopic) throws Exception {
    Map<String, Long> written = new LinkedHashMap<>();
    try (CamelBridge bridge = new CamelBridge();
        KafkaRowWriter writer = new KafkaRowWriter(bootstrapServers)) {
      for (int index = 0; index < spec.sources().size(); index++) {
        SourcePlan source = spec.sources().get(index);
        if (!LoaderHandoffSpecs.needsLoading(source)) {
          continue;
        }
        String topic = LoaderHandoffSpecs.sourceTopic(loadTopic, index);
        writer.emptyTopic(topic);
        long count =
            BoundedRowReader.forEach(bridge, source.source(), row -> writer.write(topic, row));
        writer.flush();
        written.put(source.sourceKey(), count);
        LOGGER.info("source.loaded source={} topic={} rows={}", source.sourceKey(), topic, count);
      }
    }
    return written;
  }

  /**
   * Loads caller-opened text {@code content}, parsed as {@code source} declares, to {@code topic}.
   * For a product that opens its own content, such as one pinned object-storage version.
   */
  public static long load(
      TextContent content, SourceSpec source, String bootstrapServers, String topic)
      throws Exception {
    try (KafkaRowWriter writer = new KafkaRowWriter(bootstrapServers)) {
      writer.emptyTopic(topic);
      long count = BoundedRowReader.forEach(content, source, row -> writer.write(topic, row));
      writer.flush();
      LOGGER.info("source.loaded topic={} rows={}", topic, count);
      return count;
    }
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));
    try {
      load(spec, requiredEnv(KAFKA_BOOTSTRAP_SERVERS_ENV), requiredEnv(LOAD_TOPIC_ENV));
    } catch (Exception failure) {
      LOGGER.error("SourceLoader: load failed", failure);
      System.exit(1);
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
