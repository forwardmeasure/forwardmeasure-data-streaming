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
import com.forwardmeasure.datastreaming.api.ExecutionIdentity;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.KafkaConnectorUri;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.core.SparkHandoffSpecs;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded microbatches of continuous Kafka input, mapped by Spark and handed back to a delivery
 * engine. The handoff and input offsets commit in one Kafka transaction. A failed/uncertain commit
 * terminates this process; its replacement uses the broker's committed offsets and producer
 * fencing. Correlation stays in the delivery engine, preserving contributions across microbatch
 * boundaries.
 */
public final class ContinuousSparkStageRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousSparkStageRunner.class);

  private ContinuousSparkStageRunner() {}

  public static void run(SparkSession spark, ExecutionPlan plan, String brokers) throws Exception {
    run(spark, plan, brokers, new AtomicBoolean());
  }

  static void run(SparkSession spark, ExecutionPlan plan, String brokers, AtomicBoolean stopping)
      throws Exception {
    List<KafkaConnectorUri> sources =
        plan.sources().stream()
            .map(
                source -> {
                  if (!"kafka".equals(source.source().connector())) {
                    throw new IllegalArgumentException("Continuous Spark requires Kafka sources");
                  }
                  KafkaConnectorUri uri = KafkaConnectorUri.parse(source.source().uri());
                  if (!brokers.equals(uri.bootstrapServers())) {
                    throw new IllegalArgumentException(
                        "Continuous Spark sources and handoff must share a Kafka cluster");
                  }
                  return uri;
                })
            .toList();
    String identity = ExecutionIdentity.of(plan, "fds-spark-", System.getenv("FDS_EXECUTION_ID"));
    Properties input = new Properties();
    input.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    input.put(ConsumerConfig.GROUP_ID_CONFIG, identity);
    input.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    input.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    input.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    input.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
    input.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 900_000);
    input.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    input.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Properties output = new Properties();
    output.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    output.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, identity);
    output.put(ProducerConfig.ACKS_CONFIG, "all");
    output.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    output.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (Admin admin = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
      for (int index = 0; index < sources.size(); index++) {
        String topic = SparkHandoffSpecs.continuousTopic(plan, index);
        try {
          // One ordered contribution log per source; use the broker's replication-factor policy.
          admin
              .createTopics(List.of(new NewTopic(topic, 1, (short) -1)))
              .all()
              .get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
          if (!(failure.getCause() instanceof TopicExistsException)) throw failure;
        }
      }
    }
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(input);
        KafkaProducer<String, String> producer = new KafkaProducer<>(output)) {
      producer.initTransactions();
      consumer.subscribe(sources.stream().map(KafkaConnectorUri::topic).distinct().toList());
      Thread hook =
          new Thread(
              () -> {
                stopping.set(true);
                consumer.wakeup();
              },
              "fds-spark-stop");
      Runtime.getRuntime().addShutdownHook(hook);
      try {
        JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
        ObjectMapper json = new ObjectMapper();
        while (!stopping.get()) {
          var records = consumer.poll(Duration.ofSeconds(1));
          if (records.isEmpty()) continue;
          List<ProducerRecord<String, String>> handoff = new ArrayList<>();
          for (int index = 0; index < sources.size(); index++) {
            String topic = SparkHandoffSpecs.continuousTopic(plan, index);
            List<String> values = new ArrayList<>();
            records
                .records(sources.get(index).topic())
                .forEach(record -> values.add(record.value()));
            if (values.isEmpty()) continue;
            SourcePlan source = plan.sources().get(index);
            var mapperSpec = source.mapper();
            var malformed = IngestionPipeline.MalformedRecordPolicy.from(plan.errors());
            // collect() is bounded by max.poll.records, and keeps source order across partitions.
            List<Map<String, Object>> mapped =
                context
                    .parallelize(values)
                    .mapPartitions(
                        iterator -> {
                          ObjectMapper parser = new ObjectMapper();
                          FieldMappingEngine mapper = new FieldMappingEngine();
                          List<Map<String, Object>> result = new ArrayList<>();
                          while (iterator.hasNext()) {
                            String value = iterator.next();
                            try {
                              Map<String, Object> row =
                                  parser.readValue(value, new TypeReference<>() {});
                              result.add(mapper.map(new MapSourceRow(row), mapperSpec));
                            } catch (Exception failure) {
                              if (malformed != IngestionPipeline.MalformedRecordPolicy.SKIP)
                                throw failure;
                              LOGGER.warn("Skipping malformed continuous Spark input", failure);
                            }
                          }
                          return result.iterator();
                        })
                    .collect();
            for (Map<String, Object> row : mapped) {
              Object key = plan.blockingField() == null ? null : row.get(plan.blockingField());
              handoff.add(
                  new ProducerRecord<>(
                      topic, key == null ? null : key.toString(), json.writeValueAsString(row)));
            }
          }
          Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
          for (TopicPartition partition : records.partitions()) {
            var batch = records.records(partition);
            offsets.put(partition, new OffsetAndMetadata(batch.getLast().offset() + 1));
          }
          producer.beginTransaction();
          try {
            for (var record : handoff) producer.send(record).get();
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
          } catch (Exception failure) {
            try {
              producer.abortTransaction();
            } catch (Exception abort) {
              failure.addSuppressed(abort);
            }
            throw failure;
          }
        }
      } catch (WakeupException failure) {
        if (!stopping.get()) throw failure;
      } finally {
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException shutdown) {
          /* JVM shutdown already owns the hook. */
        }
      }
    }
  }
}
