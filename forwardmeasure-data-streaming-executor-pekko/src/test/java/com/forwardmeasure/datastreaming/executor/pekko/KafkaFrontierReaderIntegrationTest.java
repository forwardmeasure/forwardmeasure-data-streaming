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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;

/**
 * A bounded Pekko run over a Kafka source with no message count - a Spark stage's handoff topic -
 * reads exactly the records present when it starts, across partitions, and then completes.
 */
@WithKafkaContainer
class KafkaFrontierReaderIntegrationTest {

  @Test
  void readsEveryRecordUpToTheFrontierCapturedAtStartThenEnds(KafkaTestContainer kafka)
      throws Exception {
    String topic = "fds-frontier-" + UUID.randomUUID();
    String brokers = kafka.bootstrapServers();
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
      admin
          .createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of("bootstrap.servers", brokers), new StringSerializer(), new StringSerializer())) {
      for (int i = 0; i < 3; i++)
        producer.send(new ProducerRecord<>(topic, i % 2, null, "{\"n\":" + i + "}")).get();
      try (KafkaFrontierReader reader = new KafkaFrontierReader(brokers, topic)) {
        producer.send(new ProducerRecord<>(topic, 0, null, "{\"n\":99}")).get();
        List<String> read = new ArrayList<>();
        for (String value = reader.next(); value != null; value = reader.next()) read.add(value);
        assertEquals(3, read.size(), read.toString());
        assertNull(reader.next());
      }
    }

    ActorSystem system =
        ActorSystem.create("frontier-" + UUID.randomUUID().toString().substring(0, 8));
    try (var bridge = new com.forwardmeasure.datastreaming.connector.camel.CamelBridge()) {
      var rows =
          PekkoIngestionRunner.rowSource(
                  bridge,
                  new SourceSpec(
                      "kafka",
                      "kafka:" + topic + "?brokers=" + brokers,
                      null,
                      null,
                      null,
                      Map.of()))
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(60, TimeUnit.SECONDS);
      assertEquals(4, rows.size(), "everything present when this second run started");
    } finally {
      system.terminate();
    }
  }
}
