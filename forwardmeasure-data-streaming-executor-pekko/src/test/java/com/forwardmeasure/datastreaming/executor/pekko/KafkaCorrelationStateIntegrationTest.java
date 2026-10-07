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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

@WithKafkaContainer
class KafkaCorrelationStateIntegrationTest {
  @Test
  void restoresBothSourcesAndRepairsAnUnacknowledgedSinkWithoutRegressingState(
      KafkaTestContainer kafka) {
    var plan = plan(kafka.bootstrapServers());
    String identity = "fds-state-test-" + UUID.randomUUID();
    var output = new AtomicReference<Map<String, Object>>();
    try (var state = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity)) {
      state.updateAndWrite(
          row("high", 10, "winner", "one"),
          merged -> {
            output.set(merged);
            return CompletableFuture.completedFuture(null);
          });
      assertThrows(
          IllegalStateException.class,
          () ->
              state.updateAndWrite(
                  row("low", 20, "loser", "two"),
                  merged ->
                      CompletableFuture.failedFuture(
                          new IllegalStateException("sink unavailable"))));
    }
    try (var restored = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity)) {
      // Input commit did not happen. Even an earlier replay must emit the restored latest merge.
      restored.updateAndWrite(
          row("low", 19, "obsolete", "old"),
          merged -> {
            output.set(merged);
            return CompletableFuture.completedFuture(null);
          });
      assertEquals("winner", output.get().get("status"));
      assertEquals(List.of("one", "two"), output.get().get("identifiers"));
    }
  }

  @Test
  void supersededWriterCannotChangeTheChangelog(KafkaTestContainer kafka) {
    var plan = plan(kafka.bootstrapServers());
    String identity = "fds-fence-test-" + UUID.randomUUID();
    try (var old = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity);
        var replacement = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity)) {
      assertThrows(
          IllegalStateException.class,
          () ->
              old.updateAndWrite(
                  row("high", 1, "old", "one"), merged -> CompletableFuture.completedFuture(null)));
      replacement.updateAndWrite(
          row("high", 2, "new", "two"),
          merged -> {
            assertEquals("new", merged.get("status"));
            return CompletableFuture.completedFuture(null);
          });
    }
  }

  @Test
  void topicAndPartitionChangesResetOffsetsButSerializationFailureCannotPoisonState(
      KafkaTestContainer kafka) {
    var plan = plan(kafka.bootstrapServers());
    String identity = "fds-offset-contract-" + UUID.randomUUID();
    var observed = new AtomicReference<Map<String, Object>>();
    java.util.function.Function<Map<String, Object>, java.util.concurrent.CompletionStage<Void>>
        sink =
            value -> {
              observed.set(value);
              return CompletableFuture.completedFuture(null);
            };
    try (var state = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity)) {
      state.updateAndWrite(row("high", 100, "original", "one"), sink);
      state.updateAndWrite(
          new KafkaCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 0, 0, Map.of("status", "new-topic")),
          sink);
      assertEquals("new-topic", observed.get().get("status"));
      state.updateAndWrite(
          new KafkaCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 0, Map.of("status", "new-partition")),
          sink);
      assertEquals("new-partition", observed.get().get("status"));
      assertThrows(
          IllegalStateException.class,
          () ->
              state.updateAndWrite(
                  new KafkaCorrelationState.Contribution(
                      "KEY", "high", "replacement-topic", 1, 1, Map.of("bad", new Object())),
                  sink));
      state.updateAndWrite(
          new KafkaCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 2, Map.of("status", "recovered")),
          sink);
      assertEquals(Map.of("status", "recovered"), observed.get());
    }
    try (var restored = new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity)) {
      restored.updateAndWrite(
          new KafkaCorrelationState.Contribution(
              "KEY", "high", "replacement-topic", 1, 1, Map.of("status", "obsolete")),
          sink);
      assertEquals(Map.of("status", "recovered"), observed.get());
    }
  }

  @Test
  void unsafeRetentionAndCorruptPersistedStateFailBeforeSourceConsumption(KafkaTestContainer kafka)
      throws Exception {
    var plan = plan(kafka.bootstrapServers());
    var config = new java.util.Properties();
    config.put(
        org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafka.bootstrapServers());
    for (String scenario : List.of("retention", "corrupt")) {
      String identity = "fds-invalid-state-" + UUID.randomUUID();
      String topic = identity + "-state-v1";
      try (var admin = org.apache.kafka.clients.admin.Admin.create(config)) {
        admin
            .createTopics(
                List.of(
                    new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1)
                        .configs(
                            Map.of(
                                "cleanup.policy",
                                scenario.equals("retention") ? "delete" : "compact"))))
            .all()
            .get(10, java.util.concurrent.TimeUnit.SECONDS);
      }
      if (scenario.equals("corrupt")) {
        config.put(
            org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(
            org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
        try (var producer =
            new org.apache.kafka.clients.producer.KafkaProducer<String, String>(config)) {
          producer
              .send(
                  new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "bad", "not-json"))
              .get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
      }
      var failure =
          assertThrows(
              IllegalStateException.class,
              () -> new KafkaCorrelationState(plan, kafka.bootstrapServers(), identity));
      org.junit.jupiter.api.Assertions.assertTrue(
          failure
              .getMessage()
              .contains(
                  scenario.equals("retention")
                      ? "Cannot prepare correlation changelog"
                      : "Invalid correlation changelog"));
    }
  }

  private static KafkaCorrelationState.Contribution row(
      String source, long offset, String status, String identifier) {
    return new KafkaCorrelationState.Contribution(
        "KEY",
        source,
        source + "-topic",
        0,
        offset,
        Map.of("status", status, "identifiers", List.of(identifier)));
  }

  private static ExecutionPlan plan(String brokers) {
    var mapper =
        new TransformSpec(
            "party",
            List.of(new TransformSpec.FieldRule("key", "key", null, null, null, null, null)));
    return ExecutionPlanCompiler.compile(
        new IngestionSpec(
            List.of(
                new SourcePlan(
                    "high",
                    new SourceSpec("kafka", "kafka:high?brokers=" + brokers, null, null),
                    mapper,
                    1),
                new SourcePlan(
                    "low",
                    new SourceSpec("kafka", "kafka:low?brokers=" + brokers, null, null),
                    mapper,
                    .5)),
            "key",
            null,
            new SinkSpec("opensearch", "http://unused", "test", null, null),
            ExecutionMode.CONTINUOUS,
            null,
            null));
  }
}
