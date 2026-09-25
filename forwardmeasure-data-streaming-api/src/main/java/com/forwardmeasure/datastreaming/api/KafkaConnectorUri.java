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
package com.forwardmeasure.datastreaming.api;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Parses the one real {@code kafka:<topic>?brokers=<brokers>[&key=value...]} URI convention this
 * repo already uses for a {@code SourceSpec}/{@code SinkSpec} whose {@code connector} is {@code
 * kafka} (confirmed real, existing usage: {@code PekkoIngestionRunner#kafkaRowSource}, {@code
 * PekkoKafkaConnectorIntegrationTest}) - the same Camel-style shape, parsed here without Camel
 * itself, for the two continuous {@code DeliveryEngine} implementations (neither depends on {@code
 * -connector-camel}) to extract a real topic name/broker list from an {@link IngestionSpec}'s own
 * {@link SourcePlan#source()}/{@code destination}, so one spec resolves identically regardless of
 * which engine the planner picks for it.
 */
public record KafkaConnectorUri(String topic, String bootstrapServers) {

  private static final String SCHEME_PREFIX = "kafka:";

  public KafkaConnectorUri {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    if (topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    if (bootstrapServers.isBlank()) {
      throw new IllegalArgumentException("bootstrapServers must not be blank");
    }
  }

  public static KafkaConnectorUri parse(String uri) {
    Objects.requireNonNull(uri, "uri");
    if (!uri.startsWith(SCHEME_PREFIX)) {
      throw new IllegalArgumentException(
          "KafkaConnectorUri: expected a 'kafka:' URI, got '" + uri + "'");
    }
    String rest = uri.substring(SCHEME_PREFIX.length());
    int queryStart = rest.indexOf('?');
    String topic = queryStart < 0 ? rest : rest.substring(0, queryStart);
    String query = queryStart < 0 ? "" : rest.substring(queryStart + 1);

    String brokers = null;
    for (String pair : query.split("&")) {
      if (pair.isBlank()) {
        continue;
      }
      int eq = pair.indexOf('=');
      String key = eq < 0 ? pair : pair.substring(0, eq);
      String value =
          eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
      if ("brokers".equals(key)) {
        brokers = value;
      }
    }
    if (brokers == null) {
      throw new IllegalArgumentException(
          "KafkaConnectorUri: '" + uri + "' is missing its own required 'brokers' query parameter");
    }
    return new KafkaConnectorUri(topic, brokers);
  }
}
