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
package com.forwardmeasure.datastreaming.mappers;

import java.util.Map;

/**
 * Writes one mapped row directly to a real sink - the terminal step both continuous {@code
 * DeliveryEngine} implementations use (see the repo's own gap-bridging plan, Phase C: neither
 * continuous path wrote to a real sink before this, only topic-in/topic-out). Deliberately
 * blocking/synchronous, not {@code CompletionStage}-returning: Kafka Streams' own {@code
 * KStream#foreach} is inherently blocking-per-record, and a caller wanting async behavior (e.g.
 * Pekko Streams' {@code mapAsync}) can trivially wrap a call in {@code
 * CompletableFuture.supplyAsync} - the reverse (making a genuinely blocking API look synchronous)
 * is not possible, so blocking is the more broadly reusable shape.
 *
 * <p>Deliberately carries no retry policy of its own - {@code write} either succeeds or throws.
 * Retry (see {@code IngestionPipeline.withSinkFailureHandlingBlocking} in {@code
 * forwardmeasure-data-streaming-core}) is the caller's job: this module does not depend on {@code
 * -core} (which itself depends on this module, for {@link TransformCharacteristicsRegistry}), so a
 * retry policy can't live here without a circular dependency.
 */
public interface SinkRowWriter extends AutoCloseable {

  void write(Map<String, Object> row);

  @Override
  void close();
}
