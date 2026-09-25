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

/**
 * Delivery-engine-agnostic execution tuning for one {@link ExecutionPlan}. {@code exactlyOnce=true}
 * maps to {@code KafkaStreamsDeliveryEngine}'s {@code EXACTLY_ONCE_V2} default (already real) or
 * {@code PekkoStreamsDeliveryEngine}'s Kafka-transactional source/sink (already real) - see the
 * repo's own gap-bridging plan for why both engines default to real exactly-once rather than a
 * weaker one being the default for either. {@code concurrency}/{@code flowControl} are the old
 * {@code ExecutionSpec}'s own fields, promoted here unchanged - real, load-bearing settings {@code
 * IngestionPipeline.run}/{@code effectiveParallelism} already consume, not new policy (see {@link
 * ConcurrencySpec}'s own javadoc for why they moved).
 */
public record DeliverySemantics(
    boolean exactlyOnce, ConcurrencySpec concurrency, FlowControlSpec flowControl) {

  public static DeliverySemantics defaults() {
    return new DeliverySemantics(true, new ConcurrencySpec(1, null), null);
  }
}
