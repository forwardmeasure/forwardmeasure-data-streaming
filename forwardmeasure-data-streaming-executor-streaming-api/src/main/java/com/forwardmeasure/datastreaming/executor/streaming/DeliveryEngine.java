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
package com.forwardmeasure.datastreaming.executor.streaming;

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;

/**
 * The only two implementations of this are {@code PekkoStreamsDeliveryEngine} and {@code
 * KafkaStreamsDeliveryEngine} - see the repo's own gap-bridging plan, "the 2x2x2 cube": Pekko
 * Streams and Kafka Streams are the only delivery engines, used in every cell of the matrix
 * including bounded ones, each branching internally on {@link ExecutionMode} rather than being
 * split into separate bounded/continuous classes.
 *
 * <p>Supersedes the old {@code StreamingStageRunnerProvider} SPI (retired 2026-09-21 once {@code
 * KafkaStreamsStageRunnerProvider}/{@code PekkoStreamingStageRunnerProvider} - the two classes that
 * implemented it - were fully migrated onto this interface, relocated logic and all): that SPI
 * could not express {@code BOUNDED} mode at all (its {@code start(spec)} always ran forever) - this
 * was a real interface change, not a rename.
 */
public interface DeliveryEngine {
  ExecutionHandle execute(ExecutionPlan plan, ExecutionMode mode);
}
