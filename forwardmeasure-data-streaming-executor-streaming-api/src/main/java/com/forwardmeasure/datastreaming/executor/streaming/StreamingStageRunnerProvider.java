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

import com.forwardmeasure.datastreaming.api.StreamingStageSpec;

/**
 * Common boundary for running one {@link StreamingStageSpec} continuously, implemented
 * independently by each execution engine - mirrors fowf's own {@code ExecutionEngineProvider} shape
 * (identity + a start/observe boundary), not its exact command/acknowledgement semantics: fowf's
 * engines process discrete commands against workflow executions that reach a terminal state; a
 * streaming stage runs forever once started (see this repo's own gap-bridging plan's
 * bootstrap-vs-reconciliation distinction), so this SPI's own shape is start (returns a live
 * handle), not submit (returns a completion).
 *
 * <p>Kafka Streams is the first real implementation. A Pekko-based implementation is planned as a
 * second, swap-in provider later - this interface is the seam that makes that possible without
 * changing any caller.
 */
public interface StreamingStageRunnerProvider {
  StreamingEngineId engineId();

  StreamingStageHandle start(StreamingStageSpec spec);
}
