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
 * The only two engines that ever own final write-to-sink, in every cell of the capability matrix
 * including bounded ones (see the repo's own gap-bridging plan, "the 2x2x2 cube"). Spark is
 * deliberately not a member of this enum - it is an optional compute stage that always hands off to
 * one of these two, never a delivery engine itself. Never author-specified; the planner resolves
 * this from a transform graph's own declared characteristics.
 */
public enum DeliveryEngineKind {
  PEKKO_STREAMS,
  KAFKA_STREAMS
}
