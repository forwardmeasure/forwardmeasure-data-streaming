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
 * One axis of the capability matrix (see the repo's own gap-bridging plan, "the 2x2x2 cube"):
 * whether an {@code IngestionSpec} declares one source or more than one. Purely about source
 * topology - independent of which compute/delivery engine runs it. {@code CORRELATED} does not
 * imply Spark; a correlated pipeline can join natively in Kafka Streams with no Spark at all.
 */
public enum SourceCardinality {
  SINGLE,
  CORRELATED
}
