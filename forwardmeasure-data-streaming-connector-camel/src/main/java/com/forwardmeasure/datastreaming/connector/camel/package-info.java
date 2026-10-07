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
/**
 * Generic Camel route-building and {@code camel-reactive-streams} bridging, parameterized by
 * whatever endpoint URI an {@code IngestionSpec} declares. Depends on {@code camel-core} and {@code
 * camel-reactive-streams} only - which protocol component (file, Kafka, OpenSearch, or any other of
 * Camel's components) is actually on the classpath at runtime is the consuming application's own
 * decision, not something hardcoded per protocol here.
 */
package com.forwardmeasure.datastreaming.connector.camel;
