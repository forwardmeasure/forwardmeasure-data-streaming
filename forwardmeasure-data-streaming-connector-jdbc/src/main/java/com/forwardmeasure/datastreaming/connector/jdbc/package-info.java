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
 * JPA/JPQL via {@code forwardmeasure-jpa}, behind a Repository, for smaller/bounded/incremental
 * polling or lookup sources - not Camel (no camel-jdbc/camel-sql precedent anywhere in this org).
 * Exposes Repository types only; any orchestration/business logic belongs in a downstream
 * application-service layer that depends on this module, never the reverse.
 *
 * <p>This is the one module in this library where MapStruct belongs (D3, corrected): converting a
 * JPA {@code @Entity} to/from a generated OpenAPI model class is a fixed-type, compile-time
 * boundary, unlike the runtime-interpreted source/target payload mapping the rest of this library
 * builds on {@code NamedTransformRegistry} for instead.
 */
package com.forwardmeasure.datastreaming.connector.jdbc;
