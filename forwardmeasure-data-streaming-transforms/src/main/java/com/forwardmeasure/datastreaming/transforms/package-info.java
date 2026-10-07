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
 * The generic, domain-agnostic named-transform function pool (date/string/number formatting,
 * country-code resolution, and similar utilities) - plain static methods, registered by name into a
 * flat runtime {@code NamedTransformRegistry} and dispatched from a {@code mapper.fields[]} spec,
 * not MapStruct-qualified. Domain-specific transforms (e.g. tied to one consumer's source schema)
 * belong downstream, not here. MapStruct has no role in this module or in source/target payload
 * mapping generally - its one real place in this architecture is JPA entity-to-model conversion, in
 * {@code forwardmeasure-data-streaming-connector-jdbc}.
 */
package com.forwardmeasure.datastreaming.transforms;
