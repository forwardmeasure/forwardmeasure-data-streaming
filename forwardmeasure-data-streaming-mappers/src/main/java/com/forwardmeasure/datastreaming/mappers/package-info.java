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
 * The runtime field-mapping engine: {@link
 * com.forwardmeasure.datastreaming.mappers.FieldMappingEngine} walks a {@code TransformSpec}/{@code
 * FieldRule} list and dispatches each field through {@code
 * forwardmeasure-data-streaming-transforms}' {@code NamedTransformRegistry} - ported directly from
 * {@code forwardmeasure-entity-intelligence}'s own {@code GenericRecordMapper}, minus its
 * Protobuf-specific coercion step (D1's separate, explicitly-deferred concern). Not MapStruct: this
 * mapping is metadata-driven, interpreted at runtime from a spec, not generated at compile time
 * (D3, corrected).
 */
package com.forwardmeasure.datastreaming.mappers;
