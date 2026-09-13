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
package com.forwardmeasure.datastreaming.executor.spark;

import java.io.Serializable;
import java.util.Map;

/**
 * One mapped source record, ready to shuffle and group by {@code blockingKey}. Generalizes fei's
 * own {@code CorrelationRecord}: {@code mappedFields} is FDS's schema-agnostic {@code
 * Map<String,Object>} (D1, corrected) rather than a Protobuf {@code DynamicMessage} serialized to
 * {@code byte[]} - a plain {@code Map<String,Object>} of standard JDK value types (the only values
 * {@link com.forwardmeasure.datastreaming.mappers.FieldMappingEngine} ever produces: {@code
 * String}, {@code List<String>}, {@code List<Map<String,Object>>}) is itself safely {@link
 * Serializable} across a Spark shuffle boundary - no {@code byte[]}/descriptor-reconstruction dance
 * needed the way a Protobuf message required.
 */
public record SparkCorrelationRecord(
    String blockingKey, String sourceKey, double trustWeight, Map<String, Object> mappedFields)
    implements Serializable {}
