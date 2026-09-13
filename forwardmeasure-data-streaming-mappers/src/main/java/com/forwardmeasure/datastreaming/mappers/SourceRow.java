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
package com.forwardmeasure.datastreaming.mappers;

/**
 * One raw source record's field accessor, by column/field name. Ported verbatim from {@code
 * forwardmeasure-entity-intelligence}'s own {@code SourceRow} (D2: its shape was never the problem
 * - keep it as-is). {@code org.apache.commons.csv.CSVRecord} satisfies this directly; a future
 * Spark-backed correlation-path source would satisfy it too, without this engine ever depending on
 * Spark or Commons CSV.
 */
@FunctionalInterface
public interface SourceRow {

  /** The raw string value of {@code fieldName}, or {@code null} if absent/blank. */
  String get(String fieldName);
}
