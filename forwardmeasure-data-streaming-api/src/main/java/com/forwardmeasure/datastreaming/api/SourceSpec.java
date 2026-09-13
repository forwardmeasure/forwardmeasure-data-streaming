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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * Where and how to read a source: which connector ({@code file}, {@code jdbc}, {@code kafka},
 * {@code object-storage}), its URI, its format, the schema its rows are described by, and (for
 * {@code jdbc} sources read via the Spark executor's own JDBC reader, D5) the query to run - {@code
 * null} for every other connector. See D5 for which connector module each {@code connector} value
 * resolves to at runtime.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SourceSpec(
    @JsonProperty("connector") String connector,
    @JsonProperty("uri") String uri,
    @JsonProperty("format") FormatSpec format,
    @JsonProperty("schema") SchemaRef schema,
    @JsonProperty("query") String query)
    implements Serializable {

  public SourceSpec(String connector, String uri, FormatSpec format, SchemaRef schema) {
    this(connector, uri, format, schema, null);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FormatSpec(@JsonProperty("type") String type) implements Serializable {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SchemaRef(@JsonProperty("ref") String ref) implements Serializable {}
}
