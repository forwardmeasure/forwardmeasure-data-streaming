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
import java.util.Map;

/**
 * Where and how to read a source: which connector ({@code file}, {@code jdbc}, {@code kafka},
 * {@code opensearch}, {@code grpc}, {@code object-storage}...), its URI, its format, the schema its
 * rows are described by, the query to run (JDBC-style connectors only - {@code null} for every
 * other connector), and a connector-specific {@code options} bag. See D5 for which connector module
 * each {@code connector} value resolves to at runtime.
 *
 * <p>{@code options} (added 2026-09-13) is deliberately a plain, open string-to-string map, not a
 * typed field per connector: both engines already have a real, generic, metadata-driven dispatch
 * mechanism for "what does this connector need beyond a URI" - Camel endpoint/component properties
 * on the Pekko side ({@code camel-kafka}'s {@code brokers}/{@code securityProtocol}, {@code
 * camel-sql}'s {@code dataSourceRef}, etc.) and Spark's own {@code DataFrameReader#options} on the
 * Spark side - so this field exists to carry exactly those key/value pairs from the spec document
 * into whichever mechanism the runner actually uses, without this module needing a Java field (or a
 * new record) per connector every time one more shows up. Never a place for a literal secret: a
 * value here that authenticates anything (a password, an API key, a bearer token) must be a
 * *reference* the runner resolves at startup (a secret name/key, a vault path) - never the secret
 * itself, since spec documents are ordinary YAML that gets checked in, logged, and passed around.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SourceSpec(
    @JsonProperty("connector") String connector,
    @JsonProperty("uri") String uri,
    @JsonProperty("format") FormatSpec format,
    @JsonProperty("schema") SchemaRef schema,
    @JsonProperty("query") String query,
    @JsonProperty("options") Map<String, String> options)
    implements Serializable {

  public SourceSpec {
    options = options == null ? Map.of() : Map.copyOf(options);
  }

  /** Pre-{@code options} shape, kept working unchanged for every caller that never needed one. */
  public SourceSpec(
      String connector, String uri, FormatSpec format, SchemaRef schema, String query) {
    this(connector, uri, format, schema, query, Map.of());
  }

  /** Pre-{@code query}/{@code options} shape, kept working unchanged for every existing caller. */
  public SourceSpec(String connector, String uri, FormatSpec format, SchemaRef schema) {
    this(connector, uri, format, schema, null, Map.of());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FormatSpec(@JsonProperty("type") String type) implements Serializable {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SchemaRef(@JsonProperty("ref") String ref) implements Serializable {}
}
