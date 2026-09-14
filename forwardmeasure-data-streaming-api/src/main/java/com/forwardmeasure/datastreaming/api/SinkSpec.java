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
import java.time.Duration;
import java.util.Map;

/**
 * Where and how to write mapped records: which connector ({@code opensearch}, {@code kafka}, {@code
 * jdbc}, {@code file}, {@code object-storage}), the real connection URI a Camel sink builder (e.g.
 * {@code forwardmeasure-data-streaming-executor-pekko}'s own {@code CamelBridge#sink} usage - not
 * linkable from here, since this module has no dependency on {@code -connector-camel}) builds a
 * producer endpoint from (added 2026-09-13 - mirrors {@link SourceSpec#uri()} exactly; before this
 * field existed, nothing could actually build a real Camel sink from a {@code SinkSpec} at all, for
 * any connector - {@code index}/{@code schema}/{@code batching} alone were never enough), the
 * target identifier (index/topic/table/path - a single one; a {@code kafka} sink that fans a record
 * out to *several* topics expresses that as an {@code options} entry, e.g. {@code additionalTopics:
 * "topic-a,topic-b"}, not by overloading this field), the schema it conforms to, batching behavior,
 * and a connector-specific {@code options} bag (same "generic metadata over a bespoke Java field
 * per connector" rationale as {@link SourceSpec#options()} - see that field's own javadoc). See D5
 * for which connector module each {@code connector} value resolves to at runtime.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SinkSpec(
    @JsonProperty("connector") String connector,
    @JsonProperty("uri") String uri,
    @JsonProperty("index") String index,
    @JsonProperty("schema") SourceSpec.SchemaRef schema,
    @JsonProperty("batching") BatchingSpec batching,
    @JsonProperty("options") Map<String, String> options)
    implements Serializable {

  public SinkSpec {
    options = options == null ? Map.of() : Map.copyOf(options);
  }

  /** Pre-{@code options} shape, kept working unchanged for every caller that never needed one. */
  public SinkSpec(
      String connector,
      String uri,
      String index,
      SourceSpec.SchemaRef schema,
      BatchingSpec batching) {
    this(connector, uri, index, schema, batching, Map.of());
  }

  /** Pre-{@code uri}/{@code options} shape, kept working unchanged for existing callers. */
  public SinkSpec(
      String connector, String index, SourceSpec.SchemaRef schema, BatchingSpec batching) {
    this(connector, null, index, schema, batching, Map.of());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record BatchingSpec(
      @JsonProperty("maxRecords") Integer maxRecords, @JsonProperty("maxWait") Duration maxWait)
      implements Serializable {}
}
