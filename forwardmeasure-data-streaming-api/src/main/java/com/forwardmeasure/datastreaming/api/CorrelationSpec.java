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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The top-level, framework-agnostic multi-source correlation contract - {@link IngestionSpec}'s
 * counterpart for the Spark path, added 2026-09-13 once the launcher work surfaced that no such
 * spec type existed: {@code IngestionSpec} models exactly one source, which is correct for the
 * Pekko path but cannot express what {@code forwardmeasure-data-streaming-executor-spark}'s own
 * {@code SparkCorrelationEngine} actually needs - several sources, each independently mapped, a
 * shared blocking key to correlate them on, and a trust weight per source for the merge step
 * (mirrors {@code SparkCorrelationEngine}'s own {@code SparkSourceConfig}, promoted here into a
 * real, loadable, standalone top-level contract the way {@code IngestionSpec} already is for the
 * single-source case).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CorrelationSpec(
    @JsonProperty("sources") List<SourceEntry> sources,
    @JsonProperty("blockingField") String blockingField,
    @JsonProperty("sink") SinkSpec sink,
    @JsonProperty("execution") ExecutionSpec execution)
    implements Serializable {

  public CorrelationSpec {
    sources = sources == null ? List.of() : List.copyOf(sources);
  }

  /**
   * One source to correlate: a {@link SourceSpec}/{@link TransformSpec} pair plus this source's
   * {@code trustWeight} for the merge step - field-for-field the same shape as {@code
   * SparkCorrelationEngine}'s own {@code SparkSourceConfig}, just declared here so it's part of
   * this library's own loadable spec contract rather than the executor module's internal type.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SourceEntry(
      @JsonProperty("sourceKey") String sourceKey,
      @JsonProperty("source") SourceSpec source,
      @JsonProperty("mapper") TransformSpec mapper,
      @JsonProperty("trustWeight") double trustWeight)
      implements Serializable {}

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory()).registerModule(new JavaTimeModule());

  public static CorrelationSpec load(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return load(in);
    }
  }

  public static CorrelationSpec load(InputStream in) throws IOException {
    return YAML.readValue(in, CorrelationSpec.class);
  }

  public static CorrelationSpec parseYaml(String yaml) {
    try {
      return YAML.readValue(yaml, CorrelationSpec.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid CorrelationSpec YAML", e);
    }
  }

  public String toYaml() throws IOException {
    return YAML.writeValueAsString(this);
  }
}
