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

/**
 * One continuous processing stage: a Kafka topic in, one {@link TransformSpec}-shaped mapping, an
 * optional {@link FilterSpec}, a Kafka topic out, and which execution engine runs it. Added
 * 2026-09-17 as FDS's analogue of a single Numaflow {@code Vertex} - deliberately not a DAG/graph
 * spec itself (one {@code StreamingStageSpec} describes exactly one stage; a fowf {@code
 * WorkflowPlan} bootstrap wires several stages' topics together - see this repo's own continuous-
 * streaming gap-bridging plan) - and deliberately not built on {@link SourceSpec}/{@link SinkSpec}:
 * those model arbitrary connectors (file/jdbc/opensearch/...) for {@link IngestionSpec}'s bounded,
 * one-shot runs, where "the topic (or table, or path) is the ISB" isn't true. Here the Kafka topic
 * *is* the inter-stage buffer, so the topic name is the whole contract - a generic connector
 * abstraction on top of it would be pure indirection.
 *
 * <p>{@code filter} is a consumer-side predicate, not a producer-side branch: a stage that needs to
 * route only some records downstream (Numaflow's conditional edges) declares its own {@code filter}
 * and reads the same upstream output topic every other downstream stage reads, dropping
 * non-matching records before running its own mapping - rather than having the *producing* stage
 * branch to per-consumer topics. Simpler than adding an edge/condition grammar for one predicate.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamingStageSpec(
    @JsonProperty("name") String name,
    @JsonProperty("inputTopic") String inputTopic,
    @JsonProperty("mapper") TransformSpec mapper,
    @JsonProperty("filter") FilterSpec filter,
    @JsonProperty("outputTopic") String outputTopic,
    @JsonProperty("execution") StreamingExecutionSpec execution)
    implements Serializable {

  /** One consumer-side predicate: keep a record only when {@code field} equals this value. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record FilterSpec(
      @JsonProperty("field") String field, @JsonProperty("equals") String equals)
      implements Serializable {}

  /**
   * Deliberately separate from {@link ExecutionSpec}: that type's {@code engine} is scoped to
   * {@code pekko}/{@code spark} for bounded, one-shot runs (see its own javadoc) and carries
   * concurrency/flow-control/failure fields meaningful for a run that terminates - none of which
   * describe a continuous Kafka Streams stage. {@code kafka-streams} is the first real value here;
   * a {@code pekko} provider is planned as a second, swap-in implementation later.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record StreamingExecutionSpec(@JsonProperty("engine") String engine)
      implements Serializable {}

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory()).registerModule(new JavaTimeModule());

  public static StreamingStageSpec load(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return load(in);
    }
  }

  public static StreamingStageSpec load(InputStream in) throws IOException {
    return YAML.readValue(in, StreamingStageSpec.class);
  }

  public static StreamingStageSpec parseYaml(String yaml) {
    try {
      return YAML.readValue(yaml, StreamingStageSpec.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid StreamingStageSpec YAML", e);
    }
  }

  public String toYaml() throws IOException {
    return YAML.writeValueAsString(this);
  }
}
