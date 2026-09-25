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
import java.util.Objects;

/**
 * The single, author-facing declarative ingestion spec - replaces the old {@code IngestionSpec}/
 * {@code CorrelationSpec}/{@code ExecutionSpec}/{@code StreamingStageSpec} as four types with one
 * (see the repo's own gap-bridging plan, "a single IngestionSpec, not four types"). {@code
 * sources.size()==1} means single-source; {@code >1} means correlated, merged on {@code
 * blockingField} - one list serves both cardinalities, there is no separate correlation type.
 *
 * <p>{@code engine}/{@code deliveryEngine} are deliberately absent: which {@link
 * DeliveryEngineKind} runs this plan (and whether a {@link SparkStagePlan} is inserted) is resolved
 * automatically by the planner from the transform graph's own declared {@link
 * TransformCharacteristics} - the author never names an engine here, only sources, transforms, and
 * a sink. {@link #executionMode} is the one thing that stays author-declared, since it's a
 * deployment-intent decision (bounded vs. continuous), not something inferable from transform
 * shape.
 *
 * <p>{@code transforms} is nullable - the simplest pipelines need nothing beyond each source's own
 * {@link SourcePlan#mapper()} field mapping, with no further stage graph before the sink.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestionSpec(
    @JsonProperty("sources") List<SourcePlan> sources,
    @JsonProperty("blockingField") String blockingField,
    @JsonProperty("transforms") TransformGraph transforms,
    @JsonProperty("sink") SinkSpec sink,
    @JsonProperty("executionMode") ExecutionMode executionMode,
    @JsonProperty("delivery") DeliverySemantics delivery,
    @JsonProperty("errors") ErrorPolicy errors)
    implements Serializable {

  public IngestionSpec {
    sources = sources == null ? List.of() : List.copyOf(sources);
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("An IngestionSpec must have at least one source");
    }
    Objects.requireNonNull(sink, "sink");
    Objects.requireNonNull(executionMode, "executionMode");
    delivery = delivery == null ? DeliverySemantics.defaults() : delivery;
  }

  /** {@code sources.size() > 1} - a correlated spec needs a real {@code blockingField}. */
  public SourceCardinality sourceCardinality() {
    return sources.size() > 1 ? SourceCardinality.CORRELATED : SourceCardinality.SINGLE;
  }

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory()).registerModule(new JavaTimeModule());

  public static IngestionSpec load(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return load(in);
    }
  }

  public static IngestionSpec load(InputStream in) throws IOException {
    return YAML.readValue(in, IngestionSpec.class);
  }

  public static IngestionSpec parseYaml(String yaml) {
    try {
      return YAML.readValue(yaml, IngestionSpec.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid IngestionSpec YAML", e);
    }
  }

  public String toYaml() throws IOException {
    return YAML.writeValueAsString(this);
  }
}
