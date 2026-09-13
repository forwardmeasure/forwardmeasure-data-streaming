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
 * The top-level, framework-agnostic ingestion contract: source, field-mapping (D3, corrected - a
 * runtime-interpreted {@link TransformSpec}, not a generated MapStruct mapper), sink, and execution
 * settings for one bounded ingestion run. Matches the YAML shape in the design plan's §2, as
 * refined by §5's build order.
 *
 * <p>{@code mapper} carries the field-mapping spec inline in the same document, per §5's own
 * correction: since the mapping is runtime-interpreted metadata rather than something a MapStruct
 * mapper would generate at compile time, it belongs structurally in this spec, not in a separate
 * downstream artifact.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestionSpec(
    @JsonProperty("source") SourceSpec source,
    @JsonProperty("mapper") TransformSpec mapper,
    @JsonProperty("sink") SinkSpec sink,
    @JsonProperty("execution") ExecutionSpec execution)
    implements Serializable {

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
