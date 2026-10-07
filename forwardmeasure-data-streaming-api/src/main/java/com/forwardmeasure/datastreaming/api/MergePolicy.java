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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** Versioned correlation rules, loaded once when compiling a plan. */
public record MergePolicy(int version, Map<String, FieldRule> fields) implements Serializable {
  public static final String DEFAULT_URI = "classpath:/merge-policies/entity-v1.yaml";
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  public enum Strategy {
    TRUST,
    UNION,
    ALIASES,
    OBJECTS
  }

  /** ALIASES preserves the highest-trust primary and demotes later primaries in a list. */
  public record FieldRule(Strategy strategy, String discriminator, String primary, String alias)
      implements Serializable {
    public FieldRule {
      Objects.requireNonNull(strategy, "strategy");
      if (strategy == Strategy.ALIASES
          && (discriminator == null
              || discriminator.isBlank()
              || primary == null
              || primary.isBlank()
              || alias == null
              || alias.isBlank())) {
        throw new IllegalArgumentException("ALIASES requires discriminator, primary and alias");
      }
    }
  }

  public MergePolicy {
    if (version != 1)
      throw new IllegalArgumentException("Unsupported merge policy version: " + version);
    fields = fields == null ? Map.of() : Map.copyOf(fields);
  }

  public static MergePolicy defaults() {
    return Defaults.VALUE;
  }

  private static final class Defaults {
    private static final MergePolicy VALUE = load(DEFAULT_URI);
  }

  /** Policies are bundled resources or mounted files; arbitrary network URLs are not fetched. */
  public static MergePolicy load(String location) {
    return load(
        location,
        Path.of(
            System.getenv()
                .getOrDefault("MERGE_POLICY_ROOT", "/etc/forwardmeasure/merge-policies")));
  }

  /** An explicitly trusted directory bounds file access, including symlink resolution. */
  public static MergePolicy load(String location, Path trustedRoot) {
    String value = location == null || location.isBlank() ? DEFAULT_URI : location;
    URI uri = URI.create(value);
    try (InputStream input = open(uri, trustedRoot)) {
      if (input == null) throw new IllegalArgumentException("Merge policy not found: " + value);
      return YAML.readValue(input, MergePolicy.class);
    } catch (IOException failure) {
      throw new IllegalArgumentException("Unable to load merge policy: " + value, failure);
    }
  }

  private static InputStream open(URI uri, Path trustedRoot) throws IOException {
    if ("classpath".equals(uri.getScheme())) {
      return MergePolicy.class.getResourceAsStream(uri.getSchemeSpecificPart());
    }
    if ("file".equals(uri.getScheme())) {
      Path file = Path.of(uri).toRealPath();
      if (!file.startsWith(trustedRoot.toRealPath())) {
        throw new IllegalArgumentException(
            "Merge policy file is outside the configured policy directory");
      }
      return Files.newInputStream(file);
    }
    throw new IllegalArgumentException("mergePolicyUri must use classpath: or file: " + uri);
  }
}
