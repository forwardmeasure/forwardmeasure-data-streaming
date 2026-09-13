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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CorrelationSpecTest {

  private static final String YAML =
      """
      sources:
        - sourceKey: core
          source:
            connector: file
            uri: /data/worldcheck/core.csv
            format:
              type: csv
          mapper:
            target: schema://party/2.0
            fields:
              - target: uid
                source: ID
              - target: name
                source: FULL_NAME
          trustWeight: 1.0
        - sourceKey: enrichment
          source:
            connector: file
            uri: /data/worldcheck/enrichment.csv
            format:
              type: csv
          mapper:
            target: schema://party/2.0
            fields:
              - target: uid
                source: ID
              - target: nationality_code
                source: NATIONALITY
                optional: true
          trustWeight: 0.4

      blockingField: uid

      sink:
        connector: opensearch
        uri: opensearch://localhost:9200
        index: party-2-0

      execution:
        engine: spark
        concurrency:
          preferred: 8
      """;

  @Test
  void parsesEveryFieldFromRealisticYaml() {
    CorrelationSpec spec = CorrelationSpec.parseYaml(YAML);

    assertEquals(2, spec.sources().size());

    CorrelationSpec.SourceEntry core = spec.sources().get(0);
    assertEquals("core", core.sourceKey());
    assertEquals("file", core.source().connector());
    assertEquals("/data/worldcheck/core.csv", core.source().uri());
    assertEquals(2, core.mapper().fields().size());
    assertEquals(1.0, core.trustWeight());

    CorrelationSpec.SourceEntry enrichment = spec.sources().get(1);
    assertEquals("enrichment", enrichment.sourceKey());
    assertEquals(0.4, enrichment.trustWeight());
    assertEquals("nationality_code", enrichment.mapper().fields().get(1).target());

    assertEquals("uid", spec.blockingField());
    assertEquals("opensearch", spec.sink().connector());
    assertEquals("opensearch://localhost:9200", spec.sink().uri());
    assertEquals("spark", spec.execution().engine());
    assertEquals(8, spec.execution().concurrency().preferred());
  }

  @Test
  void roundTripsThroughYamlWithoutLoss() throws IOException {
    CorrelationSpec original = CorrelationSpec.parseYaml(YAML);

    String serialized = original.toYaml();
    CorrelationSpec reparsed = CorrelationSpec.parseYaml(serialized);

    assertEquals(original, reparsed);
  }

  @Test
  void loadsFromPath(@TempDir Path tempDir) throws IOException {
    Path specFile = tempDir.resolve("correlation-spec.yaml");
    Files.writeString(specFile, YAML);

    CorrelationSpec spec = CorrelationSpec.load(specFile);

    assertEquals(2, spec.sources().size());
    assertEquals("uid", spec.blockingField());
  }
}
