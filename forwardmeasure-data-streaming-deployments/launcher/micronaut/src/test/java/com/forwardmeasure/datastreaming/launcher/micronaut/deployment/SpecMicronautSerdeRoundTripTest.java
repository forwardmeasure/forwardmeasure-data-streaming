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
package com.forwardmeasure.datastreaming.launcher.micronaut.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import io.micronaut.context.ApplicationContext;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Regression test for a real bug found live 2026-09-23 debugging {@code
 * DirectIngestionMatrixMicronautPekkoSmokeTest}'s real-image dispatch: {@link SourceSpec} and
 * {@link SinkSpec} each have 3 constructors (the canonical one plus two shorter "kept working
 * unchanged for existing callers" overloads - see their own javadoc). With no explicit
 * {@code @JsonCreator}, Micronaut Serde's compile-time {@code BeanIntrospection} codegen silently
 * picked the *shortest* constructor as the deserialization entry point instead of the canonical one
 * - confirmed via {@code javap} on the generated {@code instantiateInternal} bytecode, which called
 * {@code SourceSpec(connector, uri, format, schema)} (4-arg) and {@code SinkSpec(connector, uri,
 * schema, batching)} (4-arg), silently dropping {@code query}/{@code options} on every real HTTP
 * request, no matter what the JSON body contained. Plain Jackson (this module's own {@code
 * IngestionSpec#toYaml()}/{@code #parseYaml()}, and the JAX-RS {@code MessageBodyReader}) was never
 * affected - only Micronaut's own separate serde codegen path, and only for types with 3+
 * constructors (a 2-constructor type, e.g. {@code TransformSpec.FieldRule}, was confirmed
 * unaffected). Fixed with {@code @JsonCreator} on each type's own canonical/compact constructor.
 *
 * <p>No {@code @MicronautTest}/security config needed - a plain {@link ApplicationContext#run()} is
 * enough to exercise the same generated serde codegen this module's real HTTP layer uses, without
 * booting Testcontainers.
 */
class SpecMicronautSerdeRoundTripTest {

  @Test
  void sourceSpecOptionsAndQuerySurviveARealSerdeRoundTrip() throws Exception {
    try (ApplicationContext ctx = ApplicationContext.run()) {
      io.micronaut.serde.ObjectMapper mapper = ctx.getBean(io.micronaut.serde.ObjectMapper.class);

      SourceSpec original =
          new SourceSpec(
              "jdbc",
              "jdbc:postgresql://host/db",
              null,
              null,
              "select 1",
              Map.of("delimiter", "\t"));
      SourceSpec roundTripped =
          mapper.readValue(mapper.writeValueAsString(original), SourceSpec.class);

      assertEquals("select 1", roundTripped.query());
      assertEquals(Map.of("delimiter", "\t"), roundTripped.options());
    }
  }

  @Test
  void sinkSpecUriAndOptionsSurviveARealSerdeRoundTrip() throws Exception {
    try (ApplicationContext ctx = ApplicationContext.run()) {
      io.micronaut.serde.ObjectMapper mapper = ctx.getBean(io.micronaut.serde.ObjectMapper.class);

      SinkSpec original =
          new SinkSpec(
              "opensearch", "http://host:9200", "idx", null, null, Map.of("idField", "uid"));
      SinkSpec roundTripped = mapper.readValue(mapper.writeValueAsString(original), SinkSpec.class);

      assertEquals("http://host:9200", roundTripped.uri());
      assertEquals(Map.of("idField", "uid"), roundTripped.options());
    }
  }
}
