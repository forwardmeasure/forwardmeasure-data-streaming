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
package com.forwardmeasure.datastreaming.executor.pekko;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.camel.builder.RouteBuilder;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link PekkoIngestionRunner#run(IngestionSpec, ActorSystem)} and {@link
 * PekkoIngestionRunner#buildSink} genuinely honor {@code execution.failure} - added 2026-09-13
 * alongside {@link com.forwardmeasure.datastreaming.core.IngestionPipeline}'s own unit tests, which
 * prove the retry/policy *mechanism* in isolation; these prove this runner actually *wires* the
 * mechanism in, not just that the mechanism itself works.
 *
 * <p>A real, honest finding this test's own design surfaced: every function in the real {@code
 * NamedTransformRegistry} is deliberately defensive (bad/unparseable input returns {@code null}, it
 * never throws) - so the realistic way {@code FieldMappingEngine#map} throws in production is a
 * *spec-level* mistake (a field naming a transform that was never registered), which fails
 * identically for every row, not a per-row data problem. The malformed-record tests below are built
 * on that real mistake, not an artificial one invented just to make a test fail.
 */
class PekkoExecutionWiringIntegrationTest {

  @Test
  void malformedRecordSkipLetsTheRunCompleteWithNothingWritten(@TempDir Path tempDir)
      throws Exception {
    IngestionSpec spec = specWithUnregisteredTransform(tempDir, "skip");

    ActorSystem system = ActorSystem.create("malformed-record-skip-test");
    PekkoIngestionRunner.IngestionResult result;
    try {
      result = new PekkoIngestionRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    assertEquals(0, result.recordsProcessed(), "every row hits the same unregistered transform");
  }

  @Test
  void malformedRecordFailFailsTheWholeRun(@TempDir Path tempDir) throws Exception {
    IngestionSpec spec = specWithUnregisteredTransform(tempDir, "fail");

    ActorSystem system = ActorSystem.create("malformed-record-fail-test");
    try {
      // The real, verified failure here is NamedTransformRegistry#get's own
      // IllegalArgumentException("unknown named transform: ...") - propagated as-is by
      // PekkoIngestionRunner#run (which unwraps CompletableFuture#join's own CompletionException
      // wrapper), not IllegalStateException; asserting the wrong sibling type here previously went
      // unnoticed only because a then-broken source URI (fixed 2026-09-13) failed the run earlier,
      // during route creation, with an unrelated IllegalStateException of its own.
      assertThrows(
          IllegalArgumentException.class, () -> new PekkoIngestionRunner().run(spec, system));
    } finally {
      system.terminate();
    }
  }

  /**
   * {@link PekkoIngestionRunner#run(IngestionSpec, ActorSystem)} creates and owns its own {@link
   * CamelBridge} internally, so a route can't be pre-registered into it from outside. This
   * exercises {@link PekkoIngestionRunner#buildSink} directly instead, against a bridge this test
   * owns and installs the flaky route into first - the same sink-building code {@code run} itself
   * calls, so this is a real proof of the retry wiring, not a parallel reimplementation of it.
   */
  @Test
  void sinkFailureRetryEventuallySucceedsAgainstARealFlakyCamelRoute() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    SinkSpec sink = new SinkSpec("direct", "direct:flaky", "n/a", null, null);
    ExecutionSpec execution =
        new ExecutionSpec(
            "pekko",
            new ExecutionSpec.ConcurrencySpec(1, null),
            null,
            new ExecutionSpec.FailureSpec(null, "retry"));

    ActorSystem system = ActorSystem.create("sink-failure-retry-test");
    try (CamelBridge bridge = new CamelBridge()) {
      bridge
          .camelContext()
          .addRoutes(
              new RouteBuilder() {
                @Override
                public void configure() {
                  from("direct:flaky")
                      .process(
                          exchange -> {
                            if (attempts.incrementAndGet() < 3) {
                              throw new IllegalStateException("simulated transient sink failure");
                            }
                          });
                }
              });

      Sink<Map<String, Object>, CompletionStage<Done>> builtSink =
          PekkoIngestionRunner.buildSink(
              bridge, sink, execution, new ObjectMapper(), new AtomicLong());
      Source.single(Map.<String, Object>of("uid", "S1", "name", "Alice Anderson"))
          .runWith(builtSink, system)
          .toCompletableFuture()
          .get(15, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    assertEquals(3, attempts.get(), "must have retried twice before the 3rd attempt succeeded");
  }

  /**
   * The other half of sink-failure wiring, added 2026-09-14 once a direct side-by-side comparison
   * with {@code SparkExecutionWiringIntegrationTest} surfaced that this class only ever proved
   * {@code retry} eventually succeeding, never {@code fail} genuinely propagating (Spark's own test
   * suite had the opposite gap - a real, permanently-broken connection proving {@code fail}
   * propagates, but nothing proving {@code retry} recovers from a real transient failure - see that
   * class's own new test for the fix on that side). A route that *always* fails, with {@code
   * sinkFailure: fail}, must surface the real failure to the caller and must NOT have been retried.
   */
  @Test
  void sinkFailureFailPropagatesARealWriteFailureRatherThanSilentlyRetrying() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    SinkSpec sink = new SinkSpec("direct", "direct:alwaysFails", "n/a", null, null);
    ExecutionSpec execution =
        new ExecutionSpec(
            "pekko",
            new ExecutionSpec.ConcurrencySpec(1, null),
            null,
            new ExecutionSpec.FailureSpec(null, "fail"));

    ActorSystem system = ActorSystem.create("sink-failure-fail-test");
    try (CamelBridge bridge = new CamelBridge()) {
      bridge
          .camelContext()
          .addRoutes(
              new RouteBuilder() {
                @Override
                public void configure() {
                  from("direct:alwaysFails")
                      .process(
                          exchange -> {
                            attempts.incrementAndGet();
                            throw new IllegalStateException("simulated permanent sink failure");
                          });
                }
              });

      Sink<Map<String, Object>, CompletionStage<Done>> builtSink =
          PekkoIngestionRunner.buildSink(
              bridge, sink, execution, new ObjectMapper(), new AtomicLong());
      assertThrows(
          Exception.class,
          () ->
              Source.single(Map.<String, Object>of("uid", "S1", "name", "Alice Anderson"))
                  .runWith(builtSink, system)
                  .toCompletableFuture()
                  .get(15, TimeUnit.SECONDS));
    } finally {
      system.terminate();
    }

    assertEquals(1, attempts.get(), "fail policy must not retry - exactly one attempt");
  }

  private static IngestionSpec specWithUnregisteredTransform(Path tempDir, String malformedRecord)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);
    Path outputDir = tempDir.resolve("output");

    return new IngestionSpec(
        new SourceSpec(
            "file",
            "file:"
                + tempDir.toAbsolutePath()
                + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
            null,
            null),
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                new TransformSpec.FieldRule(
                    "name",
                    "FULL_NAME",
                    null,
                    null,
                    "this_transform_was_never_registered",
                    null,
                    null))),
        new SinkSpec(
            "file",
            "file:" + outputDir.toAbsolutePath() + "?fileName=output.jsonl&fileExist=Append",
            "n/a",
            null,
            null),
        new ExecutionSpec(
            "pekko",
            new ExecutionSpec.ConcurrencySpec(4, null),
            null,
            new ExecutionSpec.FailureSpec(malformedRecord, null)));
  }
}
