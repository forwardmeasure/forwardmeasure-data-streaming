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
package com.forwardmeasure.datastreaming.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocks proof of {@link IngestionPipeline}'s own real behavior - added 2026-09-13
 * alongside wiring {@code flowControl}/{@code failure} for real: this class had never had a single
 * test before, despite being the shared, engine-agnostic backbone every runner builds on.
 */
class IngestionPipelineTest {

  private ActorSystem system;

  @BeforeEach
  void startSystem() {
    system = ActorSystem.create("ingestion-pipeline-test");
  }

  @AfterEach
  void stopSystem() {
    system.terminate();
  }

  @Test
  void runsEveryElementThroughTransformAndSinkWhenNothingFails() {
    List<Integer> written = runCollecting(List.of(1, 2, 3), execution(4, null, null, null));
    assertEquals(List.of(2, 4, 6), sorted(written));
  }

  @Test
  void flowControlBoundsBufferingWithoutLosingAnyElement() {
    // A tight buffer (2) with many more elements than that in flight at once must still process
    // every one of them correctly - flowControl bounds how much may be buffered ahead, not how
    // much data survives.
    List<Integer> input = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      input.add(i);
    }
    List<Integer> written = runCollecting(input, execution(4, 2, null, null));
    assertEquals(50, written.size());
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      expected.add(i * 2);
    }
    assertEquals(expected, sorted(written));
  }

  @Test
  void malformedRecordSkipDropsTheBadRowAndKeepsTheRunGoing() {
    ExecutionSpec execution = execution(4, null, "skip", null);
    List<Integer> written =
        runCollecting(
            List.of(1, 2, 3),
            execution,
            value -> {
              if (value == 2) {
                throw new IllegalStateException("simulated malformed row");
              }
              return value * 2;
            });
    assertEquals(List.of(2, 6), sorted(written));
  }

  @Test
  void malformedRecordDeadLetterAlsoDropsTheBadRowAndKeepsTheRunGoing() {
    ExecutionSpec execution = execution(4, null, "dead-letter", null);
    List<Integer> written =
        runCollecting(
            List.of(1, 2, 3),
            execution,
            value -> {
              if (value == 2) {
                throw new IllegalStateException("simulated malformed row");
              }
              return value * 2;
            });
    assertEquals(List.of(2, 6), sorted(written));
  }

  @Test
  void malformedRecordDefaultsToSkipWhenFailureSpecIsAbsent() {
    ExecutionSpec execution = new ExecutionSpec("pekko", concurrency(4, null), null, null);
    List<Integer> written =
        runCollecting(
            List.of(1, 2, 3),
            execution,
            value -> {
              if (value == 2) {
                throw new IllegalStateException("simulated malformed row");
              }
              return value * 2;
            });
    assertEquals(List.of(2, 6), sorted(written));
  }

  @Test
  void malformedRecordFailFailsTheWholeRun() {
    ExecutionSpec execution = execution(4, null, "fail", null);
    Exception failure =
        assertThrows(
            Exception.class,
            () ->
                runCollecting(
                    List.of(1, 2, 3),
                    execution,
                    value -> {
                      if (value == 2) {
                        throw new IllegalStateException("simulated malformed row");
                      }
                      return value * 2;
                    }));
    assertTrue(
        rootCause(failure) instanceof IllegalStateException,
        () ->
            "expected the original IllegalStateException somewhere in the cause chain of "
                + failure);
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }

  @Test
  void effectiveParallelismUsesPreferredWhenMaximumIsUnset() {
    assertEquals(4, IngestionPipeline.effectiveParallelism(concurrency(4, null)));
  }

  @Test
  void effectiveParallelismClampsPreferredToMaximum() {
    assertEquals(5, IngestionPipeline.effectiveParallelism(concurrency(10, 5)));
  }

  @Test
  void effectiveParallelismLeavesPreferredUnchangedWhenBelowMaximum() {
    assertEquals(4, IngestionPipeline.effectiveParallelism(concurrency(4, 10)));
  }

  @Test
  void sinkFailureFailPolicyNeverRetries() {
    AtomicInteger attempts = new AtomicInteger();
    CompletionStage<String> result =
        IngestionPipeline.withSinkFailureHandling(
            IngestionPipeline.SinkFailurePolicy.FAIL,
            () -> {
              attempts.incrementAndGet();
              return CompletableFuture.failedFuture(new IllegalStateException("boom"));
            });

    assertThrows(Exception.class, () -> result.toCompletableFuture().join());
    assertEquals(1, attempts.get(), "FAIL must not retry at all");
  }

  @Test
  void sinkFailureRetryPolicyEventuallySucceeds() {
    AtomicInteger attempts = new AtomicInteger();
    CompletionStage<String> result =
        IngestionPipeline.withSinkFailureHandling(
            IngestionPipeline.SinkFailurePolicy.RETRY,
            () -> {
              int attempt = attempts.incrementAndGet();
              if (attempt < 3) {
                return CompletableFuture.failedFuture(new IllegalStateException("transient"));
              }
              return CompletableFuture.completedFuture("ok");
            });

    assertEquals("ok", result.toCompletableFuture().join());
    assertEquals(3, attempts.get());
  }

  @Test
  void sinkFailureRetryPolicyEventuallyGivesUp() {
    AtomicInteger attempts = new AtomicInteger();
    CompletionStage<String> result =
        IngestionPipeline.withSinkFailureHandling(
            IngestionPipeline.SinkFailurePolicy.RETRY,
            () -> {
              attempts.incrementAndGet();
              return CompletableFuture.failedFuture(new IllegalStateException("always fails"));
            });

    assertThrows(Exception.class, () -> result.toCompletableFuture().join());
    assertTrue(attempts.get() > 1, "RETRY must have attempted more than once before giving up");
  }

  @Test
  void blockingSinkFailureFailPolicyNeverRetries() {
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            IngestionPipeline.withSinkFailureHandlingBlocking(
                IngestionPipeline.SinkFailurePolicy.FAIL,
                () -> {
                  attempts.incrementAndGet();
                  throw new IllegalStateException("boom");
                }));
    assertEquals(1, attempts.get());
  }

  @Test
  void blockingSinkFailureRetryPolicyEventuallySucceeds() {
    AtomicInteger attempts = new AtomicInteger();
    IngestionPipeline.withSinkFailureHandlingBlocking(
        IngestionPipeline.SinkFailurePolicy.RETRY,
        () -> {
          if (attempts.incrementAndGet() < 3) {
            throw new IllegalStateException("transient");
          }
        });
    assertEquals(3, attempts.get());
  }

  @Test
  void blockingSinkFailureRetryPolicyEventuallyGivesUp() {
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            IngestionPipeline.withSinkFailureHandlingBlocking(
                IngestionPipeline.SinkFailurePolicy.RETRY,
                () -> {
                  attempts.incrementAndGet();
                  throw new IllegalStateException("always fails");
                }));
    assertTrue(attempts.get() > 1);
  }

  private List<Integer> runCollecting(List<Integer> input, ExecutionSpec execution) {
    return runCollecting(input, execution, value -> value * 2);
  }

  private List<Integer> runCollecting(
      List<Integer> input,
      ExecutionSpec execution,
      java.util.function.Function<Integer, Integer> transform) {
    Sink<Integer, CompletionStage<List<Integer>>> collectingSink = Sink.seq();
    CompletionStage<List<Integer>> result =
        IngestionPipeline.run(Source.from(input), execution, transform, collectingSink, system);
    return result.toCompletableFuture().join();
  }

  private static List<Integer> sorted(List<Integer> values) {
    List<Integer> copy = new ArrayList<>(values);
    copy.sort(Integer::compareTo);
    return copy;
  }

  private static ExecutionSpec execution(
      int preferred, Integer maximum, String malformedRecord, String sinkFailure) {
    ExecutionSpec.FailureSpec failure =
        (malformedRecord == null && sinkFailure == null)
            ? null
            : new ExecutionSpec.FailureSpec(malformedRecord, sinkFailure);
    return new ExecutionSpec("pekko", concurrency(preferred, maximum), null, failure);
  }

  private static ExecutionSpec.ConcurrencySpec concurrency(int preferred, Integer maximum) {
    return new ExecutionSpec.ConcurrencySpec(preferred, maximum);
  }
}
