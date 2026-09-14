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

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The small, engine-agnostic domain type behind every bounded ingestion run: source → concurrent
 * transform → sink, wired with Pekko Streams' native backpressure (D4). Deliberately knows nothing
 * about Camel, JDBC, CSV, or any specific mapping engine - callers supply an already-row-level
 * {@link Source} (an executor assembles that from whatever connector Publisher it has, doing any
 * format-specific flattening - e.g. one file into many CSV rows - upstream of this method), a plain
 * transform function, and a {@link Sink}. This is the piece that directly replaces {@code
 * SimpleSourceIngestionWorker}'s sequential {@code for} loop: {@code concurrency.preferred}
 * (clamped to {@code concurrency.maximum} when set) bounds real concurrent execution instead of one
 * record at a time.
 *
 * <p><b>{@code flowControl}/{@code failure}, wired for real 2026-09-13</b> - closing a real gap:
 * these {@link ExecutionSpec} fields parsed from YAML and did nothing before this. Two real,
 * previously-silent bugs this closes, not just missing features: with no {@code failure} handling
 * at all, a single malformed record (any {@code NamedTransform} throwing - including simply naming
 * an unregistered transform) or a single transient sink failure used to crash the *entire* run, on
 * Pekko - {@link org.apache.pekko.stream.javadsl.Flow#mapAsyncUnordered} fails the whole stream on
 * the first failed {@code CompletionStage}, and nothing here ever caught one. {@code
 * flowControl.maxInFlightRecords} bounds how many records may be buffered ahead of the transform
 * stage (via {@link Source#buffer}) - a real, separate concern from {@code concurrency}, which only
 * bounds how many transforms run *concurrently*, not how many may be queued waiting.
 */
public final class IngestionPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipeline.class);

  /**
   * Not spec-configurable today (a real, stated scope boundary, not an oversight) - {@code
   * ExecutionSpec.FailureSpec} has no attempts/backoff fields of its own; a future need for
   * per-spec tuning would add them there, not invent a separate mechanism.
   */
  private static final int SINK_FAILURE_MAX_ATTEMPTS = 3;

  private static final Duration SINK_FAILURE_INITIAL_BACKOFF = Duration.ofMillis(200);

  private IngestionPipeline() {}

  /**
   * How one malformed record is handled - see {@link ExecutionSpec.FailureSpec#malformedRecord()}.
   */
  public enum MalformedRecordPolicy {
    /** Log at WARN, drop the record, keep the run going. The default when unset. */
    SKIP,
    /** Log at ERROR with the record's own content, drop it, keep the run going - a louder skip. */
    DEAD_LETTER,
    /** Let the failure propagate and fail the whole run. */
    FAIL;

    public static MalformedRecordPolicy from(ExecutionSpec.FailureSpec failure) {
      String value = failure == null ? null : failure.malformedRecord();
      if (value == null) {
        return SKIP;
      }
      return switch (value) {
        case "dead-letter" -> DEAD_LETTER;
        case "fail" -> FAIL;
        default -> SKIP;
      };
    }
  }

  /**
   * How one sink write failure is handled - see {@link ExecutionSpec.FailureSpec#sinkFailure()}.
   */
  public enum SinkFailurePolicy {
    /** Let the failure propagate and fail the whole run. The default when unset. */
    FAIL,
    /** Retry with bounded attempts and exponential backoff before giving up. */
    RETRY;

    public static SinkFailurePolicy from(ExecutionSpec.FailureSpec failure) {
      String value = failure == null ? null : failure.sinkFailure();
      return "retry".equals(value) ? RETRY : FAIL;
    }
  }

  /**
   * Runs {@code source} through {@code transform} (bounded by {@code execution}'s own {@code
   * concurrency}/{@code flowControl}, with {@code execution.failure().malformedRecord()} deciding
   * what happens to a row that fails to transform) and into {@code sink}.
   */
  public static <S, T, Mat> Mat run(
      Source<S, ?> source,
      ExecutionSpec execution,
      Function<S, T> transform,
      Sink<T, Mat> sink,
      ActorSystem system) {
    int parallelism = effectiveParallelism(execution.concurrency());
    Integer maxInFlight =
        execution.flowControl() == null ? null : execution.flowControl().maxInFlightRecords();
    MalformedRecordPolicy malformedRecordPolicy = MalformedRecordPolicy.from(execution.failure());

    Source<S, ?> flowControlled =
        maxInFlight == null ? source : source.buffer(maxInFlight, OverflowStrategy.backpressure());

    return flowControlled
        .mapAsyncUnordered(
            parallelism,
            item ->
                CompletableFuture.supplyAsync(
                    () -> mapOrHandle(item, transform, malformedRecordPolicy)))
        .mapConcat(mapped -> mapped.isPresent() ? List.of(mapped.get()) : List.of())
        .runWith(sink, system);
  }

  /**
   * Public so both engines' own runners can size a sink's own concurrency identically to what this
   * class uses internally for the transform stage - {@code concurrency.preferred}, clamped to
   * {@code concurrency.maximum} when set.
   */
  public static int effectiveParallelism(ExecutionSpec.ConcurrencySpec concurrency) {
    int preferred = concurrency.preferred();
    Integer maximum = concurrency.maximum();
    return maximum == null ? preferred : Math.min(preferred, maximum);
  }

  private static <S, T> Optional<T> mapOrHandle(
      S item, Function<S, T> transform, MalformedRecordPolicy policy) {
    try {
      return Optional.of(transform.apply(item));
    } catch (RuntimeException failure) {
      switch (policy) {
        case FAIL -> throw failure;
        case DEAD_LETTER ->
            LOGGER.error("IngestionPipeline: malformed record dead-lettered: {}", item, failure);
        case SKIP -> LOGGER.warn("IngestionPipeline: malformed record skipped: {}", item, failure);
      }
      return Optional.empty();
    }
  }

  /**
   * Wraps one async sink-write attempt with bounded retry/backoff when {@code policy} is {@link
   * SinkFailurePolicy#RETRY}, a plain passthrough otherwise - shared by both engines' own sink code
   * so the retry policy itself is defined once.
   */
  public static <T> CompletionStage<T> withSinkFailureHandling(
      SinkFailurePolicy policy, Supplier<CompletionStage<T>> attempt) {
    if (policy == SinkFailurePolicy.FAIL) {
      return attempt.get();
    }
    return retryAsync(attempt, SINK_FAILURE_MAX_ATTEMPTS, SINK_FAILURE_INITIAL_BACKOFF);
  }

  /**
   * Blocking counterpart for Spark's own synchronous writers (e.g. {@code DataFrameWriter#save}).
   */
  public static void withSinkFailureHandlingBlocking(SinkFailurePolicy policy, Runnable attempt) {
    if (policy == SinkFailurePolicy.FAIL) {
      attempt.run();
      return;
    }
    retryBlocking(attempt, SINK_FAILURE_MAX_ATTEMPTS, SINK_FAILURE_INITIAL_BACKOFF);
  }

  private static <T> CompletionStage<T> retryAsync(
      Supplier<CompletionStage<T>> attempt, int attemptsLeft, Duration backoff) {
    CompletableFuture<T> stage = attempt.get().toCompletableFuture();
    if (attemptsLeft <= 1) {
      return stage;
    }
    return stage.exceptionallyCompose(
        failure -> {
          LOGGER.warn(
              "IngestionPipeline: sink write failed, retrying ({} attempt(s) left)",
              attemptsLeft - 1,
              failure);
          CompletableFuture<T> delayed = new CompletableFuture<>();
          CompletableFuture.delayedExecutor(backoff.toMillis(), TimeUnit.MILLISECONDS)
              .execute(
                  () ->
                      retryAsync(attempt, attemptsLeft - 1, backoff.multipliedBy(2))
                          .whenComplete(
                              (value, retryFailure) -> {
                                if (retryFailure == null) {
                                  delayed.complete(value);
                                } else {
                                  delayed.completeExceptionally(retryFailure);
                                }
                              }));
          return delayed;
        });
  }

  private static void retryBlocking(Runnable attempt, int attemptsLeft, Duration backoff) {
    try {
      attempt.run();
    } catch (RuntimeException failure) {
      if (attemptsLeft <= 1) {
        throw failure;
      }
      LOGGER.warn(
          "IngestionPipeline: sink write failed, retrying ({} attempt(s) left)",
          attemptsLeft - 1,
          failure);
      try {
        Thread.sleep(backoff.toMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw failure;
      }
      retryBlocking(attempt, attemptsLeft - 1, backoff.multipliedBy(2));
    }
  }
}
