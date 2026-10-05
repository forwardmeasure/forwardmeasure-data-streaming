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

import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared concurrency and failure policies; execution engines own their stream operators. */
public final class IngestionPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipeline.class);

  /**
   * Not spec-configurable today (a real, stated scope boundary, not an oversight) - {@link
   * ErrorPolicy} has no attempts/backoff fields of its own; a future need for per-spec tuning would
   * add them there, not invent a separate mechanism.
   */
  private static final int SINK_FAILURE_MAX_ATTEMPTS = 3;

  private static final Duration SINK_FAILURE_INITIAL_BACKOFF = Duration.ofMillis(200);

  private IngestionPipeline() {}

  /** How one malformed record is handled - see {@link ErrorPolicy#malformedRecord()}. */
  public enum MalformedRecordPolicy {
    /** Log at WARN, drop the record, keep the run going. The default when unset. */
    SKIP,
    /** Log at ERROR with the record's own content, drop it, keep the run going - a louder skip. */
    DEAD_LETTER,
    /** Let the failure propagate and fail the whole run. */
    FAIL;

    public static MalformedRecordPolicy from(ErrorPolicy errors) {
      String value = errors == null ? null : errors.malformedRecord();
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

  /** How one sink write failure is handled - see {@link ErrorPolicy#sinkFailure()}. */
  public enum SinkFailurePolicy {
    /** Let the failure propagate and fail the whole run. The default when unset. */
    FAIL,
    /** Retry with bounded attempts and exponential backoff before giving up. */
    RETRY;

    public static SinkFailurePolicy from(ErrorPolicy errors) {
      String value = errors == null ? null : errors.sinkFailure();
      return "retry".equals(value) ? RETRY : FAIL;
    }
  }

  /**
   * Public so both engines' own runners can size a sink's own concurrency identically to what this
   * class uses internally for the transform stage - {@code concurrency.preferred}, clamped to
   * {@code concurrency.maximum} when set.
   */
  public static int effectiveParallelism(ConcurrencySpec concurrency) {
    int preferred = concurrency.preferred();
    Integer maximum = concurrency.maximum();
    return maximum == null ? preferred : Math.min(preferred, maximum);
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
    CompletableFuture<T> stage;
    try {
      stage = attempt.get().toCompletableFuture();
    } catch (RuntimeException failure) {
      stage = CompletableFuture.failedFuture(failure);
    }
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
