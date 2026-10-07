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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ConcurrencySpec;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.SinkFailurePolicy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeliveryFailureContractTest {
  @Test
  void failFastNeverRepeatsAWriteAndExposesTheOriginalFailure() {
    var failure = new IllegalStateException("destination rejected");
    var attempts = new AtomicInteger();
    var future = CompletableFuture.failedFuture(failure);
    assertSame(
        future,
        IngestionPipeline.withSinkFailureHandling(
            SinkFailurePolicy.FAIL,
            () -> {
              attempts.incrementAndGet();
              return future;
            }));
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                IngestionPipeline.withSinkFailureHandlingBlocking(
                    SinkFailurePolicy.FAIL,
                    () -> {
                      attempts.incrementAndGet();
                      throw failure;
                    })));
    assertEquals(2, attempts.get());
    IngestionPipeline.withSinkFailureHandlingBlocking(
        SinkFailurePolicy.FAIL, attempts::incrementAndGet);
    assertEquals(3, attempts.get());
  }

  @Test
  void asyncRetriesAreBoundedAndExposeTheLastFailure() {
    var attempts = new AtomicInteger();
    var last = new IllegalStateException("third attempt rejected");
    var result =
        IngestionPipeline.withSinkFailureHandling(
            SinkFailurePolicy.RETRY,
            () -> {
              int number = attempts.incrementAndGet();
              return CompletableFuture.failedFuture(
                  number == 3 ? last : new IllegalStateException("transient"));
            });
    var error =
        assertThrows(
            ExecutionException.class, () -> result.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertSame(last, error.getCause());
    assertEquals(3, attempts.get());
  }

  @Test
  void blockingRetriesRecoverOrExhaustWithoutAcknowledgingFailedDelivery() {
    var attempts = new AtomicInteger();
    IngestionPipeline.withSinkFailureHandlingBlocking(
        SinkFailurePolicy.RETRY,
        () -> {
          if (attempts.incrementAndGet() < 3) throw new IllegalStateException("transient");
        });
    assertEquals(3, attempts.get());
    attempts.set(0);
    var failure = new IllegalStateException("permanent");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                IngestionPipeline.withSinkFailureHandlingBlocking(
                    SinkFailurePolicy.RETRY,
                    () -> {
                      attempts.incrementAndGet();
                      throw failure;
                    })));
    assertEquals(3, attempts.get());
  }

  @Test
  void interruptedBackoffDoesNotRetryAndRetainsCancellation() {
    var attempts = new AtomicInteger();
    var failure = new IllegalStateException("cancelled transport");
    try {
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  IngestionPipeline.withSinkFailureHandlingBlocking(
                      SinkFailurePolicy.RETRY,
                      () -> {
                        attempts.incrementAndGet();
                        Thread.currentThread().interrupt();
                        throw failure;
                      })));
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, attempts.get());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void declarativePoliciesAndParallelismAreAppliedConsistently() {
    assertEquals(MalformedRecordPolicy.SKIP, MalformedRecordPolicy.from(null));
    assertEquals(
        MalformedRecordPolicy.SKIP, MalformedRecordPolicy.from(new ErrorPolicy(null, null)));
    assertEquals(
        MalformedRecordPolicy.SKIP, MalformedRecordPolicy.from(new ErrorPolicy("skip", null)));
    assertEquals(
        MalformedRecordPolicy.DEAD_LETTER,
        MalformedRecordPolicy.from(new ErrorPolicy("dead-letter", null)));
    assertEquals(
        MalformedRecordPolicy.FAIL, MalformedRecordPolicy.from(new ErrorPolicy("fail", null)));
    assertEquals(SinkFailurePolicy.FAIL, SinkFailurePolicy.from(null));
    assertEquals(SinkFailurePolicy.FAIL, SinkFailurePolicy.from(new ErrorPolicy(null, "fail")));
    assertEquals(SinkFailurePolicy.RETRY, SinkFailurePolicy.from(new ErrorPolicy(null, "retry")));
    assertEquals(2, IngestionPipeline.effectiveParallelism(new ConcurrencySpec(2, null)));
    assertEquals(2, IngestionPipeline.effectiveParallelism(new ConcurrencySpec(2, 4)));
    assertEquals(4, IngestionPipeline.effectiveParallelism(new ConcurrencySpec(8, 4)));
  }
}
