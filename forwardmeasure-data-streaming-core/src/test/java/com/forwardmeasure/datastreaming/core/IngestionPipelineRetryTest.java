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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IngestionPipelineRetryTest {
  @Test
  void synchronousExceptionOnDelayedAttemptCannotLeaveCompletionHanging() throws Exception {
    var attempts = new AtomicInteger();
    var result =
        IngestionPipeline.withSinkFailureHandling(
            IngestionPipeline.SinkFailurePolicy.RETRY,
            () -> {
              int attempt = attempts.incrementAndGet();
              if (attempt == 1)
                return CompletableFuture.<String>failedFuture(new IllegalStateException("first"));
              if (attempt == 2) throw new IllegalStateException("second");
              return CompletableFuture.completedFuture("done");
            });
    assertEquals("done", result.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals(3, attempts.get());
  }
}
