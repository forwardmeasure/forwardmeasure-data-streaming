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

import com.forwardmeasure.datastreaming.api.DeliverySemantics;
import com.forwardmeasure.datastreaming.api.ErrorPolicy;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pekko backpressure and asynchronous transform wiring. */
public final class PekkoIngestionPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoIngestionPipeline.class);

  private PekkoIngestionPipeline() {}

  /**
   * Runs {@code source} through {@code transform} (bounded by {@code delivery}'s own {@code
   * concurrency}/{@code flowControl}, with {@code errors.malformedRecord()} deciding what happens
   * to a row that fails to transform) and into {@code sink}.
   */
  public static <S, T, Mat> Mat run(
      Source<S, ?> source,
      DeliverySemantics delivery,
      ErrorPolicy errors,
      Function<S, T> transform,
      Sink<T, Mat> sink,
      ActorSystem system) {
    return run(source, delivery, errors, transform, sink, system, false);
  }

  /**
   * As above; {@code ordered} keeps records in source order through the concurrent transform, which
   * a stateful graph after it needs so every engine sees the same order.
   */
  public static <S, T, Mat> Mat run(
      Source<S, ?> source,
      DeliverySemantics delivery,
      ErrorPolicy errors,
      Function<S, T> transform,
      Sink<T, Mat> sink,
      ActorSystem system,
      boolean ordered) {
    int parallelism = IngestionPipeline.effectiveParallelism(delivery.concurrency());
    Integer maxInFlight =
        delivery.flowControl() == null ? null : delivery.flowControl().maxInFlightRecords();
    MalformedRecordPolicy malformedRecordPolicy = MalformedRecordPolicy.from(errors);

    Source<S, ?> flowControlled =
        maxInFlight == null ? source : source.buffer(maxInFlight, OverflowStrategy.backpressure());

    org.apache.pekko.japi.function.Function<S, CompletionStage<Optional<T>>> mapped =
        item ->
            CompletableFuture.supplyAsync(
                () -> mapOrHandle(item, transform, malformedRecordPolicy));
    return (ordered
            ? flowControlled.mapAsync(parallelism, mapped)
            : flowControlled.mapAsyncUnordered(parallelism, mapped))
        .mapConcat(result -> result.isPresent() ? List.of(result.get()) : List.of())
        .runWith(sink, system);
  }

  /** A {@code null} from {@code transform} means the caller skipped the row itself. */
  private static <S, T> Optional<T> mapOrHandle(
      S item, Function<S, T> transform, MalformedRecordPolicy policy) {
    try {
      return Optional.ofNullable(transform.apply(item));
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
}
