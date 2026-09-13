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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

/**
 * The small, engine-agnostic domain type behind every bounded ingestion run: source → concurrent
 * transform → sink, wired with Pekko Streams' native backpressure (D4). Deliberately knows nothing
 * about Camel, JDBC, CSV, or any specific mapping engine - callers supply an already-row-level
 * {@link Source} (an executor assembles that from whatever connector Publisher it has, doing any
 * format-specific flattening - e.g. one file into many CSV rows - upstream of this method), a plain
 * transform function, and a {@link Sink}. This is the piece that directly replaces {@code
 * SimpleSourceIngestionWorker}'s sequential {@code for} loop: {@code parallelism} bounds real
 * concurrent execution instead of one record at a time.
 */
public final class IngestionPipeline {

  private IngestionPipeline() {}

  public static <S, T, Mat> Mat run(
      Source<S, ?> source,
      int parallelism,
      Function<S, T> transform,
      Sink<T, Mat> sink,
      ActorSystem system) {
    return source
        .mapAsyncUnordered(
            parallelism, item -> CompletableFuture.supplyAsync(() -> transform.apply(item)))
        .runWith(sink, system);
  }
}
