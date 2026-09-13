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
package com.forwardmeasure.datastreaming.connector.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreams;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.apache.camel.impl.DefaultCamelContext;
import org.reactivestreams.Publisher;

/**
 * The generic route-building/{@code camel-reactive-streams} bridge (D5/D6): turns any Camel
 * endpoint URI into a plain {@link Publisher} of {@code type}-converted message bodies, with no
 * per-protocol code in this module - which {@code camel-*} component jar answers a given URI scheme
 * (file, kafka, opensearch, or any of Camel's 300+ others) is entirely the consuming application's
 * classpath decision.
 *
 * <p>Source side only. A {@code sink(uri, type)} counterpart (wrapping {@link
 * CamelReactiveStreamsService#subscriber}) existed here until 2026-09-13, when {@code
 * forwardmeasure-data-streaming-executor-pekko}'s own real-sink test proved it silently drops data:
 * {@code ReactiveStreamsCamelSubscriber.onNext()} dispatches each item to Camel's route
 * asynchronously (fire-and-forget with an internal completion callback, confirmed by reading
 * Camel's own source) rather than blocking until the underlying producer call finishes - a consumer
 * that closes this bridge as soon as the last {@code onNext()} call *returns* (the only signal a
 * plain {@link org.reactivestreams.Subscriber} gives) can stop the {@link CamelContext} while the
 * last few sends are still in flight, silently discarding them. Removed rather than kept as a trap
 * for the next caller (it had no other real caller once {@code IngestionPipelineRunner} stopped
 * using it) - a real sink needs {@link #camelContext()}{@code .createProducerTemplate()}'s own
 * {@code asyncSendBody}, whose returned {@code CompletableFuture} is a genuine per-item completion
 * signal, wired into something like Pekko's {@code Sink.foreachAsync} so the whole sink's own
 * materialized completion only resolves once every item's producer call has actually finished - see
 * {@code IngestionPipelineRunner}'s own sink-building code for the real, proven pattern.
 *
 * <p>Owns one {@link CamelContext}'s lifecycle - construct one {@link CamelBridge} per bounded
 * ingestion run (matching D4's bounded-batch-process shape), {@link #close()} it when the run ends.
 */
public final class CamelBridge implements AutoCloseable {

  private final CamelContext context;
  private final CamelReactiveStreamsService reactiveStreams;

  public CamelBridge() {
    this.context = new DefaultCamelContext();
    // Service.start()/stop() are unchecked in Camel 4.x (a RuntimeCamelException on failure) -
    // nothing here needs a try/catch.
    this.context.start();
    this.reactiveStreams = CamelReactiveStreams.get(context);
  }

  /** A {@link Publisher} of {@code type}-converted message bodies consumed from {@code uri}. */
  public <T> Publisher<T> source(String uri, Class<T> type) {
    return reactiveStreams.from(uri, type);
  }

  /**
   * For a real sink, use {@link ProducerTemplate#asyncSendBody(String, Object)} on this context's
   * own {@code createProducerTemplate()} - see this class's own javadoc for why {@code
   * CamelReactiveStreamsService#subscriber} isn't a safe way to build one.
   */
  public CamelContext camelContext() {
    return context;
  }

  @Override
  public void close() throws Exception {
    context.stop();
  }
}
