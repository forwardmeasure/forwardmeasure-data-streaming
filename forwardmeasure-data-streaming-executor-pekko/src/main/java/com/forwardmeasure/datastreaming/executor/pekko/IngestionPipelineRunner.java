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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pekko-backed replacement for {@code SimpleSourceIngestionWorker}'s sequential {@code for}
 * loop: reads a source file through {@link CamelBridge}, splits it into rows, and processes rows
 * with real, bounded concurrency via {@link IngestionPipeline} - not one record at a time.
 *
 * <p>Today's scope is deliberately narrow (see the design plan's §5): {@code file} source, {@code
 * csv} format, and a schema-agnostic {@link FieldMappingEngine} transform producing {@code
 * Map<String, Object>} per row - not yet the typed-target coercion D1 covers. The {@code run}
 * overload taking an explicit transform/sink exists so a caller (this class's own tests included)
 * can observe or replace either stage without this class needing to grow test-only hooks.
 *
 * <p>{@link #run(IngestionSpec, ActorSystem)} writes to a real sink (added 2026-09-13, once the
 * launcher work surfaced that it never had one - see {@link SinkSpec#uri()}'s own javadoc): each
 * mapped row is serialized to JSON and sent via {@link ProducerTemplate#asyncSendBody(String,
 * Object)} - JSON because that's the one body shape every Camel producer component reliably accepts
 * regardless of which {@code SinkSpec.connector} it is, not because any one connector was proven
 * out specifically yet (see this class's own test for the actual real, no-mocks proof, using {@code
 * camel-file} - the same component already proven on the source side). Deliberately not {@link
 * CamelBridge}'s own reactive-streams-{@code Subscriber}-based sink helper - that one silently
 * drops in-flight sends when the run's own {@code CamelContext} stops (confirmed by a real test
 * failure the same day, then by reading Camel's own source; see {@code CamelBridge}'s own javadoc)
 * - {@code asyncSendBody}'s real per-item {@code CompletableFuture} is what let this class's own
 * {@link Sink#foreachAsync} genuinely wait for every send to finish, not just to have been handed
 * off.
 */
public final class IngestionPipelineRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipelineRunner.class);

  public record IngestionResult(long recordsProcessed) {}

  /**
   * Runs {@code spec} with the real mapping engine, writing each mapped row to {@code
   * spec.sink()}'s real Camel producer endpoint. Source and sink share one {@link CamelBridge}/
   * {@link org.apache.camel.CamelContext} (matching that class's own "one bridge per bounded run"
   * contract) - this overload therefore does not delegate to {@link #run(IngestionSpec,
   * ActorSystem, Function, Sink)}, which opens its own bridge scoped to the source side only.
   */
  public IngestionResult run(IngestionSpec spec, ActorSystem system) throws IOException {
    FieldMappingEngine engine = new FieldMappingEngine();
    ObjectMapper objectMapper = new ObjectMapper();
    AtomicLong processed = new AtomicLong();

    try (CamelBridge bridge = new CamelBridge()) {
      Source<SourceRow, ?> source = csvRowSource(bridge, spec.source().uri());
      int parallelism = spec.execution().concurrency().preferred();
      ProducerTemplate producerTemplate = bridge.camelContext().createProducerTemplate();
      Sink<Map<String, Object>, CompletionStage<Done>> sink =
          camelSink(producerTemplate, spec.sink().uri(), objectMapper, processed, parallelism);
      CompletionStage<Done> resultStage =
          IngestionPipeline.run(
              source, parallelism, row -> engine.map(row, spec.mapper()), sink, system);
      resultStage.toCompletableFuture().join();
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("ingestion run failed", e);
    }
    return new IngestionResult(processed.get());
  }

  /**
   * {@code Sink.foreachAsync} only signals its own overall completion once every element's own
   * {@code CompletableFuture} (from {@link ProducerTemplate#asyncSendBody(String, Object)}) has
   * actually resolved - unlike the {@code CamelBridge#sink}-based version this replaced, whose
   * reactive-streams {@code Subscriber} only reports "handed off," not "finished" (see {@link
   * CamelBridge}'s own javadoc). {@code parallelism} bounds how many sends are in flight at once,
   * matching the transform stage's own concurrency rather than serializing every send.
   */
  private static Sink<Map<String, Object>, CompletionStage<Done>> camelSink(
      ProducerTemplate producerTemplate,
      String sinkUri,
      ObjectMapper objectMapper,
      AtomicLong processed,
      int parallelism) {
    return Sink.foreachAsync(
        parallelism,
        row -> {
          processed.incrementAndGet();
          String json = writeValueAsString(row, objectMapper);
          return producerTemplate.asyncSendBody(sinkUri, json).thenApply(ignored -> null);
        });
  }

  /** A trailing newline makes the output genuinely NDJSON - one complete record per line. */
  private static String writeValueAsString(Map<String, Object> row, ObjectMapper objectMapper) {
    try {
      return objectMapper.writeValueAsString(row) + "\n";
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Runs {@code spec}'s source through {@code transform} and {@code sink} with real concurrency
   * bounded by {@code spec.execution().concurrency().preferred()}. Blocks the calling thread until
   * the run completes - matching {@code main()}'s own needs - even though row processing itself is
   * concurrent.
   */
  public <T, R> R run(
      IngestionSpec spec,
      ActorSystem system,
      Function<SourceRow, T> transform,
      Sink<T, CompletionStage<R>> sink)
      throws IOException {
    try (CamelBridge bridge = new CamelBridge()) {
      Source<SourceRow, ?> source = csvRowSource(bridge, spec.source().uri());
      int parallelism = spec.execution().concurrency().preferred();
      CompletionStage<R> resultStage =
          IngestionPipeline.run(source, parallelism, transform, sink, system);
      return resultStage.toCompletableFuture().join();
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("ingestion run failed", e);
    }
  }

  private static Source<SourceRow, ?> csvRowSource(CamelBridge bridge, String uri) {
    Publisher<String> filePublisher = bridge.source(uri, String.class);
    // Camel's file: consumer is a continuously-scheduled poller with no natural "done" signal to
    // forward as onComplete() - left unbounded, a run blocking on stream completion (this
    // pipeline's
    // own design, matching D4's bounded-batch-process goal) would hang forever. take(1) bounds it
    // here instead, independent of Camel's own completion semantics: today's design is one URI =
    // one file, so cancelling upstream after that single file-content element is both correct and
    // what actually stops Camel's polling (a Reactive Streams cancellation propagates to the
    // underlying consumer/route).
    return Source.fromPublisher(filePublisher)
        .take(1)
        .mapConcat(IngestionPipelineRunner::parseCsvRows);
  }

  private static List<SourceRow> parseCsvRows(String fileContent) {
    CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
    List<SourceRow> rows = new ArrayList<>();
    try (CSVParser parser = CSVParser.parse(fileContent, format)) {
      for (CSVRecord record : parser) {
        rows.add(new CsvSourceRow(record));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));

    ActorSystem system = ActorSystem.create("forwardmeasure-data-streaming-ingestion");
    int exitCode = 0;
    try {
      IngestionResult result = new IngestionPipelineRunner().run(spec, system);
      LOGGER.info("IngestionPipelineRunner: processed={}", result.recordsProcessed());
    } catch (Exception e) {
      LOGGER.error("IngestionPipelineRunner: run failed", e);
      exitCode = 1;
    } finally {
      system.terminate();
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("required environment variable '" + name + "' is not set");
    }
    return value.trim();
  }
}
