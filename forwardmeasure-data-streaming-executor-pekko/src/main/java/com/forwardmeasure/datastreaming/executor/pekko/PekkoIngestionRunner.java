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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.OpenSearchIndexInitializer;
import com.forwardmeasure.datastreaming.api.SecretRefs;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.sql.SqlConstants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pekko-backed replacement for {@code SimpleSourceIngestionWorker}'s sequential {@code for}
 * loop: reads a source through {@link CamelBridge}, splits it into rows, and processes rows with
 * real, bounded concurrency via {@link IngestionPipeline} - not one record at a time.
 *
 * <p>Source dispatch (see {@link #rowSource}): {@code file} sources dispatch on {@code
 * format.type()} (csv/json today - see {@link #parseTextRows}); {@code jdbc}/{@code sql} sources go
 * through {@code camel-sql} instead, whose rows already arrive Map-shaped, needing no text parsing
 * at all (see {@link #sqlRowSource}). Sink dispatch (see {@link #buildSink}): a {@code jdbc}/{@code
 * sql} sink sends the mapped row as a raw {@code Map} (matching {@code camel-sql}'s own
 * named-parameter binding, which reads named parameters from the body when it converts to a {@code
 * Map} - confirmed by reading {@code camel-sql}'s own {@code SqlHelper#lookupParameter}, not
 * assumed); every other connector still gets the JSON-string body every Camel producer component
 * reliably accepts regardless of which one it is.
 *
 * <p><b>{@code execution.flowControl}/{@code execution.failure}/{@code sink.batching}, wired for
 * real 2026-09-13</b> - see {@link IngestionPipeline}'s own javadoc for the two real, previously-
 * silent bugs this closes (a single malformed record or transient sink failure used to crash the
 * whole run). {@code sink.batching} is real for {@code jdbc}/{@code sql} (a genuine wire-level JDBC
 * batch INSERT via {@code camel-sql}'s own {@code batch=true} mode - see {@link #camelBatchSink});
 * for every other connector (no bulk wire API exists in this codebase), it's real *pacing* instead
 * - a whole group's rows dispatched together, the next group waiting for the current one - not a
 * fake bulk call.
 *
 * <p>{@link #run(IngestionSpec, ActorSystem)} writes to a real sink (added 2026-09-13, once the
 * launcher work surfaced that it never had one - see {@link SinkSpec#uri()}'s own javadoc).
 * Deliberately not {@link CamelBridge}'s own reactive-streams-{@code Subscriber}-based sink helper
 * - that one silently drops in-flight sends when the run's own {@code CamelContext} stops
 * (confirmed by a real test failure the same day, then by reading Camel's own source; see {@code
 * CamelBridge}'s own javadoc) - {@code asyncSendBody}'s real per-item {@code CompletableFuture} is
 * what let this class's own {@link Sink#foreachAsync} genuinely wait for every send to finish, not
 * just to have been handed off.
 */
public final class PekkoIngestionRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoIngestionRunner.class);
  private static final String JDBC_CONNECTOR = "jdbc";
  private static final String SQL_CONNECTOR = "sql";
  private static final String KAFKA_CONNECTOR = "kafka";
  private static final String OPENSEARCH_CONNECTOR = "opensearch";
  private static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(1);

  /**
   * A real, mapped row's own INSERT query (this class's own named-parameter convention - {@code
   * :?fieldName}) almost always contains a literal {@code ?} character - which cannot safely appear
   * anywhere in a Camel endpoint URI string, confirmed empirically (not assumed): {@code
   * DefaultComponent#createEndpoint} and {@code SqlComponent#useRawUri()==false} together mean
   * Camel resolves both the endpoint's own "path" (the query text) and its parameters by splitting
   * the URI's *decoded* scheme-specific part on its first {@code ?} - so a query-owned {@code ?} is
   * indistinguishable from the endpoint-options separator, silently truncating the query and
   * corrupting the options (this is exactly what broke {@code camelSink}/{@code camelBatchSink}
   * until 2026-09-13; percent-encoding the query's own {@code ?} as {@code %3F} does not help
   * either - {@code URISupport#prepareQuery} decodes the scheme-specific part right back to a
   * literal {@code ?} before doing that same first-{@code ?} split). The real, Camel- documented
   * way around this: put a harmless, {@code ?}-free placeholder here as the endpoint's own
   * (never-executed) query, and supply the real query per-message via the {@code CamelSqlQuery}
   * exchange header instead (confirmed real and used exactly this way by reading {@code
   * SqlProducer#process}'s own bytecode - the header, when present, takes precedence over the
   * endpoint's query and is run through the exact same named-parameter {@code
   * SqlPrepareStatementStrategy}, so batching/named-parameter binding both keep working unchanged).
   */
  private static final String SQL_QUERY_URI_PLACEHOLDER = "unused";

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
      Source<SourceRow, ?> source = rowSource(bridge, spec.source());
      Sink<Map<String, Object>, CompletionStage<Done>> sink =
          buildSink(bridge, spec.sink(), spec.execution(), objectMapper, processed);
      CompletionStage<Done> resultStage =
          IngestionPipeline.run(
              source, spec.execution(), row -> engine.map(row, spec.mapper()), sink, system);
      resultStage.toCompletableFuture().join();
    } catch (CompletionException e) {
      // join() always wraps a stage's real failure here, even one this method otherwise promises
      // to surface directly (malformedRecord=fail's own IllegalStateException, for example) -
      // unwrap it so callers see the same exception shape whether a run failed synchronously
      // (route creation, before any stream even starts) or asynchronously (mid-stream).
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof IOException ioException) {
        throw ioException;
      }
      throw new IllegalStateException("ingestion run failed", cause != null ? cause : e);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("ingestion run failed", e);
    }
    return new IngestionResult(processed.get());
  }

  /**
   * Builds the real Camel-backed sink for {@code sink}, honoring {@code execution}'s own {@code
   * failure.sinkFailure} (retry policy, see {@link IngestionPipeline#withSinkFailureHandling}) and
   * {@code sink.batching()}. Always returns a {@code Sink<Map<String,Object>, ...>} - batching,
   * when configured, is entirely internal (a {@link Flow#groupedWithin} composed onto {@link
   * #camelBatchSink} via {@link Flow#toMat}), not a different shape the caller needs to know about.
   *
   * <p>Package-visible (not {@code private}) so {@link PekkoCorrelationRunner} can reuse it as-is.
   */
  static Sink<Map<String, Object>, CompletionStage<Done>> buildSink(
      CamelBridge bridge,
      SinkSpec sink,
      ExecutionSpec execution,
      ObjectMapper objectMapper,
      AtomicLong processed) {
    if (OPENSEARCH_CONNECTOR.equals(sink.connector())) {
      OpenSearchIndexInitializer.ensureIndex(sink.uri(), sink.index(), sink.options());
    }
    int parallelism = IngestionPipeline.effectiveParallelism(execution.concurrency());
    IngestionPipeline.SinkFailurePolicy sinkFailurePolicy =
        IngestionPipeline.SinkFailurePolicy.from(execution.failure());
    SinkSpec.BatchingSpec batching = sink.batching();
    if (batching == null || batching.maxRecords() == null) {
      return camelSink(bridge, sink, objectMapper, processed, parallelism, sinkFailurePolicy);
    }
    int maxRecords = batching.maxRecords();
    Duration maxWait = batching.maxWait() != null ? batching.maxWait() : DEFAULT_MAX_WAIT;
    Sink<List<Map<String, Object>>, CompletionStage<Done>> batchSink =
        camelBatchSink(bridge, sink, objectMapper, processed, parallelism, sinkFailurePolicy);
    return Flow.<Map<String, Object>>create()
        .groupedWithin(maxRecords, maxWait)
        .toMat(batchSink, Keep.right());
  }

  /**
   * {@code Sink.foreachAsync} only signals its own overall completion once every element's own
   * {@code CompletableFuture} has actually resolved. {@code parallelism} bounds how many sends are
   * in flight at once, matching the transform stage's own concurrency rather than serializing every
   * send. Each send goes through {@link IngestionPipeline#withSinkFailureHandling} so a transient
   * failure retries (or fails the run) per {@code sinkFailurePolicy}, never silently swallowed.
   */
  private static Sink<Map<String, Object>, CompletionStage<Done>> camelSink(
      CamelBridge bridge,
      SinkSpec sink,
      ObjectMapper objectMapper,
      AtomicLong processed,
      int parallelism,
      IngestionPipeline.SinkFailurePolicy sinkFailurePolicy) {
    ProducerTemplate producerTemplate = bridge.camelContext().createProducerTemplate();
    if (isSqlConnector(sink.connector())) {
      String dataSourceName = SqlDataSources.register(bridge, sink.uri(), sink.options());
      String query = requiredOption(sink.options(), "query");
      String sqlUri = "sql:" + SQL_QUERY_URI_PLACEHOLDER + "?dataSource=#" + dataSourceName;
      return Sink.foreachAsync(
          parallelism,
          row -> {
            processed.incrementAndGet();
            // Raw Map body, deliberately not JSON-serialized: camel-sql's own named-parameter
            // binding (`:?fieldName`) resolves each parameter by converting the exchange body to a
            // Map and looking up the field name directly - a JSON string wouldn't bind at all. The
            // real query goes on the CamelSqlQuery header, not the endpoint URI - see
            // SQL_QUERY_URI_PLACEHOLDER's own javadoc for why.
            return IngestionPipeline.withSinkFailureHandling(
                sinkFailurePolicy,
                () ->
                    producerTemplate
                        .asyncSend(
                            sqlUri,
                            exchange -> {
                              exchange.getIn().setHeader(SqlConstants.SQL_QUERY, query);
                              exchange.getIn().setBody(row);
                            })
                        .<Void>thenApply(ignored -> null));
          });
    }
    return Sink.foreachAsync(
        parallelism,
        row -> {
          processed.incrementAndGet();
          return IngestionPipeline.withSinkFailureHandling(
              sinkFailurePolicy, () -> sendOneRow(sink, producerTemplate, objectMapper, row));
        });
  }

  /**
   * Batched sink counterpart to {@link #camelSink} - {@code jdbc}/{@code sql} gets a real,
   * single-wire-call JDBC batch INSERT: {@code camel-sql}'s own {@code batch=true} mode converts
   * the whole group's {@code List<Map<String,Object>>} body into an {@code Iterator} internally and
   * executes it via a single {@code PreparedStatement#executeBatch} round-trip (confirmed from
   * {@code camel-sql}'s own {@code SqlProducer}/{@code populateStatement} source, not assumed).
   * Every other connector has no bulk wire API in this codebase at all (no OpenSearch {@code _bulk}
   * integration, no multi-record Kafka/file send call) - for those, "batching" means real pacing
   * instead: the whole group's rows are dispatched together (bounded by {@code parallelism}) and
   * the sink only asks for the next group once every row in the current one has completed (or
   * failed, per {@code sinkFailurePolicy}) - a genuinely different backpressure shape from the
   * unbatched sink's continuous per-row stream, even though the wire call itself is still one send
   * per row.
   */
  private static Sink<List<Map<String, Object>>, CompletionStage<Done>> camelBatchSink(
      CamelBridge bridge,
      SinkSpec sink,
      ObjectMapper objectMapper,
      AtomicLong processed,
      int parallelism,
      IngestionPipeline.SinkFailurePolicy sinkFailurePolicy) {
    ProducerTemplate producerTemplate = bridge.camelContext().createProducerTemplate();
    if (isSqlConnector(sink.connector())) {
      String dataSourceName = SqlDataSources.register(bridge, sink.uri(), sink.options());
      String query = requiredOption(sink.options(), "query");
      String sqlBatchUri =
          "sql:" + SQL_QUERY_URI_PLACEHOLDER + "?dataSource=#" + dataSourceName + "&batch=true";
      return Sink.foreachAsync(
          parallelism,
          batch -> {
            processed.addAndGet(batch.size());
            return IngestionPipeline.withSinkFailureHandling(
                sinkFailurePolicy,
                () ->
                    producerTemplate
                        .asyncSend(
                            sqlBatchUri,
                            exchange -> {
                              exchange.getIn().setHeader(SqlConstants.SQL_QUERY, query);
                              exchange.getIn().setBody(batch);
                            })
                        .<Void>thenApply(ignored -> null));
          });
    }
    return Sink.foreachAsync(
        parallelism,
        batch -> {
          List<CompletableFuture<Void>> sends = new ArrayList<>();
          for (Map<String, Object> row : batch) {
            processed.incrementAndGet();
            sends.add(
                IngestionPipeline.withSinkFailureHandling(
                        sinkFailurePolicy,
                        () -> sendOneRow(sink, producerTemplate, objectMapper, row))
                    .toCompletableFuture());
          }
          return CompletableFuture.allOf(sends.toArray(new CompletableFuture[0]));
        });
  }

  /** One row's own send, shared by both the unbatched and batched (per-row-fan-out) sink paths. */
  private static CompletionStage<Void> sendOneRow(
      SinkSpec sink,
      ProducerTemplate producerTemplate,
      ObjectMapper objectMapper,
      Map<String, Object> row) {
    if (OPENSEARCH_CONNECTOR.equals(sink.connector())) {
      String idField = requiredOption(sink.options(), "idField");
      Object idValue = row.get(idField);
      if (idValue == null) {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(
            new IllegalArgumentException(
                "opensearch sink: a row is missing its own id field '" + idField + "'"));
        return failed;
      }
      String docUri = openSearchDocumentUri(sink.uri(), sink.index(), idValue, sink.options());
      String json = writeValueAsString(row, objectMapper);
      return producerTemplate
          .asyncSend(
              docUri,
              exchange -> {
                exchange.getIn().setBody(json);
                exchange.getIn().setHeader(Exchange.HTTP_METHOD, "PUT");
                exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
              })
          .thenApply(ignored -> null);
    }
    String sinkUri = sink.uri();
    String json = writeValueAsString(row, objectMapper);
    return producerTemplate.asyncSendBody(sinkUri, json).thenApply(ignored -> null);
  }

  /**
   * Builds a real, per-row {@code camel-http} PUT endpoint for one OpenSearch document - not a
   * fixed {@code sinkUri} the way every other connector uses, since indexing a document by id (not
   * appending to a stream) needs {@code {baseUrl}/{index}/_doc/{id}} to vary per row. Camel has no
   * dedicated OpenSearch component (the Elasticsearch one is version-coupled to a specific
   * Elasticsearch Java client, not proven compatible with OpenSearch's own fork - not assumed
   * compatible without evidence), so this goes through {@code camel-http} directly against
   * OpenSearch's own plain REST API, the same one every OpenSearch client ultimately calls anyway.
   * {@code sink.options()}' {@code user}/{@code password} (present only when the target cluster has
   * security enabled) become HTTP Basic auth via {@code camel-http}'s own {@code authUsername}/
   * {@code authPassword}/{@code authMethod} endpoint options - confirmed real, current field names
   * by reading {@code HttpCommonEndpoint}'s own source, not assumed.
   */
  private static String openSearchDocumentUri(
      String baseUrl, String index, Object idValue, Map<String, String> options) {
    String id = URLEncoder.encode(String.valueOf(idValue), StandardCharsets.UTF_8);
    StringBuilder uri =
        new StringBuilder(baseUrl).append('/').append(index).append("/_doc/").append(id);
    String user = SecretRefs.resolve(options, "user");
    String password = SecretRefs.resolve(options, "password");
    if (user != null && password != null) {
      uri.append("?authMethod=Basic&authUsername=")
          .append(URLEncoder.encode(user, StandardCharsets.UTF_8))
          .append("&authPassword=")
          .append(URLEncoder.encode(password, StandardCharsets.UTF_8));
    }
    return uri.toString();
  }

  /** A trailing newline makes the output genuinely NDJSON - one complete record per line. */
  static String writeValueAsString(Map<String, Object> row, ObjectMapper objectMapper) {
    try {
      return objectMapper.writeValueAsString(row) + "\n";
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Runs {@code spec}'s source through {@code transform} and {@code sink} with real concurrency
   * bounded by {@code spec.execution()}. Blocks the calling thread until the run completes -
   * matching {@code main()}'s own needs - even though row processing itself is concurrent. The
   * caller owns {@code sink} entirely here (a test/observation escape hatch), so {@code
   * sink.batching()} does not apply - only {@link #run(IngestionSpec, ActorSystem)}'s own real
   * Camel sink does.
   */
  public <T, R> R run(
      IngestionSpec spec,
      ActorSystem system,
      Function<SourceRow, T> transform,
      Sink<T, CompletionStage<R>> sink)
      throws IOException {
    try (CamelBridge bridge = new CamelBridge()) {
      Source<SourceRow, ?> source = rowSource(bridge, spec.source());
      CompletionStage<R> resultStage =
          IngestionPipeline.run(source, spec.execution(), transform, sink, system);
      return resultStage.toCompletableFuture().join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof IOException ioException) {
        throw ioException;
      }
      throw new IllegalStateException("ingestion run failed", cause != null ? cause : e);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("ingestion run failed", e);
    }
  }

  /**
   * Dispatches on {@code source.connector()}: {@code jdbc}/{@code sql} goes through {@link
   * #sqlRowSource} (rows already Map-shaped, no text parsing); everything else goes through {@link
   * #textRowSource} (one Camel exchange's whole body is text, split into rows by {@code
   * source.format().type()}). Package-visible (not {@code private}) so {@link
   * PekkoCorrelationEngine} can reuse it as-is.
   */
  static Source<SourceRow, ?> rowSource(CamelBridge bridge, SourceSpec source) {
    if (isSqlConnector(source.connector())) {
      return sqlRowSource(bridge, source);
    }
    if (KAFKA_CONNECTOR.equals(source.connector())) {
      return kafkaRowSource(bridge, source);
    }
    return textRowSource(bridge, source);
  }

  private static boolean isSqlConnector(String connector) {
    return JDBC_CONNECTOR.equals(connector) || SQL_CONNECTOR.equals(connector);
  }

  /**
   * Unlike {@code file} (one exchange, the whole content) or {@code jdbc}/{@code sql} (one
   * exchange, the whole result set, once {@code consumer.useIterator=false} is set), Camel's kafka
   * consumer is a genuinely unbounded polling route with no built-in "read a snapshot and stop"
   * notion at all - each message is its own exchange, arriving whenever it arrives. So this
   * connector requires an explicit {@code maxMessages} entry in {@code options} (not a sensible
   * default the way {@code header=true} is for csv): {@code take(maxMessages)} is this run's own
   * bounded-read policy, the same reactive-streams-cancellation-stops-the-route mechanism {@link
   * #textRowSource} already relies on, just parameterized by count instead of "one file, one read".
   * Each message body is parsed as one JSON object directly (a Kafka message is already one
   * discrete unit, unlike a file's "whole content in one exchange that then needs splitting") -
   * {@code format.type()} defaults to {@code json} here, not {@code csv}, since a delimited-text
   * message body isn't a realistic Kafka payload shape.
   */
  private static Source<SourceRow, ?> kafkaRowSource(CamelBridge bridge, SourceSpec source) {
    String maxMessagesValue = source.options().get("maxMessages");
    if (maxMessagesValue == null || maxMessagesValue.isBlank()) {
      throw new IllegalArgumentException(
          "PekkoIngestionRunner: a 'kafka' source needs a 'maxMessages' entry in its own options"
              + " map - Camel's own kafka consumer has no built-in 'read a snapshot and stop'"
              + " semantics, so this run needs an explicit bound on how many messages make up one"
              + " complete read");
    }
    int maxMessages = Integer.parseInt(maxMessagesValue.strip());
    Publisher<String> messagePublisher = bridge.source(source.uri(), String.class);
    return Source.fromPublisher(messagePublisher)
        .take(maxMessages)
        .map(PekkoIngestionRunner::parseOneJsonRow);
  }

  private static SourceRow parseOneJsonRow(String body) {
    try {
      Map<String, Object> row =
          new ObjectMapper().readValue(body, new TypeReference<Map<String, Object>>() {});
      return new MapSourceRow(row);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Source<SourceRow, ?> textRowSource(CamelBridge bridge, SourceSpec source) {
    Publisher<String> contentPublisher = bridge.source(source.uri(), String.class);
    String formatType = source.format() != null ? source.format().type() : "csv";
    // Camel's file: consumer is a continuously-scheduled poller with no natural "done" signal to
    // forward as onComplete() - left unbounded, a run blocking on stream completion (this
    // pipeline's own design, matching D4's bounded-batch-process goal) would hang forever. take(1)
    // bounds it here instead, independent of Camel's own completion semantics: today's design is
    // one URI = one file, so cancelling upstream after that single file-content element is both
    // correct and what actually stops Camel's polling (a Reactive Streams cancellation propagates
    // to the underlying consumer/route). Character encoding is deliberately not this class's own
    // concern - it's already a real, standard camel-file endpoint option (e.g. {@code
    // file:dir?fileName=x.csv&charset=ISO-8859-1}), applied by Camel itself during its own
    // byte[]->String body conversion before this method ever sees the content.
    return Source.fromPublisher(contentPublisher)
        .take(1)
        .mapConcat(content -> parseTextRows(content, formatType, source.options()));
  }

  /**
   * Format-driven, not connector-driven: adding a format needs one new small parser function
   * registered here by format name, not a new connector class. Deliberately small (csv/json today)
   * - unlike {@code camel-sql} rows (already Map-shaped, no parsing needed at all, see {@link
   * #sqlRowSource}), turning raw file bytes into rows always needs *some* format-specific code
   * somewhere; the fix here is dispatching on {@code format.type()} instead of always assuming CSV,
   * not eliminating that code entirely.
   */
  private static List<SourceRow> parseTextRows(
      String content, String formatType, Map<String, String> options) {
    return switch (formatType) {
      case "csv" -> parseCsvRows(content, options);
      case "json", "ndjson", "jsonl" -> parseJsonRows(content);
      default ->
          throw new IllegalArgumentException(
              "PekkoIngestionRunner: unsupported source format '"
                  + formatType
                  + "' - only csv/json/ndjson today");
    };
  }

  /**
   * {@code delimiter} (added 2026-09-14, a real gap: the real WorldCheck reference-population
   * export - see {@code entity-intelligence-specifications}' own checked-in {@code
   * schema-worldcheck-reference-population.yaml} - is genuinely tab-delimited, not comma, and this
   * method previously had no way to express that at all) is a single character in {@code
   * source.options()}, same generic per-source metadata bag every other connector's own options
   * already use - not a new typed {@code FormatSpec} field, for the same "metadata over a bespoke
   * Java field" reason {@code SourceSpec#options}'s own javadoc gives. Defaults to comma,
   * preserving every existing spec that never set it.
   */
  private static List<SourceRow> parseCsvRows(String fileContent, Map<String, String> options) {
    String delimiterOption = options.get("delimiter");
    char delimiter =
        delimiterOption == null || delimiterOption.isEmpty() ? ',' : delimiterOption.charAt(0);
    CSVFormat format =
        CSVFormat.DEFAULT
            .builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setDelimiter(delimiter)
            .get();
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

  /**
   * Accepts both shapes: a single JSON array of objects, or newline-delimited JSON (one object per
   * line) - decided by the content's own leading character, not a further format-name distinction,
   * since both are unambiguously "json" to a spec author and the two real shapes in practice.
   */
  private static List<SourceRow> parseJsonRows(String content) {
    ObjectMapper mapper = new ObjectMapper();
    List<SourceRow> rows = new ArrayList<>();
    String trimmed = content.strip();
    try {
      if (trimmed.startsWith("[")) {
        List<Map<String, Object>> parsed =
            mapper.readValue(trimmed, new TypeReference<List<Map<String, Object>>>() {});
        for (Map<String, Object> row : parsed) {
          rows.add(new MapSourceRow(row));
        }
      } else {
        for (String line : trimmed.lines().toList()) {
          if (line.isBlank()) {
            continue;
          }
          Map<String, Object> row =
              mapper.readValue(line, new TypeReference<Map<String, Object>>() {});
          rows.add(new MapSourceRow(row));
        }
      }
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  /**
   * {@code camel-sql}'s own consumer already hands back {@code List<Map<String,Object>>} rows - no
   * text parsing needed at all, unlike {@link #textRowSource}. {@code useIterator=false} (default
   * is {@code true} - that default would instead emit *one exchange per row*, which this method's
   * own {@code take(1)} would then wrongly truncate to just the first row) makes the whole result
   * set arrive as one exchange whose body is the full {@code List}, matching every other source in
   * this class ("one URI = one bounded read"). {@code routeEmptyResultSet=true} (default {@code
   * false}) is needed for the same reason a real query can legitimately match zero rows: without
   * it, an empty result produces no exchange at all and this method's {@code take(1)} would hang
   * forever waiting for one that never comes. Neither option takes a {@code consumer.} prefix -
   * confirmed 2026-09-13 from {@code camel-sql}'s own real {@code sql.json} component metadata
   * (only options like {@code exceptionHandler} that explicitly declare their own {@code
   * optionalPrefix: "consumer."} accept that prefix; {@code useIterator}/{@code
   * routeEmptyResultSet} do not, so the earlier {@code consumer.useIterator}/{@code
   * consumer.routeEmptyResultSet} spelling here was rejected outright as unknown parameters at
   * endpoint-resolution time - a real bug in this method, not a mistaken assumption about the query
   * text this method builds).
   *
   * <p>This method's own query text (a plain {@code SELECT}, never {@code :?}-parameterized in this
   * codebase today) is safe to embed directly in the endpoint URI - unlike {@link #camelSink}/
   * {@link #camelBatchSink}'s own INSERT queries, see {@link #SQL_QUERY_URI_PLACEHOLDER}'s javadoc
   * for why a query containing a literal {@code ?} cannot be.
   */
  private static Source<SourceRow, ?> sqlRowSource(CamelBridge bridge, SourceSpec source) {
    String dataSourceName = SqlDataSources.register(bridge, source.uri(), source.options());
    String query = source.query();
    if (query == null || query.isBlank()) {
      throw new IllegalArgumentException(
          "PekkoIngestionRunner: a 'jdbc'/'sql' source needs a non-blank query() - the SQL text to"
              + " run");
    }
    String sqlUri =
        "sql:"
            + query
            + "?dataSource=#"
            + dataSourceName
            + "&useIterator=false&routeEmptyResultSet=true";
    @SuppressWarnings("rawtypes")
    Publisher<List> resultPublisher = bridge.source(sqlUri, List.class);
    return Source.fromPublisher(resultPublisher)
        .take(1)
        .mapConcat(PekkoIngestionRunner::toSourceRows);
  }

  @SuppressWarnings("unchecked")
  private static List<SourceRow> toSourceRows(List<?> rawRows) {
    List<SourceRow> rows = new ArrayList<>();
    for (Object rawRow : rawRows) {
      rows.add(new MapSourceRow((Map<String, Object>) rawRow));
    }
    return rows;
  }

  private static String requiredOption(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "PekkoIngestionRunner: a 'jdbc'/'sql' sink needs a non-blank '"
              + key
              + "' entry in its own options map");
    }
    return value;
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("INGESTION_SPEC_PATH");
    IngestionSpec spec = IngestionSpec.load(Path.of(specPath));

    ActorSystem system = ActorSystem.create("forwardmeasure-data-streaming-ingestion");
    int exitCode = 0;
    try {
      IngestionResult result = new PekkoIngestionRunner().run(spec, system);
      LOGGER.info("PekkoIngestionRunner: processed={}", result.recordsProcessed());
    } catch (Exception e) {
      LOGGER.error("PekkoIngestionRunner: run failed", e);
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
