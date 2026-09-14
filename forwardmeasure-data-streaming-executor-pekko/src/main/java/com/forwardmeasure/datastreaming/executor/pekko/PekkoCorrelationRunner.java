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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.camel.ProducerTemplate;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standalone entrypoint for {@link PekkoCorrelationEngine} - {@code CorrelationSpec}'s {@code
 * engine: pekko} counterpart to {@code SparkCorrelationRunner}'s own {@code CorrelationSpec}
 * handling, added 2026-09-13 so multi-source correlation isn't Spark-only. Same real sink mechanism
 * {@link PekkoIngestionRunner} already proved this session ({@link ProducerTemplate#asyncSendBody(
 * String, Object)} + {@link Sink#foreachAsync}): any {@code SinkSpec.connector} Camel supports
 * works here, via {@link PekkoIngestionRunner#buildSink}. <b>Correction, 2026-09-14</b>: this
 * javadoc originally contrasted that with a "{@code SparkCorrelationRunner}'s own {@code
 * file}-sink-only restriction" - true when this class was first written, but {@code
 * SparkCorrelationRunner} was generalized the same day (2026-09-13) to delegate to {@code
 * SparkSinks#write} (file/kafka/jdbc/ opensearch/any Spark-native format) just like {@code
 * SparkIngestionRunner} does - see that class's own current javadoc. Both correlation runners
 * support the same connector set today; there is no asymmetry to call out here any more.
 */
public final class PekkoCorrelationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PekkoCorrelationRunner.class);

  private PekkoCorrelationRunner() {}

  /** Summary of one correlation run - how many sources were read and how many groups resulted. */
  public record CorrelationResult(long sourceCount, long groupCount) {}

  /**
   * Runs {@code spec}: reads and maps every declared source, correlates them on {@code
   * spec.blockingField()}, merges each group by trust weight (see {@link
   * PekkoCorrelationEngine#correlate}), and writes the merged rows to {@code spec.sink()}.
   */
  public static CorrelationResult run(CorrelationSpec spec, ActorSystem system) throws IOException {
    AtomicLong written = new AtomicLong();
    IngestionPipeline.MalformedRecordPolicy malformedRecordPolicy =
        IngestionPipeline.MalformedRecordPolicy.from(spec.execution().failure());
    try (CamelBridge bridge = new CamelBridge()) {
      List<CompletableFuture<List<PekkoCorrelationEngine.PekkoCorrelationRecord>>> reads =
          spec.sources().stream()
              .map(
                  entry ->
                      PekkoCorrelationEngine.readAndMap(
                              bridge,
                              system,
                              entry.source(),
                              entry.mapper(),
                              spec.blockingField(),
                              entry.sourceKey(),
                              entry.trustWeight(),
                              Map.of(),
                              malformedRecordPolicy)
                          .toCompletableFuture())
              .toList();
      CompletableFuture.allOf(reads.toArray(new CompletableFuture[0])).join();
      List<List<PekkoCorrelationEngine.PekkoCorrelationRecord>> mappedSources =
          reads.stream().map(CompletableFuture::join).toList();

      List<Map<String, Object>> merged = PekkoCorrelationEngine.correlate(mappedSources);

      ObjectMapper objectMapper = new ObjectMapper();
      Sink<Map<String, Object>, CompletionStage<Done>> sink =
          PekkoIngestionRunner.buildSink(
              bridge, spec.sink(), spec.execution(), objectMapper, written);
      Source.from(merged).runWith(sink, system).toCompletableFuture().join();

      return new CorrelationResult(spec.sources().size(), merged.size());
    } catch (CompletionException e) {
      // join() always wraps a stage's real failure here, even one this method otherwise promises
      // to surface directly - unwrap it so callers see the same exception shape regardless of
      // which of this method's several join() calls actually failed. See PekkoIngestionRunner's
      // own identical fix (2026-09-13) for the full reasoning.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof IOException ioException) {
        throw ioException;
      }
      throw new IllegalStateException("correlation run failed", cause != null ? cause : e);
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("correlation run failed", e);
    }
  }

  public static void main(String[] args) throws Exception {
    String specPath = args.length > 0 ? args[0] : requiredEnv("CORRELATION_SPEC_PATH");
    CorrelationSpec spec = CorrelationSpec.load(Path.of(specPath));

    ActorSystem system = ActorSystem.create("forwardmeasure-data-streaming-correlation");
    int exitCode = 0;
    try {
      CorrelationResult result = run(spec, system);
      LOGGER.info(
          "PekkoCorrelationRunner: sourceCount={} groupCount={}",
          result.sourceCount(),
          result.groupCount());
    } catch (Exception e) {
      LOGGER.error("PekkoCorrelationRunner: run failed", e);
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
