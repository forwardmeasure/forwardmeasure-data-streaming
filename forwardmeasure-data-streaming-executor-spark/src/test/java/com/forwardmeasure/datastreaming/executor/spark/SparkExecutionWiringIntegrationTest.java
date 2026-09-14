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
package com.forwardmeasure.datastreaming.executor.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof that {@link SparkIngestionRunner#run} genuinely honors {@code
 * execution.failure} on the Spark engine, mirroring {@code PekkoExecutionWiringIntegrationTest}'s
 * own Pekko-side proof. Same real finding applies here too: every real {@code
 * NamedTransformRegistry} function is defensive (never throws on bad data), so a genuinely
 * malformed row in this system is a spec-level mistake (an unregistered transform name), uniform
 * across every row - not per-row bad data.
 */
class SparkExecutionWiringIntegrationTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-execution-wiring-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void malformedRecordSkipLetsTheRunCompleteWithNothingWritten(@TempDir Path tempDir)
      throws Exception {
    IngestionSpec spec = specWithUnregisteredTransform(tempDir, "skip");

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, spec);

    assertEquals(0, result.recordsWritten(), "every row hits the same unregistered transform");
  }

  @Test
  void malformedRecordFailFailsTheWholeRun(@TempDir Path tempDir) throws Exception {
    IngestionSpec spec = specWithUnregisteredTransform(tempDir, "fail");

    assertThrows(Exception.class, () -> SparkIngestionRunner.run(spark, spec));
  }

  @Test
  void sinkFailureFailPropagatesARealWriteFailureRatherThanSilentlySucceeding(@TempDir Path tempDir)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\n", StandardCharsets.UTF_8);

    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec("file", sourceCsv.toString(), null, null),
            new TransformSpec(
                "party",
                List.of(new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null))),
            // No real Postgres listening on this port - a real, deterministic connection failure.
            new SinkSpec(
                "jdbc",
                "jdbc:postgresql://127.0.0.1:1/nonexistent",
                "party_sink",
                null,
                null,
                java.util.Map.of("driver", "org.postgresql.Driver", "user", "x", "password", "x")),
            new ExecutionSpec("spark", null, null, new ExecutionSpec.FailureSpec(null, "fail")));

    assertThrows(Exception.class, () -> SparkIngestionRunner.run(spark, spec));
  }

  /**
   * The other half of sink-failure wiring, added 2026-09-14 once a direct side-by-side comparison
   * with {@code PekkoExecutionWiringIntegrationTest} surfaced that this class only ever proved
   * {@code fail} genuinely propagating a real, permanently-broken connection - nothing proved
   * {@code retry} actually recovering from a real transient failure the way the Pekko side's own
   * {@code sinkFailureRetryEventuallySucceedsAgainstARealFlakyCamelRoute} does (a real flaky
   * in-process Camel route there; a real flaky in-process HTTP server here, since Spark's own
   * {@code opensearch} sink has no Camel route to install into - {@link SparkSinks#write} is called
   * directly, the same "call the sink-building code directly" pattern the Pekko test uses via
   * {@code PekkoIngestionRunner#buildSink}).
   */
  @Test
  void sinkFailureRetryEventuallySucceedsAgainstARealFlakyHttpServer() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            if (attempts.incrementAndGet() < 3) {
              exchange.sendResponseHeaders(500, -1);
            } else {
              byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, body.length);
              exchange.getResponseBody().write(body);
            }
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      SinkSpec sink =
          new SinkSpec(
              "opensearch",
              "http://localhost:" + server.getAddress().getPort(),
              "flaky-index",
              null,
              null,
              Map.of("idField", "uid"));
      ExecutionSpec execution =
          new ExecutionSpec("spark", null, null, new ExecutionSpec.FailureSpec(null, "retry"));

      JavaRDD<Map<String, Object>> mapped =
          new JavaSparkContext(spark.sparkContext())
              .parallelize(List.of(Map.of("uid", "S1", "name", "Alice Anderson")));
      SparkSinks.write(spark, mapped, sink, execution, List.of());
    } finally {
      server.stop(0);
    }

    assertEquals(3, attempts.get(), "must have retried twice before the 3rd attempt succeeded");
  }

  private static IngestionSpec specWithUnregisteredTransform(Path tempDir, String malformedRecord)
      throws Exception {
    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);
    Path outputDir = tempDir.resolve("output");

    return new IngestionSpec(
        new SourceSpec("file", sourceCsv.toString(), null, null),
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                new TransformSpec.FieldRule(
                    "name",
                    "FULL_NAME",
                    null,
                    null,
                    "this_transform_was_never_registered",
                    null,
                    null))),
        new SinkSpec("file", outputDir.toString(), "n/a", null, null),
        new ExecutionSpec(
            "spark", null, null, new ExecutionSpec.FailureSpec(malformedRecord, null)));
  }
}
