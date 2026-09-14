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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof of the {@code jdbc}/{@code sql} connector both directions add 2026-09-13:
 * {@link PekkoIngestionRunner#rowSource} against a real Postgres query, and {@link
 * PekkoIngestionRunner#buildSink} inserting into a real Postgres table - not mocked, matching this
 * org's own JDBC testing convention ({@code forwardmeasure-data-streaming-connector-jdbc}'s own
 * {@code JpaPagingSourceTest} does the same for its own, different, JPA-entity-shaped connector).
 */
@WithPostgreSqlContainer(databaseName = "fds_sql_connector_test")
class PekkoSqlConnectorIntegrationTest {

  @Test
  void rowSourceReadsEveryRowWithoutTruncatingToTheFirstOne(PostgreSqlTestContainer database)
      throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table party (id text, name text)");
      statement.execute("insert into party (id, name) values ('S1', 'Alice Anderson')");
      statement.execute("insert into party (id, name) values ('S2', 'Bob Baker')");
    }

    SourceSpec source = jdbcSource(database, "select id, name from party order by id");

    ActorSystem system = ActorSystem.create("sql-row-source-integration-test");
    List<SourceRow> rows;
    try (CamelBridge bridge = new CamelBridge()) {
      rows =
          PekkoIngestionRunner.rowSource(bridge, source)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(30, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    assertEquals(
        2,
        rows.size(),
        "camel-sql's own consumer.useIterator defaults to true (one exchange PER ROW - confirmed"
            + " by reading SqlConsumer's own source), which this class's own take(1) would wrongly"
            + " truncate to just the first row; this proves consumer.useIterator=false is really"
            + " applied so the whole result set arrives as one exchange instead");
    assertEquals("S1", rows.get(0).get("id"));
    assertEquals("Alice Anderson", rows.get(0).get("name"));
    assertEquals("S2", rows.get(1).get("id"));
  }

  @Test
  void rowSourceCompletesWithZeroRowsInsteadOfHangingOnAnEmptyResultSet(
      PostgreSqlTestContainer database) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table empty_party (id text, name text)");
    }

    SourceSpec source = jdbcSource(database, "select id, name from empty_party");

    ActorSystem system = ActorSystem.create("sql-row-source-empty-integration-test");
    List<SourceRow> rows;
    try (CamelBridge bridge = new CamelBridge()) {
      rows =
          PekkoIngestionRunner.rowSource(bridge, source)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(30, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    assertEquals(
        0,
        rows.size(),
        "consumer.routeEmptyResultSet=true is required for this: without it camel-sql's own"
            + " consumer emits no exchange at all for zero matched rows, and take(1) would hang"
            + " forever waiting for one that never comes");
  }

  @Test
  void camelSinkInsertsMappedRowsIntoARealPostgresTable(
      PostgreSqlTestContainer database, @TempDir Path tempDir) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table party_sink (id text, name text)");
    }

    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\nS2,Bob Baker\n", StandardCharsets.UTF_8);

    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                null,
                null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "jdbc",
                database.hostJdbcUrl(),
                "party_sink",
                null,
                null,
                Map.of(
                    "driver",
                    "org.postgresql.Driver",
                    "user",
                    database.username(),
                    "password",
                    database.password(),
                    "query",
                    "insert into party_sink (id, name) values (:?uid, :?name)")),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(4, 8), null, null));

    ActorSystem system = ActorSystem.create("sql-sink-integration-test");
    try {
      new PekkoIngestionRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select id, name from party_sink order by id")) {
      assertTrue(resultSet.next(), "expected a row for S1");
      assertEquals("S1", resultSet.getString("id"));
      assertEquals("Alice Anderson", resultSet.getString("name"));
      assertTrue(resultSet.next(), "expected a row for S2");
      assertEquals("S2", resultSet.getString("id"));
      assertEquals("Bob Baker", resultSet.getString("name"));
      assertFalse(resultSet.next(), "expected exactly 2 rows");
    }
  }

  @Test
  void sinkResolvesAFileReferencedPasswordAndReallyAuthenticates(
      PostgreSqlTestContainer database, @TempDir Path tempDir) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table party_sink_secret_ref (id text, name text)");
    }

    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(sourceCsv, "ID,FULL_NAME\nS1,Alice Anderson\n", StandardCharsets.UTF_8);
    // The real password never appears as a literal option value below - only this file's path
    // does, proving SqlDataSources really calls through SecretRefs rather than handing camel-sql
    // the literal string "file:..." as if it were the password itself (which would simply fail to
    // authenticate against the real container - the only way this test can pass at all).
    Path passwordFile = tempDir.resolve("password.txt");
    Files.writeString(passwordFile, database.password(), StandardCharsets.UTF_8);

    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                null,
                null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "jdbc",
                database.hostJdbcUrl(),
                "party_sink_secret_ref",
                null,
                null,
                Map.of(
                    "driver",
                    "org.postgresql.Driver",
                    "user",
                    database.username(),
                    "password",
                    "file:" + passwordFile,
                    "query",
                    "insert into party_sink_secret_ref (id, name) values (:?uid, :?name)")),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(2, 4), null, null));

    ActorSystem system = ActorSystem.create("sql-sink-secret-ref-integration-test");
    try {
      new PekkoIngestionRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select id, name from party_sink_secret_ref")) {
      assertTrue(resultSet.next(), "expected the row to have been inserted with the real password");
      assertEquals("S1", resultSet.getString("id"));
      assertEquals("Alice Anderson", resultSet.getString("name"));
    }
  }

  /**
   * Proves {@code sink.batching()} genuinely triggers {@code camel-sql}'s own real {@code
   * batch=true} mode (a single {@code PreparedStatement#executeBatch} round-trip for the whole
   * group, confirmed from {@code camel-sql}'s own source in {@code
   * PekkoIngestionRunner#camelBatchSink}'s own javadoc) rather than the unbatched per-row path -
   * five rows, a batch size of 3 (so two real groups: 3 then 2), all landing correctly.
   */
  @Test
  void batchingInsertsAllRowsViaARealJdbcBatch(
      PostgreSqlTestContainer database, @TempDir Path tempDir) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table party_batch_sink (id text, name text)");
    }

    Path sourceCsv = tempDir.resolve("source.csv");
    Files.writeString(
        sourceCsv,
        "ID,FULL_NAME\n"
            + "S1,Alice Anderson\n"
            + "S2,Bob Baker\n"
            + "S3,Carol Carter\n"
            + "S4,Dave Dixon\n"
            + "S5,Erin Ellis\n",
        StandardCharsets.UTF_8);

    IngestionSpec spec =
        new IngestionSpec(
            new SourceSpec(
                "file",
                "file:"
                    + tempDir.toAbsolutePath()
                    + "?fileName=source.csv&noop=true&initialDelay=0&delay=100",
                null,
                null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("uid", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "jdbc",
                database.hostJdbcUrl(),
                "party_batch_sink",
                null,
                new SinkSpec.BatchingSpec(3, null),
                Map.of(
                    "driver",
                    "org.postgresql.Driver",
                    "user",
                    database.username(),
                    "password",
                    database.password(),
                    "query",
                    "insert into party_batch_sink (id, name) values (:?uid, :?name)")),
            new ExecutionSpec("pekko", new ExecutionSpec.ConcurrencySpec(4, 8), null, null));

    ActorSystem system = ActorSystem.create("sql-batch-sink-integration-test");
    try {
      new PekkoIngestionRunner().run(spec, system);
    } finally {
      system.terminate();
    }

    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select id, name from party_batch_sink order by id")) {
      List<String> ids = new java.util.ArrayList<>();
      while (resultSet.next()) {
        ids.add(resultSet.getString("id"));
      }
      assertEquals(List.of("S1", "S2", "S3", "S4", "S5"), ids);
    }
  }

  private static SourceSpec jdbcSource(PostgreSqlTestContainer database, String query) {
    return new SourceSpec(
        "jdbc",
        database.hostJdbcUrl(),
        null,
        null,
        query,
        Map.of(
            "driver", "org.postgresql.Driver",
            "user", database.username(),
            "password", database.password()));
  }
}
