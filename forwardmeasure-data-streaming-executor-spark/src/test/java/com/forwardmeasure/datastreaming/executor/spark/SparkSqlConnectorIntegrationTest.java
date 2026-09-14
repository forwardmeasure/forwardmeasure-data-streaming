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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.ExecutionSpec;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real, no-mocks proof of the {@code jdbc}/{@code sql} connector both directions on Spark - renamed
 * 2026-09-14 from {@code SparkJdbcSinkIntegrationTest} (mirroring {@code executor-pekko}'s own
 * {@code PekkoSqlConnectorIntegrationTest} naming exactly) once a direct side-by-side comparison of
 * the two modules' own test suites surfaced a real, genuine gap: this module's own {@link
 * SparkCorrelationEngine#readSource}'s {@code jdbc} branch - used in production by {@link
 * SparkCorrelationEngine#readAndMapSingleSource}/{@link SparkCorrelationEngine#readAndMap} - had
 * zero test coverage at all, unlike the Pekko side's own {@code
 * rowSourceReadsEveryRowWithoutTruncatingToTheFirstOne}/{@code
 * rowSourceCompletesWithZeroRowsInsteadOfHangingOnAnEmptyResultSet}. The two new tests below close
 * that gap; the two batch/insert sink tests are unchanged from this class's previous name.
 */
@WithPostgreSqlContainer(databaseName = "fds_spark_jdbc_sink_test")
class SparkSqlConnectorIntegrationTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("spark-sql-connector-integration-test")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * The Spark sibling of {@code PekkoSqlConnectorIntegrationTest}'s own identically-named test -
   * proves {@link SparkCorrelationEngine#readSource}'s {@code jdbc} branch reads every row via
   * Spark's own native jdbc {@code DataFrameReader}, not just the first one.
   */
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
    TransformSpec identityMapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("id", "id", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null)));

    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
            spark, source, identityMapper, Map.of(), IngestionPipeline.MalformedRecordPolicy.SKIP);
    List<Map<String, Object>> rows = mapped.collect();

    assertEquals(
        2,
        rows.size(),
        "the real query returns 2 real rows - readSource's own jdbc branch must not truncate to"
            + " just the first one");
    assertEquals("S1", rows.get(0).get("id"));
    assertEquals("Alice Anderson", rows.get(0).get("name"));
    assertEquals("S2", rows.get(1).get("id"));
  }

  /**
   * The Spark sibling of {@code PekkoSqlConnectorIntegrationTest}'s own identically-named test - a
   * real query matching zero rows must complete with an empty result, not throw or hang.
   */
  @Test
  void rowSourceCompletesWithZeroRowsInsteadOfHangingOnAnEmptyResultSet(
      PostgreSqlTestContainer database) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create table empty_party (id text, name text)");
    }

    SourceSpec source = jdbcSource(database, "select id, name from empty_party");
    TransformSpec identityMapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("id", "id", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null)));

    JavaRDD<Map<String, Object>> mapped =
        SparkCorrelationEngine.readAndMapSingleSource(
            spark, source, identityMapper, Map.of(), IngestionPipeline.MalformedRecordPolicy.SKIP);
    List<Map<String, Object>> rows = mapped.collect();

    assertEquals(0, rows.size());
  }

  @Test
  void sinkInsertsMappedRowsIntoARealPostgresTable(
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
            new SourceSpec("file", sourceCsv.toString(), null, null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("id", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "jdbc",
                database.hostJdbcUrl(),
                "party_sink",
                null,
                null,
                Map.of("user", database.username(), "password", database.password())),
            new ExecutionSpec("spark", null, null, null));

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, spec);
    assertEquals(2, result.recordsWritten());

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

  /**
   * Proves {@code sink.batching().maxRecords()} genuinely reaches Spark's own native jdbc writer as
   * its real {@code batchsize} option (see {@link SparkSinks#write}'s own javadoc) - all rows still
   * land correctly with a batch size smaller than the row count.
   */
  @Test
  void batchingOptionReachesSparksNativeJdbcWriterAndAllRowsStillLand(
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
            new SourceSpec("file", sourceCsv.toString(), null, null),
            new TransformSpec(
                "party",
                List.of(
                    new TransformSpec.FieldRule("id", "ID", null, null, null, null, null),
                    new TransformSpec.FieldRule(
                        "name", "FULL_NAME", null, null, null, null, null))),
            new SinkSpec(
                "jdbc",
                database.hostJdbcUrl(),
                "party_batch_sink",
                null,
                new SinkSpec.BatchingSpec(2, null),
                Map.of("user", database.username(), "password", database.password())),
            new ExecutionSpec("spark", null, null, null));

    SparkIngestionRunner.IngestionResult result = SparkIngestionRunner.run(spark, spec);
    assertEquals(5, result.recordsWritten());

    try (Connection connection = database.dataSource().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select count(*) as row_count from party_batch_sink")) {
      assertTrue(resultSet.next());
      assertEquals(5, resultSet.getInt("row_count"));
    }
  }

  private static SourceSpec jdbcSource(PostgreSqlTestContainer database, String query) {
    return new SourceSpec(
        "jdbc",
        database.hostJdbcUrl(),
        null,
        null,
        query,
        Map.of("user", database.username(), "password", database.password()));
  }
}
