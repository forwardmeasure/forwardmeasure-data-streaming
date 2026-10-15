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
package com.forwardmeasure.datastreaming.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
import com.forwardmeasure.testcontainers.cassandra.CassandraTestContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The journal FDS keeps continuous-run state on, provisioned in real PostgreSQL and Cassandra. */
class DataStreamingMigrationsMainTest {

  @Test
  void createsTheRuntimeRoleAndEachTenantsJournalSchemaIdempotently() throws Exception {
    try (PostgreSqlTestContainer postgres = new PostgreSqlTestContainer().start()) {
      // fowf's migrations, which run first in the platform migrations stage, create this.
      new com.forwardmeasure.jpa.liquibase.TenantRegistry(postgres.dataSource()).migrate();
      Map<String, String> environment =
          Map.of(
              "DATA_STREAMING_CONTROL_PLANE_DATABASE_URL",
              postgres.hostJdbcUrl(),
              "DATA_STREAMING_ADMIN_DATABASE_USERNAME",
              postgres.username(),
              "DATA_STREAMING_ADMIN_DATABASE_PASSWORD",
              postgres.password(),
              "DATA_STREAMING_RUNTIME_DATABASE_USERNAME",
              "datastreaming",
              "DATA_STREAMING_RUNTIME_DATABASE_PASSWORD",
              "runtime-secret",
              "DATA_STREAMING_TENANTS",
              "acme:Acme,globex:Globex");

      DataStreamingMigrationsMain.run(environment::get);
      DataStreamingMigrationsMain.run(environment::get);

      for (String alias : new String[] {"acme", "globex"}) {
        String tenantUrl =
            DataStreamingMigrationsMain.databaseUrlPrefix(postgres.hostJdbcUrl())
                + TenantDatabase.forAlias(alias).value();
        try (Connection runtime =
            DriverManager.getConnection(tenantUrl, "datastreaming", "runtime-secret")) {
          runtime
              .createStatement()
              .execute(
                  "INSERT INTO data_streaming.event_journal (persistence_id, sequence_number,"
                      + " writer, event_ser_id, event_ser_manifest, event_payload) VALUES ('p', 1,"
                      + " 'w', 1, 'm', '\\x00')");
          try (ResultSet count =
              runtime
                  .createStatement()
                  .executeQuery("SELECT count(*) FROM data_streaming.event_journal")) {
            assertTrue(count.next());
            assertEquals(1, count.getInt(1));
          }
          try (ResultSet snapshots =
              runtime
                  .createStatement()
                  .executeQuery("SELECT count(*) FROM data_streaming.snapshot")) {
            assertTrue(snapshots.next());
          }
        }
      }
    }
  }

  @Test
  void onCassandraCreatesTheJournalKeyspacesAndARuntimeLoginLimitedToThem() {
    try (PostgreSqlTestContainer postgres = new PostgreSqlTestContainer().start();
        CassandraTestContainer cassandra =
            new CassandraTestContainer().withPasswordAuthentication().start()) {
      new com.forwardmeasure.jpa.liquibase.TenantRegistry(postgres.dataSource()).migrate();
      String contactPoint =
          cassandra.contactPoint().getHostString() + ":" + cassandra.contactPoint().getPort();
      Map<String, String> environment =
          Map.ofEntries(
              Map.entry("DATA_STREAMING_CONTROL_PLANE_DATABASE_URL", postgres.hostJdbcUrl()),
              Map.entry("DATA_STREAMING_ADMIN_DATABASE_USERNAME", postgres.username()),
              Map.entry("DATA_STREAMING_ADMIN_DATABASE_PASSWORD", postgres.password()),
              Map.entry("DATA_STREAMING_RUNTIME_DATABASE_USERNAME", "datastreaming"),
              Map.entry("DATA_STREAMING_RUNTIME_DATABASE_PASSWORD", "it's-runtime"),
              Map.entry("DATA_STREAMING_TENANTS", "acme:Acme"),
              Map.entry("DATA_STREAMING_CASSANDRA_CONTACT_POINTS", contactPoint),
              Map.entry("DATA_STREAMING_CASSANDRA_LOCAL_DATACENTER", cassandra.localDatacenter()),
              Map.entry("DATA_STREAMING_CASSANDRA_USERNAME", cassandra.superuserName()),
              Map.entry("DATA_STREAMING_CASSANDRA_PASSWORD", cassandra.superuserPassword()),
              Map.entry("DATA_STREAMING_CASSANDRA_MIGRATION_KEYSPACE", "datastreaming_migrations"),
              Map.entry("DATA_STREAMING_CASSANDRA_REPLICATION_FACTOR", "1"));

      DataStreamingMigrationsMain.run(environment::get);
      DataStreamingMigrationsMain.run(environment::get);

      try (CqlSession runtime =
          CqlSession.builder()
              .addContactPoint(cassandra.contactPoint())
              .withLocalDatacenter(cassandra.localDatacenter())
              .withAuthCredentials("datastreaming", "it's-runtime")
              .build()) {
        runtime.execute(
            "INSERT INTO datastreaming_journal.messages (persistence_id, partition_nr,"
                + " sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest,"
                + " event_manifest, event) VALUES ('p', 0, 1, now(), 'b', 'w', 1, 'm', '', 0x00)");
        assertEquals(
            1,
            runtime
                .execute(
                    "SELECT * FROM datastreaming_journal.messages WHERE persistence_id = 'p'"
                        + " AND partition_nr = 0")
                .all()
                .size());
        runtime.execute(
            "INSERT INTO datastreaming_snapshot.snapshots (persistence_id, sequence_nr, timestamp,"
                + " ser_id, ser_manifest, snapshot) VALUES ('p', 1, 0, 1, 'm', 0x00)");
        // Never the migration bookkeeping, nor anyone else's keyspace.
        assertThrows(
            UnauthorizedException.class,
            () ->
                runtime.execute(
                    "SELECT * FROM datastreaming_migrations.databasechangelog LIMIT 1"));
      }
    }
  }

  @Test
  void tenantEntriesAndContactPointsParseStrictly() {
    assertEquals(2, DataStreamingMigrationsMain.tenants("a:A, b:B,").size());
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> DataStreamingMigrationsMain.tenants("a"));
    var points = DataStreamingMigrationsMain.contactPoints("cassandra:9142, other");
    assertEquals(9142, points.get(0).getPort());
    assertEquals(9042, points.get(1).getPort());
  }
}
