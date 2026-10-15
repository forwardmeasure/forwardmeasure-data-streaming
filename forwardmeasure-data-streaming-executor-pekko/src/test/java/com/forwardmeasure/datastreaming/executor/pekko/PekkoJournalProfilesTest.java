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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The journal settings a worker builds from the environment the launcher gives it. */
class PekkoJournalProfilesTest {

  private static final Map<String, String> POSTGRESQL =
      Map.of(
          "FDS_JOURNAL_CONTROL_PLANE_URL",
              "jdbc:postgresql://localhost:5432/forwardmeasure_control_plane",
          "FDS_JOURNAL_POSTGRES_USER", "datastreaming",
          "FDS_JOURNAL_POSTGRES_PASSWORD", "secret",
          "TENANT_ID", "6f1c5d2e-0000-4000-8000-000000000001");

  @Test
  void postgresqlUsesTheTenantsOwnDatabaseFoundFromItsTenantId() {
    var asked = new AtomicReference<List<String>>();
    Config config =
        PekkoJournalProfiles.forEngine(
            "pekko-postgresql",
            POSTGRESQL::get,
            (url, user, password, tenant) -> {
              asked.set(List.of(url, user, password, tenant));
              return "forwardmeasure_acme";
            });
    assertEquals(
        List.of(
            "jdbc:postgresql://localhost:5432/forwardmeasure_control_plane",
            "datastreaming",
            "secret",
            "6f1c5d2e-0000-4000-8000-000000000001"),
        asked.get());
    assertEquals("jdbc-journal", config.getString("pekko.persistence.journal.plugin"));
    assertEquals(
        "jdbc-snapshot-store", config.getString("pekko.persistence.snapshot-store.plugin"));
    assertEquals(
        "jdbc:postgresql://localhost:5432/forwardmeasure_acme",
        config.getString("pekko-persistence-jdbc.shared-databases.fds.db.url"));
    assertEquals(
        "datastreaming", config.getString("pekko-persistence-jdbc.shared-databases.fds.db.user"));
    for (String table :
        List.of(
            "jdbc-journal.tables.event_journal.schemaName",
            "jdbc-journal.tables.event_tag.schemaName",
            "jdbc-snapshot-store.tables.snapshot.schemaName"))
      assertEquals("data_streaming", config.getString(table), table);
  }

  @Test
  void cassandraUsesFdsOwnKeyspacesAndLogin() {
    Map<String, String> environment =
        Map.of(
            "FDS_JOURNAL_CASSANDRA_CONTACT_POINTS", "a:9042, b:9042,",
            "FDS_JOURNAL_CASSANDRA_LOCAL_DATACENTER", "dc1",
            "FDS_JOURNAL_CASSANDRA_USERNAME", "datastreaming",
            "FDS_JOURNAL_CASSANDRA_PASSWORD", "secret");
    Config config =
        PekkoJournalProfiles.forEngine(
            "pekko-cassandra",
            environment::get,
            (url, user, password, tenant) -> {
              throw new AssertionError("Cassandra needs no tenant database");
            });
    assertEquals(
        "pekko.persistence.cassandra.journal",
        config.getString("pekko.persistence.journal.plugin"));
    assertEquals(
        "datastreaming_journal", config.getString("pekko.persistence.cassandra.journal.keyspace"));
    assertEquals(
        "datastreaming_snapshot",
        config.getString("pekko.persistence.cassandra.snapshot.keyspace"));
    assertFalse(config.getBoolean("pekko.persistence.cassandra.journal.keyspace-autocreate"));
    assertFalse(config.getBoolean("pekko.persistence.cassandra.journal.tables-autocreate"));
    assertEquals(
        List.of("a:9042", "b:9042"),
        config.getStringList("datastax-java-driver.basic.contact-points"));
    assertEquals(
        "dc1",
        config.getString("datastax-java-driver.basic.load-balancing-policy.local-datacenter"));
    assertEquals(
        "datastreaming", config.getString("datastax-java-driver.advanced.auth-provider.username"));
    Map<String, String> unauthenticated = new HashMap<>(environment);
    unauthenticated.remove("FDS_JOURNAL_CASSANDRA_USERNAME");
    unauthenticated.remove("FDS_JOURNAL_CASSANDRA_PASSWORD");
    assertFalse(
        PekkoJournalProfiles.forEngine("pekko-cassandra", unauthenticated::get, null)
            .hasPath("datastax-java-driver.advanced.auth-provider.class"));
  }

  @Test
  void everyMissingSettingIsNamedAndOtherEnginesHaveNoJournal() {
    for (String missing : POSTGRESQL.keySet()) {
      Map<String, String> environment = new HashMap<>(POSTGRESQL);
      environment.remove(missing);
      var failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  PekkoJournalProfiles.forEngine(
                      "pekko-postgresql", environment::get, (u, n, p, t) -> "forwardmeasure_acme"));
      assertTrue(failure.getMessage().contains(missing), failure.getMessage());
    }
    Map<String, String> cassandra = Map.of("FDS_JOURNAL_CASSANDRA_CONTACT_POINTS", "a:9042");
    assertTrue(
        assertThrows(
                IllegalStateException.class,
                () -> PekkoJournalProfiles.forEngine("pekko-cassandra", cassandra::get, null))
            .getMessage()
            .contains("FDS_JOURNAL_CASSANDRA_LOCAL_DATACENTER"));
    Map<String, String> passwordless =
        Map.of(
            "FDS_JOURNAL_CASSANDRA_CONTACT_POINTS", "a:9042",
            "FDS_JOURNAL_CASSANDRA_LOCAL_DATACENTER", "dc1",
            "FDS_JOURNAL_CASSANDRA_USERNAME", "datastreaming");
    assertThrows(
        IllegalStateException.class,
        () -> PekkoJournalProfiles.forEngine("pekko-cassandra", passwordless::get, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> PekkoJournalProfiles.forEngine("kafka-streams", POSTGRESQL::get, null));
  }
}
