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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The Pekko Persistence configuration for the installation's journal, chosen by the platform {@code
 * engine} setting: {@code pekko-postgresql} keeps FDS's state in the run's tenant database ({@code
 * data_streaming} schema, FDS's runtime role), found from the worker's {@code TENANT_ID} through
 * the control plane's tenant registry - a worker is never told a database name; {@code
 * pekko-cassandra} in FDS's own {@code datastreaming_journal} and {@code datastreaming_snapshot}
 * keyspaces. Connection settings come from the worker environment the launcher builds; credentials
 * arrive there through secret references, never in a spec or workflow input.
 */
final class PekkoJournalProfiles {

  static final String PLATFORM_ENGINE_ENV = "FDS_PLATFORM_ENGINE";

  private PekkoJournalProfiles() {}

  /**
   * The journal configuration for this process, or empty config when the launcher configured none
   * (a bounded run, or a continuous run that keeps no state); {@link #requireJournal} then stops a
   * stateful run before it starts.
   */
  static Config fromEnvironment() {
    String engine = System.getenv(PLATFORM_ENGINE_ENV);
    return engine == null || engine.isBlank() || engine.equals("kafka-streams")
        ? ConfigFactory.empty()
        : forEngine(engine, System::getenv, PekkoJournalProfiles::registeredTenantDatabase);
  }

  /** Fails unless {@code system} has a journal: continuous state must survive a restart. */
  static void requireJournal(org.apache.pekko.actor.ActorSystem system) {
    Config config = system.settings().config();
    if (!config.hasPath("pekko.persistence.journal.plugin")
        || config.getString("pekko.persistence.journal.plugin").isBlank()) {
      throw new IllegalStateException(
          "This continuous run keeps state, which needs the installation's journal: the launcher"
              + " must pass "
              + PLATFORM_ENGINE_ENV
              + " and the journal settings");
    }
  }

  /**
   * @param tenantDatabase the tenant's database name, given the control-plane URL, the runtime
   *     credentials and the tenant id
   */
  static Config forEngine(
      String platformEngine,
      Function<String, String> environment,
      TenantDatabaseLookup tenantDatabase) {
    return switch (platformEngine.strip()) {
      case "pekko-postgresql" -> postgresql(environment, tenantDatabase);
      case "pekko-cassandra" -> cassandra(environment);
      default ->
          throw new IllegalArgumentException(
              "No Pekko journal for platform engine '" + platformEngine + "'");
    };
  }

  /** Finds a tenant's database name in the control plane's tenant registry. */
  @FunctionalInterface
  interface TenantDatabaseLookup {
    String find(String controlPlaneUrl, String user, String password, String tenantId);
  }

  private static Config postgresql(
      Function<String, String> environment, TenantDatabaseLookup tenantDatabase) {
    String controlPlaneUrl = required(environment, "FDS_JOURNAL_CONTROL_PLANE_URL");
    String user = required(environment, "FDS_JOURNAL_POSTGRES_USER");
    String password = required(environment, "FDS_JOURNAL_POSTGRES_PASSWORD");
    String database =
        tenantDatabase.find(controlPlaneUrl, user, password, required(environment, "TENANT_ID"));
    String url = controlPlaneUrl.substring(0, controlPlaneUrl.lastIndexOf('/') + 1) + database;
    Map<String, Object> db =
        Map.of(
            "connectionPool",
            "HikariCP",
            "driver",
            "org.postgresql.Driver",
            "url",
            url,
            "user",
            user,
            "password",
            password,
            "numThreads",
            4,
            "maxConnections",
            4,
            "minConnections",
            1);
    return ConfigFactory.parseString(
            """
            pekko.persistence.journal.plugin = "jdbc-journal"
            pekko.persistence.snapshot-store.plugin = "jdbc-snapshot-store"
            pekko-persistence-jdbc.shared-databases.fds.profile = "slick.jdbc.PostgresProfile$"
            jdbc-journal.use-shared-db = "fds"
            jdbc-snapshot-store.use-shared-db = "fds"
            jdbc-journal.tables.event_journal.schemaName = "data_streaming"
            jdbc-journal.tables.event_tag.schemaName = "data_streaming"
            jdbc-snapshot-store.tables.snapshot.schemaName = "data_streaming"
            """)
        .withValue(
            "pekko-persistence-jdbc.shared-databases.fds.db", ConfigValueFactory.fromMap(db));
  }

  private static Config cassandra(Function<String, String> environment) {
    List<String> contactPoints =
        Arrays.stream(required(environment, "FDS_JOURNAL_CASSANDRA_CONTACT_POINTS").split(","))
            .map(String::strip)
            .filter(point -> !point.isEmpty())
            .toList();
    Config config =
        ConfigFactory.parseString(
                """
                pekko.persistence.journal.plugin = "pekko.persistence.cassandra.journal"
                pekko.persistence.snapshot-store.plugin = "pekko.persistence.cassandra.snapshot"
                pekko.persistence.cassandra.journal.keyspace = "datastreaming_journal"
                pekko.persistence.cassandra.journal.keyspace-autocreate = false
                pekko.persistence.cassandra.journal.tables-autocreate = false
                pekko.persistence.cassandra.snapshot.keyspace = "datastreaming_snapshot"
                pekko.persistence.cassandra.snapshot.keyspace-autocreate = false
                pekko.persistence.cassandra.snapshot.tables-autocreate = false
                pekko.persistence.cassandra.events-by-tag.enabled = false
                datastax-java-driver.advanced.reconnect-on-init = true
                """)
            .withValue(
                "datastax-java-driver.basic.contact-points",
                ConfigValueFactory.fromIterable(contactPoints))
            .withValue(
                "datastax-java-driver.basic.load-balancing-policy.local-datacenter",
                ConfigValueFactory.fromAnyRef(
                    required(environment, "FDS_JOURNAL_CASSANDRA_LOCAL_DATACENTER")));
    String username = environment.apply("FDS_JOURNAL_CASSANDRA_USERNAME");
    if (username != null && !username.isBlank()) {
      config =
          config
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.class",
                  ConfigValueFactory.fromAnyRef("PlainTextAuthProvider"))
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.username",
                  ConfigValueFactory.fromAnyRef(username))
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.password",
                  ConfigValueFactory.fromAnyRef(
                      required(environment, "FDS_JOURNAL_CASSANDRA_PASSWORD")));
    }
    return config;
  }

  static String registeredTenantDatabase(
      String controlPlaneUrl, String user, String password, String tenantId) {
    org.postgresql.ds.PGSimpleDataSource controlPlane = new org.postgresql.ds.PGSimpleDataSource();
    controlPlane.setURL(controlPlaneUrl);
    controlPlane.setUser(user);
    controlPlane.setPassword(password);
    return new com.forwardmeasure.jpa.liquibase.TenantRegistry(controlPlane)
        .resolve(com.forwardmeasure.jpa.tenancy.TenantId.parse(tenantId))
        .orElseThrow(
            () ->
                new IllegalStateException("Tenant " + tenantId + " is not in the tenant registry"))
        .value();
  }

  private static String required(Function<String, String> environment, String name) {
    String value = environment.apply(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          "The Pekko journal needs environment variable '" + name + "'");
    }
    return value.strip();
  }
}
