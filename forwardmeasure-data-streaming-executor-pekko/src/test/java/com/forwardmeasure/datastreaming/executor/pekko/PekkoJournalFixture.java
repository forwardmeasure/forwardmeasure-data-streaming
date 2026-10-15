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

import com.forwardmeasure.datastreaming.migrations.DataStreamingMigrationsMain;
import com.forwardmeasure.jpa.liquibase.TenantRegistry;
import com.forwardmeasure.jpa.tenancy.Did;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.testcontainers.cassandra.CassandraTestContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.pekko.actor.ActorSystem;

/**
 * The installation's journal as a deployment provisions it, for Pekko tests: the control plane's
 * tenant registry with one tenant, FDS's migrations run against it (the {@code data_streaming}
 * schema in the tenant's database, the runtime role, and - for Cassandra - the keyspaces and
 * login), and the worker environment the launcher would build. {@link #system} starts an actor
 * system configured exactly as a worker's, through {@link PekkoJournalProfiles}.
 */
final class PekkoJournalFixture implements AutoCloseable {

  enum Backend {
    POSTGRESQL("pekko-postgresql"),
    CASSANDRA("pekko-cassandra");

    final String platformEngine;

    Backend(String platformEngine) {
      this.platformEngine = platformEngine;
    }
  }

  static final String RUNTIME_ROLE = "datastreaming";
  static final String RUNTIME_PASSWORD = "journal-runtime-secret";
  static final Did TENANT = Did.parse("did:web:journal.example");

  private final Backend backend;
  private final PostgreSqlTestContainer postgres;
  private final CassandraTestContainer cassandra;
  private final Map<String, String> workerEnvironment = new HashMap<>();

  private PekkoJournalFixture(Backend backend) {
    this.backend = backend;
    this.postgres = new PostgreSqlTestContainer().start();
    this.cassandra =
        backend == Backend.CASSANDRA
            ? new CassandraTestContainer().withPasswordAuthentication().start()
            : null;
  }

  static PekkoJournalFixture start(Backend backend) {
    PekkoJournalFixture fixture = new PekkoJournalFixture(backend);
    try {
      fixture.provision();
      return fixture;
    } catch (RuntimeException failure) {
      fixture.close();
      throw failure;
    }
  }

  private void provision() {
    TenantRegistry registry = new TenantRegistry(postgres.dataSource());
    registry.migrate();
    registry.register(TENANT, "journal", TenantDatabase.forAlias("journal"));
    Map<String, String> migrations = new HashMap<>();
    migrations.put("DATA_STREAMING_CONTROL_PLANE_DATABASE_URL", postgres.hostJdbcUrl());
    migrations.put("DATA_STREAMING_ADMIN_DATABASE_USERNAME", postgres.username());
    migrations.put("DATA_STREAMING_ADMIN_DATABASE_PASSWORD", postgres.password());
    migrations.put("DATA_STREAMING_RUNTIME_DATABASE_USERNAME", RUNTIME_ROLE);
    migrations.put("DATA_STREAMING_RUNTIME_DATABASE_PASSWORD", RUNTIME_PASSWORD);
    migrations.put("DATA_STREAMING_TENANTS", "journal:Journal");
    workerEnvironment.put("TENANT_ID", TenantId.forDid(TENANT).toString());
    workerEnvironment.put("FDS_JOURNAL_CONTROL_PLANE_URL", postgres.hostJdbcUrl());
    workerEnvironment.put("FDS_JOURNAL_POSTGRES_USER", RUNTIME_ROLE);
    workerEnvironment.put("FDS_JOURNAL_POSTGRES_PASSWORD", RUNTIME_PASSWORD);
    if (cassandra != null) {
      String contactPoint =
          cassandra.contactPoint().getHostString() + ":" + cassandra.contactPoint().getPort();
      migrations.put("DATA_STREAMING_CASSANDRA_CONTACT_POINTS", contactPoint);
      migrations.put("DATA_STREAMING_CASSANDRA_LOCAL_DATACENTER", cassandra.localDatacenter());
      migrations.put("DATA_STREAMING_CASSANDRA_USERNAME", cassandra.superuserName());
      migrations.put("DATA_STREAMING_CASSANDRA_PASSWORD", cassandra.superuserPassword());
      migrations.put("DATA_STREAMING_CASSANDRA_MIGRATION_KEYSPACE", "datastreaming_migrations");
      migrations.put("DATA_STREAMING_CASSANDRA_REPLICATION_FACTOR", "1");
      workerEnvironment.put("FDS_JOURNAL_CASSANDRA_CONTACT_POINTS", contactPoint);
      workerEnvironment.put("FDS_JOURNAL_CASSANDRA_LOCAL_DATACENTER", cassandra.localDatacenter());
      workerEnvironment.put("FDS_JOURNAL_CASSANDRA_USERNAME", RUNTIME_ROLE);
      workerEnvironment.put("FDS_JOURNAL_CASSANDRA_PASSWORD", RUNTIME_PASSWORD);
    }
    DataStreamingMigrationsMain.run(migrations::get);
  }

  Backend backend() {
    return backend;
  }

  /** The worker environment (journal settings and tenant id) the launcher would pass. */
  Map<String, String> workerEnvironment() {
    return Map.copyOf(workerEnvironment);
  }

  /** The journal configuration a worker with {@link #workerEnvironment} builds. */
  Config journalConfig() {
    return PekkoJournalProfiles.forEngine(
        backend.platformEngine,
        workerEnvironment::get,
        PekkoJournalProfiles::registeredTenantDatabase);
  }

  /** A fresh actor system on the journal - one per simulated worker process. */
  ActorSystem system(String name) {
    return ActorSystem.create(
        name + "-" + UUID.randomUUID().toString().substring(0, 8),
        journalConfig().withFallback(ConfigFactory.load()));
  }

  PostgreSqlTestContainer postgres() {
    return postgres;
  }

  @Override
  public void close() {
    try {
      if (cassandra != null) cassandra.close();
    } finally {
      postgres.close();
    }
  }
}
