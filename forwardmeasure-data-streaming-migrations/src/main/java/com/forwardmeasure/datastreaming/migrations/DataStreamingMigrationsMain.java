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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.forwardmeasure.jpa.datasource.TenantDataSourceRegistry;
import com.forwardmeasure.jpa.datasource.TenantDataSourceTemplate;
import com.forwardmeasure.jpa.tenancy.FunctionalSchema;
import com.forwardmeasure.jpa.tenancy.TenantDatabase;
import com.forwardmeasure.openworkflow.migration.CassandraMigrationTarget;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Provisions the Pekko journal FDS keeps continuous-run state on (D6), in the platform migrations
 * stage, after fowf's own migrations:
 *
 * <ul>
 *   <li><b>PostgreSQL</b> (every installation): FDS's runtime role, created or rotated, with read
 *       access to the control plane's tenant registry, and in each tenant's database the {@code
 *       data_streaming} schema with Pekko's journal and snapshot tables, owned by that role.
 *   <li><b>Cassandra</b> ({@code pekko-cassandra} installations, when {@code
 *       DATA_STREAMING_CASSANDRA_CONTACT_POINTS} is set): the {@code datastreaming_journal} and
 *       {@code datastreaming_snapshot} keyspaces and their tables, and - with authentication on -
 *       the runtime role as a Cassandra login limited to those two keyspaces.
 * </ul>
 */
public final class DataStreamingMigrationsMain {

  static final String CHANGELOG = "db/changelog/data-streaming-changelog.xml";
  static final String CASSANDRA_CHANGELOG =
      "db/changelog/cassandra/data-streaming-cassandra-master.xml";

  private DataStreamingMigrationsMain() {}

  public static void main(String[] arguments) {
    run(System::getenv);
  }

  /** Runs the migrations with {@code environment} as the process environment. */
  public static void run(java.util.function.Function<String, String> environment) {
    java.util.function.Function<String, String> required = name -> required(environment, name);
    String url = required.apply("DATA_STREAMING_CONTROL_PLANE_DATABASE_URL");
    String username = required.apply("DATA_STREAMING_ADMIN_DATABASE_USERNAME");
    String password = required.apply("DATA_STREAMING_ADMIN_DATABASE_PASSWORD");
    String runtimeUsername = required.apply("DATA_STREAMING_RUNTIME_DATABASE_USERNAME");
    String runtimePassword = required.apply("DATA_STREAMING_RUNTIME_DATABASE_PASSWORD");

    DataSource platformDataSource = new DriverManagerDataSource(url, username, password);
    TenantDataSourceRegistry tenantDataSources =
        new TenantDataSourceRegistry(
            new TenantDataSourceTemplate(databaseUrlPrefix(url), username, password));
    OpenWorkflowTenantMigrator journal =
        new OpenWorkflowTenantMigrator(
            platformDataSource,
            tenantDataSources,
            runtimeUsername,
            CHANGELOG,
            FunctionalSchema.DATA_STREAMING);
    journal.ensureRuntimeRole(runtimePassword);
    // Workers carry a tenant id, never a database name: they find the tenant's database in the
    // control plane's tenant_registry as this role.
    journal.grantTenantRegistryRead();
    for (TenantDatabase database : tenants(required.apply("DATA_STREAMING_TENANTS"))) {
      journal.provisionAndMigrate(database);
    }

    String contactPoints = environment.apply("DATA_STREAMING_CASSANDRA_CONTACT_POINTS");
    if (contactPoints != null && !contactPoints.isBlank()) {
      CassandraMigrationTarget target =
          new CassandraMigrationTarget(
              contactPoints(contactPoints),
              required.apply("DATA_STREAMING_CASSANDRA_LOCAL_DATACENTER"),
              Optional.ofNullable(environment.apply("DATA_STREAMING_CASSANDRA_USERNAME")),
              Optional.ofNullable(environment.apply("DATA_STREAMING_CASSANDRA_PASSWORD")),
              required.apply("DATA_STREAMING_CASSANDRA_MIGRATION_KEYSPACE"),
              "datastreaming_journal",
              Integer.parseInt(required.apply("DATA_STREAMING_CASSANDRA_REPLICATION_FACTOR")));
      OpenWorkflowCassandraMigrator.migrate(target, CASSANDRA_CHANGELOG);
      // With authentication on, workers log in as the same runtime role, with the same password,
      // limited to FDS's two keyspaces - never as the cluster's superuser.
      if (target.username().isPresent())
        ensureCassandraRuntimeRole(target, runtimeUsername, runtimePassword);
    }
  }

  static final List<String> CASSANDRA_KEYSPACES =
      List.of("datastreaming_journal", "datastreaming_snapshot");

  /** Creates the runtime role if missing, sets its password, and grants it FDS's keyspaces. */
  static void ensureCassandraRuntimeRole(
      CassandraMigrationTarget target, String role, String password) {
    if (!role.matches("[a-z][a-z0-9_]{0,47}"))
      throw new IllegalArgumentException("Invalid Cassandra role name: " + role);
    String literal = "'" + password.replace("'", "''") + "'";
    CqlSessionBuilder builder =
        CqlSession.builder()
            .withLocalDatacenter(target.localDatacenter())
            .withAuthCredentials(target.username().orElseThrow(), target.password().orElseThrow());
    target.contactPoints().forEach(builder::addContactPoint);
    try (CqlSession session = builder.build()) {
      session.execute(
          "CREATE ROLE IF NOT EXISTS "
              + role
              + " WITH PASSWORD = "
              + literal
              + " AND LOGIN = true");
      session.execute("ALTER ROLE " + role + " WITH PASSWORD = " + literal);
      for (String keyspace : CASSANDRA_KEYSPACES)
        session.execute("GRANT ALL PERMISSIONS ON KEYSPACE " + keyspace + " TO " + role);
    }
  }

  /** {@code alias:display-name} entries; the database always derives from the alias. */
  static List<TenantDatabase> tenants(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(entry -> !entry.isEmpty())
        .map(
            entry -> {
              String[] parts = entry.split(":", 2);
              if (parts.length != 2) {
                throw new IllegalArgumentException("Tenant entries must be alias:display-name");
              }
              return TenantDatabase.forAlias(parts[0]);
            })
        .toList();
  }

  static List<InetSocketAddress> contactPoints(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(entry -> !entry.isEmpty())
        .map(
            entry -> {
              int colon = entry.lastIndexOf(':');
              return colon < 0
                  ? InetSocketAddress.createUnresolved(entry, 9042)
                  : InetSocketAddress.createUnresolved(
                      entry.substring(0, colon), Integer.parseInt(entry.substring(colon + 1)));
            })
        .toList();
  }

  static String databaseUrlPrefix(String url) {
    int lastSlash = url.lastIndexOf('/');
    if (lastSlash < 0) {
      throw new IllegalArgumentException(
          "DATA_STREAMING_CONTROL_PLANE_DATABASE_URL must be a JDBC URL with a database name: "
              + url);
    }
    return url.substring(0, lastSlash + 1);
  }

  private static String required(
      java.util.function.Function<String, String> environment, String name) {
    String value = environment.apply(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " is required");
    }
    return value.trim();
  }

  private record DriverManagerDataSource(String url, String username, String password)
      implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, username, password);
    }

    @Override
    public Connection getConnection(String suppliedUsername, String suppliedPassword)
        throws SQLException {
      return DriverManager.getConnection(url, suppliedUsername, suppliedPassword);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter output) throws SQLException {
      DriverManager.setLogWriter(output);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      throw new SQLException("Not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }
}
