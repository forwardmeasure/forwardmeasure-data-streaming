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
package com.forwardmeasure.datastreaming.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.jpa.liquibase.TenantSchemaMigrator;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocks proof: {@link JpaPagingSource} pages through every row of a real Postgres table -
 * the same {@code WithPostgreSqlContainer}/{@code TenantSchemaMigrator}/{@code RESOURCE_LOCAL}
 * persistence-unit pattern {@code forwardmeasure-jpa-locking}'s own real integration test uses.
 */
@WithPostgreSqlContainer(databaseName = "data_streaming_connector_jdbc_test")
class JpaPagingSourceTest {

  private static final int ROW_COUNT = 250;
  private static final int PAGE_SIZE = 32;

  @Test
  void pagesThroughEveryRowInIdOrder(PostgreSqlTestContainer database) throws Exception {
    TenantSchema tenant = TenantSchema.forTenant(new TenantId(UUID.randomUUID()));
    database.createSchema(tenant.value());
    new TenantSchemaMigrator(
            database.dataSource(),
            "db/changelog/forwardmeasure-data-streaming-connector-jdbc-test.xml",
            getClass().getClassLoader())
        .migrate(tenant);

    try (EntityManagerFactory entityManagers =
        Persistence.createEntityManagerFactory(
            "forwardmeasure-data-streaming-connector-jdbc-test",
            Map.of(
                "jakarta.persistence.jdbc.url", database.hostJdbcUrl(),
                "jakarta.persistence.jdbc.user", database.username(),
                "jakarta.persistence.jdbc.password", database.password(),
                "jakarta.persistence.jdbc.driver", "org.postgresql.Driver",
                "hibernate.default_schema", tenant.value()))) {
      insertWidgets(entityManagers, ROW_COUNT);

      WidgetRepository repository = new WidgetRepository();
      EntityManager readManager = entityManagers.createEntityManager();
      repository.bindPersistenceContext(readManager);

      ActorSystem system = ActorSystem.create("jpa-paging-source-test");
      ExecutorService executor = Executors.newFixedThreadPool(4);
      List<String> names;
      try {
        names =
            JpaPagingSource.page(repository, PAGE_SIZE, executor)
                .map(Widget::getName)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .join();
      } finally {
        system.terminate();
        executor.shutdown();
        readManager.close();
      }

      assertEquals(ROW_COUNT, names.size());
      for (int i = 0; i < ROW_COUNT; i++) {
        assertEquals("widget-" + i, names.get(i), "row " + i);
      }
    }
  }

  private void insertWidgets(EntityManagerFactory entityManagers, int count) {
    EntityManager entityManager = entityManagers.createEntityManager();
    var transaction = entityManager.getTransaction();
    try {
      transaction.begin();
      for (int i = 0; i < count; i++) {
        entityManager.persist(new Widget("widget-" + i));
      }
      transaction.commit();
    } catch (RuntimeException | Error failure) {
      if (transaction.isActive()) {
        transaction.rollback();
      }
      throw failure;
    } finally {
      entityManager.close();
    }
  }
}
