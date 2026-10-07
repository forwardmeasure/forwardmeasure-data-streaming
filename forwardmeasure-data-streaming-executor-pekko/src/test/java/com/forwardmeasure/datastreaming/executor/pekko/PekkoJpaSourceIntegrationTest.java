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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer
class PekkoJpaSourceIntegrationTest {
  @Test
  void repositoryPagesDrainThroughPekkoWithoutDroppingThePartialFinalPage(
      PostgreSqlTestContainer database) throws Exception {
    // Disposable connector fixture; no deployed application schema is involved.
    try (var connection = database.dataSource().getConnection();
        var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE paging_contract (id BIGINT PRIMARY KEY, version INTEGER NOT NULL, name"
              + " VARCHAR(255))");
      statement.execute(
          "INSERT INTO paging_contract SELECT i, 0, 'row-' || i FROM generate_series(1, 11) AS i");
    }
    var config =
        new Configuration()
            .addAnnotatedClass(PagingRow.class)
            .setProperty("jakarta.persistence.jdbc.url", database.hostJdbcUrl())
            .setProperty("jakarta.persistence.jdbc.user", database.username())
            .setProperty("jakarta.persistence.jdbc.password", database.password())
            .setProperty("hibernate.hbm2ddl.auto", "none");
    var system = ActorSystem.create("jpa-paging-contract");
    try (var factory = config.buildSessionFactory();
        var manager = factory.createEntityManager();
        var executor = Executors.newSingleThreadExecutor()) {
      var repository = new PagingRepository();
      repository.bindPersistenceContext(manager);
      var rows =
          PekkoJpaSource.page(repository, null, 4, executor)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
      assertEquals(
          java.util.stream.LongStream.rangeClosed(1, 11).boxed().toList(),
          rows.stream().map(PagingRow::getId).toList());
      assertEquals("row-11", rows.getLast().name);
      var first =
          PekkoJpaSource.page(repository, null, 4, executor)
              .take(2)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
      assertEquals(List.of(1L, 2L), first.stream().map(PagingRow::getId).toList());
      assertThrows(
          ExecutionException.class,
          () ->
              PekkoJpaSource.page(repository, null, 0, executor)
                  .runWith(Sink.seq(), system)
                  .toCompletableFuture()
                  .get(10, TimeUnit.SECONDS));
    } finally {
      system.terminate();
    }
  }

  @Entity(name = "PagingRow")
  @Table(name = "paging_contract")
  public static class PagingRow extends AbstractBaseEntity<Long> {
    @Id private Long id;
    private String name;

    public PagingRow() {}

    @Override
    public Long getId() {
      return id;
    }

    @Override
    public void setId(Long value) {
      id = value;
    }
  }

  public static class PagingRepository extends AbstractBaseRepository<PagingRow, Long> {}
}
