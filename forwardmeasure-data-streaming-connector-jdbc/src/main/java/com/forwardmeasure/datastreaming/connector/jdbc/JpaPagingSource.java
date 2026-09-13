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

import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import com.forwardmeasure.jpa.core.query.JpaSpecification;
import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.core.query.PageRequest;
import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.javadsl.Source;

/**
 * Turns an already-bound {@link AbstractBaseRepository} into a plain Pekko {@link Source} of its
 * entities - the smaller/bounded/incremental polling or lookup case this connector exists for (D5),
 * not Camel-backed (there's no camel-jdbc/camel-sql precedent anywhere in this org; JPA/JPQL is the
 * universal one). Walks pages via the repository's own real {@link AbstractBaseRepository#page}
 * (offset/limit, optionally filtered by a {@link JpaSpecification}) until a page comes back empty,
 * emitting individual entities downstream rather than whole pages.
 *
 * <p>Per manifesto Principle 7, this module exposes {@code Repository} types only - the caller owns
 * constructing and binding the repository (an {@code EntityManager} obtained however its own
 * deployment shape requires: a framework-injected one inside a live application, or one built
 * directly via {@code Persistence.createEntityManagerFactory(...)} inside a plain {@code main()}
 * batch worker, the same "no live application context" shape the file-based connector's own workers
 * already run under). Each page query runs on {@code executor}, not the calling thread - JDBC I/O
 * is blocking, and this connector plugs into the same Pekko graph the Camel-backed connectors do,
 * which assumes non-blocking stages.
 */
public final class JpaPagingSource {

  private JpaPagingSource() {}

  public static <T extends AbstractBaseEntity<I>, I extends Serializable> Source<T, NotUsed> page(
      AbstractBaseRepository<T, I> repository, int pageSize, Executor executor) {
    return page(repository, null, pageSize, executor);
  }

  public static <T extends AbstractBaseEntity<I>, I extends Serializable> Source<T, NotUsed> page(
      AbstractBaseRepository<T, I> repository,
      JpaSpecification<T> specification,
      int pageSize,
      Executor executor) {
    return Source.unfoldAsync(
            0,
            offset ->
                CompletableFuture.supplyAsync(
                    () -> nextPage(repository, specification, offset, pageSize), executor))
        .mapConcat(items -> items);
  }

  private static <T extends AbstractBaseEntity<I>, I extends Serializable>
      Optional<Pair<Integer, List<T>>> nextPage(
          AbstractBaseRepository<T, I> repository,
          JpaSpecification<T> specification,
          int offset,
          int pageSize) {
    Page<T> page = repository.page(new PageRequest(offset, pageSize, List.of()), specification);
    if (page.items().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(Pair.create(offset + page.items().size(), page.items()));
  }
}
