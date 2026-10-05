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
import com.forwardmeasure.jpa.core.query.PageRequest;
import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Engine-neutral asynchronous repository paging. The caller owns the persistence context. */
public final class JpaPagingSource {
  private JpaPagingSource() {}

  public static <T extends AbstractBaseEntity<I>, I extends Serializable>
      CompletionStage<List<T>> page(
          AbstractBaseRepository<T, I> repository,
          JpaSpecification<T> specification,
          int offset,
          int pageSize,
          Executor executor) {
    if (offset < 0 || pageSize < 1) throw new IllegalArgumentException("Invalid paging bounds");
    return CompletableFuture.supplyAsync(
        () ->
            List.copyOf(
                repository
                    .page(new PageRequest(offset, pageSize, List.of()), specification)
                    .items()),
        executor);
  }
}
