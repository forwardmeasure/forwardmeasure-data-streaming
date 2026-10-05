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

import com.forwardmeasure.datastreaming.connector.jdbc.JpaPagingSource;
import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import com.forwardmeasure.jpa.core.query.JpaSpecification;
import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.javadsl.Source;

/** Adapts repository pages to a backpressured Pekko source. */
public final class PekkoJpaSource {
  private PekkoJpaSource() {}

  public static <T extends AbstractBaseEntity<I>, I extends Serializable> Source<T, NotUsed> page(
      AbstractBaseRepository<T, I> repository,
      JpaSpecification<T> specification,
      int pageSize,
      Executor executor) {
    return Source.unfoldAsync(
            0,
            offset ->
                JpaPagingSource.page(repository, specification, offset, pageSize, executor)
                    .thenApply(
                        items ->
                            items.isEmpty()
                                ? Optional.empty()
                                : Optional.of(Pair.create(offset + items.size(), items))))
        .mapConcat(items -> items);
  }
}
