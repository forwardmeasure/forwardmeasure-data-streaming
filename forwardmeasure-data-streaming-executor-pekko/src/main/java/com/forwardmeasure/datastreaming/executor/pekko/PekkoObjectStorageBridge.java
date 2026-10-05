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

import com.forwardmeasure.datastreaming.connector.objectstorage.ObjectStorageBridge;
import com.forwardmeasure.datastreaming.connector.objectstorage.ObjectStorageBridge.WriteRequest;
import com.forwardmeasure.objectstorage.ObjectInfo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

/** Stream operators belong to the Pekko executor, not the shared storage connector. */
public final class PekkoObjectStorageBridge {
  private final ObjectStorageBridge storage;

  public PekkoObjectStorageBridge(ObjectStorageBridge storage) {
    this.storage = storage;
  }

  private record Cursor(String token, boolean done) {}

  public Source<ObjectInfo, NotUsed> list(String bucket, String prefix) {
    return Source.unfoldAsync(
            new Cursor(null, false),
            cursor -> {
              if (cursor.done()) return CompletableFuture.completedFuture(Optional.empty());
              return storage
                  .listPage(bucket, prefix, cursor.token())
                  .thenApply(
                      page -> {
                        if (page.isTruncated()
                            && (page.nextContinuationToken() == null
                                || page.nextContinuationToken().equals(cursor.token()))) {
                          throw new IllegalStateException(
                              "Object storage pagination did not advance");
                        }
                        // A filtered page can be empty and still carry a continuation token.
                        return Optional.of(
                            Pair.create(
                                new Cursor(page.nextContinuationToken(), !page.isTruncated()),
                                page.objects()));
                      });
            })
        .mapConcat(items -> items);
  }

  public Source<String, NotUsed> readAll(String bucket, String prefix, int parallelism) {
    return list(bucket, prefix).mapAsyncUnordered(parallelism, storage::read);
  }

  public Sink<WriteRequest, CompletionStage<Done>> sink(int parallelism) {
    return Flow.<WriteRequest>create()
        .mapAsyncUnordered(
            parallelism, request -> storage.write(request).thenApply(ignored -> Done.getInstance()))
        .toMat(Sink.ignore(), Keep.right());
  }
}
