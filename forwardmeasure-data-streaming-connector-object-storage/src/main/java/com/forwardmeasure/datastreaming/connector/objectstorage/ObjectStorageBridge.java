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
package com.forwardmeasure.datastreaming.connector.objectstorage;

import com.forwardmeasure.objectstorage.ObjectInfo;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageClient.GetObjectRequest;
import com.forwardmeasure.objectstorage.StorageClient.ListObjectsRequest;
import com.forwardmeasure.objectstorage.StorageClient.ListObjectsResponse;
import com.forwardmeasure.objectstorage.StorageClient.PutObjectRequest;
import com.forwardmeasure.objectstorage.StorageObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

/**
 * Thin adapter over {@link StorageClient} (D5's one deliberate exception to "prefer Camel" - no
 * Camel S3/GCS component beats a provider-neutral interface this org already built and controls).
 * Not Camel-backed, so it does not go through {@code camel-reactive-streams} (D5/D6) - source and
 * sink here are built directly on plain Pekko Streams operators instead.
 *
 * <p>{@code source(...)} lists objects under a prefix (paginated via {@link StorageClient}'s own
 * continuation-token protocol, walked the same {@code Source.unfoldAsync} way {@code
 * forwardmeasure-data-streaming-connector-jdbc}'s {@code JpaPagingSource} pages through JPA
 * results), then fetches each object's content concurrently. {@code sink(...)} writes each incoming
 * {@link WriteRequest} as one object. Every {@link StorageClient} call is blocking I/O, so both run
 * on the supplied {@code executor}, not the calling thread - the same reason the JDBC connector
 * does.
 */
public final class ObjectStorageBridge {

  private final StorageClient client;
  private final Executor executor;

  public ObjectStorageBridge(StorageClient client, Executor executor) {
    this.client = client;
    this.executor = executor;
  }

  /** One object to write: {@code bucketName}/{@code key}, its raw bytes, and its content type. */
  public record WriteRequest(String bucketName, String key, byte[] content, String contentType) {}

  public Source<ObjectInfo, NotUsed> list(String bucketName, String prefix) {
    return Source.unfoldAsync(
            new ListState(null, false),
            state ->
                CompletableFuture.supplyAsync(
                    () -> nextListPage(bucketName, prefix, state), executor))
        .mapConcat(items -> items);
  }

  public Source<String, NotUsed> readAll(String bucketName, String prefix, int parallelism) {
    return list(bucketName, prefix)
        .mapAsyncUnordered(
            parallelism, info -> CompletableFuture.supplyAsync(() -> fetchContent(info), executor));
  }

  public Sink<WriteRequest, CompletionStage<Done>> sink(int parallelism) {
    return Flow.<WriteRequest>create()
        .mapAsyncUnordered(
            parallelism,
            request ->
                CompletableFuture.supplyAsync(
                    () -> {
                      putObject(request);
                      return Done.getInstance();
                    },
                    executor))
        .toMat(Sink.ignore(), Keep.right());
  }

  private record ListState(String continuationToken, boolean done) {}

  private Optional<Pair<ListState, List<ObjectInfo>>> nextListPage(
      String bucketName, String prefix, ListState state) {
    if (state.done()) {
      return Optional.empty();
    }
    ListObjectsResponse response =
        client.listObjects(
            new ListObjectsRequest(bucketName, prefix, null, 0, state.continuationToken()));
    if (response.objects().isEmpty()) {
      return Optional.empty();
    }
    ListState nextState = new ListState(response.nextContinuationToken(), !response.isTruncated());
    return Optional.of(Pair.create(nextState, response.objects()));
  }

  private String fetchContent(ObjectInfo info) {
    try (StorageObject object =
        client.getObject(GetObjectRequest.of(info.bucketName(), info.key()))) {
      return new String(object.content().readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void putObject(WriteRequest request) {
    client.putObject(
        PutObjectRequest.fromBytes(
            request.bucketName(),
            request.key(),
            request.content(),
            request.contentType(),
            Map.of()));
  }
}
