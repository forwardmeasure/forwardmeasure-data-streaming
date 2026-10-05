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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Engine-neutral asynchronous object-storage operations on a caller-owned I/O executor. */
public final class ObjectStorageBridge {

  private final StorageClient client;
  private final Executor executor;

  public ObjectStorageBridge(StorageClient client, Executor executor) {
    this.client = client;
    this.executor = executor;
  }

  /** One object to write: {@code bucketName}/{@code key}, its raw bytes, and its content type. */
  public record WriteRequest(String bucketName, String key, byte[] content, String contentType) {}

  public CompletionStage<ListObjectsResponse> listPage(
      String bucketName, String prefix, String continuationToken) {
    return CompletableFuture.supplyAsync(
        () ->
            client.listObjects(
                new ListObjectsRequest(bucketName, prefix, null, 0, continuationToken)),
        executor);
  }

  public CompletionStage<String> read(ObjectInfo info) {
    return CompletableFuture.supplyAsync(() -> fetchContent(info), executor);
  }

  public CompletionStage<Void> write(WriteRequest request) {
    return CompletableFuture.runAsync(() -> putObject(request), executor);
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
