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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.connector.objectstorage.ObjectStorageBridge;
import com.forwardmeasure.datastreaming.connector.objectstorage.ObjectStorageBridge.WriteRequest;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.s3.S3StorageClient;
import com.forwardmeasure.testcontainers.minio.MinioTestContainer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;

class PekkoObjectStorageContractTest {
  @Test
  void streamCompletionMeansAllWritesAndAllPagesHaveReachedTheRealStore() throws Exception {
    var system = ActorSystem.create("storage-stream-contract");
    try (var minio = new MinioTestContainer().start();
        var storage =
            new S3StorageClient(
                config(minio.hostEndpoint().toString(), minio.accessKey(), minio.secretKey()));
        var executor = Executors.newFixedThreadPool(4)) {
      String bucket = "pekko-storage-contract";
      storage.createBucket(new StorageClient.CreateBucketRequest(bucket, Map.of(), Map.of()));
      var bridge = new PekkoObjectStorageBridge(new ObjectStorageBridge(storage, executor));
      var writes =
          IntStream.range(0, 1003)
              .mapToObj(
                  i ->
                      new WriteRequest(
                          bucket,
                          "rows/" + i,
                          ("row-" + i + "-é").getBytes(StandardCharsets.UTF_8),
                          "text/plain"))
              .toList();
      Source.from(writes)
          .runWith(bridge.sink(8), system)
          .toCompletableFuture()
          .get(60, TimeUnit.SECONDS);
      var values =
          bridge
              .readAll(bucket, "rows/", 8)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(60, TimeUnit.SECONDS);
      assertEquals(1003, values.size(), "Every page and write must be drained before completion");
      assertEquals(
          IntStream.range(0, 1003).mapToObj(i -> "row-" + i + "-é").collect(Collectors.toSet()),
          Set.copyOf(values));
      assertEquals(
          List.of(),
          bridge
              .readAll(bucket, "absent/", 2)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS));
      assertThrows(
          ExecutionException.class,
          () ->
              Source.single(new WriteRequest("missing-bucket", "bad", new byte[0], "text/plain"))
                  .runWith(bridge.sink(1), system)
                  .toCompletableFuture()
                  .get(10, TimeUnit.SECONDS));
    } finally {
      system.terminate();
    }
  }

  @Test
  void emptyFilteredPagesAdvanceAndBrokenContinuationFailsInsteadOfHanging() throws Exception {
    var system = ActorSystem.create("pagination-protocol-contract");
    try {
      for (String mode : List.of("advance", "missing", "repeat")) {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var requests = new AtomicInteger();
        server.createContext(
            "/",
            exchange -> {
              int page = requests.incrementAndGet();
              boolean last = mode.equals("advance") && page == 2;
              String xml =
                  "<ListBucketResult"
                      + " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>fixture</Name><IsTruncated>"
                      + !last
                      + "</IsTruncated>"
                      + (mode.equals("missing") || last
                          ? ""
                          : "<NextContinuationToken>next</NextContinuationToken>")
                      + (last
                          ? "<Contents><Key>one</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>etag</ETag><Size>1</Size></Contents>"
                          : "")
                      + "</ListBucketResult>";
              byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, bytes.length);
              exchange.getResponseBody().write(bytes);
              exchange.close();
            });
        server.start();
        try (var storage =
                new S3StorageClient(
                    config(
                        "http://127.0.0.1:" + server.getAddress().getPort(),
                        "fixture",
                        "fixture"));
            var executor = Executors.newSingleThreadExecutor()) {
          var bridge = new PekkoObjectStorageBridge(new ObjectStorageBridge(storage, executor));
          var result = bridge.list("fixture", "").runWith(Sink.seq(), system).toCompletableFuture();
          if (mode.equals("advance")) {
            assertEquals(
                List.of("one"),
                result.get(10, TimeUnit.SECONDS).stream().map(info -> info.key()).toList());
          } else {
            var failure =
                assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertEquals(
                "Object storage pagination did not advance", failure.getCause().getMessage());
          }
          assertEquals(mode.equals("missing") ? 1 : 2, requests.get());
        } finally {
          server.stop(0);
        }
      }
    } finally {
      system.terminate();
    }
  }

  private static NamedStorageClientConfig config(String endpoint, String key, String secret) {
    return new NamedStorageClientConfig() {
      @Override
      public String backend() {
        return "s3";
      }

      @Override
      public Optional<String> bucket() {
        return Optional.empty();
      }

      @Override
      public Optional<String> endpoint() {
        return Optional.of(endpoint);
      }

      @Override
      public Optional<String> publicEndpoint() {
        return Optional.of(endpoint);
      }

      @Override
      public Optional<String> region() {
        return Optional.of("us-east-1");
      }

      @Override
      public Optional<String> accessKey() {
        return Optional.of(key);
      }

      @Override
      public Optional<String> secretKey() {
        return Optional.of(secret);
      }

      @Override
      public Optional<Boolean> pathStyleAccess() {
        return Optional.of(true);
      }

      @Override
      public Map<String, String> properties() {
        return Map.of();
      }
    };
  }
}
