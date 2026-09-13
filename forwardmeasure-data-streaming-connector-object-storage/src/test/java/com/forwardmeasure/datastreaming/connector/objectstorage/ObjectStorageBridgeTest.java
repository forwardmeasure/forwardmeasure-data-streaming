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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.connector.objectstorage.ObjectStorageBridge.WriteRequest;
import com.forwardmeasure.objectstorage.ObjectInfo;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientRegistry;
import com.forwardmeasure.objectstorage.core.StorageConfigGroup;
import com.forwardmeasure.testcontainers.minio.MinioTestContainer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocks proof: {@link ObjectStorageBridge} writes and lists/reads objects against a real
 * S3-compatible server (MinIO) through the real, unmodified {@code forwardmeasure-object-storage}
 * client - the same {@code MinioTestContainer}/{@code StorageClientRegistry} setup {@code
 * forwardmeasure-object-storage}'s own {@code S3StorageClientIntegrationTest} uses.
 */
class ObjectStorageBridgeTest {

  private static final String BUCKET = "data-streaming-connector-object-storage-test";
  private static final int ROW_COUNT = 40;

  @Test
  void writesThenListsAndReadsBackEveryObject() throws Exception {
    try (MinioTestContainer minio = new MinioTestContainer().start();
        StorageClientRegistry registry = new StorageClientRegistry(configurationGroup(minio))) {
      StorageClient storage = registry.getDefault();
      storage.createBucket(new StorageClient.CreateBucketRequest(BUCKET, Map.of(), Map.of()));

      ExecutorService executor = Executors.newFixedThreadPool(4);
      ActorSystem system = ActorSystem.create("object-storage-bridge-test");
      try {
        ObjectStorageBridge bridge = new ObjectStorageBridge(storage, executor);

        List<WriteRequest> writes =
            IntStream.range(0, ROW_COUNT)
                .mapToObj(
                    i ->
                        new WriteRequest(
                            BUCKET,
                            "evidence/row-" + i + ".txt",
                            ("row " + i).getBytes(StandardCharsets.UTF_8),
                            "text/plain"))
                .toList();

        CompletionStage<Done> writeCompletion = Source.from(writes).runWith(bridge.sink(4), system);
        writeCompletion.toCompletableFuture().join();

        List<ObjectInfo> listed =
            bridge
                .list(BUCKET, "evidence/")
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .join();
        assertEquals(ROW_COUNT, listed.size());

        List<String> contents =
            bridge
                .readAll(BUCKET, "evidence/", 4)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .join();
        Set<String> expected =
            IntStream.range(0, ROW_COUNT).mapToObj(i -> "row " + i).collect(Collectors.toSet());
        assertEquals(ROW_COUNT, contents.size());
        assertTrue(Set.copyOf(contents).containsAll(expected));
      } finally {
        system.terminate();
        executor.shutdown();
      }
    }
  }

  private static NamedStorageClientConfig configuration(MinioTestContainer minio) {
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
        return Optional.of(minio.hostEndpoint().toString());
      }

      @Override
      public Optional<String> publicEndpoint() {
        return Optional.of(minio.hostEndpoint().toString());
      }

      @Override
      public Optional<String> region() {
        return Optional.of("us-east-1");
      }

      @Override
      public Optional<String> accessKey() {
        return Optional.of(minio.accessKey());
      }

      @Override
      public Optional<String> secretKey() {
        return Optional.of(minio.secretKey());
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

  private static StorageConfigGroup configurationGroup(MinioTestContainer minio) {
    return new StorageConfigGroup() {
      @Override
      public Optional<String> defaultClient() {
        return Optional.of("evidence");
      }

      @Override
      public Map<String, NamedStorageClientConfig> clients() {
        return Map.of("evidence", configuration(minio));
      }

      @Override
      public Map<String, String> schemeClients() {
        return Map.of("s3", "evidence");
      }

      @Override
      public Map<String, String> uriClients() {
        return Map.of();
      }
    };
  }
}
