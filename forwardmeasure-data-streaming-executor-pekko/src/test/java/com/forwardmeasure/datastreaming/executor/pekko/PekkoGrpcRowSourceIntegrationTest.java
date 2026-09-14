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

import com.forwardmeasure.datastreaming.api.TransformSpec;
import com.forwardmeasure.datastreaming.executor.pekko.grpctest.PartyFeedGrpc;
import com.forwardmeasure.datastreaming.executor.pekko.grpctest.PartyRecord;
import com.forwardmeasure.datastreaming.executor.pekko.grpctest.StreamPartiesRequest;
import com.forwardmeasure.datastreaming.mappers.FieldMappingEngine;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real, no-mocks proof of {@link GrpcRowSource} (added 2026-09-13, the gRPC-as-a-source mechanism):
 * a real in-process gRPC server streams two real {@code PartyRecord} messages back to a real
 * generated blocking stub call, whose {@code Iterator<PartyRecord>} return value {@link
 * GrpcRowSource#from} turns into a {@code Source<SourceRow, ?>}, then real {@link
 * FieldMappingEngine} mapping runs on top - the same shape every other connector's own test proves
 * end to end. The {@code .proto} behind {@code PartyFeedGrpc}/{@code PartyRecord} is a toy,
 * test-only service definition (see {@code src/test/proto/party_feed.proto}) - there is no real
 * external gRPC source yet, so this proves the reusable adapter, not a specific production service.
 */
class PekkoGrpcRowSourceIntegrationTest {

  private Server server;
  private ManagedChannel channel;

  @BeforeEach
  void startServer() throws Exception {
    String serverName = "grpc-row-source-test-" + UUID.randomUUID();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(
                new PartyFeedGrpc.PartyFeedImplBase() {
                  @Override
                  public void streamParties(
                      StreamPartiesRequest request, StreamObserver<PartyRecord> responseObserver) {
                    responseObserver.onNext(
                        PartyRecord.newBuilder().setUid("S1").setName("Alice Anderson").build());
                    responseObserver.onNext(
                        PartyRecord.newBuilder().setUid("S2").setName("Bob Baker").build());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  @AfterEach
  void stopServer() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  @Test
  void readsARealGrpcServerStreamingResponseAsMappedRows() throws Exception {
    PartyFeedGrpc.PartyFeedBlockingStub stub = PartyFeedGrpc.newBlockingStub(channel);
    Iterator<PartyRecord> responses = stub.streamParties(StreamPartiesRequest.newBuilder().build());

    FieldMappingEngine engine = new FieldMappingEngine();
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null)));

    ActorSystem system = ActorSystem.create("grpc-row-source-integration-test");
    List<Map<String, Object>> mapped;
    try {
      mapped =
          GrpcRowSource.from(
                  responses, record -> Map.of("uid", record.getUid(), "name", record.getName()))
              .map(row -> engine.map(row, mapper))
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(15, TimeUnit.SECONDS);
    } finally {
      system.terminate();
    }

    assertEquals(2, mapped.size());
    assertEquals("S1", mapped.get(0).get("uid"));
    assertEquals("Alice Anderson", mapped.get(0).get("name"));
    assertEquals("S2", mapped.get(1).get("uid"));
    assertEquals("Bob Baker", mapped.get(1).get("name"));
  }
}
