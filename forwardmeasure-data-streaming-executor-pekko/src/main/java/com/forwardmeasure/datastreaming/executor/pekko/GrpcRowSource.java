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

import com.forwardmeasure.datastreaming.mappers.MapSourceRow;
import com.forwardmeasure.datastreaming.mappers.SourceRow;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.pekko.NotUsed;
import org.apache.pekko.stream.javadsl.Source;

/**
 * Turns a gRPC server-streaming call's response stream into a {@link Source} of {@link SourceRow} -
 * added 2026-09-13, the source-side counterpart of {@code file}/{@code jdbc}/{@code kafka}. gRPC is
 * deliberately not routed through {@code camel-grpc} the way those go through Camel: a generated
 * blocking stub's own streaming-method call already returns a plain {@code Iterator<T>} directly
 * from {@code grpc-java} itself - exactly the shape {@link Source#fromIterator} wants - so a Camel
 * component would add a layer without adding a real capability (the same reasoning {@code
 * forwardmeasure-data-streaming-connector-object-storage}'s own {@code ObjectStorageBridge} already
 * documents for why it isn't Camel-backed either).
 *
 * <p>Unlike {@code file}/{@code jdbc}/{@code kafka} (one universal wire format FDS can parse
 * generically), a gRPC service's response message is a generated Protobuf class with no universal
 * "field by name" reflection this library can rely on - each real gRPC source genuinely needs its
 * own generated stub (from that service's own {@code .proto}) and a small function converting one
 * response message to a {@code Map<String,Object>}. That's real, unavoidable per-service glue, not
 * a design shortfall: there is no metadata-only way to consume an arbitrary gRPC service the way
 * there is for file/kafka/jdbc, since gRPC has no self-describing universal payload shape without
 * the generated code. This class is the one reusable piece every such source shares - turning an
 * already fetched {@code Iterator<T>} into rows - not a per-service class.
 */
public final class GrpcRowSource {

  private GrpcRowSource() {}

  /**
   * {@code responses} is already the caller's own generated blocking stub call's return value (e.g.
   * {@code SomeServiceGrpc.newBlockingStub(channel).streamThings(request)}) - this class never
   * touches a {@code Channel}/stub/service definition itself, only the resulting stream of already-
   * received messages, converted to rows via {@code toFields}.
   */
  public static <T> Source<SourceRow, NotUsed> from(
      Iterator<T> responses, Function<T, Map<String, Object>> toFields) {
    return Source.fromIterator(() -> responses)
        .map(response -> new MapSourceRow(toFields.apply(response)));
  }
}
