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
package com.forwardmeasure.datastreaming.connector.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Proves the bridge layer works with no Pekko involved (step 3's own definition of done): a real
 * file on disk gets read through {@link CamelBridge#source} into a {@link Publisher} that a plain,
 * hand-rolled Reactive Streams {@link Subscriber} can consume.
 */
class CamelBridgeTest {

  @Test
  void realFileFlowsThroughSourceIntoAPlainSubscriber(@TempDir Path tempDir) throws Exception {
    String content = "hello from the camel bridge";
    Files.writeString(tempDir.resolve("input.txt"), content, StandardCharsets.UTF_8);

    List<String> received = new CopyOnWriteArrayList<>();

    try (CamelBridge bridge = new CamelBridge()) {
      String uri =
          "file:"
              + tempDir.toAbsolutePath()
              + "?fileName=input.txt&noop=true&initialDelay=0&delay=100";
      Publisher<String> source = bridge.source(uri, String.class);

      source.subscribe(
          new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
              subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(String item) {
              received.add(item);
            }

            @Override
            public void onError(Throwable throwable) {
              throw new AssertionError("unexpected error from Camel source", throwable);
            }

            @Override
            public void onComplete() {}
          });

      awaitAtLeastOneItem(received, Duration.ofSeconds(10));
    }

    assertTrue(received.size() >= 1, "expected at least one item read through the bridge");
    assertEquals(content, received.get(0));
  }

  private static void awaitAtLeastOneItem(List<String> received, Duration timeout)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    while (received.isEmpty() && Instant.now().isBefore(deadline)) {
      Thread.sleep(50);
    }
  }
}
