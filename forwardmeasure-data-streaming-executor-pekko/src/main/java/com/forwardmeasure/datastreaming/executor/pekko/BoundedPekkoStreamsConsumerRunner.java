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

import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.executor.streaming.CompletedExecutionHandle;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.pekko.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one {@code BOUNDED} ingestion via Pekko - named the same way {@link
 * BoundedKafkaStreamsConsumerRunner} already is on the Kafka Streams side (extracted 2026-09-25 out
 * of {@link PekkoStreamsDeliveryEngine}, a pure relocation, so both engines' bounded/continuous
 * logic reads as parallel, equally-discoverable classes).
 *
 * <p>Reconstructs an equivalent {@link IngestionSpec} from {@code plan} and delegates straight to
 * the existing, proven {@link PekkoIngestionRunner#run(IngestionSpec, ActorSystem)} (single-source)
 * or {@link PekkoCorrelationRunner#run(IngestionSpec, ActorSystem)} (correlated) - a legitimate
 * reuse, not a hack: {@link ExecutionPlan} carries every field an {@code IngestionSpec} does, so
 * the reconstruction is lossless, and rewriting already-tested Camel/backpressure logic a second
 * time against a different input type would add real risk for zero real benefit. Unlike the Kafka
 * Streams side, this is deliberately not a single class wrapping one runner - it stays a thin
 * dispatch over two already-separate, already-tested engine implementations, since Kafka Streams'
 * own bounded path never had that cardinality split to begin with (it doesn't support correlation
 * at all yet).
 */
final class BoundedPekkoStreamsConsumerRunner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BoundedPekkoStreamsConsumerRunner.class);

  private BoundedPekkoStreamsConsumerRunner() {}

  static CompletedExecutionHandle run(ExecutionPlan plan, ExecutionMode mode, ActorSystem system) {
    IngestionSpec spec =
        new IngestionSpec(
            plan.sources(),
            plan.blockingField(),
            plan.transforms(),
            plan.destination(),
            mode,
            plan.delivery(),
            plan.errors());
    String id = "fds-pekko-bounded-" + UUID.randomUUID();
    long startMillis = System.currentTimeMillis();
    try {
      if (spec.sources().size() == 1) {
        PekkoIngestionRunner.IngestionResult result = new PekkoIngestionRunner().run(spec, system);
        LOGGER.info(
            "run.completed engine=PEKKO_STREAMS mode=BOUNDED cardinality=SINGLE"
                + " recordsProcessed={} elapsedMs={}",
            result.recordsProcessed(),
            System.currentTimeMillis() - startMillis);
      } else {
        PekkoCorrelationRunner.CorrelationResult result = PekkoCorrelationRunner.run(spec, system);
        LOGGER.info(
            "run.completed engine=PEKKO_STREAMS mode=BOUNDED cardinality=CORRELATED"
                + " sourceCount={} groupCount={} elapsedMs={}",
            result.sourceCount(),
            result.groupCount(),
            System.currentTimeMillis() - startMillis);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("BoundedPekkoStreamsConsumerRunner: run failed", e);
    }
    return new CompletedExecutionHandle(id);
  }
}
