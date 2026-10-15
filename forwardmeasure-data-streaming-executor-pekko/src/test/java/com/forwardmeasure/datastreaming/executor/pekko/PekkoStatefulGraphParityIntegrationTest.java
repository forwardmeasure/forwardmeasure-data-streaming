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
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.ExecutionPlan;
import com.forwardmeasure.datastreaming.core.ExecutionPlanCompiler;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import com.forwardmeasure.datastreaming.executor.streaming.ExecutionHandle;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Case;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphFixtures.Input;
import com.forwardmeasure.datastreaming.testfixtures.StatefulGraphParity;
import com.forwardmeasure.testcontainers.junit.kafka.WithKafkaContainer;
import com.forwardmeasure.testcontainers.junit.opensearch.WithOpenSearchContainer;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every stateful parity case through the real Pekko engine - real topics in, the real OpenSearch
 * sink out, continuous state on the real journal - with exactly the outputs the fixtures define
 * (and {@code KafkaStreamsStatefulGraphParityIntegrationTest} asserts for Kafka Streams).
 */
@WithKafkaContainer
@WithOpenSearchContainer
class PekkoStatefulGraphParityIntegrationTest {

  private static PekkoJournalFixture journal;
  private static ActorSystem system;

  @BeforeAll
  static void start() {
    journal = PekkoJournalFixture.start(PekkoJournalFixture.Backend.POSTGRESQL);
    system = journal.system("pekko-stateful-parity");
  }

  @AfterAll
  static void stop() {
    system.terminate();
    system.getWhenTerminated().toCompletableFuture().join();
    journal.close();
  }

  static Stream<Arguments> runs() {
    return StatefulGraphFixtures.all().stream()
        .flatMap(
            fixture ->
                Stream.of(
                    Arguments.of(fixture.name(), ExecutionMode.BOUNDED),
                    Arguments.of(fixture.name(), ExecutionMode.CONTINUOUS)));
  }

  static Stream<String> cases() {
    return StatefulGraphFixtures.all().stream().map(Case::name);
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("runs")
  @Timeout(180)
  void writesExactlyTheFixturesOutputs(
      String name, ExecutionMode mode, KafkaTestContainer kafka, OpenSearchTestContainer opensearch)
      throws Exception {
    Case fixture = fixture(name);
    var run = StatefulGraphParity.prepare(fixture, kafka.bootstrapServers());
    ExecutionPlan plan = plan(run, mode, kafka, opensearch);
    ExecutionHandle handle = new PekkoStreamsDeliveryEngine(system).execute(plan, mode);
    List<Map<String, Object>> expected =
        mode == ExecutionMode.BOUNDED ? fixture.bounded() : fixture.continuous();
    List<Map<String, Object>> written;
    try {
      if (mode == ExecutionMode.BOUNDED) {
        assertFalse(handle.isRunning(), "a bounded run completes before execute returns");
        written = StatefulGraphParity.documents(run, opensearch.hostEndpoint());
      } else {
        written =
            StatefulGraphParity.awaitDocuments(
                run, opensearch.hostEndpoint(), expected.size(), Duration.ofSeconds(90));
      }
    } finally {
      handle.stop();
    }
    assertEquals(
        StatefulGraphFixtures.canonical(expected), StatefulGraphFixtures.canonical(written));
  }

  /**
   * A continuous worker stopped midway and started again - a new worker for the same run - resumes
   * from its committed offsets and its journal state: together the two write exactly an
   * uninterrupted run's outputs.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  @Timeout(240)
  void aRestartedContinuousWorkerResumesFromTheJournal(
      String name, KafkaTestContainer kafka, OpenSearchTestContainer opensearch) throws Exception {
    Case fixture = fixture(name);
    int half = fixture.inputs().size() / 2;
    var run = StatefulGraphParity.topics(fixture, kafka.bootstrapServers());
    StatefulGraphParity.produce(run, kafka.bootstrapServers(), 0, half);
    ExecutionPlan plan = plan(run, ExecutionMode.CONTINUOUS, kafka, opensearch);

    ExecutionHandle first =
        new PekkoStreamsDeliveryEngine(system).execute(plan, ExecutionMode.CONTINUOUS);
    try {
      StatefulGraphParity.awaitDocuments(
          run, opensearch.hostEndpoint(), outputsOf(fixture, half), Duration.ofSeconds(60));
    } finally {
      first.stop();
    }
    StatefulGraphParity.produce(run, kafka.bootstrapServers(), half, fixture.inputs().size());
    ExecutionHandle second =
        new PekkoStreamsDeliveryEngine(system).execute(plan, ExecutionMode.CONTINUOUS);
    List<Map<String, Object>> written;
    try {
      written =
          StatefulGraphParity.awaitDocuments(
              run, opensearch.hostEndpoint(), fixture.continuous().size(), Duration.ofSeconds(90));
    } finally {
      second.stop();
    }
    assertEquals(
        StatefulGraphFixtures.canonical(fixture.continuous()),
        StatefulGraphFixtures.canonical(written));
  }

  private static ExecutionPlan plan(
      StatefulGraphParity.Run run,
      ExecutionMode mode,
      KafkaTestContainer kafka,
      OpenSearchTestContainer opensearch) {
    return ExecutionPlanCompiler.compile(
        StatefulGraphParity.spec(run, mode, kafka.bootstrapServers(), opensearch.hostEndpoint()),
        DeliveryEngineKind.PEKKO_STREAMS);
  }

  /** How many outputs a continuous run has written after the first {@code count} inputs. */
  private static int outputsOf(Case fixture, int count) {
    StatefulGraph graph = GraphRecords.bounded(fixture.graph(), fixture.blockingField());
    int outputs = 0;
    for (Input input : fixture.inputs().subList(0, count))
      outputs += graph.process(input.sourceKey(), input.record()).size();
    return outputs;
  }

  private static Case fixture(String name) {
    return StatefulGraphFixtures.all().stream()
        .filter(candidate -> candidate.name().equals(name))
        .findFirst()
        .orElseThrow();
  }
}
