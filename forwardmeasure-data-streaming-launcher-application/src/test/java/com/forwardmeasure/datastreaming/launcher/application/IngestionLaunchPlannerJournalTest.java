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
package com.forwardmeasure.datastreaming.launcher.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import com.forwardmeasure.datastreaming.api.ExecutionMode;
import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.datastreaming.api.SourcePlan;
import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Continuous Pekko workers that keep state get the installation's journal settings - the password
 * only as a Secret reference, a Cloud SQL Auth Proxy sidecar where configured - and no other worker
 * does.
 */
class IngestionLaunchPlannerJournalTest {

  private static final String WORKER = "example/worker@sha256:" + "a".repeat(64);
  private static final String PROXY = "example/cloud-sql-proxy@sha256:" + "b".repeat(64);

  private static final RunnerImages.PekkoJournal JOURNAL =
      new RunnerImages.PekkoJournal(
          "pekko-postgresql",
          Map.of(
              "FDS_JOURNAL_CONTROL_PLANE_URL", "jdbc:postgresql://localhost:5432/control",
              "FDS_JOURNAL_POSTGRES_USER", "datastreaming"),
          Map.of(
              "FDS_JOURNAL_POSTGRES_PASSWORD",
              new RunnerImages.SecretRef("data-streaming-database-credentials", "password")),
          new RunnerImages.DatabaseProxy(
              PROXY,
              new RunnerImages.SecretRef(
                  "data-streaming-database-credentials", "db-cloud-sql-instance"),
              "data-streaming-worker"));

  private static IngestionLaunchPlanner planner(
      RunnerImages.PekkoJournal journal, Set<String> images) {
    return new IngestionLaunchPlanner(
        IngestionJobPolicy.configured(Set.of("workers"), images),
        new RunnerImages(
            DeliveryEngineKind.PEKKO_STREAMS,
            new RunnerImages.Runner(WORKER, "pekko"),
            null,
            new RunnerImages.Runner(WORKER, "spark"),
            "broker:9092",
            journal),
        List.of());
  }

  private static IngestionSpec continuous(int sources) {
    TransformSpec mapper =
        new TransformSpec(
            "party",
            List.of(
                new TransformSpec.FieldRule("uid", "uid", null, null, null, null, null),
                new TransformSpec.FieldRule("name", "name", null, null, null, null, null)));
    List<SourcePlan> plans =
        java.util.stream.IntStream.range(0, sources)
            .mapToObj(
                i ->
                    new SourcePlan(
                        "s" + i,
                        new SourceSpec(
                            "kafka", "kafka:in-" + i + "?brokers=broker:9092", null, null),
                        mapper,
                        1.0 / (i + 1)))
            .toList();
    return new IngestionSpec(
        plans,
        sources > 1 ? "uid" : null,
        null,
        new SinkSpec("opensearch", "http://search:9200", "people", null, null),
        ExecutionMode.CONTINUOUS,
        null,
        null);
  }

  private static Map<?, ?> primary(IngestionLaunchPlanner planner, IngestionSpec spec) {
    return (Map<?, ?>)
        planner
            .workflowInput(
                new DirectLaunchRequest("run", "workers", spec, Map.of(), Map.of(), null), "t:run")
            .get("primary");
  }

  @Test
  void aCorrelatingContinuousWorkerGetsTheJournalWithItsPasswordOnlyAsASecretReference() {
    Map<?, ?> worker = primary(planner(JOURNAL, Set.of(WORKER, PROXY)), continuous(2));
    Map<?, ?> env = (Map<?, ?>) worker.get("env");
    assertEquals("pekko-postgresql", env.get("FDS_PLATFORM_ENGINE"));
    assertEquals(
        "jdbc:postgresql://localhost:5432/control", env.get("FDS_JOURNAL_CONTROL_PLANE_URL"));
    assertEquals("datastreaming", env.get("FDS_JOURNAL_POSTGRES_USER"));
    assertEquals(
        Map.of(
            "secretKeyRef",
            Map.of("name", "data-streaming-database-credentials", "key", "password")),
        env.get("FDS_JOURNAL_POSTGRES_PASSWORD"));
    assertFalse(env.containsKey("TENANT_ID"), "fowf supplies the tenant id, never the launcher");
    assertEquals(
        Map.of(
            "image",
            PROXY,
            "instanceSecretName",
            "data-streaming-database-credentials",
            "instanceSecretKey",
            "db-cloud-sql-instance",
            "port",
            5432),
        worker.get("databaseProxy"));
    assertEquals("data-streaming-worker", worker.get("serviceAccountName"));
  }

  @Test
  void theProxyImagePassesTheSameImagePolicyAsTheWorker() {
    assertThrows(
        SecurityException.class, () -> primary(planner(JOURNAL, Set.of(WORKER)), continuous(2)));
  }

  @Test
  void aStatefulRunWithoutAConfiguredJournalIsRefusedBeforeDispatch() {
    var failure =
        assertThrows(
            IllegalStateException.class,
            () -> primary(planner(null, Set.of(WORKER)), continuous(2)));
    assertTrue(failure.getMessage().contains("journal"), failure.getMessage());
  }

  @Test
  void aStatelessContinuousWorkerGetsNoJournalAndNoSidecar() {
    Map<?, ?> worker = primary(planner(JOURNAL, Set.of(WORKER, PROXY)), continuous(1));
    Map<?, ?> env = (Map<?, ?>) worker.get("env");
    assertFalse(env.containsKey("FDS_PLATFORM_ENGINE"));
    assertFalse(env.containsKey("FDS_JOURNAL_POSTGRES_PASSWORD"));
    assertNull(worker.get("databaseProxy"));
    assertNull(worker.get("serviceAccountName"));
    // Nor does a journal without a proxy add one.
    var noProxy =
        new RunnerImages.PekkoJournal("pekko-cassandra", JOURNAL.env(), JOURNAL.secretEnv(), null);
    assertNull(primary(planner(noProxy, Set.of(WORKER)), continuous(2)).get("databaseProxy"));
  }

  @Test
  void configuredJournalSettingsParseFromTheLauncherProperties() {
    RunnerImages images =
        RunnerImages.configured(
            "pekko-postgresql",
            WORKER,
            "pekko",
            null,
            null,
            null,
            null,
            WORKER,
            "spark",
            "broker:9092",
            "FDS_JOURNAL_CONTROL_PLANE_URL=jdbc:postgresql://localhost:5432/control;FDS_JOURNAL_POSTGRES_USER=datastreaming",
            "FDS_JOURNAL_POSTGRES_PASSWORD=data-streaming-database-credentials/password",
            PROXY,
            "data-streaming-database-credentials/db-cloud-sql-instance",
            "data-streaming-worker");
    assertEquals(JOURNAL, images.journal());
    RunnerImages withoutProxy =
        RunnerImages.configured(
            "pekko-postgresql",
            WORKER,
            "pekko",
            null,
            null,
            null,
            null,
            WORKER,
            "spark",
            "broker:9092",
            "A=b",
            "",
            "",
            "",
            "");
    assertNull(withoutProxy.journal().databaseProxy());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RunnerImages.configured(
                "pekko-postgresql",
                WORKER,
                "pekko",
                null,
                null,
                null,
                null,
                WORKER,
                "spark",
                "broker:9092",
                "A=b",
                "",
                PROXY,
                "s/k",
                " "));
  }
}
