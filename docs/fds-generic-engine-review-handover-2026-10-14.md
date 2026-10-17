# Review handover: FDS generic engine change set (2026-10-14)

For a reviewer starting with no context. It lists every change in this change set: what changed,
why, and what to check. The design and the user's decisions are in
`fds-generic-engine-plan-2026-10-14.md` in this directory. Read its "Decisions" and
"Implementation record" sections first.

**Current FEI coordination, 2026-10-15:** lifecycle/deletion, customer-master identity and the
directory are owned by the
[consolidated FEI implementation plan](../../forwardmeasure-entity-intelligence/docs/fei-lifecycle-identity-and-entity-directory-implementation-plan-2026-10-15.md).
This review handover retains the historical engine-change scope. Its statements about another
agent handling GEMS work do not supersede that current FEI ownership.

Nothing is committed. Every repo is on its working branch with uncommitted changes. Some of those
changes belong to other work in progress; section 9 says which parts are **not** this change set.

## 1. Repositories and how to build them

All repositories are under `/home/pn/Documents/code/forwardmeasure/`.

| Repository | What changed here |
|---|---|
| `forwardmeasure-data-streaming` (FDS) | Most of the work: transform catalog, engine choice, loader, journal, stateful graphs |
| `forwardmeasure-entity-intelligence` (FEI) | Domain transforms moved in; simple-source ingestion on either engine |
| `forwardmeasure-openworkflow` (fowf) | Kubernetes Deployment secret env and Cloud SQL proxy sidecar; migrations Helm; image and service-account policy |
| `forwardmeasure-platform` | Data contract enum; dependency management; shared Helm values; platform-secrets; greenfield OpenTofu |
| `forwardmeasure-testcontainers` | Cassandra container with password authentication |
| `openworkflow-k8s-setup` | Terraform: a runtime-password secret and a workload identity |

Build order (each repo installs to `~/.m2` for the next):
testcontainers, then fowf, then platform, then FDS, then FEI.

The commands used:

```
mvn -B -pl forwardmeasure-testcontainers-cassandra spotless:apply install -DskipTests            # testcontainers
mvn -B -pl openworkflow-migration,openworkflow-kubernetes-deployment-lifecycle,openworkflow-operation-adapter/openworkflow-operation-adapter-kubernetes-deployment spotless:apply install -DskipTests -Djacoco.skip=true   # fowf
mvn -B -N install -DskipTests && mvn -B -pl forwardmeasure-platform-data-contracts install -DskipTests -Djacoco.skip=true   # platform
mvn -B spotless:apply && mvn -B clean install -DskipTests -Djacoco.skip=true                       # FDS, then FEI
```

`-DskipTests` still compiles the tests. RAT (licence headers), Spotless check and the Maven
enforcer rules run in these builds.

**Status:** all six build successfully: testcontainers, fowf's changed modules, platform, the FDS
clean install (29 modules) and the FEI clean install (82 modules). **No new or changed test has
been run yet.**

House rules the code must follow:
- Real infrastructure through forwardmeasure-testcontainers. Mockito and H2 are banned.
- Every change covers Quarkus, Spring and Micronaut.
- Neither engine's jars may be on the other engine's classpath.
- No cyclic dependencies between repos.
- FDS, fowf and platform are Apache-licensed (ASF header). FEI and platform data-contracts are
  proprietary (ForwardMeasure header).
- Workers carry a tenant id (`TENANT_ID`, which fowf injects), never a database name.
- Secrets reach pods only as `secretKeyRef`; fowf journals workflow input verbatim, so a secret
  value must never be in a workflow input.

## 2. Part A: the customer-master ingestion fixes

Background: a customer-master monitored-population run rejected 29% of rows with
`Document violates contract: /entity_kind: enum`. The full analysis is in the plan, Part A.

- **Contract enum widened.** In `forwardmeasure-platform/forwardmeasure-platform-data-contracts`,
  the `entity_kind` enum embedded in `customer-master-source-schema-v1.yaml` now lists all seven
  concrete kinds. Before, it listed two. The schema's hash changed, so six FEI test-fixture
  changesets that pin it were re-pinned to
  `1f17dbf4845ef2f6f290ddd44afb4afd515cf8e87a7defcd50daddcdbb14d4d5`. They are in
  `forwardmeasure-entity-intelligence-migrations/src/main/resources/db/changelog/test-fixtures/`.
  - Check that the hash matches the file, and that `ResolutionTargetTest` (FEI worker-common)
    passes.
- **A3, the violation names the value.** `DocumentSchema.describe` (FEI worker-common,
  `worker/ingestion/contract/DocumentSchema.java`) now includes the offending value for `enum` and
  `const` violations.
- **A4/D5, single logging.** `PekkoIngestionPipeline.mapOrHandle` (FDS executor-pekko) uses
  `Optional.ofNullable`. A `null` from the transform now means the caller already skipped and
  logged the row, so the row is no longer logged a second time with a `NullPointerException`.
- **A2, party kind.** FEI's customer-master party-kind rule: `Individual` is a person, any other
  non-blank type is an organization, blank is rejected. `ProviderMappingSemanticsTest` (FEI
  ingestion-worker) was updated to that confirmed rule.
  - FDS's old copy of the rule now exists only as the test transform
    `test_classify_customer_master_party_kind` (section 3).

## 3. D1, D2 and D3: the transform catalog, and domain logic out of FDS

**FDS `forwardmeasure-data-streaming-api`**
- `TransformCharacteristics`: added `PURE_ROW_FUNCTION`, the default for a named transform.

**FDS `forwardmeasure-data-streaming-transforms`**
- `NamedTransform`: each transform now declares its own `characteristics()`; `of(chars, fn)`
  builds one.
- New `NamedTransformProvider`, a ServiceLoader interface through which products contribute
  transforms.
- New `NamedTransformCatalog`, an immutable, Serializable catalog (Spark ships it to executors):
  - `builtIns()`: FDS's generic transforms only;
  - `installed()`: built-ins plus every `NamedTransformProvider` on the classpath;
  - `with(Map)`: adds caller transforms;
  - `get`, `characteristics`, `contains`, `names`.
  - A duplicate name is an error, never an override.
- `NamedTransformRegistry` is **deleted**.
- `NamedTransformFunctions`: every domain function is removed. What remains are the domain-free
  parsers: country, date, identifier, lists, URLs, `join_labeled_fields`.
- The entity-matching dependency is removed from this module's pom.
- Tests: `NamedTransformCatalogTest`; `ProviderTransformContractTest` now goes through the catalog.

**FDS `forwardmeasure-data-streaming-mappers`**
- `FieldMappingEngine` has two constructors: `FieldMappingEngine()` uses
  `NamedTransformCatalog.installed()`, and `FieldMappingEngine(NamedTransformCatalog)` takes a
  catalog. The three-argument `map` that took a transform map is removed.
- `TransformCharacteristicsRegistry` and its test are **deleted**; characteristics come from the
  catalog.

**FDS `forwardmeasure-data-streaming-core`**
- `ExecutionPlanCompiler.compile(spec, engine)` and `compile(spec, catalog, engine)`. The engine
  is passed in; the planner only decides whether to add a Spark stage (HEAVY transforms).
  `installationEngine()` reads `FDS_DELIVERY_ENGINE` (an enum name) for the Spark runners.
- `SparkHandoffSpecs` passes `plan.transforms()` (the graph) to the delivery-stage spec.

**FDS `forwardmeasure-data-streaming-test-fixtures` (test scope only)**
- `TestTransforms` holds test-only transforms, all named `test_*`:
  - `test_classify_party_category`, `test_classify_party_kind`, `test_classify_party_categories`;
  - `test_parse_tilde_delimited_locations`;
  - `test_classify_customer_master_party_kind`, `test_build_customer_master_locations`,
    `test_build_customer_master_identifiers`;
  - `test_screen_against_reference`: HEAVY, with simple self-contained token-overlap (Jaccard)
    scoring, to exercise the Spark route.
- `TestTransformProvider` registers them through
  `META-INF/services/...NamedTransformProvider`. This proves the plug-in mechanism.
- `TestTransformImages` builds `<image>-test-transforms` images: the production image plus this
  module's classes in `/deployments/extensions/`. The executor, loader and Spark jars carry
  `Class-Path: extensions/` in their manifests, so a product or a test image adds transforms
  without rebuilding FDS.
- Fixture YAMLs were renamed to the `test_*` transforms. `TestTransformsTest` was added.
- `SharedSourceContractTest` was **deleted**; see section 9.
- The pom now depends on transforms, slf4j, testcontainers, jackson and kafka-clients. It dropped
  data-contracts and core.

**FDS root `pom.xml` enforcer rule:** `test-fixtures` may be used only in test scope, and
entity-matching and platform data-contracts are banned in every scope.
- Check: grep FDS main code for domain names (WorldCheck, customer master, screening). Only
  comments and test code should mention them.

**FEI**
- `forwardmeasure-entity-intelligence-worker-common`:
  - new `worker/ingestion/mapping/ProviderVocabularyTransforms` (and its test): the provider
    vocabulary transforms that left FDS;
  - `IngestionTransforms.catalog()`: FEI's catalog, which is FDS's `installed()` plus FEI's own
    transforms;
  - `IngestionContract` maps through that catalog.
- `ingestion-worker`: the FDS test-fixtures dependency is removed. New `CustomerMasterSampleRows`
  and `CustomerMasterSampleRowsContractTest` replace what FEI used to borrow from FDS.

## 4. D4: the platform engine governs FDS

The installation has exactly one delivery engine: forwardmeasure-platform's `platform.engine`
(`kafka-streams`, `pekko-postgresql` or `pekko-cassandra`), the same value fowf uses.

**API**
- `DeliveryEngineKind.fromPlatformEngine(String)` maps `kafka-streams` to `KAFKA_STREAMS` and the
  two Pekko values to `PEKKO_STREAMS`.

**New module `forwardmeasure-data-streaming-source-readers`**
- Moved here from executor-pekko so both engines and the loader share them: `CsvSourceRow`,
  `CsvHeader`, `StreamingTextRows`, `TextContent`, `SqlDataSources` (now public).
- New `BoundedRowReader`.
- The old copies in executor-pekko are deleted.

**New module `forwardmeasure-data-streaming-loader`** (image `data-streaming-loader`)
- On a Kafka Streams installation, a bounded file load is two Jobs. The loader writes each source's
  rows, as unmapped JSON, to `<loadTopic>-source-<i>` (one partition, 7-day retention). Then the
  Kafka Streams delivery Job runs the original mappers over those topics.
- Classes: `KafkaRowWriter`, and `SourceLoader` (`load(...)` and `main`; env `FDS_LOAD_TOPIC`,
  `KAFKA_BOOTSTRAP_SERVERS`).
- Tests: `SourceLoaderIntegrationTest`. There is a Dockerfile.

**Core**
- New `LoaderHandoffSpecs`: `needsLoader` and `needsLoading` decide when a load stage is needed;
  `sourceTopic` and `deliveryStageSpec(plan, brokers, loadTopic)` build the delivery stage.
  Tested in `LoaderHandoffSpecsTest`.

**Launcher (`forwardmeasure-data-streaming-launcher-application`)**
- New `RunnerImages`: the installation's runners and settings.
  - Record fields: `engine`, `delivery`, `loader`, `spark`, `kafkaBootstrapServers`, `journal`.
  - Spark and the brokers are always required. The loader is required on Kafka Streams and
    rejected on Pekko.
  - `configured(...)` takes the raw properties. `assignments("K=v;K=v")` parses literal env;
    `secretAssignments("K=secret/key;...")` parses secret references.
  - Nested records: `PekkoJournal(platformEngine, env, secretEnv, databaseProxy)`,
    `DatabaseProxy(image, instance SecretRef, serviceAccountName)`, `SecretRef(name, key)`.
- `IngestionLaunchPlanner(policy, RunnerImages, pullSecrets)`:
  - `Stage` is `SPARK`, `LOAD` or `DELIVERY`; `firstStage(plan)` picks the first.
  - `loadTopic(executionKey)`; `deliveryPlan` and `deliveryStageSpec`;
    `runner(stage)`; `stageEnvironment` adds `FDS_EXECUTION_ID`, `FDS_DELIVERY_ENGINE` and the
    Spark or load env.
  - `worker()`: see section 6 for the journal additions.
- `DirectIngestionLauncher`: `launchFirstStage`. Observe and cancel handle the `:load` stage the
  way they already handled `:spark`, sequenced by the existing first-stage/handoff-ConfigMap
  mechanism.
- Tests updated: `DirectIngestionLauncherTest` and `IngestionLaunchPlannerTest` (including new
  loader-stage tests), plus the real-image integration tests.

**Framework bindings (all three) and the launcher deployments' `application.yaml` (all three)**
- They read `datastreaming.launcher.engine` (required), the pekko, kafka-streams, loader and spark
  image and command settings, and the journal settings (section 6).

**Deployments**
- About 36 launcher smoke tests now set the engine, loader, Spark and Kafka settings.
- New `DirectIngestionMatrix{Quarkus,Spring,Micronaut}KafkaStreamsCsvLoadSmokeTest`: a CSV file
  load through the real loader and Kafka Streams images.
- `deployments/pom.xml` gains `fds.acceptance.loader.image`.

**Engine-independence enforcer rules**, per module, `bannedDependencies` with transitive search:
- executor-pekko bans kafka-streams;
- executor-kafka-streams and the loader ban Pekko;
- source-readers, executor-spark, launcher-application and migrations ban both.

**Helm (FDS `deploy/helmfile`)**
- `helmfiles/launcher.yaml.gotmpl`:
  - sets `DATASTREAMING_LAUNCHER_ENGINE` from `platform.engine`, and only that engine's runner
    images, plus the loader on Kafka Streams;
  - `K8S_IMAGES` is the launcher's own image allowlist;
  - **fix:** image references are `repo@digest` when a digest is pinned and `repo:tag` otherwise.
    Before, an unpinned image rendered as `repo@`.
- `environments/base.yaml`: the loader image pin (digest empty until pushed).

**FEI: simple-source ingestion on the selected engine**
- `worker-common`, new package `worker/ingestion/simple`: `SimpleSourceRows` (shared row
  processing: map, write, checkpoint, rejection accounting) and `SimpleSourceMappings.parse`.
- `ingestion-worker`: `SimpleSourceIngestionWorker`, the Pekko path, is refactored onto
  `SimpleSourceRows`.
- `delivery-worker`:
  - `SimpleSourceLoadStage`, `SimpleSourceDeliveryStage` and `SimpleSourceStageJob` (env
    `INGESTION_STAGE=load|deliver`, `LOAD_TOPIC`, `KAFKA_BOOTSTRAP_SERVERS`);
  - the pom adds the FDS loader, and kotlin-stdlib as provided (it fixes `-Werror` Kotlin metadata
    warnings from camel-sql and spring-jdbc).
- New deployment leaves `deployments/simple-source-stage-worker/{quarkus,spring,micronaut}`, with
  images `entity-intelligence-simple-source-stage-worker-*`. Each has a main, a binding, an
  enforcer rule (no Pekko), JCL, a Kafka testcontainer and a two-stage smoke test.
- `ingestion-service`:
  - `SimpleSourceEngine` (and its test);
  - both worker-config records gain a `simpleSourceEngine` component;
  - the bindings (all three) read `entityintelligence.ingestion.engine` and
    `simple-source-stage-worker.image`;
  - the `application.yaml` files use `${ENTITY_INTELLIGENCE_INGESTION_ENGINE}`;
  - test configs use `pekko-postgresql`.
- `workflows/.../simple-source-ingestion.yaml` 1.5.0:
  - an `engine` input; on Kafka Streams `stageJobImage`, `kafkaBootstrapServers` and `loadTopic`
    are required, otherwise `jobImage` is;
  - steps `runIngestion` (Pekko), and `runLoad` then `runDelivery` (Kafka Streams);
  - the AsyncAPI gains `simple-source-load.*` and `simple-source-delivery.*` channels;
  - `SimpleSourceIngestionWorkflowCompilerTest` is updated.
- Helm: `releases/entity-intelligence-services/base.yaml.gotmpl` adds
  `ENTITY_INTELLIGENCE_INGESTION_ENGINE` and `..._SIMPLE_SOURCE_STAGE_WORKER_IMAGE`;
  `environments/base.yaml` pins `entityIntelligenceSimpleSourceStageWorker` (digests empty).

## 5. D6: stateful transform graphs on both engines

**Model (`TransformGraph` in FDS api, rewritten)**
- Node kinds:
  - `map`: a `TransformSpec`;
  - `deduplicate`: `key`, `timeField`, `window`;
  - `window_aggregate`: `key`, `timeField`, tumbling `window`, `grace`, and `aggregations` (count,
    sum, min, max, first, last, collect);
  - `window_join`: `left` and `right` source keys, `timeField`, `window`; it joins on the spec's
    `blockingField` and must be the entry node.
- Time is event time: ISO instant, offset date-time, date, or epoch milliseconds.
- The watermark is the latest event time seen, minus `grace`.
- A graph is one chain: no fan-out, and no conditional edges (rejected).
- `ExecutionPlan` accepts graphs now. It checks that a `window_join` names exactly the plan's two
  sources.

**Engine-neutral logic (FDS core, package `core/stateful`)**
- `KeyedState` is the state interface: get, put, remove, and sorted keys.
- `InMemoryKeyedState`.
- `StagedKeyedState`: writes held until `commit()`; `discard()` drops them; `changes()` lists them
  in order.
- `PrefixedKeyedState`: one node's view of a shared store.
- `StatefulGraph` makes every decision: `process(sourceKey, record)`, `finish()` (closes every
  window at the end of a bounded input), and `eventTime`. The engines only supply state and decide
  when to commit.
- `GraphRecords`:
  - `bounded(graph, blockingField)`;
  - `process(...)` applies the error policy (fail, skip or dead-letter) to records the graph
    rejects;
  - `boundedCorrelated(...)` feeds a joining graph every source's records in event-time order, so
    the result doesn't depend on read order.

**Bounded runs (both engines)** use in-memory state.
- Pekko: `PekkoGraphStages.bounded` (lazyFlow, mapConcat, then `concatLazy(finish)`).
  `PekkoIngestionPipeline.run(..., ordered)` keeps source order when a graph follows.
  `PekkoIngestionRunner` and `PekkoCorrelationRunner` run the graph.
- Kafka Streams: `BoundedKafkaStreamsConsumerRunner` and `BoundedKafkaStreamsCorrelationRunner`
  have graph overloads; `KafkaStreamsDeliveryEngine` passes `plan.transforms()`.

**Continuous Kafka Streams: `KafkaStreamsGraph` (new)**
- One processor per node. Stateful nodes `selectKey` and repartition into a persistent key-value
  store, with exactly-once processing (EOS v2).
- Joins carry the source in `__fds_source`.
- **Fix in this session:** the repartition topics now have exactly **one partition**. Before, they
  inherited the input topic's partition count, so each partition had its own watermark and
  windows closed differently from Pekko's single run-wide watermark.
  - Review this trade-off: all of a run's graph state is on one task, which caps throughput the
    same way Pekko's one-entity-per-run design does.
- Used by `ContinuousKafkaStreamsConsumerRunner` and `ContinuousKafkaStreamsCorrelationRunner`.

**Continuous Pekko: Pekko Persistence on the installation's journal**
- `GraphStateEntity`: an EventSourcedBehavior; snapshot every 100 events.
- `JournalSerializable` is bound to jackson-json in `executor-pekko/src/main/resources/reference.conf`.
- `JournalKeyedState`: `open` recovers the state; `persist(changes)` makes one event; `close()`.
  - **Fix in this session:** `close()` now waits for the entity to stop (`Patterns.gracefulStop`).
    Before, it stopped asynchronously, so reopening the same run in the same actor system could
    fail with "actor name is not unique".
- `JournalGraph`:
  - one entity per run (`fds-graph|<identity>`), with every node's state under its own prefix, so
    one record's changes to all nodes are a single event;
  - `process`, then `commit()` after the sink write, or `discard()` if the write failed;
  - the identity is `ExecutionIdentity.of(plan, "fds-pekko-graph-", FDS_EXECUTION_ID)`, so a
    restarted worker recovers the same state.
- `JournalCorrelationState` replaces `KafkaCorrelationState` (**deleted**, along with its test).
  Each source's latest contribution per blocking key is persisted before the merged write.
- Delivery order per record: write the outputs, then commit the state, then commit the offset
  (at-least-once; a redelivered record meets the state it met the first time).
- `ContinuousPekkoStreamsConsumerRunner` (`sendThroughGraph`) and
  `ContinuousPekkoStreamsCorrelationRunner` (`writeThroughGraph`) use these.
  `PekkoStreamsExecutionHandle` closes the state; `PekkoStreamsDeliveryEngine` loads the journal
  settings with `PekkoJournalProfiles.fromEnvironment()`.
- `PekkoJournalProfiles` turns the worker env into Pekko Persistence config:
  - `pekko-postgresql`: persistence-jdbc into the tenant's database, schema `data_streaming`, as
    the runtime role. The database name is resolved from `TENANT_ID` through the control plane's
    `TenantRegistry` (`registeredTenantDatabase`). Env: `FDS_JOURNAL_CONTROL_PLANE_URL`,
    `FDS_JOURNAL_POSTGRES_USER`, `FDS_JOURNAL_POSTGRES_PASSWORD`.
  - `pekko-cassandra`: keyspaces `datastreaming_journal` and `datastreaming_snapshot`, with
    autocreate off. Env: `FDS_JOURNAL_CASSANDRA_CONTACT_POINTS`, `..._LOCAL_DATACENTER`,
    `..._USERNAME`, `..._PASSWORD`.
  - `requireJournal(system)`: a stateful continuous run without a journal fails before it starts.
- Pom additions: persistence-typed, serialization-jackson, persistence-jdbc,
  persistence-cassandra, postgresql, forwardmeasure-jpa-liquibase, source-readers. A duplicate
  test postgresql dependency was removed.
- **Fix in this session:** a variable-name clash in `PekkoIngestionPipeline.run` (the lambda
  parameter `mapped` shadowed a local) broke compilation; it is renamed to `result`.

**New module `forwardmeasure-data-streaming-migrations`** (image `data-streaming-migrations`)
- `DataStreamingMigrationsMain.run(env)`:
  1. `ensureRuntimeRole`: create or rotate the `datastreaming` role and its password;
  2. `grantTenantRegistryRead`: let the role read the control plane's tenant registry;
  3. per tenant, `provisionAndMigrate`: the `data_streaming` schema, using fowf's
     `OpenWorkflowTenantMigrator` with `FunctionalSchema.DATA_STREAMING`;
  4. Cassandra, when `DATA_STREAMING_CASSANDRA_CONTACT_POINTS` is set: the keyspaces and tables
     through fowf's `OpenWorkflowCassandraMigrator.migrate(target, changelog)`.
     - **New in this session:** with authentication on, `ensureCassandraRuntimeRole` creates or
       rotates a `datastreaming` Cassandra login with the same runtime password, and grants it
       only the two FDS keyspaces. The password is put into CQL as an escaped literal, because
       role options take no bind markers; the role name is validated against `[a-z][a-z0-9_]{0,47}`.
- Env: `DATA_STREAMING_CONTROL_PLANE_DATABASE_URL`, `..._ADMIN_DATABASE_USERNAME/PASSWORD`,
  `..._RUNTIME_DATABASE_USERNAME/PASSWORD`, `DATA_STREAMING_TENANTS` (`alias:Name,...`), and the
  `DATA_STREAMING_CASSANDRA_*` settings.
- Changelogs: `db/changelog/data-streaming-changelog.xml` (event_journal, event_tag, snapshot) and
  `db/changelog/cassandra/data-streaming-cassandra-master.xml`.
- `run` is public, so Pekko tests can provision a real journal.
- The pom declares `java-driver-core` explicitly.
- **Fix in this session:** the module was missing from the root pom's `dependencyManagement` and
  the FDS BOM. Both are added.

## 6. Getting the journal to a worker: launcher, workflow, fowf, Helm, Terraform

**Launcher planner (`IngestionLaunchPlanner.worker`)**
- A continuous Pekko delivery worker that correlates (more than one source) or runs a graph gets:
  - `FDS_PLATFORM_ENGINE`;
  - the journal's literal env;
  - each secret as `{"secretKeyRef": {"name", "key"}}`.
- If the journal isn't configured, it throws before dispatch.
- **New in this session:** when a database proxy is configured, the worker map also gets:
  - `databaseProxy: {image, instanceSecretName, instanceSecretKey, port: 5432}`; the proxy image
    passes the same image policy as the worker;
  - `serviceAccountName`, the ServiceAccount whose Workload Identity authorizes the proxy.
- Launcher properties:
  - `datastreaming.launcher.journal.env` and `.secret-env`;
  - **new:** `.database-proxy.image`, `.database-proxy.instance-secret` (`secret/key`) and
    `.database-proxy.service-account`. The service account is required when the image is set.
  - All three bindings and all three `application.yaml` files read them.
- Test: `IngestionLaunchPlannerJournalTest` (new).

**FDS workflow `forwardmeasure-data-streaming-api/src/main/resources/workflows/ingestion.yaml`**
(version 1.1.0 to 1.2.0)
- `applyContinuousWorker` and `applyContinuousDelivery` forward `databaseProxy` and
  `serviceAccountName` from the worker map to fowf's kubernetes-deployment call.
- The `env` object already passes through as-is, `secretKeyRef` entries included.

**fowf**
- `openworkflow-kubernetes-deployment-lifecycle`:
  - `KubernetesDeploymentSpec` gains `secretEnv` (a map of `SecretKeyRef`) and `databaseProxy`
    (`DatabaseProxy(image, instanceSecretName, instanceSecretKey, port)`). The older constructors
    are kept, so existing callers compile.
  - `KubernetesDeploymentLifecycle` turns secret env into `valueFrom.secretKeyRef` and adds the
    proxy. **Changed in this session:**
    - the proxy is a **native sidecar**: an init container with `restartPolicy: Always`, plus a
      startup probe on `/startup`, port 9801 (`--health-check --http-address=0.0.0.0`), so the
      worker starts only after the proxy is listening on localhost;
    - it runs with `runAsUser: 65532`, because the distroless image's user is the non-numeric
      `nonroot` and kubelet can't verify `runAsNonRoot` against a name;
    - args are `--private-ip --structured-logs --port=N ... $(CLOUD_SQL_INSTANCE)`, with the
      instance name read from a Secret.
  - Tests in `KubernetesDeploymentLifecycleTest` on real K3s:
    - `secretEnvReachesTheContainerOnlyAsASecretReference`: the pod stays up only if the
      container sees the secret's value, and the Deployment carries no value;
    - `aDatabaseProxyIsASidecarConfiguredFromItsSecret`: checks the sidecar's shape.
- `openworkflow-operation-adapter-kubernetes-deployment`, `AsyncApiKubernetesDeploymentOperationExecutor`:
  - `literalEnv` takes text entries; `secretEnv` takes `secretKeyRef` entries, and refuses one for
    `TENANT_ID`, `OPENWORKFLOW_TENANT` or `TENANT_DATABASE`. Anything else is an error.
  - `databaseProxy(executionId, payload.databaseProxy)` authorizes the proxy image against the
    tenant's image policy.
  - The trailing licence block, dropped by an earlier edit, is restored.
- `openworkflow-migration`: `OpenWorkflowCassandraMigrator.migrate(target, changelog)` is public,
  so another product can apply its own Cassandra changelog with the same bootstrap.
- Helm `deploy/helmfile/helmfiles/migrations.yaml.gotmpl`: a new `data-streaming-migrations`
  release, modelled on `entity-intelligence-migrations`.
  - The admin password and `DATA_STREAMING_RUNTIME_DATABASE_PASSWORD` come from the bootstrap
    credentials Secret.
  - On `pekko-cassandra`, it adds the Cassandra settings and the k8ssandra superuser Secret
    (`openworkflow-superuser`).
  - Image pin `dataStreamingMigrations` in `environments/image-versions.yaml`, digest empty.
- Helm `deploy/helmfile/environments/platform-derived.yaml.gotmpl`, which builds the per-tenant
  allowlists:
  - images now include the FDS loader, FEI's simple-source stage worker and, on GCP, the Cloud SQL
    proxy. Only images with a pinned digest are admitted;
  - **new:** `kubernetesDeployment.serviceAccountAllowlist` admits FDS's
    `workerServiceAccountName` (`data-streaming-worker`) for every tenant.

**FDS Helm (`deploy/helmfile`)**
- `launcher.yaml.gotmpl`, on Pekko installations:
  - `pekko-postgresql`: `DATASTREAMING_LAUNCHER_JOURNAL_ENV` is
    `FDS_JOURNAL_CONTROL_PLANE_URL=jdbc:postgresql://localhost:5432/<controlPlane>;FDS_JOURNAL_POSTGRES_USER=datastreaming`;
  - `pekko-cassandra`: it is the Cassandra contact points, data centre and username. The default
    contact point is `openworkflow-dc1-service.<openworkflow ns>.svc.cluster.local:9042`, data
    centre `dc1`; both can be overridden with `journal.cassandra.*`;
  - `..._JOURNAL_SECRET_ENV` is the password from `data-streaming-database-credentials/password`;
  - on GCP (`cloudSqlProxy.enabled` and not Cassandra): the proxy image, the instance secret, the
    service account, and the proxy image added to `K8S_IMAGES`.
- New release `data-streaming-worker-identity`, a local chart
  (`releases/data-streaming-worker-identity`). It is one ServiceAccount with the
  `iam.gke.io/gcp-service-account` annotation and `automountServiceAccountToken: false`.
- `environments/base.yaml`: `workerServiceAccountName: data-streaming-worker`.
- `environments/gcp-platform-cluster.yaml.gotmpl`: `cloudSqlProxy.enabled: true`.

**Platform Helm (`forwardmeasure-platform/deploy/helmfile`)**
- `shared/common.yaml.gotmpl`: `databases.runtimeRoles.dataStreaming: datastreaming`, and
  `secrets.dataStreamingDatabase: data-streaming-database-credentials`.
- `releases/platform-secrets/gcp/base.yaml.gotmpl`:
  - `DATA_STREAMING_RUNTIME_DATABASE_PASSWORD` is added to fowf's credentials Secret, for the
    migrations Job;
  - a new `data-streaming-database-credentials` ExternalSecret in the FDS namespace carries
    `username`, `password` and `db-cloud-sql-instance`.
  - Both exist only when `secretStore.remoteKeys.dataStreamingRuntimeDatabasePassword` is set.
- `environments/gcp-openworkflow-prod.yaml.gotmpl` and `gcp-greenfield-example.yaml`: the remote
  key `platform-data-streaming-runtime-database-password`.
- `releases/platform-secrets/gcp/openworkflow-prod.credentials.yaml.gotmpl`: fetches it from
  Secret Manager. The greenfield credentials file gets a placeholder.
- `shared/clusters/gcp-openworkflow-prod.yaml.gotmpl` and `gcp-greenfield-example.yaml.gotmpl`:
  `platform.cloud.gcp.dataStreaming.serviceAccount`.
- `k8s-infrastructure/opentofu/gcp/variables.tf` (greenfield): a `data-streaming` workload
  identity with `roles/cloudsql.client`, bound to `data-streaming/data-streaming-worker`.

**Terraform (`openworkflow-k8s-setup/terraform/gcp`)**
- `main.tf`: the generated secret `data-streaming-runtime-database-password`, 40 characters.
- `terraform.tfvars`: the `data_stream` workload identity (`roles/cloudsql.client`, namespace
  `data-streaming`, KSA `data-streaming-worker`). The key is shortened so the account id is a
  whole word within 30 characters: `openworkflow-prod-data-stream`.

**forwardmeasure-testcontainers**
- `CassandraTestContainer.withPasswordAuthentication()` (call before `start()`) rewrites
  `cassandra.yaml` to `PasswordAuthenticator` and `CassandraAuthorizer`, as k8ssandra clusters run.
  `superuserName()` and `superuserPassword()` give the default superuser.
- Test: `passwordAuthenticationAdmitsTheSuperuserAndEnforcesGrants`.

## 7. Tests added in this change set (none run yet)

| Test | Repo / module | What it proves |
|---|---|---|
| `StatefulGraphTest` | FDS core | Every parity fixture, bounded and continuous, exact outputs in order. Recovery at every split point gives the same outputs. Discard-and-replay is idempotent. Staged and prefixed state, event-time parsing, error policies, join validation, chain-shape rejections. |
| `StatefulGraphFixtures` (fixture) | FDS test-fixtures | Five cases (deduplicate, window_aggregate, window_aggregate_with_grace, window_join, chain), each with exact bounded and continuous expected outputs. |
| `StatefulGraphParity` (harness) | FDS test-fixtures | Puts a case's inputs on real topics (one partition each, in order), builds the spec with an id-writing map step and an OpenSearch sink, and reads the documents back. |
| `PekkoStatefulGraphParityIntegrationTest` | FDS executor-pekko | Every case, bounded and continuous, through the real Pekko engine (continuous on a real PostgreSQL journal), plus a worker stopped midway and restarted. |
| `KafkaStreamsStatefulGraphParityIntegrationTest` | FDS executor-kafka-streams | The same, on Kafka Streams. |
| `PekkoJournalIntegrationTest` | FDS executor-pekko | Real PostgreSQL and authenticated Cassandra journals. Ports every case of the deleted `KafkaCorrelationStateIntegrationTest`: restore after a failed sink, topic and partition offset reset, unserializable input not poisoning state, and a superseded writer refused (PostgreSQL only; see section 8). Also: corrupt state fails before consumption, a missing journal fails, graph state survives a restart after every record, a discarded record is gone after a restart, state is in the tenant database's `data_streaming` schema as the runtime role, and an unregistered tenant finds no journal. |
| `PekkoJournalFixture` (fixture) | FDS executor-pekko | Provisions a journal the way a deployment does: tenant registry, FDS migrations, worker env. |
| `PekkoJournalProfilesTest` | FDS executor-pekko | Config for both backends; every missing setting is named. |
| `PekkoStreamsDeliveryEngineIntegrationTest` (changed) | FDS executor-pekko | Now runs on a journal-backed actor system, because continuous correlation needs the journal. |
| `DataStreamingMigrationsMainTest` | FDS migrations | PostgreSQL, idempotent. **New:** authenticated Cassandra keyspaces, plus a runtime login that can write FDS's keyspaces and is refused elsewhere. |
| `IngestionLaunchPlannerJournalTest` | FDS launcher-application | Journal env with the password only as a secret reference; proxy and service account; proxy image policy; refusal without a journal; stateless workers get nothing; property parsing. |
| `KubernetesDeploymentLifecycleTest` (two new tests) | fowf | Secret env on real K3s; the proxy native sidecar's shape. |
| `CassandraTestContainerTest` (one new test) | testcontainers | Authentication and grants are enforced. |

Earlier in the change set (also not yet run): `NamedTransformCatalogTest`, `TestTransformsTest`,
`LoaderHandoffSpecsTest`, `SourceLoaderIntegrationTest`, the source-readers tests,
`SimpleSourceEngineTest`, `CustomerMasterSampleRowsContractTest`, the three
`...KafkaStreamsCsvLoadSmokeTest`s, the three simple-source stage-worker smoke tests, and the
updated workflow compiler test (13 tests; this one did pass).

## 8. Where to look hardest (known risks and judgement calls)

1. **At-least-once delivery on Pekko.** Outputs are written, then state is committed, then the
   offset. A crash after the write but before the state commit replays the record against the old
   state, so a window's output can be written twice. The sink writes by document id, so a repeat is
   an overwrite with the same content. Check that every graph output has a stable id.
2. **Single writer.** One run is one Persistence entity. Deployments run one replica with the
   `Recreate` strategy (fowf `KubernetesDeploymentLifecycle`), so two pods never overlap.
   PostgreSQL's journal would also refuse a second writer (primary key on persistence id and
   sequence number). Cassandra would not, so there the guarantee is only the Recreate strategy.
3. **Kafka Streams single-partition repartition** (section 5): correct for parity, but it serializes
   a run's graph. Check this is acceptable.
4. **`StatefulGraph` scans every key.** `closeWindows` and the periodic purges go through all of a
   node's keys, and `StagedKeyedState.keys()` and `PrefixedKeyedState.keys()` copy and filter the
   whole store, on each record. That is linear in state size per record: fine for modest state,
   slow for very many open windows or keys. Not fixed.
5. **Late records** for an already-closed window are dropped silently (no log, no dead letter).
   That matches the documented semantics, but check it is the wanted behaviour.
6. **Cassandra contact point default** in FDS Helm: `openworkflow-dc1-service.<ns>:9042`, from
   fowf's kind environment. fowf's own GCP values default to `cassandra:9042`. Prod runs Kafka
   Streams, so neither is exercised there. Also check whether a NetworkPolicy lets the FDS
   namespace reach Cassandra in fowf's namespace.
7. **The Cassandra login password** is put into CQL as a quoted literal with single quotes
   doubled. Check the escaping, and that the statements aren't logged.
8. **The proxy probe binds `0.0.0.0:9801` inside the pod** (needed for kubelet probes). Check
   whether anything else could use that port, and the NetworkPolicy.
9. **Image pins.** The loader, migrations and FEI stage-worker digests are empty until pushed.
   fowf's allowlist admits only pinned digests, so those Jobs are refused until the digests are
   filled in (FDS `environments/base.yaml`, FEI `environments/base.yaml`, fowf
   `image-versions.yaml`). `sync-image-digests.sh` re-resolves only digests that are already
   pinned.
10. **The FDS workflow version** went from 1.1.0 to 1.2.0. The launcher starts workflows by name,
    so check that publishing replaces the active revision.
11. **No K3s smoke test** yet runs a journal-backed continuous Pekko worker end to end (proxy,
    registry lookup, journal). The engine-level tests cover the same code against real databases.
12. **`window_join` combined with a HEAVY (Spark) transform** isn't handled: the Spark route runs
    no graph.

## 8a. Fixed after the first Codex review (2026-10-14)

All six findings were confirmed in the code and fixed. The core tests covering them pass (37 of 37).

1. **The planner threw on stateful steps.** `ExecutionPlanCompiler` read `node.mapper().fields()`
   on every graph node, but deduplicate, window_aggregate and window_join steps have no mapper.
   It now reads field rules from map steps only (`graphFieldRules`).
   Test: `ExecutionPlanCompilerTest.everyStatefulGraphPlansOnBothEnginesInBothModes`.
2. **A crash between state commit and offset commit double-applied a record (Pekko).**
   `JournalGraph.process` now takes the record's `InputPosition` (topic, partition, offset). When a
   record changes state, the partition's applied offset is staged in the same journal event, under
   a NUL-prefixed key no node can use. A redelivered record at or below that offset changes nothing
   and emits nothing, because its outputs were written before its state was committed. Both
   continuous Pekko runners pass the position; the non-join correlation path uses the position of
   the contribution that produced the merge.
   Test: `PekkoJournalIntegrationTest.aRecordRedeliveredAfterItsStateWasCommittedIsAppliedOnce`
   (every fixture, both journals, redelivery after every record).
3. **A HEAVY transform inside a graph could never finish its Spark handoff; a bounded join behind
   Spark lost its two sides.** Both are now built, not refused (owner's decision: Spark runs the
   graph prefix).
   - **Planner.** `SparkStagePlan.graphSteps` is the number of graph steps, counted from the entry
     step through the last map step that uses a HEAVY transform, that the Spark stage runs
     (`ExecutionPlanCompiler.sparkGraphSteps`). `TransformGraph.first(n)` and `after(n)` split the
     chain between the Spark stage and the delivery stage.
   - **Bounded Spark** (`SparkGraphSteps`). Each owned step runs through the same `StatefulGraph`.
     Map steps run distributed: a range partition keeps record order when spreading one partition
     out. Stateful steps meet the whole ordered input on one partition and close their windows at
     the end. A joining prefix reads each source in event-time order (`GraphRecords.joinOrder`).
     A correlated, non-joining prefix reads the merged records in event-time order
     (`GraphRecords.orderMerged`), and the engines' bounded correlated path now uses the same
     order.
   - **Continuous Spark** (`ContinuousSparkGraph`). Correlation and stateful steps run on the
     driver, in arrival order; map steps run distributed. State lives on a compacted
     `<handoff>-graph-state` topic, written in the same transaction as the outputs and the input
     offsets, and applied to the driver's copy only after the commit. It is restored at startup,
     so a restart resumes exactly.
   - **Handoff** (`SparkHandoffSpecs.deliveryStageSpec`) has three shapes:
     - Spark ran part of the graph: one stream, and the delivery stage runs `graph.after(n)` with a
       pass-through mapper over the fields those steps produce (`GraphFields.after`).
     - Each source separately (continuous, or a bounded join; `handsOffEachSource` and
       `sourceTopic`).
     - Correlated: one merged stream.
   - **Found while building:** bounded Pekko could not read any Spark handoff topic, because its
     Kafka source required `maxMessages`. No test had ever run Spark followed by Pekko. Without
     `maxMessages`, it now reads up to the end offsets captured at start (`KafkaFrontierReader`),
     as Kafka Streams does. Test: `KafkaFrontierReaderIntegrationTest`.
   - **Found while testing:** `TransformCharacteristics` was not serializable. Since D1 each
     transform carries its characteristics, so **every bounded Spark run failed** to ship the
     catalog to its executors. It is now `Serializable`.
   - Tests: `SparkStatefulGraphIntegrationTest` runs every fixture entirely in Spark, bounded, and
     continuous with a restart midway, with the same expected outputs as the engines. It also
     covers the per-source join handoff. `ExecutionPlanCompilerTest` covers the split, the three
     handoff shapes, and a HEAVY step after a join. The whole Spark suite passes (41 tests).
4. **One rejected value discarded a record's other outputs.** `StatefulGraph.process` and `finish`
   now take a `Rejection` and decide per value and per step, so only the rejected value is dropped,
   or the run fails under the fail policy. A step changes its state only after accepting a value,
   so a rejected value leaves that step's state unchanged. `GraphRecords.process` and the new
   `GraphRecords.finish(graph, policy)` map the policy; both engines' bounded `finish` calls use it.
   Test: `StatefulGraphTest.aRejectedValueDropsOnlyItselfNotTheRecordsOtherOutputs`.
5. **A repeated load appended to its load topic.** `KafkaRowWriter.ensureTopic` is now
   `emptyTopic`: it creates the topic, or deletes every record an earlier attempt left behind, so
   each attempt starts from nothing. FEI's load stage empties its summary topic before writing
   rows. FEI's delivery stage now checks the row count from the topic's offsets **before** writing
   anything, and keeps the existing check after.
   Test: `SourceLoaderIntegrationTest.aRepeatedLoadReplacesTheEarlierAttemptsRows`.
6. **The window ordering key overflowed.** `StatefulGraph.sortableTime` is now
   `String.format("%016x", millis ^ Long.MIN_VALUE)`: flipping the sign bit turns signed order into
   unsigned order, which fixed-width hex preserves.
   Test: `StatefulGraphTest.windowsClosingTogetherAreEmittedInTimeOrder`, which closes eight windows
   at once across eleven hours, plus key ordering from `Long.MIN_VALUE` to `Long.MAX_VALUE`.

Section 8, item 1 is superseded by fix 2. Item 12 is superseded by fix 3.

## 9. Not this change set (in the same working trees; don't review as part of it)

- **FEI:** the domain-entity renames (subject to entity), the population deletion and purge work,
  `FailedIngestionJobCleanup`, the studio webapp changes, `WorkerImagePullSecrets`, the
  `ingestion-k3s-verification` renames, and the docs listed as untracked other than this one's
  references. Another agent's GEMS_ID/KYC_ID identifier work is separate, and the user handles it.
- **fowf:** about 120 files of other in-progress rehabilitation work. This change set touches only
  `openworkflow-migration`, `openworkflow-kubernetes-deployment-lifecycle`,
  `openworkflow-operation-adapter-kubernetes-deployment`, and the Helm files named in section 6.
- **FDS, earlier work by others** (build on it, don't revert): the CSV header-rule changes, now
  moved into source-readers; `executor-spark` `SparkCorrelationEngine` and `CsvHeaderColumns`; the
  customer-master fixture YAML and Java; `asyncapi/ingestion-job.yaml`.
- **Lost work:** deleting `SharedSourceContractTest` discarded someone else's uncommitted edits to
  it. Its intent was recovered from the compiled class and ported to FEI as
  `CustomerMasterSampleRowsContractTest`. Its WorldCheck half was not ported.

## 10. Suggested review order

1. `core/stateful/StatefulGraph.java` and `StatefulGraphFixtures.java`: are the semantics right,
   and do the expected outputs follow from them?
2. `JournalGraph`, `JournalKeyedState` and `GraphStateEntity`, then the two continuous Pekko
   runners: commit ordering and failure handling.
3. `KafkaStreamsGraph`: the same semantics on Kafka Streams.
4. `PekkoJournalProfiles`, `DataStreamingMigrationsMain`, and the Helm and Terraform from section 6:
   can a worker actually reach its journal on GCP?
5. The fowf deployment executor and lifecycle: secret handling and the sidecar.
6. The D1-D4 changes: the catalog, the planner's engine choice, the loader, and FEI's two-stage
   ingestion.
