# FDS generic, engine parity and the FEI customer-master fix: one change set (2026-10-14)

**For:** the agent implementing this across `forwardmeasure-platform`, `forwardmeasure-entity-matching`,
`forwardmeasure-data-streaming` (FDS) and `forwardmeasure-entity-intelligence` (FEI).

**Customer-master identity coordination, updated 2026-10-15:**
[`customer-master-entity-identifier-handover-2026-10-14.md`](../../forwardmeasure-entity-intelligence/docs/customer-master-entity-identifier-handover-2026-10-14.md)
has its implementation coordinated through the
[combined FEI lifecycle identity and entity directory plan](../../forwardmeasure-entity-intelligence/docs/fei-lifecycle-identity-and-entity-directory-implementation-plan-2026-10-15.md),
at the owner's direction. This changes coordination, not the accepted identity behavior or this
plan's generic-engine scope. Codex reviewed M5 and §6 on 2026-10-14: declared entity identifiers
match across the tenant by their qualified key, and qualified DID aliases enforce ownership,
with explicit retries of losing batch transactions. Rows sharing `GEMS_ID`, or sharing a
`BUSINESS_ENTITY_RECORD_ID` when GEMS is absent, resolve to one entity while retaining per-row
record identity. This is a design decision; its implementation and REST concurrency tests
remain outstanding. Entity matching and alias ownership belong in FEI; FDS remains generic.

## How this work runs

- **One change set.** Everything below goes in together: the FEI fixes already made, making FDS
  generic, FDS honouring the platform engine, and Pekko state support. It is pushed and tested as
  one set.
- **The implementer** writes all the code, tests and fixtures, and gets every repo compiling with
  RAT and formatting checks passing.
- **The user** pushes and tests live.
- **The implementer** then runs the full test suites, including the three-framework deployment
  leaves and K3s, and fixes what they find.
- No time estimates and no phasing. All of it is in scope.

## Part A: why the customer-master ingestion failed

The monitored-population run for `customer-master-1` (`state-street-customer-master-ind.csv`,
pipe-delimited, 174,877 rows, `SINK_TYPE=RESOLUTION`) reported 1,572 failed rows out of the first
5,356. Over the whole file, **51,203 rows (29%) would have been dropped.** Every failure was
`Document violates contract: /entity_kind: enum`.

### A1. Three conflicting definitions of `EntityKind`

| Definition | Values |
|---|---|
| Output contract `resolved-entity-record-schema-v1.yaml` (platform data-contracts) | `person`, `organization` |
| FEI API spec `EntityKind`, generated as `com.forwardmeasure.entityintelligence.common.model.EntityKind` | `person`, `organization`, `trust`, `location`, `physical_asset`, `account`, `digital_identity`, `unknown` |
| entity-matching's hand-written `com.forwardmeasure.entitymatching.EntityKind` (also used by FDS) | `PERSON`, `ORGANIZATION`, `UNKNOWN` |

- **The contract was narrower than the API it feeds.** A trust could not be ingested even though
  the API and database accept one.
- **Three converters collapsed everything else to `UNKNOWN`:** `EntityApplicationService.toMatchingEntityKind`,
  `ScreeningRequestEvidenceMapper.toEntityKind` and `PopulationScreeningWorker.toMatchingEntityKind`.
  Any change to one definition silently diverged from the others.
- **No single source of truth.** Nothing tied the three lists together.

### A2. The party-kind transform didn't recognise most of this customer's types

The mapping (`customer-master-resolved-entity-mapping-v1.yaml`) derives `entity_kind` from
`ENTITYS_TYPE` through FEI's `customer_master_party_kind` (`CanonicalFieldTransforms.customerKind`).
It mapped `Individual` to `person` and 14 hard-coded types to `organization`. Everything else
returned the string `"unknown"`, which the contract rejects.

Types that weren't recognised, with row counts:

| Type | Rows |
|---|---|
| Special Purpose Vehicle (SPV) | 20,468 |
| Pension Plan (the list only had "Pension Fund") | 11,908 |
| Non-FI Private Company | 6,200 |
| Investment Advisor (Regulated) | 6,131 |
| Charity, Religious Group and Non-Profit | 1,481 |
| Securities & Commodities (B/D) | 1,285 |
| Non-FI Public Company | 1,212 |
| University/Hospital | 923 |
| Foundation/Endowment | 760 |
| Investment Advisor (Unregulated) | 620 |
| Sovereign Wealth Fund, Supra National Organization, Money Service Business, Personal Investment Vehicle | 215 |

This logic was duplicated, too: FDS's `classify_party_kind_test_customer_master` already had the
right rule (`Individual` is a person, anything else non-blank is an organization). FEI kept its own
diverging copy.

### A3. The failure message didn't say what failed

`DocumentSchema.validate` reported only `/entity_kind: enum`, without the offending value, so the
cause could only be found by reading the source file.

### A4. Every failed row was logged twice

When a row fails, `SimpleSourceIngestionWorker` logs it, counts it and returns `null`. FDS's
`PekkoIngestionPipeline.mapOrHandle` then calls `Optional.of(null)`, which throws, and logs the
same row again as "malformed record dead-lettered" with a `NullPointerException`. The counts were
right, but the log suggested a second failure.

### A5. FEI's generator mappings were silently ignored

All five FEI generator configurations wrote mappings as `<Problem>com...Problem</Problem>`. The
plugin expects `<importMapping>Problem=com...Problem</importMapping>` and ignores entries without
`=`. As a result:
- FEI generated its own `Problem` and `Violation` since 2026-10-01, despite the comment saying it
  imports the shared ones;
- `AssertionsResource` built FEI's own copy;
- mapping `EntityKind` to a shared class couldn't work.

### A6. The ingestion ran on Pekko, though the platform declares Kafka Streams

The monitored-population worker calls FDS's `PekkoIngestionRunner` directly (see B3).

### A7. Source-data quirks (handled; noted for the fixtures)

- `ENTITY_TYPE_CODE` is space-padded (`'I'` or `'C'` followed by about 99 spaces). It isn't used
  for `entity_kind`.
- Header names repeat, for example `ENTITYS_FULL_NAME` and `ENTITY_LOCATION_1_COUNTRY`. The
  existing rule applies: the first column wins and unnamed columns are skipped.
- 3,782 trusts are coded `I` and 3,496 `C`. Trusts map to `organization` for now (see "Not in
  scope").

### What is already done (uncommitted, compiled, unit-tested)

- **A1, shared `EntityKind`.** It is generated once in `forwardmeasure-platform-api-contracts`
  (`META-INF/forwardmeasure/openapi/entity-kind-v1.json` →
  `com.forwardmeasure.platform.api.model.EntityKind`; second generator execution; package
  `com.forwardmeasure.platform.api.model`).
  - **Output contract:** lists all seven concrete kinds (not `unknown`). `EntityKindContractTest`
    in api-contracts enforces it, and api-contracts has a test dependency on data-contracts.
  - **entity-matching:** its own enum is deleted. `EntityEvidence.matchingKind` is the one explicit
    rule: matching rules exist for person and organization only, and every other kind is matched as
    unknown, exactly as the old converters did. `EntityMatcherTest.kindsWithoutMatchingRulesAreMatchedAsUnknown`
    covers it.
  - **FEI:** maps its spec's `EntityKind` to the shared class in every generator. The three
    converters are deleted. `EntityKindSchemaContractTest` in `common-models` checks the spec and
    the shared enum list the same values. Micronaut gets `@SerdeImport(EntityKind.class)` in
    `MicronautProblemModelIntrospection`.
  - **FDS:** `NamedTransformFunctions` imports the shared class.
  - **Test fixture:** the pinned contract hash in FEI's `test-fixtures/reference-population-contracts.xml`
    is updated (`a0416370…`).
- **A2.** `customerKind` returns `person` for `Individual`, `organization` for any other non-blank
  type, and `unknown` (rejected) for blank. `CanonicalFieldTransformsTest` covers it.
- **A5.** Every FEI mapping now uses `Key=Value` entries. `AssertionsResource` and the three
  `AssertionsCrudResourceTest`s use the shared `Problem`.
- **entity-matching is proprietary.** ForwardMeasure header on every file, `src/rat/`, `LICENSE`,
  and pom `<licenses>` overriding the platform parent's Apache entry. It builds with RAT.

### Still to do for Part A

- **A3:** include the offending value in contract violations, for example
  `/entity_kind: enum (value "Pension Plan")`.
- **A4:** `Optional.ofNullable` in `mapOrHandle`, so a `null` return means "skipped".
- **A2 duplication:** after B2, exactly one customer-master party-kind transform exists, in FEI.
- **A6:** resolved by B3.

## Part B: FDS problems

### B1. FDS contains product and customer logic

FDS is a generic, Apache-licensed engine with no knowledge of FEI. Claude sessions moved these into
its main code:

| Transform | What it is | Used by FEI |
|---|---|---|
| `screen_against_worldcheck_reference` | Screens a row against hard-coded fictional WorldCheck entities with proprietary entity-matching. Added 2026-09-26 (`1576475`) only to give the planner a `HEAVY` example. | No |
| `classify_party_category`, `classify_party_categories`, `classify_party_kind` | Sanctions-list category taxonomy (`classify_party_categories` was copied from FEI on 2026-09-14) | Yes |
| `parse_tilde_delimited_locations` | WorldCheck location format | Yes |
| `classify_party_kind_test_customer_master`, `build_test_customer_master_locations`, `build_test_customer_master_identifiers` | One customer's file layout | Yes |

- **Licence conflict:** FDS (Apache) depends on entity-matching (proprietary) only for
  `screen_against_worldcheck_reference`.
- **Comments:** FDS main code elsewhere (`OpenSearchIndexInitializer`, `TransformSpec`,
  `ingestion-spec.yaml`, `SparkHandoffSpecs`, `ExecutionPlanCompiler`, `SparkCorrelationEngine`,
  `FieldMappingEngine`, `BoundedKafkaStreamsConsumerRunner`, `LauncherMicronautBinding`) mentions
  WorldCheck, customer master or screening only in comments. Rewrite those comments generically.

### B2. FDS can't take a product's transforms

`TransformCharacteristicsRegistry` is a static map in FDS. `ExecutionPlanCompiler` throws for any
name not in it, which is why domain transforms kept being moved into FDS.

### B3. FDS ignores the platform engine, and Pekko lacks state support

- **Who reads the setting.** `platform.engine` (`kafka-streams | pekko-postgresql | pekko-cassandra`,
  in `forwardmeasure-platform/deploy/helmfile/shared/common.yaml.gotmpl`) is read only by fowf.
- **Pekko by default.** `ExecutionPlanCompiler.resolveEngine` picks Pekko for every bounded
  non-Kafka source, which means every file load. A Claude session decided that in `d55c272`
  (2026-09-25). The approved plan said simple transforms run on "either delivery engine"; the user
  never chose Pekko.
- **`STATEFUL` forces Kafka Streams.** The Pekko engine has no state support. That was another
  unilateral decision, and it breaks the user's parity rule: every capability on both engines.
- **FEI bypasses selection.** `SimpleSourceIngestionWorker` calls `PekkoIngestionRunner` directly.
- **Pekko where it shouldn't be.** `launcher-application` depends on `executor-pekko` at compile
  scope, so the launcher image ships Pekko whatever the engine.
- **Already Pekko-free (checked):** `core`, `connector-object-storage` and `executor-kafka-streams`.

## Decisions (confirmed by the user, 2026-10-14)

- **D1, transform catalog.**
  - `NamedTransform` declares its own `TransformCharacteristics` (default: a pure row function).
  - FDS keeps a catalog of only its generic built-ins.
  - Callers pass their own transforms into both `FieldMappingEngine` and the planner.
  - The static registry goes.
- **D2, domain transforms move to FEI.** All seven in B1 move to FEI `worker-common`, registered
  through D1.
  - `screen_against_worldcheck_reference` is deleted.
  - FDS keeps only the domain-free parsers: country, date, identifier, list, URL and
    `join_labeled_fields`.
  - FDS drops entity-matching.
- **D3, testing is not handicapped.**
  - **Domain-like test code is allowed.** Fixtures and named transforms written only for tests may
    stay in FDS, in `src/test` or the test-scoped `test-fixtures` module, including a `HEAVY` one
    for the Spark route and a screening-style one with simple self-contained scoring.
  - **Never exposed elsewhere.** A build check (enforcer or ArchUnit) fails if a main-scope module
    depends on `test-fixtures`, or a production catalog registers a test transform.
  - **Registered through D1**, so the tests prove the plug-in mechanism.
  - **No proprietary artifacts** in FDS test code.
  - **FEI owns its tests and fixtures.** It tests its own transforms through FDS's mapping engine
    and planner, with its own WorldCheck and customer-master fixtures, instead of borrowing FDS's.
- **D4, the platform engine governs FDS.**
  - **Engine choice.** FDS reads the same `engine` value as fowf. The planner no longer chooses the
    engine; it only decides whether to add a Spark stage.
  - **Kafka Streams installation.** A file load is a loader Job (plain Kafka producer, no Pekko)
    writing rows to a per-run topic, then `BoundedKafkaStreamsConsumerRunner`. This is the
    two-Job shape FDS already uses for Spark handoffs.
  - **Pekko installations** (`pekko-postgresql`, `pekko-cassandra`) run file loads with Pekko.
  - **FEI's simple-source worker uses the selected engine,** never Pekko directly. On Kafka Streams,
    monitored- and reference-population loads become loader, then an FEI Kafka consumer delivering
    through FEI's REST API, the pattern `CorrelatedResolutionDeliveryWorker` already uses. The
    workflow definitions (`simple-source-ingestion.yaml` and related) follow.
  - **The launcher** no longer carries `executor-pekko` on Kafka Streams; engine-specific code
    moves behind the engine choice.
  - **Build checks:** an image fails its build if the other engine's jars are on its classpath.
- **D5.** `Optional.ofNullable` in `mapOrHandle`.
- **D6, Pekko gets state support (parity).** The Pekko delivery engine supports `STATEFUL`
  transforms with the same semantics as Kafka Streams:
  - keyed state;
  - windowed joins and aggregation;
  - deduplication;
  - state that survives restarts, held through Pekko Persistence on the installation's journal
    (PostgreSQL or Cassandra, matching `pekko-postgresql` or `pekko-cassandra`).

  Every spec runs on either engine, and no capability forces one. Parity tests run the same
  stateful specs on both engines and compare the outputs.

## Done when

- FDS main code has no domain transforms or names. Domain-like code exists only in test code,
  behind the D3 build check. Nothing in FDS depends on entity-matching.
- One `EntityKind` exists, generated in platform api-contracts, and every list of kinds is tested
  against it.
- Contract violations name the offending value, and failed rows are logged once.
- FDS follows `platform.engine`:
  - a Kafka Streams install ingests a CSV end to end on K3s with no Pekko jar in any image it runs;
  - a Pekko install does the same with no Kafka Streams jar;
  - stateful specs pass the parity tests on both.
- FEI's monitored-population ingestion of the customer-master file leaves no `entity_kind`
  failures, on the Kafka Streams path.
- All four repos build with RAT and formatting checks. All FDS and FEI tests pass, including the
  deployment leaves on Quarkus, Spring and Micronaut.

## Not in scope (parked, not dropped)

- Open-sourcing the generic parsers as a separate library.
- Parameterised transforms (customer mappings as configuration, e.g. a `value_map`).
- Matching rules for kinds other than person and organization, and mapping trusts to `trust`.

## State at handover (2026-10-14, nothing committed)

- **Branch:** all four repos are on `develop`.
- **Earlier work by others (build on it, don't revert):**
  - FDS CSV header-rule changes in `executor-pekko` (`CsvSourceRow`, `StreamingTextRows` and their
    tests);
  - `executor-spark/SparkCorrelationEngine`;
  - the `test-fixtures` customer-master YAML and Java, and the `*CustomerMasterSmokeTest`s;
  - `api/.../asyncapi/ingestion-job.yaml`;
  - `deploy/helmfile/environments/base.yaml`.
- **Today's work:** everything under "What is already done" in Part A. It compiles across the whole
  FEI reactor with RAT and formatting checks. The unit tests in the touched modules pass, and
  entity-matching's full suite passes, including real OpenSearch.
- **Not yet run:** the FEI deployment-leaf REST tests on all three frameworks, now more important
  because error responses use the shared `Problem`.
- **Local Maven repository:** the platform and entity-matching changes are installed in the local
  `~/.m2`.

## Implementation record (2026-10-14, D1-D6 code complete, nothing committed)

Written after the code was finished and building. "Not yet run" below means exactly that.

### Decisions made while building (user-confirmed)

- **D6 journal location.** On PostgreSQL, each tenant's own database holds the journal in a
  `data_streaming` schema, owned by FDS's own runtime role `datastreaming`. A new
  `forwardmeasure-data-streaming-migrations` image creates both, running in fowf's migrations stage
  as `data-streaming-migrations`. On Cassandra, FDS has its own `datastreaming_journal` and
  `datastreaming_snapshot` keyspaces, and the same migrations create a `datastreaming` login that
  can use only those two keyspaces.
- **Workers carry a tenant id, never a database name.** A worker finds its tenant's database
  through the control plane's tenant registry, as the `datastreaming` role. That role is granted
  read access to the registry.
- **Database access on GCP.** fowf's kubernetes-deployment protocol gained an optional Cloud SQL
  Auth Proxy sidecar. It runs as a native sidecar with a startup probe, so the worker starts only
  once the proxy is listening on `localhost:5432`. The worker runs as a new `data-streaming-worker`
  ServiceAccount, whose Workload Identity authorizes the proxy.
- **Secrets.** Passwords reach worker pods only as `secretKeyRef` entries. fowf records workflow
  input verbatim, so no password value ever passes through a workflow input.

### Where things live

- **D1/D2/D3.** `NamedTransformCatalog` with a ServiceLoader `NamedTransformProvider`. FEI's
  transforms are in `worker-common` (`ProviderVocabularyTransforms`, `IngestionTransforms.catalog()`).
  Test transforms are `test_*` in `test-fixtures` (`TestTransforms`, `TestTransformProvider`).
  Enforcer rules keep `test-fixtures` test-scope only and ban entity-matching and data-contracts
  from FDS.
- **D4.** The launcher reads `datastreaming.launcher.engine` (Helm sets it from `platform.engine`).
  On Kafka Streams, a bounded file load runs as a loader Job (`forwardmeasure-data-streaming-loader`)
  writing to `<loadTopic>-source-<i>`, then the Kafka Streams delivery Job. FEI's simple-source
  ingestion uses the Pekko-free stage worker (`INGESTION_STAGE=load|deliver`) when the engine is
  `kafka-streams`. Per-module enforcer rules keep each engine's jars off the other engine's images.
- **D6.** Engine-neutral `StatefulGraph` (map, deduplicate, window_aggregate, window_join) over a
  `KeyedState` interface.
  - **Bounded runs** use in-memory state.
  - **Kafka Streams** uses one processor per node, repartitioned onto one partition so the
    watermark covers the whole run, as on Pekko, with persistent stores.
  - **Pekko** keeps one Persistence entity per run (`JournalGraph`). Each record's state changes
    are committed only after its outputs are written; a failed write discards them.
  - **Correlation state** moved from a compacted Kafka topic to the journal
    (`JournalCorrelationState`); `KafkaCorrelationState` is deleted.

### Deviations from the plan

- The three customer-master transforms became FDS test transforms (`test_*`), not FEI transforms.
  FEI's own provider vocabulary transforms are in `worker-common`.
- Deleting `SharedSourceContractTest` lost someone else's uncommitted edits to it. Its intent was
  recovered from the compiled class and ported to FEI (`CustomerMasterSampleRowsContractTest`).
  **Verification (2026-10-14):** the WorldCheck half and the "contract not shadowed" check had been
  dropped. Both are now restored in FEI: `WorldCheckSampleContractTest` checks FEI's own
  `premium-world-check-sample.csv` against the WorldCheck source contract, and the customer-master
  test checks for a single copy again.
- ~~A `window_join` combined with a `HEAVY` (Spark) transform is not handled.~~ **Out of date
  (verified 2026-10-14):** the Spark stage runs graph steps, joins included
  (`SparkGraphSteps.joinOrder`), and `SparkStatefulGraphIntegrationTest` runs every parity fixture,
  `window_join` included, inside Spark, bounded and continuous.

### Tests added (not yet run)

- `StatefulGraphTest` (core): every parity fixture, bounded and continuous; recovery at every split
  point; discard-and-replay; error policies.
- `PekkoJournalIntegrationTest`: real PostgreSQL and Cassandra journals. It covers correlation
  state across a restart, a superseded writer (PostgreSQL), corrupt state, graph state across a
  restart after every record, and a discarded record after a restart. It also checks that state
  lives in the tenant database's `data_streaming` schema, written as the runtime role.
- `PekkoStatefulGraphParityIntegrationTest` and `KafkaStreamsStatefulGraphParityIntegrationTest`:
  the same fixtures (`StatefulGraphFixtures`, `StatefulGraphParity`) end to end, from real topics
  to real OpenSearch, bounded and continuous, plus a restart midway.
- `PekkoJournalProfilesTest`, `IngestionLaunchPlannerJournalTest`,
  `DataStreamingMigrationsMainTest` (PostgreSQL and authenticated Cassandra).
- fowf `KubernetesDeploymentLifecycleTest`: secret env and the proxy sidecar on K3s.
- forwardmeasure-testcontainers `CassandraTestContainer.withPasswordAuthentication()` and its test.

### Before this runs live

- **Images to build and push:** `data-streaming-loader`, `data-streaming-migrations`, the FEI
  `entity-intelligence-simple-source-stage-worker-{quarkus,spring,micronaut}`, and rebuilt
  launcher, Pekko and Kafka Streams executor images. Then pin their digests in the environment
  files. fowf's image allowlist admits only pinned digests, so the loader and stage worker are
  refused until then.
- **Terraform** (openworkflow-k8s-setup): the `data-streaming-runtime-database-password` secret
  and the `data_stream` workload identity. The greenfield OpenTofu gets the `data-streaming`
  identity.
- **Platform Helm:** `runtimeRoles.dataStreaming`, the `DATA_STREAMING_RUNTIME_DATABASE_PASSWORD`
  key in the migrations credentials, and the `data-streaming-database-credentials` Secret in the
  FDS namespace.
- **No K3s smoke test yet** covers a journal-backed continuous Pekko worker end to end (proxy,
  registry lookup, journal). The engine-level tests above cover the same code against real
  databases.

## Working rules

- **Licences.** FDS, platform and fowf are Apache (ASF header). FEI, entity-matching and platform
  data-contracts are proprietary (ForwardMeasure header; RAT denies AL). Run RAT on every repo.
- **Engines.**
  - The engines are independent: no Pekko on a Kafka Streams classpath, and no Kafka Streams on a
    Pekko one.
  - Every capability exists on both engines.
  - Never pick an engine, or any architecture, silently.
- **Dependencies and models.** No cyclic dependencies between repos. A shared concept has one
  generated model, at the lowest layer.
- **Tests.**
  - Real infrastructure through forwardmeasure-testcontainers; no Mockito, no H2.
  - Every change covers Quarkus, Spring and Micronaut.
  - Product behaviour is tested through the REST API in the deployment leaves.
  - Never shrink or narrow a test.
- **Builds.**
  - One Maven build at a time, teed to `/tmp/mvn-<repo>.out`.
  - Use `clean` after any generator or model change.
  - Spotless runs as `check`, so apply it to the files you changed.
  - Build order: platform → entity-matching → FDS → FEI.
- **Reporting.** Plain language. Nothing is "done" without a full-suite or live run.
