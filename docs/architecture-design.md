# ForwardMeasure Data Streaming (FDS): Architecture Design for Verification

**Date:** 2026-10-04. Revised the same day after the product owner approved the decisions in §8.
**Repository:** `forwardmeasure-data-streaming`.

**Purpose:** state what FDS must be, rule by rule, so that an independent reviewer can check the code
against each rule and report pass or fail with evidence. The current code is not an authority for any
rule here.

---

## 0. How to use this document

The conventions are the same as §0 of `forwardmeasure-openworkflow/docs/architecture-design.md`:

- Every rule is a requirement, and its source is cited.
- A derived rule has a **Basis** line explaining how it follows from its source.
- **State** is either:
  - **BUG**: the code violates the rule, measured on 2026-10-04, with the evidence given; or
  - **NOT ASSESSED**: not yet checked against the code. This is not a pass.

**Source keys,** in order of precedence:

1. **[U:date]**: a product-owner decision.
   - **[U:2026-10-04 §8.n]** is decision *n* in §8.
   - The earlier FDS decisions are recorded in the plan `shimmying-tickling-mango.md`:
     - the 2026-09-20 capability-model correction;
     - the 2026-09-21 scope decisions, "confirmed with the user, do not relitigate";
     - the 2026-09-25 correlation section.
2. **[D:n]**: decision D*n* in `forwardmeasure-openworkflow/docs/forwardmeasure-data-streaming-plan.md`.
   The FDS README makes these binding. D4 is superseded (§8.4).
3. **[E:n]**: principle *n* of the org-wide engineering manifesto.
4. **[H:doc]**: an FDS `docs/` handoff.

Verify on real infrastructure (Kafka, K3s, OpenSearch, Postgres, Keycloak, MinIO), on all three
launcher frameworks. Compilation, tests against fakes, and earlier "verified" claims are not
evidence.

## 1. Scope

FDS is the org's metadata-driven data-movement product:

- **Input:** one declarative `IngestionSpec`.
- **Planning:** the planner (`ExecutionPlanCompiler`) compiles the spec into an execution plan.
- **Delivery:** one of two delivery engines, Pekko Streams or Kafka Streams, runs the plan in bounded or
  continuous mode. An optional Spark compute stage may come first.
- **Invocation:** the launcher starts a run either from a direct REST call or through a fowf workflow.

## 2. Component map

| Area | Modules |
|---|---|
| Contract | `-api` (`IngestionSpec`, `ingestion-spec.yaml`) |
| Mapping | `-transforms` (`NamedTransformRegistry`), `-mappers` (`FieldMappingEngine`) |
| Core and planning | `-core` (`ExecutionPlanCompiler`, `SparkHandoffSpecs`), `-executor-streaming-api` (`DeliveryEngine`, `ExecutionPlan`) |
| Delivery engines | `-executor-pekko` (`PekkoStreamsDeliveryEngine`), `-executor-kafka-streams` (`KafkaStreamsDeliveryEngine`, `InputFrontier`) |
| Compute stage | `-executor-spark` |
| Connectors | `-connector-camel`, `-connector-jdbc`, `-connector-object-storage` |
| Launcher | `-launcher-application` (`DirectIngestionLauncher`, `WorkflowIngestionLauncher`), `-launcher-jaxrs` (`/ingestion-runs`, `/workflow-runs`), `-framework-bindings/{quarkus,spring,micronaut}`, `-deployments/launcher/*` |

---

## 3. Rules

### 3.1 The model (SMOD)

- **SMOD-01**: There is exactly one author-facing type, `IngestionSpec`. It holds:
  - `sources: []`. One source means a single-source run, and more than one means a correlated run.
    There is no separate correlation spec.
  - a transform list;
  - a destination;
  - the `ExecutionMode`.

  `ExecutionPlan` and `ExecutionProfile` are compiled internal types. Authors never write them.
  - **Source:** [U:2026-09-20]
  - **Check:** `-api` exposes only `IngestionSpec` to authors, and no `CorrelationSpec` or
    `StreamingStageSpec` remains.
  - **State:** NOT ASSESSED.
- **SMOD-02**: The author declares only the `ExecutionMode` (`BOUNDED` or `CONTINUOUS`), never an engine.
  The planner decides the delivery engine, and whether a Spark stage is needed, from the
  `TransformCharacteristics` each named transform declares (cardinality, state, cost, order
  sensitivity, determinism):
  - a stateful transform selects Kafka Streams;
  - a heavy or large-scale correlated transform inserts a Spark stage.
  - **Source:** [U:2026-09-20]
  - **Check:** planner unit and integration tests for each row of the selection table, and no spec
    field names an engine.
  - **State:** NOT ASSESSED.
- **SMOD-03**: The capability space is exactly 2×2×2:
  - source cardinality: single or correlated;
  - execution mode: bounded or continuous;
  - delivery engine: Pekko Streams or Kafka Streams.

  Spark and Camel are not axes.
  - **Source:** [U:2026-09-20]
  - **Check:** a design review, and a test or support statement for each of the 8 cells.
  - **State:** NOT ASSESSED.
- **SMOD-04**: There are exactly two delivery engines, `PekkoStreamsDeliveryEngine` and
  `KafkaStreamsDeliveryEngine`. Both are permanent. Each branches internally on `ExecutionMode`, and
  each owns the final write and its lifecycle in every cell. No other runner classes remain.
  - **Source:** [U:2026-09-20] [U:2026-10-04 §8.4]
  - **Check:** search for runner and provider classes outside the two engines.
  - **State:** NOT ASSESSED.
- **SMOD-05**: Spark is only an optional compute stage, and it may appear anywhere before delivery. It
  never writes to a business destination. It always hands off to the selected delivery engine, through
  Kafka or an in-process handoff. `SparkSinks` cannot construct a non-Kafka writer.
  - **Source:** [U:2026-09-20, 2026-09-21]
  - **Check:** `SparkSinks.write` throws for anything but Kafka, and a test asserts that.
  - **State:** NOT ASSESSED.
- **SMOD-06**: "Correlated" never implies Spark. A delivery engine can run correlation natively, for
  example as a Kafka Streams `KTable` join.
  - **Source:** [U:2026-09-20]
  - **Check:** a correlated, continuous Kafka Streams pipeline that uses no Spark.
  - **State:** NOT ASSESSED.
- **SMOD-07**: Camel is connector plumbing only, used where a source or sink type needs it. It is never
  a pipeline engine or a delivery mechanism.
  - **Source:** [U:2026-09-20] [D:5, D:6]
  - **Check:** a design review.
  - **State:** NOT ASSESSED.
- **SMOD-08**: A bounded Kafka Streams run terminates against a fixed input frontier. It captures the
  end offsets when it starts. It succeeds only when every partition has crossed its frontier and all
  output is committed. Lag reaching zero is not a termination condition.
  - **Source:** [U:2026-09-20]
  - **Check:** `InputFrontier` tests, including records that arrive after the run starts.
  - **State:** NOT ASSESSED.

### 3.2 Invocation and dispatch (SINV)

- **SINV-01**: Every `IngestionSpec` can be invoked both ways, by direct REST and through a fowf
  workflow, for every matrix cell. This is non-negotiable.
  - **Source:** [U:2026-09-20]
  - **Check:** an end-to-end test for each invocation path, on a sample of cells.
  - **State:** BUG for Spark-staged specs. See SINV-05.
- **SINV-02**: A `BOUNDED` run dispatches a Kubernetes `Job`. A `CONTINUOUS` run dispatches a Kubernetes
  `Deployment`, through fowf's `kubernetes-deployment` protocol. Both modes are reachable the same way
  from either invocation path.
  - **Source:** [U:2026-09-20, 2026-09-21]
  - **Check:** K3s tests for both modes.
  - **State:** NOT ASSESSED.
- **SINV-03**: The Kubernetes Job lifecycle (launch, poll, cancel) comes from the shared library
  extracted from fowf, `openworkflow-kubernetes-job-lifecycle`. FDS does not implement it again.
  - **Source:** [H:README]
  - **Check:** the launcher depends on the shared library.
  - **State:** NOT ASSESSED.
- **SINV-04**: FDS uses fowf only where fowf genuinely fits. Being free of dependency conflicts is not
  enough reason to use it.
  - **Source:** [U:2026-09-17]
  - **Check:** a design review.
  - **State:** NOT ASSESSED.
- **SINV-05**: In both invocation paths, the FDS planner (`ExecutionPlanCompiler`) computes the
  execution plan, including the Spark stage and the derived delivery spec. A fowf workflow passes FDS's
  launcher the same `IngestionSpec` a REST caller does. The workflow never computes a plan or a derived
  spec, and never encodes one.
  - **Source:** [U:2026-10-04 §8.2]
  - **Check:** a Spark-staged spec started through a fowf workflow runs both the Spark Job and the
    delivery Job, and the delivery Job uses the plan the planner computed. Verify end to end on K3s.
  - **State:** BUG. Workflow mode has never dispatched a Spark-staged spec. `WorkflowIngestionLauncher`
    has no reference to the planner or to `SparkHandoffSpecs`, and Spark-then-delivery sequencing
    exists only inside `DirectIngestionLauncher`. See
    `docs/fowf-workflow-spark-staged-dispatch-undesigned-2026-09-25.md`.

### 3.3 Mapping and typing (SMAP)

- **SMAP-01**: There is no Protobuf. Source and target typing use OpenAPI or JSON Schema, with real
  generated model classes wherever typing is needed.
  - **Source:** [D:1]
  - **Check:** search for `protobuf`.
  - **State:** NOT ASSESSED.
- **SMAP-02**: Field mapping is metadata-driven. A new source needs only a YAML mapping spec, never new
  Java code. `FieldMappingEngine` interprets that spec at run time, through one flat
  `NamedTransformRegistry`. MapStruct is used only for JPA entity ↔ model conversion in
  `-connector-jdbc`.
  - **Source:** [D:3]
  - **Check:** onboarding a source touches only YAML.
  - **State:** NOT ASSESSED.
- **SMAP-03**: Source-specific schemas and mapping specs live in the consuming product (for example FEI),
  not in FDS.
  - **Source:** [H:README]
  - **Check:** FDS main code contains no source-specific mapping.
  - **State:** NOT ASSESSED.
- **SMAP-04**: The correlated merge works as follows:
  - Sources are ordered by trust, highest first.
  - For singular fields, the highest-trust source that resolved the field wins.
  - Values from lower-trust sources fold in as aliases and corroboration.
  - Identifiers are unioned across sources.
  - A merge-policy artifact, referenced by URI per revision, carries this policy. It is not a
    hard-coded rule.
  - All correlation runners share the same semantics.
  - **Source:** [U:2026-09-09] [U:2026-10-04 §8.1]. The 2026-09-09 design is recorded in the session
    memory note `project_fei_ingestion_spark_correlation_design.md` ("highest-trust source wins
    singular fields, others fold in as aliases/corroboration, identifiers union", and "a new
    merge-policy YAML artifact (`mergePolicyUri`) ... should carry this, not a hardcoded rule"). It is
    not yet recorded in an FDS repository document.
  - **Check:** a three-source test per runner, covering a singular field, an alias-bearing field and
    an identifier list. The test asserts that the trust winner is kept, the aliases are retained, and
    the identifiers are unioned.
  - **State:** BUG. Every runner merges with `putIfAbsent` over sources sorted by trust. That keeps the
    winner's value for every field, so aliases and identifiers from lower-trust sources are dropped,
    and the merge rule is hard-coded. The merge sites are:
    - `PekkoCorrelationEngine.java:152`;
    - `ContinuousPekkoStreamsCorrelationRunner.java:228`;
    - `SparkCorrelationEngine.java:215`;
    - `BoundedKafkaStreamsCorrelationRunner.java:231`;
    - `ContinuousKafkaStreamsCorrelationRunner.java:179`.

### 3.4 Connectors (SCON)

- **SCON-01**: Object storage goes through `forwardmeasure-object-storage` only.
  - **Source:** [D:5] [E:9]
  - **Check:** no S3, GCS or Azure SDK is used directly.
  - **State:** NOT ASSESSED.
- **SCON-02**: Relational sources go through `forwardmeasure-jpa`:
  - repositories and services build on its bases;
  - queries use the Criteria API over the JPA static metamodel, except where that is impractical;
  - `EntityManager` is used only in repositories.

  Bulk partitioned extraction stays on Spark's JDBC reader.
  - **Source:** [D:5] [U:2026-10-04]
  - **Check:** review `-connector-jdbc`.
  - **State:** BUG until triaged. `JpaPagingSource` uses `EntityManager` directly.
- **SCON-03**: Camel connectivity is one generic module, parameterized by URI, rather than one module
  per protocol. It is bridged into Pekko through `camel-reactive-streams`.
  - **Source:** [D:5, D:6]
  - **Check:** the module layout.
  - **State:** NOT ASSESSED.

### 3.5 Delivery engines and module dependencies (SIND)

- **SIND-01**: Pekko Streams and Kafka Streams are both permanent FDS delivery engines (SMOD-04). No rule
  removes or restricts either one.
  - **Source:** [U:2026-09-20] [U:2026-10-04 §8.4]
- **SIND-02**: The modules shared by both delivery engines and the Spark stage (`-api`, `-core`,
  `-mappers`, `-transforms`, `-connector-*` and `-executor-streaming-api`) depend on neither delivery
  engine.
  - Each delivery engine, and the Spark stage, carries only its own engine's dependencies:
    - the Kafka Streams engine and the Spark stage carry no Pekko;
    - the Pekko engine carries no Kafka Streams.
  - Engine-specific code lives in that engine's module. For example, the Pekko `Source`/`Sink` adapters
    for the connectors live in the Pekko engine's module.
  - **Source:** [U:2026-10-04]
  - **Check:** a dependency rule, plus the resolved classpath of each executor image:
    - no `org/apache/pekko` artifact on the Kafka Streams or Spark images;
    - no Kafka Streams on the Pekko image.
  - **State:** BUG. `-core`, `-connector-jdbc` and `-connector-object-storage` each depend on
    `pekko-stream` (`pom.xml` lines 25, 27 and 20). As a result, `-executor-kafka-streams` and
    `-executor-spark` carry Pekko through `-core`.

### 3.6 Durability (SDUR)

- **SDUR-01**: Correlation state survives restarts on both delivery engines. A pod restart never
  silently loses partial correlation progress. On the Pekko engine, the state is made durable with a
  Kafka compacted changelog topic:
  - every update writes through to the topic;
  - on startup, the engine replays the topic to rebuild the state before serving.

  The merge is idempotent, so at-least-once replay is safe.
  - **Source:** [U:2026-09-25] [U:2026-10-04 §8.3]
  - **Check:** restart mid-correlation on both engines. The final merged output matches a run with no
    restart.
  - **State:** BUG. `ContinuousPekkoStreamsCorrelationRunner` holds its merge state only in an in-memory
    `ConcurrentHashMap` (lines 106–107). A pod restart silently loses all partial correlation progress,
    with no replay and no error. Kafka Streams' correlation runner is not affected: its `KTable` is
    backed by a changelog topic.

### 3.7 Security and API (SSEC)

- **SSEC-01**: Every launcher operation is authorized by AuthZEN, through the shared
  `forwardmeasure-authzen` library.
  - Authorization happens in `-launcher-application`, not in the JAX-RS layer.
  - Identity comes from the JWT's single active Organization.
  - Failures fail closed.
  - **Source:** [H:fds-enterprise-hardening-plan 1b, 1d]
  - **Check:** contract tests with a real Keycloak.
  - **State:** NOT ASSESSED.
- **SSEC-02**: The launcher returns RFC 9457 `Problem` responses and the same 401/404 matrix as fowf, on
  all three frameworks.
  - **Source:** [U:2026-10-01]
  - **Check:** per-framework tests.
  - **State:** NOT ASSESSED.
- **SSEC-03**: The launcher API is contract-first: an OpenAPI document with generated server interfaces
  and generated clients, never hand-written ones.
  - **Source:** [E:8] [U: never hand-write OpenAPI clients]
  - **Check:** the resources implement generated interfaces.
  - **State:** BUG. `IngestionRunResource` and `WorkflowRunResource` are hand-written JAX-RS, and no
    openapi-generator configuration exists for the launcher.
- **SSEC-04**: The launcher runs on Quarkus, Spring and Micronaut with identical behaviour. The runners
  are plain `main()` programs that use no framework.
  - **Source:** [E:10] [H:README]
  - **Check:** per-framework launcher contract tests.
  - **State:** NOT ASSESSED.
- **SSEC-05**: The ingestion job policy, `IngestionJobPolicy`, is kept and enforced. It allowlists images
  and namespaces.
  - **Source:** [H:fds-authorization-remediation-guide 3.3]
  - **Check:** tests that the policy rejects disallowed images and namespaces.
  - **State:** NOT ASSESSED.

### 3.8 Testing and operations (STST)

- **STST-01**: The acceptance bar is the 18-cell matrix: 3 frameworks × 2 delivery engines × 3
  invocation-and-mode combinations:
  - bounded, by direct REST, dispatching a Job;
  - bounded, through a fowf workflow;
  - continuous, through a fowf workflow, dispatching a Deployment.

  The matrix runs for both the WorldCheck and the Customer Master single-source mappings, and those
  mappings are exact field-by-field ports of the legacy mappings.
  - **Source:** [U:2026-09-21]
  - **Check:** the matrix test results.
  - **State:** NOT ASSESSED.
- **STST-02**: Kafka in tests comes from a `forwardmeasure-testcontainers-kafka` module, not from ad hoc
  `KafkaContainer` use.
  - **Source:** [U:2026-09-21]
  - **Check:** search for `KafkaContainer`.
  - **State:** NOT ASSESSED.
- **STST-03**: Every pipeline stage and test cell logs in a structured, extractable form, so results can
  be checked by reading the logs.
  - **Source:** [U:2026-09-21]
  - **Check:** review the logs.
  - **State:** NOT ASSESSED.
- **STST-04**: The project meets the org baseline:
  - real infrastructure in tests, with no Mockito and no H2;
  - versions come from the platform BOMs;
  - services deploy with the shared Helm chart;
  - the build gates pass from a clean checkout.
  - **Source:** [E:6, E:9, E:11] [U]
  - **Check:** a pom and test review, plus `mvn clean install`.
  - **State:** NOT ASSESSED.

---

## 4. Bugs measured on 2026-10-04

| Rule | Bug | Evidence |
|---|---|---|
| SIND-02 | Pekko on the Kafka Streams and Spark classpaths | `-core`, `-connector-jdbc` and `-connector-object-storage` depend on `pekko-stream` |
| SMAP-04 | The correlation merge never unions identifiers or aliases, and its policy is hard-coded | `putIfAbsent` at the five merge sites listed in SMAP-04 |
| SDUR-01 | Pekko correlation state is not durable | `ContinuousPekkoStreamsCorrelationRunner.java:106-107` |
| SINV-05, SINV-01 | Spark-staged specs cannot run through a fowf workflow | `WorkflowIngestionLauncher` has no planner use; `docs/fowf-workflow-spark-staged-dispatch-undesigned-2026-09-25.md` |
| SSEC-03 | Launcher API is not contract-first | hand-written `IngestionRunResource` and `WorkflowRunResource`; no generator configuration |
| SCON-02 | `EntityManager` outside a repository | `JpaPagingSource` |

## 5. Open questions

None. The product owner decided every question raised on 2026-10-04 (§8).

## 6. Verification procedure

This is the same procedure as fowf §7:

1. Run each rule's Check and record the evidence.
2. List any behaviour that no rule covers as a candidate unilateral decision.
3. Never count compilation, a test against fakes, or an earlier document's claim as a pass.

## 7. Fix sequence for FDS

FDS is second in the cross-product order (fowf, then FDS, then FEI). It relies on fowf's engine-neutral
operation adapters and its Kubernetes Job and Deployment protocols, so it starts after fowf's fix
sequence is done.

1. **Enforcement first.** Write the dependency rule for SIND-02, so the build fails on today's bug.
2. **Module separation** (SIND-02). Move the Pekko `Source`/`Sink` code out of `-core`,
   `-connector-jdbc` and `-connector-object-storage` into `-executor-pekko`, or into Pekko-specific
   connector modules. The shared modules keep engine-neutral connector logic only.
3. **Correlation merge** (SMAP-04). Build the merge-policy artifact and one shared merge implementation
   (trust winner for singular fields, aliases folded in, identifiers unioned), and use it in all five
   runners.
4. **Pekko correlation durability** (SDUR-01). Add the compacted changelog topic, with write-through and
   replay on startup.
5. **Planner in both invocation paths** (SINV-05). The workflow path passes the `IngestionSpec`, and
   FDS's planner computes the Spark stage and the delivery plan exactly as it does for REST.
6. **Contract-first launcher API** (SSEC-03). Write the OpenAPI document and generate the server
   interfaces and clients. FEI and the fowf workflows then call FDS through the generated client.
7. **Connector persistence** (SCON-02). Move `EntityManager` behind a repository, or justify the
   exception.
8. **Verify** every remaining rule, ending with the 18-cell matrix (STST-01).

## 8. Decision log: decisions the product owner approved on 2026-10-04 that affect FDS

The full log is in §9 of the fowf document.

1. The correlation merge is a bug against the 2026-09-09 design. The highest-trust source wins
   singular fields, and identifiers and aliases are unioned (SMAP-04).
2. For Spark-staged runs started through a fowf workflow, the FDS planner computes the delivery plan,
   exactly as it does for direct REST (SINV-05).
3. Pekko correlation durability uses a Kafka compacted changelog topic (SDUR-01). The alternative,
   Valkey/Redis-held state, was considered and not chosen.
4. Decision D4 is superseded by the 2026-09-20 model: both delivery engines are permanent (SMOD-04,
   SIND-01).
