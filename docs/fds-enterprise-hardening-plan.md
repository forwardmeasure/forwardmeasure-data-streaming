# FDS Enterprise Hardening — Security, Parity, Concurrency, Validation

## Context

A direct, line-by-line audit of `forwardmeasure-data-streaming` (FDS) found 16 real, verified gaps spanning
security (an unauthenticated REST API fronting a privileged Kubernetes RBAC grant), functional parity between
the Pekko and Spark engines (a fully-built connector nobody wired up; a connector alias that only works on one
engine), and correctness/reliability (unpooled JDBC connections under concurrency, silent config-typo
swallowing, a URI-encoding bug, a shared-pool concurrency leak, non-deterministic cross-engine merge results).
Every finding below was confirmed by reading the actual code and grepping for counter-evidence, not inferred —
citations are file:line. This plan closes all 16, in dependency order, and — per explicit instruction — adds
substantially more test coverage than exists today, since several of these gaps escaped detection specifically
*because* the layer that broke (framework-binding wiring, config parsing, cross-engine dispatch) had zero tests
of its own.

While designing the authorization fix, a second, larger question surfaced: FDS's own AuthZEN implementation
would have been the **third** independent copy of the same code — `forwardmeasure-openworkflow` (fowf) has the
real original (`openworkflow-authorization-{api,authzen,testkit}`), `forwardmeasure-entity-intelligence` (fei)
already ported it verbatim once, and FDS was about to port it a second time. There is no evidence anywhere in
either repo that keeping these separate was a deliberate choice — it reads as accumulated debt, not a
decision. Given that, and per explicit direction, this plan now **extracts a real, shared
`forwardmeasure-authzen` library and migrates all three products (fowf, fei, FDS) onto it** — this is a
materially larger, three-repository undertaking than the rest of the plan, sequenced first since FDS's own
Phase 1 depends on it, but independent of and non-blocking for Phases 2–4 (those are FDS-only and can proceed
in parallel or in any order once Phase 1's shared contracts exist).

HikariCP, used in Phase 4b, is likewise reused rather than invented — already the org's established
connection-pool choice (`forwardmeasure-platform/pom.xml`). Nothing in this plan is a new architectural
direction for the org — it's consolidating and extending conventions the org's other products already
established.

---

## Phase 1 — Shared `forwardmeasure-authzen` library, then AuthN + AuthZ in fowf/fei/FDS

**Closes finding #1** (no authentication/authorization on any of the 9 launcher REST endpoints), directly
implements `/tmp/.../fds-authorization-remediation-guide.md`, and — per explicit decision — eliminates the
third-copy duplication that guide's own approach would otherwise have created.

### 1.0 New repo: `forwardmeasure-authzen`

A new, dedicated repo (matching this org's established convention — `forwardmeasure-jpa`,
`forwardmeasure-testcontainers`, `forwardmeasure-object-storage` are each already their own shared-concern
repo; a new one for AuthZEN is not a new pattern). Extracted from fowf's real original
(`openworkflow-authorization-{api,authzen,testkit}` — the least-removed-from-source copy; diff against fei's
port during extraction to fold in anything fei fixed along the way, per fei's own authorization module
comments), split so the **generic transport layer is shared** while **domain vocabulary stays per-product**:

- **`forwardmeasure-authzen-api`**: `AuthorizationService` (`evaluate`/`evaluateBatch`/`requireAuthorized`),
  `AuthorizationDecision`, `AuthorizationDeniedException`, `AuthorizationUnavailableException`,
  `ActiveOrganization`, `ActiveOrganizationProvider`, `KeycloakOrganizationClaims`. **One real, necessary
  change from every existing copy**: `AuthorizationRequest.action` becomes a plain `String` (the raw
  `"resource:verb"` scope), not a typed `AuthorizationAction` enum — the enum itself is inherently
  product-specific vocabulary and can't be shared, but the string it produces via `.scope()` is all the
  transport layer actually needs. `AuthorizationResource` stays a generic `(type, id, properties)` record with
  **no static factory methods** in the shared library — each product keeps its own factories
  (`ingestionRun(id)`, `referencePopulation(id)`, fowf's own `definition(id)`, etc.) as a thin, local,
  product-owned file, calling the shared constructor directly.
- **`forwardmeasure-authzen-client`**: the real `AuthzenAuthorizationService` (fail-closed Keycloak AuthZEN
  PDP HTTP client — any non-200/malformed/network-error/correlation-mismatch denies; decision caching with
  TTL+eviction; batch evaluation), taking plain constructor/builder arguments (issuer, client-id, client-secret,
  timeouts, cache settings) — **not** `@ConfigProperty`-annotated itself, so it stays framework-neutral; each
  product's own framework-binding producer (staying local to that product, reading that product's own config
  property names) constructs it.
- **`forwardmeasure-authzen-testkit`**: `StubAuthorizationService.permitAll()` — already fully generic.
- **Per-framework JWT extraction** (`{Quarkus,Spring,Micronaut}ActiveOrganizationProvider`): the extraction
  *logic* (read a validated JWT, run `KeycloakOrganizationClaims.extract(claims, clientId)`) is identical
  across products — only the config *value* for `clientId`/audience differs. These become real, shared classes
  in `forwardmeasure-authzen-api` (or a small `forwardmeasure-authzen-framework-support` addition if keeping
  `-api` framework-neutral matters more — decide during implementation by checking whether `-api`'s pom already
  tolerates a `provided`-scope Quarkus/Spring/Micronaut dependency without forcing it on non-framework
  consumers), parameterized by an injected config value rather than a hardcoded property name — maximizing the
  actual DRY benefit of consolidating, not just moving the duplication one level up.

### 1.1 Migrate fowf onto the shared library

fowf is the origin, so migrating it first validates the extraction against real, already-proven-correct
behavior before either downstream product depends on it. Replace `openworkflow-authorization-{api,authzen,
testkit}`'s content with a dependency on `forwardmeasure-authzen-{api,client,testkit}`; keep fowf's own
`AuthorizationAction`/`AuthorizationResource` factories as thin, local, product-owned files; update every
`AuthorizationRequest` construction call site to pass `action.scope()` instead of the enum instance (mechanical,
but must be a *complete* sweep — grep for every construction site, not just the obvious ones, matching this
session's own standing "full parity, no partial fixes" lesson); re-run fowf's full existing authorization test
suite against the migrated code with zero behavior change expected.

### 1.2 Migrate fei onto the shared library

Same treatment: depend on `forwardmeasure-authzen-{api,client,testkit}`, delete fei's own now-redundant local
copies (`forwardmeasure-entity-intelligence-authorization-{api,authzen,testkit}`'s ported classes), keep fei's
own `AuthorizationAction`/`AuthorizationResource`, sweep every call site for the `.scope()` change, re-run fei's
full existing authorization test suite (real Keycloak+AuthZEN testcontainer tests included) with zero behavior
change expected.

### 1.3 FDS — build on the shared library from day one

FDS never has its own copy to migrate away from. No `forwardmeasure-data-streaming-authorization-{api,authzen,
testkit}` modules — those would just be a fourth copy of what now lives in `forwardmeasure-authzen`. Instead,
add three thin dependencies (`com.forwardmeasure.authzen:forwardmeasure-authzen-{api,client,testkit}`, the last
`test`-scoped) to `forwardmeasure-data-streaming-launcher-application`, and write only what's genuinely
FDS-specific: the `AuthorizationAction`/`AuthorizationResource` vocabulary and the wiring below (validated
against FDS's own real call chain via a Plan sub-agent that read the actual files — citations throughout).
`ActiveOrganization` (from `forwardmeasure-authzen-api`) carries `com.forwardmeasure.jpa:
forwardmeasure-jpa-tenancy`'s `TenantId` — already in FDS's root `pom.xml` dependency management, zero new
heavy dependency.

### 1b. Authorization sits in `launcher-application`, not the JAX-RS layer

Fei's *real* call sites (`ReferencePopulationApplicationService`, `IngestionPipelineTriggerApplicationService`)
call `authorization.requireAuthorized(...)` inside an application-service class, first statement, before any
side effect — never inside a JAX-RS resource. FDS's `launcher-application` classes (`DirectIngestionLauncher`,
`DirectCorrelationLauncher`, `WorkflowIngestionLauncher`) already play exactly that role (framework-neutral,
sit between JAX-RS and K8s/fowf mechanics, already own one authorization concern via `IngestionJobPolicy` one
layer down) — so no new layer is introduced. Each of their `launch`/`observe`/`cancel` methods gains a leading
`ActiveOrganization actor` parameter and an `AuthorizationService` constructor field, and calls
`authorization.requireAuthorized(new AuthorizationRequest(actor, AuthorizationResource.ingestionRun(id),
AuthorizationAction.INGESTION_RUN_LAUNCH.scope(), correlationId, Map.of()))` as the first statement (`.scope()`
because the shared library's `AuthorizationRequest.action` is a plain `String`, per 1.0) — before building
the `EnvLaunchRequest`, so `IngestionJobPolicy`'s existing, unchanged 403 allowlist check still runs
independently, one layer further down, exactly as today.

`IngestionRunResource`/`CorrelationRunResource`/`WorkflowRunResource` (`launcher-jaxrs`) stay thin: each gains
one constructor-injected `ActiveOrganizationProvider organizations` (fei's exact interface), resolved fresh
per call via `organizations.current()` — never cached in a field — and passed straight into the launcher call.
`WorkflowRunResource.get(executionId)` has no `correlationId` for its authorization-audit context; synthesize
one (`UUID.randomUUID().toString()`) rather than widening the API, since it's used for cache-keying/audit, not
security matching.

**Do not remove or weaken `IngestionJobPolicy`** (`launcher-application/.../IngestionJobPolicy.java`) — it
authorizes the *request payload* (namespace/sha256-pinned image), a different, still-needed concern from
caller authorization.

### 1c. Vocabulary and config

```java
public enum AuthorizationAction {
  INGESTION_RUN_LAUNCH("ingestion-run:launch"), INGESTION_RUN_READ("ingestion-run:read"),
  INGESTION_RUN_CANCEL("ingestion-run:cancel"),
  CORRELATION_RUN_LAUNCH("correlation-run:launch"), CORRELATION_RUN_READ("correlation-run:read"),
  CORRELATION_RUN_CANCEL("correlation-run:cancel"),
  WORKFLOW_RUN_LAUNCH("workflow-run:launch"), WORKFLOW_RUN_READ("workflow-run:read"),
  WORKFLOW_RUN_CANCEL("workflow-run:cancel");
}
```
`AuthorizationResource.ingestionRun(id)`/`.correlationRun(id)`/`.workflowRun(id)` — same `type`/`id`/
`properties` factory shape as fei's `AuthorizationResource.ingestionPipeline(id)`.

Config, extending FDS's own real, established `datastreaming.launcher.*` namespace (not fei's
`entityintelligence.*`):
```
datastreaming.launcher.authorization.{issuer,client-id,client-secret,request-timeout,decision-ttl,
  maximum-cache-entries,policy-version,organization-client-id}
```
Wired once per framework via new `{Quarkus,Spring,Micronaut}AuthorizationServiceProducer` classes in the
existing `forwardmeasure-data-streaming-framework-bindings/{quarkus,spring,micronaut}` modules — each reads
FDS's own config below and constructs `forwardmeasure-authzen-client`'s `AuthzenAuthorizationService` (a plain
constructor call now, not a per-product copy of the HTTP client itself), gated to
`StubAuthorizationService.permitAll()` (from `forwardmeasure-authzen-testkit`) under a new test build-profile
split (FDS has no precedent for this yet — introduce it, mirroring fowf/fei's own post-migration
`@UnlessBuildProfile("test")`/equivalent).

### 1d. Authentication — JWT → `ActiveOrganization`, per framework (net-new for FDS)

Use `forwardmeasure-authzen-api`'s shared `{Quarkus,Spring,Micronaut}ActiveOrganizationProvider` classes (1.0)
directly — no per-product copy to write, only the framework's own security dependency and FDS's own config
value:
- Quarkus: add `quarkus-oidc`, supply FDS's own `organization-client-id` config value.
- Spring: add `spring-boot-starter-oauth2-resource-server`, wire a `SecurityFilterChain` with
  `.oauth2ResourceServer().jwt(...)`, supply FDS's own config value.
- Micronaut: add `micronaut-security-jwt`, supply FDS's own config value.

Add issuer/audience config to each of the 3 `forwardmeasure-data-streaming-deployments/launcher/*`
`application.yml` files (as **required**, no default — see Phase 4c).

**Real collision, fixed at the shared-library level (1.0), not worked around locally in FDS**:
`KeycloakOrganizationClaims` in fei's current code throws plain `SecurityException` on malformed-JWT, mapped
by fei's own 401 mapper. FDS *already has* a `SecurityException`→**403** mapper
(`launcher-jaxrs/.../mapper/SecurityExceptionMapper.java`, serving `IngestionJobPolicy`, must stay unchanged
per the instruction above) — JAX-RS permits exactly one `ExceptionMapper<SecurityException>`, so a straight
reuse of that type would collide. Since `KeycloakOrganizationClaims` now lives in the shared
`forwardmeasure-authzen-api` (1.0), fix it once, at the source: define a new, dedicated
`AuthenticationRequiredException` (plain `RuntimeException`, not `SecurityException`) **in the shared library
itself**, and have `KeycloakOrganizationClaims` throw that instead. fowf/fei's own migrations (1.1/1.2) update
their existing 401 mappers to catch this new shared type instead of `SecurityException` (their own mappers
already map malformed-JWT to 401 today, so this is a type-rename in an existing mapper, not new behavior for
either); FDS adds a brand-new mapper for it, also → 401 — no collision anywhere, and the fix is now part of
the shared contract instead of an FDS-only workaround. `AuthorizationDeniedException` (caller authenticated
but not entitled) gets its own FDS mapper → **403**, matching fei's own established convention and RFC 9110
semantics — same code `IngestionJobPolicy` already uses, but a genuinely distinct *exception type*, mapper
class, and message text, enough to keep the two failure modes distinguishable in logs/responses without
touching `SecurityExceptionMapper`'s existing, tested contract.

---

## Phase 2 — Launcher constructor hardening (closes finding #2)

`DirectIngestionLauncher`/`DirectCorrelationLauncher` each have a 4-arg (single-engine) constructor that
silently passes `null` for the *other* engine's image/command to the real 6-arg constructor. All three
framework bindings use only the 4-arg form — confirmed by grep, zero uses of the 6-arg constructor anywhere
outside `launcher-application`'s own tests — so `execution.engine: spark` on `DirectIngestionLauncher` (and
`engine: pekko` on `DirectCorrelationLauncher`) is a permanent 400 in every real deployment, and there was no
config property that could even supply the missing value.

**Fix**:
- Delete the 4-arg constructors entirely from both classes. The 6-/7-arg constructor (now also taking
  `AuthorizationService` per Phase 1) becomes the only public one.
- Add validation in that constructor: reject (throw `IllegalArgumentException` at construction time, not at
  first request) if *both* engines' image/command are blank — at least one must be configured. Validate each
  non-blank image against the same `@sha256:`-pinned shape `IngestionJobPolicy.authorizeImage` already checks,
  so a malformed image config fails at boot, not at first launch.
- Add the missing config properties to all three `application.yml` files: `datastreaming.launcher.spark.*` to
  the ingestion-launcher producer (currently only reads `.pekko.*`), and `datastreaming.launcher.pekko.*` to
  the correlation-launcher producer (currently only reads `.spark.*`). Update all three
  `Launcher{Quarkus,Spring,Micronaut}Binding` producer methods to read and pass both.
- New test: for each framework, a real test that goes through the *actual* binding-produced launcher instance
  (not a hand-constructed one, closing the exact test-layer gap that let this ship) and asserts a
  `spark`-engine ingestion request and a `pekko`-engine correlation request both dispatch correctly.

---

## Phase 3 — Connector parity (closes findings #3 and #4)

### 3a. What `object-storage` was built for, and why it's not wired up

`forwardmeasure-data-streaming-connector-object-storage`'s `ObjectStorageBridge` is a real, complete,
MinIO-tested Pekko-Streams source/sink — built as part of this session's "any-to-any" connector architecture
generalization, alongside `jdbc`/`kafka`/`opensearch`. `SourceSpec`/`SinkSpec`'s own javadoc explicitly lists
`object-storage` as a supported `connector` value on par with those three. Unlike `categories`/`locations`
(deliberately deferred, documented reasons), there is no comment anywhere explaining why `object-storage` was
left out of dispatch — it's a genuine gap in the connector-architecture rollout: the bridge was built, but the
step wiring it into each engine's `connector()` dispatch switch was simply never done. `executor-pekko/pom.xml`
even already declares a Maven dependency on the connector module that nothing in that module's source imports
— further confirming it was intended to be wired, not deliberately excluded.

**Fix**: wire `object-storage` into both engines' dispatch, symmetric with how `opensearch`/`kafka` were done:
- `PekkoIngestionRunner.rowSource`/`buildSink`: add an `object-storage` case calling `ObjectStorageBridge`,
  alongside the existing `sql`/`kafka` special-cases.
- `SparkCorrelationEngine.readSource`/`SparkSinks.write`: add an `object-storage` case. Spark has no native
  `object-storage` format, so (mirroring how `opensearch` is handled there today — a genuinely bespoke path,
  not a `DataFrameReader.format(connector)` call) this needs its own real implementation, not a generic
  fall-through.

### 3b. `jdbc`/`sql` alias parity, and preventing this class of gap recurring

Pekko treats `"jdbc"` and `"sql"` as synonyms (`PekkoIngestionRunner.isSqlConnector`); Spark's dispatch
(`SparkCorrelationEngine.readSource`, `SparkSinks.write`) checks only the literal `"jdbc"` — confirmed by grep,
zero `"sql"` handling anywhere in `executor-spark`. The identical spec works on one engine, breaks on the
other.

**Fix**:
- Add the same `"sql"` alias handling to Spark's dispatch.
- **Structural fix to stop this recurring**: extract connector-name constants into one shared
  `ConnectorNames` class in `forwardmeasure-data-streaming-api` (`FILE`, `JDBC`, `SQL`, `KAFKA`, `OPENSEARCH`,
  `OBJECT_STORAGE`, plus a static `isSqlConnector(String)` helper), and have *both* engines' dispatch code
  reference it instead of each declaring its own private string constants. This doesn't make divergence
  impossible, but it makes the connector vocabulary visibly, greppably shared rather than independently
  duplicated in two files that can silently drift.
- **New cross-engine parity test suite**: one shared fixture — a list of every `SourceSpec`/`SinkSpec`
  `connector` value FDS claims to support — run against both `PekkoIngestionRunner` and
  `SparkIngestionRunner`/`SparkCorrelationEngine`, asserting symmetric dispatch (both succeed, or both fail
  with the same "unsupported connector" shape) for every declared value. This is the test that would have
  caught both 3a and 3b immediately; add it to both `executor-pekko`/`executor-spark` test trees (or a new
  shared test-fixtures location both depend on, so the fixture list itself can't drift between the two
  modules — evaluate during implementation which is cleaner given Maven test-jar sharing conventions already
  used elsewhere in this reactor).

---

## Phase 4 — Correctness and reliability fixes

### 4a. Typed URI/query construction — no more string concatenation (closes findings #6, #14)

Two real bugs from raw concatenation:
- `PekkoIngestionRunner.sqlRowSource` builds `"sql:" + query + "?dataSource=#" + ...` with **no guard**
  against a literal `?` in `query` — the exact bug class already found and fixed on the *sink* side
  (`camelSink`/`camelBatchSink`, via the `CamelSqlQuery` header). The javadoc's own claim that the query is
  "never `:?`-parameterized... today" is asserted, not enforced.
- `openSearchDocumentUri` (Pekko) and its Spark equivalent in `SparkSinks` use `URLEncoder.encode` — form
  encoding — for a URL **path segment**, not a query string; wrong API, can silently write a document under
  the wrong id if the id contains a space or other char `URLEncoder` treats differently from RFC 3986 path
  encoding. Same class also builds the Basic-auth query string (`?authMethod=...&authUsername=...`) by
  concatenation.

**Fix, using `org.apache.hc.core5.net.URIBuilder`** (confirmed already transitively available on
`executor-pekko`'s classpath via `camel-http`'s own dependency on `httpcore5`/`httpclient5`; add an explicit
`httpcore5` dependency to `executor-spark` so both engines use the identical, typesafe builder — real parity,
not just "fixed twice differently"):
- `openSearchDocumentUri`/its Spark equivalent: build via `new URIBuilder(baseUrl).appendPath(index)
  .appendPath("_doc").appendPath(id)` (correct path-segment encoding) and `.addParameter("authUsername", ...)`
  /`.addParameter("authPassword", ...)` for Basic auth query params (correct query encoding) — replacing every
  manual `URLEncoder.encode`/string concatenation in both files.
- `sqlRowSource`: since Camel's *consumer*-side `sql:` endpoint has no header-based query-override mechanism
  (unlike the producer side, which is why `camelSink` could route around this), the real, safe fix is
  **explicit validation + rejection**: check the query for an unescaped `?` before building the endpoint URI,
  and throw a clear `IllegalArgumentException` naming the problem if found — consistent with finding #12's
  "explicitly reject" direction, applied here too, since there's no safe way to route around Camel's own
  consumer-side constraint.
- `OpenSearchIndexInitializer`/`SecretRefs` (shared `-api` module, deliberately JDK-only, no Camel/Spark/Pekko
  coupling per its own javadoc): keep that module dependency-light — use `java.net.URI`'s multi-arg
  constructor (`new URI(scheme, authority, path, query, fragment)`), the JDK-native typesafe equivalent,
  rather than pulling `httpcore5` into a module whose whole design point is having no framework dependency.

### 4b. Real connection pooling for the JDBC/SQL connector (closes finding #7)

`SqlDataSources.register` builds a Spring `DriverManagerDataSource` — no pooling, a fresh physical connection
per `getConnection()` call — backing sink writes that run with real concurrency
(`Sink.foreachAsync(parallelism, ...)`, up to 32 in the checked-in WorldCheck spec). Under load: up to
`parallelism` simultaneous fresh TCP+auth handshakes per batch, no reuse, real risk of exhausting the
database's connection limit.

**The significance, concretely**: `DriverManagerDataSource.getConnection()` opens a brand-new physical
connection — full TCP handshake + database auth — on *every single call*, with no reuse at all. A real pool
(HikariCP, already the org's established choice per `forwardmeasure-platform/pom.xml`) keeps a warm set of
already-authenticated connections, checked out and returned per request instead of opened and discarded.

**Fix** (validated against `CamelBridge`'s real lifecycle, not assumed):
- Add `com.zaxxer:hikaricp` dependency management to FDS's root `pom.xml` (mirroring the existing
  `forwardmeasure-jpa.version` pattern) and a plain dependency to `executor-pekko/pom.xml`.
- `SqlDataSources.register` gains an `int maximumPoolSize` parameter. `camelSink`/`camelBatchSink` pass their
  already-computed `parallelism` local variable (same value driving `Sink.foreachAsync`, zero config drift —
  it's literally the same variable, not a duplicated setting); `sqlRowSource` (a single blocking poll, not
  `foreachAsync`-driven) passes a small fixed size.
- **Lifecycle — real gap, confirmed**: `CamelBridge.close()` currently only calls `context.stop()`; Camel's
  registry `bind()` does not track or close arbitrary registered beans. A `DriverManagerDataSource` holds
  nothing worth leaking, so this was invisible until now — a `HikariDataSource` genuinely leaks a whole
  connection pool per registration if not closed. Add closeable-tracking to `CamelBridge` (a
  `registerCloseable(AutoCloseable)` method, closed alongside `context.stop()` in `close()`), and have
  `SqlDataSources.register` register its new `HikariDataSource` there.
- **Idempotency — a new gap this change would otherwise introduce**: `register()`'s own javadoc claims
  re-registration is harmless — true only because `DriverManagerDataSource` has no state to deduplicate. A
  second `register()` call for the same `url`/`options` (e.g. a source and sink against the same database)
  must look up and reuse an existing `HikariDataSource` bean by name before constructing a new one, or the fix
  itself leaks a second pool.

### 4c. Explicit-reject validation everywhere config is currently lenient (closes findings #8, #11, #12, #13)

Four related, previously-silent gaps, one consistent fix: **fail loud and specific, at the earliest possible
point, never silently substitute a default for an invalid value.**

- **#8** `IngestionPipeline.MalformedRecordPolicy.from()` silently maps *any* unrecognized `malformedRecord`
  value (a typo, wrong case) to `SKIP` — silent data-dropping — while its sibling `SinkFailurePolicy.from()`
  defaults unrecognized values to the safe choice (`FAIL`). Fix: both `from()` methods become symmetric — a
  `null`/absent field keeps today's documented default (`SKIP`/`FAIL` respectively, no behavior change for
  every existing spec); any **non-null, non-matching** value throws `IllegalArgumentException` naming the bad
  value and the valid options.
- **#12** Every spec record in `forwardmeasure-data-streaming-api` (`IngestionSpec`, `SourceSpec`, `SinkSpec`,
  `TransformSpec`, `TransformSpec.FieldRule`, `ExecutionSpec`, ...) uses `@JsonIgnoreProperties(ignoreUnknown =
  true)` — a typo in `repeated`/`metadata` (this session's own nested-object mechanism) is silently dropped,
  not rejected, silently degrading a real production document. Fix: remove `ignoreUnknown = true` everywhere
  in this module (Jackson's default, strict behavior, throws `UnrecognizedPropertyException` on any unknown
  field) — these are internal spec types with no external forward-compatibility need, so strictness has no
  real cost.
- **#11** `ExecutionSpec.ConcurrencySpec` is nullable/unvalidated; an omitted `execution.concurrency` NPEs
  several calls deep inside `IngestionPipeline.effectiveParallelism`. Fix: add a `validate()` step, invoked
  from `IngestionSpec.load`/`parseYaml` and `CorrelationSpec`'s equivalent right after deserialization, that
  checks every required nested field (source/sink/mapper non-null, `execution.concurrency.preferred` present
  and positive, etc.) and throws one `IllegalArgumentException` listing **every** problem found, not just the
  first — genuinely enterprise-grade fail-fast, not a single NPE with no context.
- **#13** All three deployment leaves default required config (`datastreaming.launcher.k8s.*`, `.pekko.image`,
  `.spark.image`, `.fowf.keycloak.*`) to blank strings in `application.yml`, so `@ConfigProperty`/`@Value`
  injection never fails — misconfiguration surfaces at first request, not at boot. Fix: remove the blank
  defaults for genuinely required properties so each framework's own native "missing config fails at startup"
  behavior fires (Quarkus build-time validation, Spring context-refresh failure, Micronaut startup validation).
  Keep blank-means-absent only for properties that are legitimately optional (`image-pull-secrets`), and say so
  explicitly in the YAML comment.

### 4d. `OpenSearchIndexInitializer` concurrent-create race (closes finding #10)

Two Jobs starting around the same time, targeting the same not-yet-created index, can both pass the
`indexExists` HEAD check as false and both PUT — the loser gets a 400 `resource_already_exists_exception`,
currently turned into a run-aborting `IllegalStateException`. Fix: on a 400 from `createIndex`, re-check
`indexExists` once more before throwing — if the index now exists (someone else just created it), treat as
success; only throw if it still doesn't exist.

### 4e. Keycloak token-fetch failures get real exception mapping (closes finding #9)

`KeycloakClientCredentialsTokenSupplier` throws plain `IllegalStateException`/`UncheckedIOException` on any
Keycloak failure, called with no try/catch from the generated client's bearer-auth path — bypasses this
module's own exception-mapper layer entirely, surfacing as an unmapped 500 instead of this API's normal
`ErrorResponse` shape. Fix: wrap failures in a new, dedicated unchecked `TokenAcquisitionException`, with its
own `ExceptionMapper` → 502 Bad Gateway (an upstream-dependency failure, not a client error), matching the
pattern every other real failure mode in this module already gets.

### 4f. Dedicated executor for the transform stage (closes finding #15)

`IngestionPipeline.run()` does `CompletableFuture.supplyAsync(() -> mapOrHandle(...))` with no explicit
`Executor` — silently runs on the JVM-wide shared `ForkJoinPool.commonPool()`, competing with anything else in
the process. `mapAsyncUnordered(parallelism, ...)` already strictly bounds concurrency regardless of which
pool backs it — the bug is pool *isolation*, not degree of concurrency.

**Fix — virtual threads, with a verified-safe lifecycle** (this is the part that's easy to get wrong):
- Add an `Executor` parameter to `IngestionPipeline.run(...)`, used only in the existing `supplyAsync` call.
- **Do not** create/close the executor inside `IngestionPipeline.run()` itself — `run()` returns
  immediately after `Source...runWith(sink, system)` materializes the stream asynchronously; both real callers
  (`PekkoIngestionRunner`'s two `run` overloads) then `.join()` on the result **outside** this method. Closing
  an `ExecutorService` right after `runWith()` returns — but before the still-running stream finishes
  submitting later transform tasks — risks `RejectedExecutionException` mid-run, exactly the "one failure kills
  the whole run" problem this class exists to prevent.
- **Correct placement**: the executor is owned by the code that actually blocks on `.join()`. Widen
  `PekkoIngestionRunner`'s existing `try (CamelBridge bridge = new CamelBridge()) { ... }` blocks (both `run`
  overloads) to also open `ExecutorService transformExecutor = Executors.newVirtualThreadPerTaskExecutor()` in
  the same try-with-resources, spanning the `.join()` call — identical lifecycle shape to how `CamelBridge` is
  already scoped in that exact method, no new pattern introduced.
- Virtual threads over a bounded platform pool: `mapAsyncUnordered` already caps in-flight work at
  `parallelism`, so a fixed-size platform pool would need to be kept in lockstep with that config for no real
  benefit; virtual threads need no sizing, handle a transform that unexpectedly blocks on I/O for free, and
  cost nothing extra for the common case (pure field mapping).

### 4g. Standardize `malformedRecord: fail` semantics across engines (closes finding #16)

Pekko fails on the very first malformed-record exception (proven by an existing test asserting exactly 1
attempt). Spark's equivalent exception, thrown inside a `mapPartitions` closure, is a *task* failure, which
Spark retries per `spark.task.maxFailures` (default 4) before failing the job — same policy name, 4x the
latency and log churn, because retrying a deterministic mapping error is never useful (it fails identically
every time — task retry exists for *transient* infra failures, not logic errors). Fix: when
`execution.failure().malformedRecord() == "fail"`, set `spark.task.maxFailures = 1` in
`SparkExecutorConfig`/`SparkSessionFactory`'s Spark config for that run, matching Pekko's proven single-attempt
behavior exactly. (Sink-failure retry, a separate, already-application-level mechanism via
`withSinkFailureHandlingBlocking`, is unaffected by this Spark config value.)

---

## Testing — significantly expanded, per explicit instruction

Every fix above ships with a real (no-mocks, container-backed where applicable, matching this repo's existing
discipline) test proving both the fix and the regression it prevents:

- **AuthZ**: real Keycloak+AuthZEN-PDP testcontainer round trip per framework — mint a real signed JWT, hit
  each of the 9 endpoints unauthenticated (expect 401), authenticated-but-wrong-role (expect 403), and
  authenticated-and-authorized (expect the existing happy-path behavior, no regression against
  `EnvConfiguredJobLauncherTest`'s real K3s assertions). Stub-service unit tests for the constructor-level
  `requireAuthorized` call sites, independent of any live PDP.
- **Constructor hardening**: unit test rejecting dual-null-engine construction; a real test per framework
  proving Spark-engine ingestion and Pekko-engine correlation both dispatch through the *actual*
  binding-produced launcher (not hand-constructed) — the exact layer that had zero coverage before.
- **object-storage**: real MinIO-container test through the full `IngestionSpec`/`PekkoIngestionRunner.run`
  and `SparkIngestionRunner.run` path (source read *and* sink write), both engines.
- **Connector parity suite**: the shared fixture-driven test described in 3b, both engines, every declared
  connector value.
- **`sqlRowSource` `?` rejection**: a query containing a literal `?` in a string literal now fails clearly at
  spec-validation time with a real assertion on the error message, not a silent truncation.
- **HikariCP**: a real concurrency test asserting connections are reused (pool size bounded, no unbounded
  connection growth) under `Sink.foreachAsync`-driven load; a leak test asserting the pool is closed when
  `CamelBridge` closes; an idempotency test asserting a second `register()` for the same url/options reuses the
  existing pool.
- **Explicit-reject validation**: typo'd `malformedRecord`/`sinkFailure` values now throw with a clear message;
  an unknown/typo'd YAML field in any spec type now throws `UnrecognizedPropertyException`; a spec omitting
  `execution.concurrency` now throws one aggregated, multi-problem `IllegalArgumentException`, not an NPE.
- **Config boot-fail**: per framework, a test asserting the application fails to start (not just first
  request) when a required property is missing.
- **Index-creation race**: two threads racing `ensureIndex` against the same not-yet-existing index both
  succeed without exception.
- **Virtual-thread executor**: assert transform-stage work runs on threads *not* named/prefixed like
  `ForkJoinPool.commonPool()`'s own worker threads.
- **URIBuilder correctness**: an OpenSearch document id containing a space/`+`/non-ASCII character round-trips
  correctly on both engines (the exact case `URLEncoder` got wrong).
- **Spark fail-fast parity**: a new Spark test mirroring the existing Pekko one — `malformedRecord: fail`
  aborts on the first bad record, asserted via task-attempt count, not up to 4.

## Verification

1. `mvn clean install` on the new `forwardmeasure-authzen` repo first (handed to the user to run, per this
   session's own standing no-self-build convention), then on `forwardmeasure-openworkflow` and
   `forwardmeasure-entity-intelligence` after their migrations (1.1/1.2) — both products' **full existing**
   test suites green with zero behavior change, not just the new/changed files. This is the step with the most
   blast radius in this whole plan (two already-shipped products), so it does not get skipped or sampled.
2. `mvn clean install` across the full FDS reactor — every module green, including every new test above.
3. Manual smoke check of the AuthZ path against a real Keycloak instance (not just testcontainers) before
   calling this production-ready, since it's the highest-stakes change.
4. Re-run the exact repo-wide greps from the remediation guide (`AuthorizationService`, `@RolesAllowed`/
   `SecurityContext` usage) and confirm they now return real hits, not zero.
