# FDS Authorization Gap — Remediation Guide

**Audience:** the engineer/agent implementing `forwardmeasure-data-streaming` (FDS).
**Severity:** High. Every JAX-RS endpoint in this repo — including three that launch, observe, and cancel real Kubernetes Jobs — currently runs with **zero caller authentication and zero authorization**. Any network caller who can reach these ports can launch or cancel arbitrary (allowlisted-image) Kubernetes Jobs with no notion of who is doing it or whether they're entitled to.

This was confirmed by direct code inspection on 2026-09-14, not inferred. Citations below are file:line.

---

## 1. What was checked, and what was found

### 1.1 Every JAX-RS resource in the repo has no identity/permission check

All three resource classes live in `forwardmeasure-data-streaming-launcher-jaxrs/src/main/java/.../jaxrs/`:

| Class | Path | Methods | Authorization call before business logic? |
|---|---|---|---|
| `IngestionRunResource.java` | `/ingestion-runs` | `POST /` (launch), `GET /{correlationId}` (observe), `POST /{correlationId}/cancel` | **No.** Constructor takes `DirectIngestionLauncher` + `KubernetesClient` directly (~L68–71); each method calls the launcher immediately (~L77, ~L92–95, ~L103). No `@Context SecurityContext`, no `@RolesAllowed`, no identity check anywhere. |
| `CorrelationRunResource.java` | `/correlation-runs` | same shape, over `DirectCorrelationLauncher` | **No.** Identical structure (~L48–91). |
| `WorkflowRunResource.java` | `/workflow-runs` | `POST /` (launch via fowf), `GET /{executionId}`, `POST /{executionId}/cancel` | **No.** Only validates the `If-Match` header and `correlationId` presence (~L87–92) — that's optimistic-concurrency request validation, not caller authorization. |

No other `@Path` classes exist anywhere in the repo (confirmed by a repo-wide grep for `@Path` across all non-`target` `.java` files).

### 1.2 There is no authorization code anywhere — and no authentication either

Repo-wide grep for `@RolesAllowed`, `@Secured`, `SecurityContext`, `hasRole`, `AccessControl`, `PDP`, `AuthZ`, `authzen`, `PermissionCheck`, and `AuthorizationService` returned **zero hits**, in both main and test source.

The only "authoriz*"-named code that does exist is **not caller authorization**:

- `IngestionJobPolicy.authorizeNamespace(...)` / `.authorizeImage(...)` (`IngestionJobPolicy.java:33,35,46,54`), called from `EnvConfiguredJobLauncher.launch(...)` (`EnvConfiguredJobLauncher.java:88-89`). This checks whether the **request payload** — a namespace string, a sha256-pinned image reference — is on a static allowlist. It never consults a principal, role, tenant, or any notion of *who is calling*. On failure it throws `SecurityException`, caught only by `SecurityExceptionMapper.java:36-43` → HTTP 403. **Keep this as defense-in-depth — it is a real, useful control — but it is not a substitute for caller authorization and must not be mistaken for one.**
- `IngestionJobPolicy.java:23-30`'s own javadoc explicitly states: *"this launcher has no multi-tenant model of its own today"* — this is a documented, acknowledged gap, not an oversight I'm inferring.

There is also **no inbound authentication**. The only JWT/Keycloak code in the repo is FDS acting as an OAuth2 *client* — `KeycloakClientCredentialsTokenSupplier.java`, wired identically in all three framework bindings (`LauncherQuarkusBinding.java:59-72`, `LauncherSpringBinding.java:63-72`, `LauncherMicronautBinding.java`) — used only to fetch a token so FDS can call the external fowf (openworkflow) API as a client. Nothing validates or extracts identity from an *incoming* request. There is no `ContainerRequestFilter`, no `@Context SecurityContext` usage, no OIDC adapter, and no security dependency (`quarkus-oidc`, `smallrye-jwt`, `spring-security-*`, `micronaut-security-*`) in any `pom.xml` in the repo.

### 1.3 No contract declares a security requirement, because there is no contract

No `api-specifications` module and no OpenAPI/AsyncAPI spec file exists anywhere in this repo (confirmed via `find` for `*api-specification*` and for any `.yaml` containing `openapi:`/`paths:` — only test fixtures and the three `application.yml` deployment-config files exist). So there's no `security:`/`securitySchemes:` block missing — there's no contract file at all in which one could live.

### 1.4 The concrete attack surface

All three of these currently run **fully open**:

- `POST /ingestion-runs` → `IngestionRunResource.java:76-84` → `DirectIngestionLauncher.launch` → `EnvConfiguredJobLauncher.launch` (`EnvConfiguredJobLauncher.java:87-110`) → `KubernetesJobLifecycle.launch` — **creates a real Kubernetes Job.**
- `POST /ingestion-runs/{id}/cancel` → `IngestionRunResource.java:98-105` — **deletes a running Job.**
- `POST /correlation-runs` / `POST /correlation-runs/{id}/cancel` → `CorrelationRunResource.java:53-64,78-85` — same shape, via `DirectCorrelationLauncher`.
- `EnvConfiguredJobLauncher` accepts a fully caller-supplied `image`/`command`/`args`/`env` (`EnvLaunchRequest`) — the *only* gate on any of this is the static namespace/sha256-pinned-image allowlist described in §1.2, never who is calling.

### 1.5 Framework bindings confirm the gap is total, not a wiring omission

All three bindings (`LauncherQuarkusBinding.java`, `LauncherSpringBinding.java`, `LauncherMicronautBinding.java`) were read in full: each is pure DI wiring of launchers, `KubernetesClient`, `ExecutionsApi`, and the three JAX-RS resources. None registers a security filter, an `AuthorizationService`-equivalent bean, or any interceptor. There is nothing to "turn on" — the mechanism doesn't exist yet.

---

## 2. Reference pattern already proven in `forwardmeasure-entity-intelligence` (fei)

fei is a sibling product in this same organization that faced and solved the exact same problem — every application service there needs to authorize a caller before acting, against a real Keycloak-backed AuthZEN policy decision point (PDP), wired identically across Quarkus, Spring, and Micronaut. This isn't a theoretical suggestion — it's real, working, verified code you can read directly. Repo path: `/home/pn/Documents/code/forwardmeasure/forwardmeasure-entity-intelligence`.

### 2.1 The shape

- **`forwardmeasure-entity-intelligence-authorization-api`** (module, framework-agnostic): defines the contract.
  - `AuthorizationService` — an interface with something like `authorize(ActiveOrganization actor, AuthorizationResource resource, AuthorizationAction action)`, throwing an unchecked exception (fail-closed) when denied.
  - `AuthorizationAction` — an enum of `"resource:verb"` action strings, one entry per privileged operation (e.g. `INGESTION_PIPELINE_TRIGGER("ingestion-pipeline:trigger")`).
  - `AuthorizationResource` — static factory methods that build the resource identifier the PDP evaluates against (e.g. `AuthorizationResource.ingestionPipeline(String id)`).
  - `ActiveOrganization` — the already-derived actor/tenant identity (id, tenant, org, roles), produced once per request by a JWT-extraction layer (see §2.3) and passed down into every application service — **never re-derived from the request inside a resource method**.

- **`forwardmeasure-entity-intelligence-authorization-authzen`** (module, real implementation): `AuthzenAuthorizationService` — a genuine, fail-closed HTTP client to a real Keycloak 26.7 AuthZEN Evaluation/Evaluations endpoint. Concretely, on every check it:
  - POSTs to the PDP with a Bearer token (via `BearerTokenSupplier`/`OAuthClientCredentialsTokenSupplier`, a client-credentials token FDS already knows how to fetch — see §1.2).
  - Treats **any** failure as deny, not allow: non-200 response, malformed JSON, a network error, or a mismatched `X-Request-ID` correlation echo (anti-spoofing on the response) all throw `AuthorizationUnavailableException` rather than silently permitting.
  - Enforces a tenant-mismatch guard: if a resource's own `tenant_id` disagrees with the caller's active organization, it's rejected before ever reaching the PDP.
  - Caches decisions with a TTL + max-entries eviction, and exposes a batch-evaluation path for multi-action checks in one round trip.
  - This class was itself ported from fowf's (`forwardmeasure-openworkflow`) own real `AuthzenAuthorizationService` — so there are now two independent, working reference implementations of the same pattern to read.

- **`-testkit` module**: `StubAuthorizationService` — a permissive `permitAll()` stand-in, used **only** in test builds.

- **Wiring, in all three top-level, product-wide framework-bindings modules** (`QuarkusAuthorizationServiceProducer`, `SpringAuthorizationBinding`, `MicronautAuthorizationServiceProducer`): each produces the real `AuthzenAuthorizationService` as the production `AuthorizationService` bean, reading config (`entityintelligence.authorization.issuer/client-id/client-secret/request-timeout/decision-ttl/maximum-cache-entries/policy-version`), gated `@UnlessBuildProfile("test")` (Quarkus) so tests automatically get the stub instead. Because this is wired **once, at the top level**, every application service that simply injects `AuthorizationService` gets real enforcement for free — no per-resource boilerplate beyond calling `authorize(...)`.

- **Caller identity extraction** (a separate, earlier layer, upstream of authorization): each framework's top-level binding also produces an `ActiveOrganizationProvider`-equivalent that derives tenant/actor/roles from a Keycloak-issued JWT on the incoming request — Quarkus via `JsonWebToken`, Spring via `SecurityContextHolder`'s `JwtAuthenticationToken`, Micronaut via `SecurityService`. **This is the piece FDS is missing entirely (§1.2)** — without it, there is no `ActiveOrganization` to pass into `authorize(...)` at all, so authentication has to be built first, authorization second.

### 2.2 Concrete files worth reading directly, in order

1. `forwardmeasure-entity-intelligence-authorization/forwardmeasure-entity-intelligence-authorization-api/.../AuthorizationService.java` — the interface.
2. `forwardmeasure-entity-intelligence-authorization/forwardmeasure-entity-intelligence-authorization-api/.../AuthorizationAction.java` and `AuthorizationResource.java` — the enum/factory shape.
3. `forwardmeasure-entity-intelligence-authorization/forwardmeasure-entity-intelligence-authorization-authzen/.../AuthzenAuthorizationService.java` — the real HTTP client (this is the biggest, most load-bearing file to port from).
4. `forwardmeasure-entity-intelligence-framework-bindings/quarkus/.../QuarkusAuthorizationServiceProducer.java` (and the Spring/Micronaut equivalents) — how it's wired as a CDI/Spring/Micronaut bean per framework.
5. Any application service that calls `authorization.authorize(...)` before its first side effect — e.g. `IngestionPipelineTriggerApplicationService.trigger(...)` — as a concrete example of the call-site pattern.

### 2.3 One important architectural distinction for FDS specifically

In fei, `AuthorizationService` is called from the **application-service layer**, not the JAX-RS resource layer — the resource just extracts identity and delegates. **FDS's `IngestionRunResource`/`CorrelationRunResource`/`WorkflowRunResource` currently have no application-service layer of their own between the JAX-RS resource and the launcher** — the resource calls the launcher directly. You have two reasonable options, and this guide isn't prescribing which:

- **(a)** Add a thin application-service layer (mirrors fei's own layering, and is more consistent with the rest of this project's architecture) that does `authorize(...)` then delegates to the existing `DirectIngestionLauncher`/`DirectCorrelationLauncher`/`WorkflowIngestionLauncher` — resources stay thin.
- **(b)** Call `authorize(...)` directly inside each resource method, before invoking the launcher — smaller, faster fix, less architectural surgery, but couples the JAX-RS layer to the authorization contract instead of keeping it in a business-logic layer.

Either way, **authorization must run before any launcher method executes**, on every one of the 9 endpoints listed in §1.1 (3 resources × 3 methods each).

---

## 3. What FDS needs to build

This is a two-part gap — authentication (who is calling) is missing as a prerequisite to authorization (are they allowed) — so both need to be built, in this order:

### 3.1 Authentication — extract caller identity from the incoming request

FDS has none of this today (§1.2). Needed, per framework, mirroring fei's `ActiveOrganizationProvider` pattern:
- Add the relevant security dependency per framework (`quarkus-oidc` or `smallrye-jwt` for Quarkus; `spring-boot-starter-oauth2-resource-server` for Spring; `micronaut-security-jwt` for Micronaut).
- Add Keycloak issuer/audience config to each deployment leaf's `application.yml`.
- Produce one shared `ActiveOrganization`-equivalent record (or reuse fei's own class/module directly if these two repos are meant to share it — confirm this with whoever owns the org-wide dependency graph before duplicating it) per framework binding, derived from the validated JWT.

### 3.2 Authorization — real, fail-closed permission checks

- Define an `AuthorizationAction` per privileged operation. Minimum set, one per method on the three resources:
  - `INGESTION_RUN_LAUNCH` (`ingestion-run:launch`), `INGESTION_RUN_READ` (`ingestion-run:read`), `INGESTION_RUN_CANCEL` (`ingestion-run:cancel`)
  - `CORRELATION_RUN_LAUNCH`, `CORRELATION_RUN_READ`, `CORRELATION_RUN_CANCEL`
  - `WORKFLOW_RUN_LAUNCH`, `WORKFLOW_RUN_READ`, `WORKFLOW_RUN_CANCEL`
- Build (or depend on, if sharing is the right call — see the note in §3.1) an `AuthorizationService` interface + a real AuthZEN-backed implementation, following §2.1–2.2 above. FDS already has the client-credentials token-fetching machinery this needs (`KeycloakClientCredentialsTokenSupplier`) — the PDP HTTP client itself is the new piece.
- Call `authorize(actor, AuthorizationResource.ingestionRun(correlationId), AuthorizationAction.INGESTION_RUN_LAUNCH)` (or the read/cancel equivalents) as the **first** thing each of the 9 methods does, per the two options in §2.3.
- Add a `StubAuthorizationService`/`permitAll()` equivalent for tests, wired only under the test profile — do not let real tests depend on a live PDP, matching fei's own precedent.

### 3.3 Do not remove or weaken `IngestionJobPolicy`

The namespace/image allowlist (§1.2) is a real, independent control — keep it exactly as-is. It protects against a different failure mode (a caller, even a legitimately authorized one, launching an unpinned or unapproved image) than caller authorization does (is this caller allowed to launch anything at all). Both are needed; neither substitutes for the other.

---

## 4. Verification checklist

Mirror fei's own no-mocks, real-testcontainer discipline:

- [ ] An unauthenticated request to any of the 9 endpoints returns 401, not 200/202.
- [ ] An authenticated-but-unauthorized request (a real JWT, wrong role/tenant) returns 403.
- [ ] An authenticated-and-authorized request still succeeds exactly as it does today (no regression on the happy path — including the existing `EnvConfiguredJobLauncherTest`'s real K3s-backed launch/observe/cancel assertions).
- [ ] A live Keycloak testcontainer round trip: mint a real signed JWT, make a real HTTP request through each framework's deployment leaf, confirm claims are extracted and the AuthZEN PDP call actually happens (not bypassed).
- [ ] `SecurityException` from `IngestionJobPolicy` (the existing allowlist) and a new authorization-denied exception both map to their correct, distinct HTTP status codes, and don't get confused with each other in the exception-mapper layer.
- [ ] Repeat the same repo-wide greps from §1.2 (`AuthorizationService`, `@RolesAllowed`/`SecurityContext` usage) — confirm they now return real hits, not zero.

---

## 5. Summary of citations

| Finding | File:line |
|---|---|
| `IngestionRunResource` no-auth constructor/methods | `IngestionRunResource.java:68-71,77,92-95,103` |
| `CorrelationRunResource` no-auth methods | `CorrelationRunResource.java:48-91` |
| `WorkflowRunResource` only validates `If-Match`/`correlationId`, not identity | `WorkflowRunResource.java:87-92` |
| Namespace/image allowlist (not caller authz) | `IngestionJobPolicy.java:33,35,46,54` |
| Allowlist invoked from launcher | `EnvConfiguredJobLauncher.java:87-110` |
| Allowlist failure → 403 mapping | `SecurityExceptionMapper.java:36-43` |
| Documented "no multi-tenant model" gap | `IngestionJobPolicy.java:23-30` |
| Outbound-only OAuth2 client (not inbound auth) | `KeycloakClientCredentialsTokenSupplier.java`; wired at `LauncherQuarkusBinding.java:59-72`, `LauncherSpringBinding.java:63-72`, `LauncherMicronautBinding.java` |
| Job-launch attack surface | `IngestionRunResource.java:76-84,98-105`; `CorrelationRunResource.java:53-64,78-85`; `EnvConfiguredJobLauncher.java:87-110` |

**Reference implementation to port from (working, verified code in a sibling repo):**
`/home/pn/Documents/code/forwardmeasure/forwardmeasure-entity-intelligence/forwardmeasure-entity-intelligence-authorization/` (both `-api` and `-authzen` modules), plus the `*AuthorizationServiceProducer`/`*AuthorizationBinding` classes in `forwardmeasure-entity-intelligence-framework-bindings/{quarkus,spring,micronaut}/`.
