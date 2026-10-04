# FDS launcher: RFC 9457 errors and security parity (2026-10-01)

**Status:** source changes only. Nothing is built, tested or committed. The user runs every Maven
build. The commands are in
`forwardmeasure-openworkflow/docs/request-contract-and-api-security-parity-handoff-2026-10-01.md`,
§4b "Build and test commands".

**Why:** every forwardmeasure service must answer identically on Quarkus, Spring and Micronaut.
fowf and fei already return RFC 9457 `Problem` bodies through the shared library in
forwardmeasure-platform. FDS answered with its own `ErrorResponse {message}`, and its 401s differed
per framework:

- Spring and Quarkus sent an empty body.
- Micronaut sent its own JSON error format.
- On Micronaut, an invalid token sent to health got through.

The user approved moving FDS onto the shared library on 2026-10-01. This changes FDS's error
contract.

## What changed

- **Error body.** Every error is now a `Problem` (`application/problem+json`): `type`, `title`,
  `status`, `detail`, plus `violations` for a request that breaks the contract. `ErrorResponse` is
  deleted.
  - The seven launcher mappers build it through `LauncherProblems`, on top of
    forwardmeasure-platform's `RequestProblems`.
  - A record refusing a null argument (`Objects.requireNonNull(spec, "spec")`) is reported as a
    `REQUIRED` violation on that field. This holds both in the shared JSON-binding mapper and in
    `NullPointerExceptionMapper`.
  - Any other `NullPointerException` is a server bug, so it now gets the shared 500 `Problem`. The
    old mapper answered every NPE with a 400.
- **Unknown run.** `GET /ingestion-runs/{id}` for a run that doesn't exist now throws
  `NotFoundException` (a 404 `Problem` with a detail) instead of an empty-bodied 404.
- **Dependencies:**
  - `forwardmeasure-data-streaming-launcher-jaxrs` → `forwardmeasure-platform-server-jaxrs` and
    `openworkflow-common-models` (managed in the root pom with `${openworkflow.version}`).
  - Spring binding → `forwardmeasure-platform-spring-security`. It registers
    `RequestProblemsFeature`, and the chain uses the Problem 401/403 handlers.
  - Quarkus binding → `forwardmeasure-platform-quarkus-security`. Its `ProblemChallengeMechanism`
    writes every HTTP-layer 401.
  - Micronaut binding → `forwardmeasure-platform-micronaut-validation` plus
    `micronaut-validation-processor`; Serde imports for `Problem`/`Violation` replace
    `ErrorResponse`'s.
- **Config:**
  - Micronaut: `serde.serialization.inclusion: ALWAYS` and `reject-not-found: false`. Its
    `intercept-url-map` now ends with `/health` and `/health/**` → `isAnonymous()`, then `/**` →
    `isAuthenticated()`.
  - Quarkus: authenticated by default (`/*`), health open.
  - Spring: health permitted; actuator added.
  - Micronaut: `micronaut-management`, health only.

## One answer per request, every framework

| Request | Spring, Quarkus, Micronaut |
|---|---|
| No token, any protected path (unknown ones too) | 401 `Problem` + `WWW-Authenticate: Bearer` |
| Invalid token, any path (health too) | 401 `Problem` + `Bearer` |
| Valid token, unknown path | 404 `Problem` |
| `GET /ingestion-runs/{id}` without `namespace` | 400 `Problem`, detail `the 'namespace' query parameter is required` |
| No token, health | 200 |

## Tests

- Updated:
  - The four mapper unit tests now assert the `Problem`.
  - `IngestionRunResourceTest` now expects `NotFoundException` for a missing run.
- Added: five checks per framework in `DirectIngestionMatrix{Spring,Quarkus,Micronaut}KafkaStreamsSmokeTest`, one for each row above except the plain no-token row:
  - `unknownPath_unauthenticatedIsRejected`
  - `unknownPath_authenticatedIsNotFound`
  - `health_isOpenWithoutAToken`
  - `health_invalidTokenIsRejected`
  - `aMissingNamespaceIsABadRequestProblem`
