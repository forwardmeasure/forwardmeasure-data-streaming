# Handover from the fowf-side session (2026-09-17)

Written for the Claude session working on FDS's continuous-streaming plan
(`~/.claude/plans/shimmying-tickling-mango.md`). Two independent topics: a real compile break in
this repo (unrelated to the streaming plan, but blocking `mvn test` right now), and the
`kubernetes-deployment` capability your Phase 2 bootstrap needs.

## Part 1 — Fix the stale 4-arg `ActiveOrganization` constructor (8 files, this repo)

`forwardmeasure-authzen-api`'s `ActiveOrganization` record picked up a new field, `tenantDatabase`.
Confirmed via `javap` on the real installed jar (`~/.m2/repository/com/forwardmeasure/authzen/
forwardmeasure-authzen-api/1.0.0/forwardmeasure-authzen-api-1.0.0.jar`) — there is only one
constructor, no legacy overload:

```java
public ActiveOrganization(
    TenantId tenantId,
    TenantDatabase tenantDatabase,
    String organizationId,
    String actorId,
    Set<String> organizationRoles)
```

This repo has 8 test files still calling the old 4-arg shape (`TenantId, String, String, Set`) —
confirmed via `grep -rn "new ActiveOrganization(" --include="*.java" . | grep -v /target/` and
reading each block directly, not just counting grep hits (grep alone can't tell 4-arg from 5-arg).
All 8 use the identical literal pattern, no shared fixture constant to update — each file is an
independent fix:

```
forwardmeasure-data-streaming-launcher-application/src/test/java/.../DirectIngestionLauncherTest.java
forwardmeasure-data-streaming-launcher-application/src/test/java/.../DirectCorrelationLauncherTest.java
forwardmeasure-data-streaming-launcher-application/src/test/java/.../WorkflowIngestionLauncherTest.java
forwardmeasure-data-streaming-launcher-application/src/test/java/.../DirectCorrelationLauncherRealImageIntegrationTest.java
forwardmeasure-data-streaming-launcher-application/src/test/java/.../DirectIngestionLauncherRealImageIntegrationTest.java
forwardmeasure-data-streaming-launcher-jaxrs/src/test/java/.../IngestionRunResourceTest.java
forwardmeasure-data-streaming-launcher-jaxrs/src/test/java/.../CorrelationRunResourceTest.java
forwardmeasure-data-streaming-launcher-jaxrs/src/test/java/.../WorkflowRunResourceTest.java
```

Current (broken) shape in each, e.g. `DirectIngestionLauncherTest.java:64-68`:

```java
new ActiveOrganization(
    new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
    "org-1",
    "actor-1",
    Set.of("reviewer"));
```

Fix — insert a `TenantDatabase` as the 2nd positional arg. **These are test files with no real
tenant registry available, so a literal `TenantDatabase.forAlias(...)` is the right fix here** (not
a `TenantDatabaseResolver`/registry lookup — that's only needed in production code resolving a real
tenant's real database; see `com.forwardmeasure.jpa.tenancy.TenantDatabase`'s own javadoc). The
alias regex is `[a-z][a-z0-9_-]{0,47}` (`TenantDatabase.java:49`) — `"test-tenant"` or similar is
fine, doesn't need to be unique across files:

```java
new ActiveOrganization(
    new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
    TenantDatabase.forAlias("test-tenant"),
    "org-1",
    "actor-1",
    Set.of("reviewer"));
```

`import com.forwardmeasure.jpa.tenancy.TenantDatabase;` — no new Maven dependency needed. This
repo's root `pom.xml` and `forwardmeasure-data-streaming-connector-jdbc/pom.xml` already depend on
`forwardmeasure-jpa-tenancy` (confirmed), and since all 8 files already import `TenantId` from the
same module, `TenantDatabase` is already on their classpath.

**A real gotcha that cost real time on the fowf side, apply it here too**: Maven's incremental
compiler can report a module as "compiling clean" off a stale `target/classes` directory even after
its source changed underneath it — `openworkflow-operation-adapter-core` looked fine on a plain
`mvn compile`, then failed immediately once `target/` was deleted first. **Always `rm -rf target`
before trusting a "compiles clean" result** while fixing these 8 files.

Verification, per touched module:
```
rm -rf forwardmeasure-data-streaming-launcher-application/target && \
  mvn -o test-compile -pl forwardmeasure-data-streaming-launcher-application
rm -rf forwardmeasure-data-streaming-launcher-jaxrs/target && \
  mvn -o test-compile -pl forwardmeasure-data-streaming-launcher-jaxrs
```
Then a repo-wide re-grep for `new ActiveOrganization(` to confirm every remaining call site is now
5-arg (a stray 4-arg call will fail test-compile, so a clean test-compile is real proof, not just
the grep).

## Part 2 — The `kubernetes-deployment` capability, and how to wire your Phase 2 bootstrap into it

This is the fowf-side gap you flagged (no existing call could provision a long-running K8s
`Deployment`). It's built, in `forwardmeasure-openworkflow`, **currently uncommitted** — you'll need
it committed (or to coordinate directly) before treating it as a real dependency. It compiles clean
from a truly deleted `target/`, independently re-verified.

**Design, in one line**: a plain `call: asyncapi` PUBLISH-mode step against a new
`kubernetes-deployment` protocol — not a new reserved `CallPlan.Kind`/function constant. Reason:
`HUMAN_TASK_FUNCTION`/`CORRELATED_WORKER_FUNCTION` aren't generic `FUNCTION`-kind calls at runtime —
`OpenWorkflowCompiler.CALL_VARIANT_TO_KIND` promotes only those two literals to dedicated
`CallPlan.Kind`s at compile time; anything else resolves to `Kind.FUNCTION`, which gets **inlined as
child PlanSteps at compile time**, never dispatched at runtime at all. A Deployment-provisioning
call is architecturally just "apply and return once accepted" (no terminal state to wait for, unlike
a Job) — exactly what a plain ASYNC_API PUBLISH call already supports today, with zero compiler/DSL
changes.

**What's built:**
- `openworkflow-kubernetes-deployment-lifecycle` — `KubernetesDeploymentLifecycle.apply()`/`delete()`/
  `deterministicName()` via real fabric8 Server-Side Apply. No watch/terminal-state machinery
  (Deployments have no terminal condition).
- `openworkflow-operation-adapter-kubernetes-deployment` — `KubernetesDeploymentPolicy` (deny-by-default,
  sha256-digest-pinned image allowlist, tenant namespace/replica-ceiling allowlists) +
  `AsyncApiKubernetesDeploymentOperationExecutor` (parses the PUBLISH payload, applies the
  Deployment, reports one `terminal=true` ACCEPTED observation).
- One new registration in `KafkaProtocolOperationExecutors.create(...)`
  (`openworkflow-operation-adapter-kafka`) under `DriverKey(Kind.ASYNC_API, "kubernetes-deployment")` —
  shared by **both** Pekko and Kafka-Streams engines automatically via the same factory.
- Live-verified: 8/8 against real K3s (apply/idempotent-reapply-updates-in-place/delete, policy
  rejection), plus a real compiler→materializer→executor→real-Deployment dispatch test on both
  engine seams.

**Update, same day**: the executor also now supports watching the applied Deployment until it's
actually `Available` (real readiness — pod started, not just "API server accepted the YAML"), fixing
a real gap you'd have hit immediately: the original PUBLISH-only version reported success the
instant Kubernetes *accepted the manifest*, with zero visibility into whether the pod ever came up.
See `docs/kubernetes-deployment-readiness-monitoring-gap-2026-09-17.md` in fowf for the full
investigation. **This changes your bootstrap from 1 step to 2** — read the second example below
before you build Phase 2's WorkflowPlan.

**Exact `PlanStep` shape your Phase 2 bootstrap writes** (real, working examples, from
`RealAsyncApiKubernetesDeploymentDispatchTest.java` and `RealKubernetesDeploymentOperationExecutorTest.java`):

Step 1 — apply (unchanged from before):
```yaml
do:
  - applyStreamWorker:
      call: asyncapi
      with:
        document:
          endpoint: https://specs.forwardmeasure.com/data-streaming/asyncapi/stream-worker-kubernetes-deployment.yaml
        channel: stream-worker.commands
        message:
          payload:
            namespace: my-namespace
            image: my-registry/stream-worker@sha256:...
            replicas: 2
            command: ["sh", "-c", "..."]
```

Step 2 — **new**: wait for it to actually become ready, bounded by a real timeout:
```yaml
  - awaitStreamWorkerReady:
      call: asyncapi
      with:
        document:
          endpoint: https://specs.forwardmeasure.com/data-streaming/asyncapi/stream-worker-kubernetes-deployment.yaml
        channel: stream-worker.commands
        subscription: {}
        message:
          payload:
            namespace: my-namespace
            name: ${.applyStreamWorker.output.name}
            readinessTimeoutSeconds: 120
```

**Read this carefully — it's not optional, and it's not like `correlated-worker`**: `correlated-worker`
gets its command/events/cancellation legs auto-correlated by one shared `lifecycleId`
(`ProtocolOperationMaterializer.materializeCorrelatedWorker`). A plain `call: asyncapi` step has no
such magic — the PUBLISH step above and the SUBSCRIBE step above are two **independent** compiled
`PlanStep`s with unrelated, independently-assigned `operationId`s. The watch step genuinely cannot
figure out which Deployment to watch on its own. **You must pass `name` explicitly in step 2's own
payload**, sourced from step 1's own reported `output.name` via ordinary step-to-step data flow (the
`${.applyStreamWorker.output.name}` reference above — adjust to whatever JQ/data-flow syntax your
actual workflow definition uses for referencing a prior step's output; confirm the exact syntax
against a real compiled example in this repo rather than assuming the placeholder above is exact).

**The AsyncAPI document at that `endpoint`** — fowf has no `src/main/resources` convention for
protocol-adapter AsyncAPI documents (confirmed: `openworkflow-operation-adapter-kubernetes-job` has
none either; even its own real dispatch test embeds the document as a literal string). You'll need
to host a real one — this is the exact fixture shape to mirror, **now with a `subscribe` channel
too** (updated from the original single-channel version — the "nothing to watch" reasoning no longer
holds now that readiness-watching is real):

```yaml
asyncapi: 2.6.0
info:
  title: Stream Worker Deployment (kubernetes-deployment)
  version: 1.0.0
servers:
  cluster:
    url: k8s://in-cluster
    protocol: kubernetes-deployment
channels:
  stream-worker.commands:
    servers: [cluster]
    publish:
      message: {name: ApplyStreamWorkerDeployment}
    subscribe:
      message: {name: StreamWorkerReadinessEvent}
```

**Payload fields the executor actually parses**, exact field names, all under `message.payload`:

*PUBLISH* (`AsyncApiKubernetesDeploymentOperationExecutor.apply(...)`):

| Field | Required | Type | Notes |
|---|---|---|---|
| `namespace` | yes | string | must pass `KubernetesDeploymentPolicy.authorizeNamespace` |
| `image` | yes | string | must pass `authorizeImage` — digest-pinned allowlist, plan for `image@sha256:...` |
| `replicas` | no (default 1) | int | must pass `authorizeScale` |
| `command` | no | string[] | |
| `args` | no | string[] | |
| `env` | no | object (string→string) | |
| `resources.requests` | no | object (string→string) | |
| `resources.limits` | no | object (string→string) | |
| `ports` | no | int[] | container ports |
| `imagePullSecretNames` | no | string[] | |

*SUBSCRIBE* (`AsyncApiKubernetesDeploymentOperationExecutor.watch(...)`, **new**):

| Field | Required | Type | Notes |
|---|---|---|---|
| `namespace` | yes | string | same authorization check as PUBLISH |
| `name` | yes | string | **not derived automatically** — see above, must come from step 1's own output |
| `readinessTimeoutSeconds` | no (default 300) | long | bounded wait; reports a real `FAILED` terminal if exceeded, not a hang |

The Deployment's own K8s resource name is **not** something you pass in the PUBLISH step — it's
derived deterministically from that step's own compiler-assigned `operationId` via
`KubernetesDeploymentLifecycle.deterministicName(...)`, so re-dispatching the same PUBLISH step (e.g.
on recovery) always targets the same Deployment object (`apply` is create-or-replace, not one-shot
create). The SUBSCRIBE step's `name`, by contrast, must be passed explicitly — it has no
`operationId`-derived default, precisely because it's a different step with a different `operationId`.

**Response, PUBLISH** (the `ACCEPTED` observation's `output`, available to downstream steps via
`outputAs`/`.`): `namespace`, `name`, `uid`, `resourceVersion`, `generation` — the real applied
Deployment's metadata, read directly off the fabric8 object, not echoed input.

**Response, SUBSCRIBE (new)**: reports `PROGRESS` events on change (`readyReplicas`,
`desiredReplicas`, `phase`: `pending`/`available`, `unavailableReason` when set) while waiting, then
exactly one terminal — either `AVAILABLE` with `output.{readyReplicas,desiredReplicas}`, or `FAILED`
with `error.{type,title,detail}` if the timeout elapses first or the Deployment disappears. A bad
image, a missing `imagePullSecret`, or an unschedulable resource request now surfaces as a real
`FAILED` on this step instead of the bootstrap silently reporting `COMPLETED` regardless.

**Three things to check before you build against this:**
1. Uncommitted in fowf right now — coordinate before depending on a real artifact.
2. Tenant authorization config needs real values before this works in a live deployment: the
   3 operation-adapter framework bindings (Quarkus/Spring/Micronaut) expose
   `openworkflow.operations.kubernetes-deployment.{namespace,image,max-replicas}-allowlist` — all
   deny-by-default (empty allowlist), matching `kubernetes-job`'s existing security posture. Nothing
   will actually apply until these are populated for FDS's tenant/namespace/image.
3. **Testing "never becomes ready" yourself?** Don't use a fast-crash-loop container (e.g. a command
   that just `exit 1`s) — Kubernetes' `Available` condition only requires `readyReplicas >= desired`
   for `minReadySeconds` (default `0`), so a container that's briefly `Ready` between restarts flips
   `Available` `True` even while crash-looping. Found this the hard way building the executor's own
   timeout test; use a genuinely unschedulable pod instead (an absurd `resources.requests.memory`
   works well) if you need a real "this never comes up" test case.
