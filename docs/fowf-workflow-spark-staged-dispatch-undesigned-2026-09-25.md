# fowf workflow-mode has never dispatched a Spark-staged spec — handover (2026-09-25)

Written for whoever on the fowf side looks at extending workflow-triggered ingestion to cover
Spark-staged specs. This is a real, open design gap, not a bug — nothing is broken, because nothing
exists yet. Every claim below is grounded in a direct read of this repo's own current code.

## What's proven today (direct-REST mode only)

`DirectIngestionLauncher` (`forwardmeasure-data-streaming-launcher-application`) now dispatches a
real, two-Job Spark-then-delivery pipeline end to end, live-verified against real K3s/Kafka/
OpenSearch: a spec whose compiled `ExecutionPlan` carries a `sparkStage` (because at least one
transform is classified `HEAVY` — see `ExecutionPlanCompiler.resolveSparkStage`) dispatches a Spark
Job first, which maps and screens every row and writes it to a real Kafka handoff topic; a second,
independently-dispatched delivery Job then reads that topic and writes the final rows to the real
destination. This is genuinely proven now (`DirectIngestionLauncherSparkTwoJobRealImageIntegrationTest`),
including catching and fixing two real bugs along the way (a handoff-topic-name derivation race
between the launcher and the Spark pod, and a silent stringification bug in the delivery-side
`raw`-field passthrough).

**The entire two-Job sequencing mechanism lives inside `DirectIngestionLauncher`'s own process,
with no fowf involvement at all**:

1. `launch()` compiles the spec via `ExecutionPlanCompiler`, sees `plan.sparkStage()` is present,
   and only ever launches the Spark Job (`launchSparkStage`). Before doing so, it computes the
   delivery-stage's own derived `IngestionSpec` via `SparkHandoffSpecs.deliveryStageSpec(plan,
   kafkaBootstrapServers)` — one identity (`raw: true`) field rule per distinct target field name
   across the original spec's own mappers, sourced from the handoff topic — and stores that
   derived spec's YAML in a small Kubernetes `ConfigMap` (`fds-handoff-<hash>`), keyed by the same
   correlation id. It also passes the already-computed handoff topic name to the Spark pod
   explicitly via a `SPARK_HANDOFF_TOPIC` env var (today's own bug fix — see
   `SparkIngestionRunner#withHandoffTopicOverride`'s javadoc for why: letting the pod re-derive the
   topic name independently from a YAML-round-tripped spec is not safe).
2. `observe()` is what actually advances the pipeline: while the Spark Job hasn't reached
   `SUCCEEDED` yet, its own observation *is* the result. Once it has, the *next* `observe()` call
   reads the ConfigMap, parses the derived spec back out, compiles it, and launches the delivery Job
   for the first time (idempotently) — all lazily, with zero background threads/watchers, entirely
   driven by whoever polls `observe()` next.

## Why fowf workflow-mode can't just reuse this

`WorkflowIngestionLauncher` (same module) is a thin, deliberately dumb wrapper around fowf's own
generated `ExecutionsApi` client — `launch()` calls `POST /v1/executions` with a `revisionId`+
`input`, `observe()` calls `GET /v1/executions/{id}`. It has zero knowledge of Kubernetes, Spark, or
Kafka at all — every real decision (which image to run, how to sequence multiple Jobs, where the
handoff topic is) lives inside whichever *workflow definition* fowf executes, not in this launcher.
Confirmed directly: nothing in this class or its own test suite references `SparkStagePlan`,
`SparkHandoffSpecs`, or anything Spark-related.

This means the entire two-Job sequencing mechanism above — the ConfigMap handoff, the
lazy-reconciliation-via-observe() pattern — simply doesn't exist on the workflow-mode path, and
can't be reused as-is: there is no FDS launcher process running continuously that a workflow
execution could delegate lazy reconciliation to. A workflow-mode dispatch would need the sequencing
expressed *inside the workflow definition itself*, the same way `RealFowfWorkflowBoundedIngestionTest`
already expresses a single-Job dispatch as one `correlated-worker`/`kubernetes-job` step, and the
continuous-mode end-to-end test expresses a two-step apply+watch sequence as two chained
`asyncapi`/`kubernetes-deployment` steps.

## The real open question: who computes the derived delivery spec?

Chaining two `correlated-worker`/`kubernetes-job` steps (Spark, then delivery) is the mechanically
obvious shape, mirroring the already-proven two-step `asyncapi` pattern. But it doesn't close the
gap by itself, because of one thing neither step can do on its own: **something has to compute the
derived, identity-field delivery-stage `IngestionSpec`** (today, `SparkHandoffSpecs.deliveryStageSpec`'s
job) **before the second step's own payload can be assembled** — that derivation needs the original
spec's *compiled* `ExecutionPlan` (which fields exist, which target names are duplicated across
sources), not something a workflow author can hand-write reliably or reproduce with a jq expression
over the raw spec YAML.

On the direct-REST path, this derivation happens for free because `DirectIngestionLauncher` is a
real JVM process already running FDS's own code at dispatch time. A pure fowf workflow definition
has no equivalent — fowf's engine evaluates jq expressions and dispatches payloads, it doesn't run
arbitrary Java. Two directions worth thinking through, genuinely unresolved by this session:

1. **FDS exposes the derivation as a real, callable operation** (e.g., a small stateless HTTP
   endpoint, or a value computed once and stashed somewhere at workflow-*definition*-authoring time)
   that a first workflow step could call to get back the derived spec YAML, which then flows into
   the second step's own payload the same way `RealFowfWorkflowBoundedIngestionTest`'s workflow
   passes a base64-encoded spec into its own single step today. This is new integration surface on
   both sides — nothing like it exists yet.
2. **The workflow author hand-authors the derived spec** for each Spark-staged workflow definition,
   accepting that it can silently drift out of sync with the original spec's own mapper if either
   one changes later. Fragile, but zero new integration surface — the same trade-off already made,
   knowingly, for the continuous-mode `payload.name`-in-both-steps correlation pattern (see
   `kubernetes-deployment-two-step-engine-stall-2026-09-21.md`'s own resolution).

Neither direction has been chosen. This is the actual design work — the two-step *shape* is obvious
by analogy to what already exists; computing the second step's own payload without a running FDS
process to do it is the open problem.

## Pointers into this repo for context

- `forwardmeasure-data-streaming-core/.../SparkHandoffSpecs.java` — the derivation logic itself.
- `forwardmeasure-data-streaming-launcher-application/.../DirectIngestionLauncher.java` — the real,
  proven two-Job sequencing (`launchSparkStage`/`observe`/`reconcileDeliveryJob`), for the shape a
  workflow-mode equivalent would need to reproduce declaratively.
- `forwardmeasure-data-streaming-launcher-application/src/test/java/.../fowf/RealFowfWorkflowBoundedIngestionTest.java`
  — today's real, single-Job `correlated-worker`/`kubernetes-job` workflow pattern, the nearest
  existing precedent for a first Spark-stage step.
- `forwardmeasure-data-streaming-launcher-application/src/test/java/.../DirectIngestionLauncherSparkTwoJobRealImageIntegrationTest.java`
  — today's real end-to-end proof of the direct-REST path this handover describes.
