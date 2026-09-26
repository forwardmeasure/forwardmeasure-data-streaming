> **Retraction (2026-09-25), by the same author who wrote the analysis below**: the central claim in
> this document - that `DirectIngestionLauncher` can only ever run `PekkoIngestionRunner` *or*
> `PekkoCorrelationRunner`, never select between them - is **wrong**, confirmed by direct evidence,
> not superseded by later work. `forwardmeasure-data-streaming-executor-pekko/pom.xml:177` pins the
> packaged image's real `mainClass` to `com.forwardmeasure.datastreaming.executor.pekko
> .PekkoStreamsDeliveryEngine` - `git diff` against `d55c272` (the exact commit this analysis was
> done against) shows that line has been unchanged all along, so this isn't something that got fixed
> since; it was true when the analysis below was written and the analysis missed it.
> `PekkoStreamsDeliveryEngine.execute(...)` already branched on `spec.sources().size() == 1` and
> delegated to the correct one of `PekkoIngestionRunner`/`PekkoCorrelationRunner` *inside the pod*,
> driven by the real compiled plan, in that same commit - `PekkoIngestionRunner.singleSource`/
> `PekkoCorrelationRunner.run`'s own guards (quoted below) are internal safety checks on the
> delegate, not the top-level dispatch contract this document wrongly assumed them to be.
>
> **Root cause of the error**: the original research grepped for `main(String[] args)` across the
> Pekko executor module and got three hits back (`PekkoIngestionRunner`, `PekkoCorrelationRunner`,
> `PekkoStreamsDeliveryEngine`) - then only opened the first two before writing the "gap" analysis
> below, never opening `PekkoStreamsDeliveryEngine.java` itself or checking the packaged image's
> actual `pom.xml` `mainClass` to see which of the three is the real, deployed entrypoint. Concluding
> a launcher's dispatch behavior from which classes merely *have* a `main()` method, without checking
> which one the packaging config actually wires up, is exactly the kind of gap this org's own
> "confidence without verification" standard exists to catch - noted here so it isn't repeated.
>
> **Do not build this document's proposed fix** (a second Pekko image/command pair + cardinality
> branching added to `DirectIngestionLauncher`) - it would reintroduce split-image complexity for
> something the single image, dispatching through `PekkoStreamsDeliveryEngine`, already does
> correctly. The one finding below that does survive, independent of the wrong premise: **no
> real-image K8s-dispatch test proves this end-to-end for a correlated spec today.**
> `DirectIngestionLauncherRealImageIntegrationTest` only exercises a single-source spec; the prior
> test that covered the correlated case (`DirectCorrelationLauncherRealImageIntegrationTest`) was
> deleted, not replaced, during the 2026-09-21 collapse - that part of the diagnosis holds. See the
> corrected recommendation at the bottom of this document.

# `DirectIngestionLauncher` correlated-dispatch: real gap was a missing test, not a missing feature — handover (2026-09-25)

Written for whoever works on `DirectIngestionLauncher`/correlation dispatch in this repo next.
Surfaced from a FEI-side session reviewing FDS's new unified `IngestionSpec` to plan FEI's own
ingestion-flow rebuild against it — not something anyone here had flagged as an open item, so
there's a real chance this has been silently assumed "done" (the correlation *engines* are real and
tested, which may be why). It isn't done at the launcher layer. Every claim below is grounded in a
direct read of this repo's own current code, not inferred from comments or class names.

## TL;DR

- `PekkoCorrelationRunner`/`PekkoCorrelationEngine` and `SparkCorrelationRunner`/
  `SparkCorrelationEngine` are real, and each has a real, passing integration test that correlates
  two sources and writes the merged result. The correlation *logic* is not in question.
- `DirectIngestionLauncher` — the launcher that runs a spec as one real Kubernetes Job with no fowf
  involved — **cannot actually dispatch a correlated (`sources.size() > 1`) spec to either engine**.
  It only ever launches one configured Pekko image/command and one configured Kafka Streams image/
  command, chosen by `DeliveryEngineKind` alone; there is no branch anywhere on
  `spec.sourceCardinality()`. Whichever single-source/correlated shape the deployment's
  `pekkoRunnerCommand` happens to be pointed at is the only one that works — a spec of the other
  shape crashes immediately at launch (`IllegalArgumentException` from either `PekkoIngestionRunner
  .singleSource` or `PekkoCorrelationRunner.run`'s own guard).
- This isn't a live-hit bug because nothing has yet driven a correlated spec through
  `DirectIngestionLauncher` for real. But it also isn't proven safe: the previous real-image test
  that *would* have caught this, `DirectCorrelationLauncherRealImageIntegrationTest`, was deleted
  outright (not merged) during the 2026-09-21 `DirectIngestionLauncher`/`DirectCorrelationLauncher`
  collapse. No test anywhere exercises a correlated spec through this launcher today.
- Kafka Streams has no correlation runner at all — not a launcher-wiring gap, a real, documented,
  intentional deferral at the engine level itself (`KafkaStreamsDeliveryEngine`'s own javadoc says
  so). Not urgent: nothing in `ExecutionPlanCompiler`'s current transform-characteristics table
  would ever route a correlated spec there anyway (every registered transform is `LIGHT`/
  `STATELESS`).
- Separately, in case this is *also* a source of confusion: `KafkaStreamsDeliveryEngine` **is** a
  real, tested single-source Kafka Streams runner — it's just not named `KafkaStreamsIngestionRunner`
  the way the Pekko equivalent is `PekkoIngestionRunner`, which makes it easy to overlook. It's not
  a gap; it just doesn't read as a sibling of `PekkoIngestionRunner` at a glance.

## What's real and proven (the engines)

- `PekkoCorrelationRunner.run(IngestionSpec, ActorSystem)` (`forwardmeasure-data-streaming-executor-
  pekko`): reads and maps every declared source in parallel, correlates on `spec.blockingField()`,
  merges each group by `trustWeight` (`PekkoCorrelationEngine.correlate`), writes to any Camel-
  supported sink via `PekkoIngestionRunner.buildSink` (shared, not reimplemented). Has its own real
  `main()` (env var `CORRELATION_SPEC_PATH`, or positional arg). Proven by
  `PekkoCorrelationRunnerIntegrationTest.runCorrelatesTwoSourcesAndWritesTheMergedResult` and
  `PekkoCorrelationEngineIntegrationTest` — real, no mocks.
- `SparkCorrelationRunner`/`SparkCorrelationEngine` (`forwardmeasure-data-streaming-executor-spark`):
  the same capability on Spark, generalized 2026-09-13 to the same sink set `SparkIngestionRunner`
  supports (file/kafka/jdbc/opensearch/any Spark-native format via `SparkSinks#write`) — no longer
  file-sink-only. Proven by `SparkCorrelationRunnerIntegrationTest`'s identically-named test.
- Both consume the exact same unified `IngestionSpec` single-source specs do (`sources.size() > 1`
  is the only distinguishing signal — no separate `CorrelationSpec` type exists any more).

## What's missing (the launcher)

`DirectIngestionLauncher.launch(KubernetesClient, DirectLaunchRequest, ActiveOrganization)`
(`forwardmeasure-data-streaming-launcher-application`):

```java
ExecutionPlan plan = ExecutionPlanCompiler.compile(spec);
...
String image;
String command;
if (plan.profile().deliveryEngine() == DeliveryEngineKind.KAFKA_STREAMS) {
  image = kafkaStreamsRunnerImage;
  command = kafkaStreamsRunnerCommand;
} else {
  image = pekkoRunnerImage;
  command = pekkoRunnerCommand;
}
```

The constructor takes exactly one `pekkoRunnerImage`/`pekkoRunnerCommand` pair and one
`kafkaStreamsRunnerImage`/`kafkaStreamsRunnerCommand` pair — nothing here, or anywhere else in this
class, inspects `spec.sourceCardinality()` or `spec.sources().size()`. For the Pekko branch, that
one configured command has to be either `PekkoIngestionRunner`'s entrypoint or
`PekkoCorrelationRunner`'s — never both. Confirmed via both mains' own guards:

- `PekkoIngestionRunner.singleSource(spec)`: `throw new IllegalArgumentException("expected exactly
  one source, got " + spec.sources().size() + " - use PekkoCorrelationRunner for a correlated
  spec")` when `sources.size() != 1`.
- `PekkoCorrelationRunner.run(spec, system)`: the exact inverse guard, `throw new
  IllegalArgumentException("expected more than one source, got " + spec.sources().size() + " - use
  PekkoIngestionRunner for a single-source spec")` when `sources.size() <= 1`.

So today, whichever of the two the deployment's `pekkoRunnerCommand` is actually configured to run —
and the one real-image-tested proof that exists, `DirectIngestionLauncherRealImageIntegrationTest`,
only exercises the single-source `PekkoIngestionRunner` path — a spec of the *other* cardinality
dispatched through `DirectIngestionLauncher` fails immediately at pod startup with one of the two
messages above, not at spec-compile or authorization time.

**The test that would have caught this was removed, not replaced.** `DirectIngestionLauncherRealImageIntegrationTest`'s own javadoc says so directly:

> **2026-09-21**: this class's former Spark sibling (`DirectCorrelationLauncherRealImageIntegrationTest`, which pulled the real `data-streaming-executor-spark` image and ran `SparkCorrelationRunner.main()` directly) was removed, not merged in — `DirectIngestionLauncher` no longer accepts a raw Spark image at all now that Spark is an optional compute stage, never a delivery engine... A real second image proof belongs here once a real, pushed Kafka Streams executor image exists... not before, since there is nothing real yet to point it at.

That reasoning is correct for *why the Spark-direct test specifically* was removed (Spark genuinely
stopped being a delivery engine in the unified model — `DirectIngestionLauncher` explicitly rejects
any plan whose `sparkStage` is present, by design, since no real `HEAVY` transform exists yet to
trigger one). But removing that test also removed the **only** real-image coverage of correlated
dispatch through this launcher, for any engine — Pekko included. That part looks like it wasn't a
deliberate decision, just a side effect of the Spark-specific cleanup.

I did not check whether the fowf-workflow path (`WorkflowIngestionLauncher`) has the equivalent gap
— it doesn't select a runner image itself (a published fowf workflow definition does), so it isn't
broken the *same* way, but there's no workflow-definition YAML checked into this repo to confirm
whether a published definition correctly targets `PekkoCorrelationRunner` for a correlated spec
either. Worth checking before assuming that path is clean.

## Kafka Streams: no correlation runner, but that's a documented, deliberate deferral

`KafkaStreamsDeliveryEngine.execute(ExecutionPlan, ExecutionMode)` rejects `plan.sources().size() !=
1` outright, and there is no `KafkaStreamsCorrelationRunner`-equivalent class anywhere in
`forwardmeasure-data-streaming-executor-kafka-streams` (confirmed — that module has exactly 6 source
files: `StageRecordSerde`, `KafkaStreamsExecutionHandle`, `InputFrontier`,
`BoundedKafkaConsumerRunner`, `KafkaStreamsDeliveryEngine`, and its own test). The engine's own
javadoc is explicit about this being intentional, not an oversight: *"Deliberately scoped to
single-source, no-Spark-stage plans for now... correlated/Spark-staged dispatch through this engine
is real future work, not silently unsupported."*

Not currently reachable in practice either way: `ExecutionPlanCompiler.resolveEngine` only ever
routes to `KAFKA_STREAMS` when the folded transform characteristics are `STATEFUL`, or the spec is
`BOUNDED` with a Kafka source — cardinality alone never selects it, and every transform in
`NamedTransformRegistry` today is `LIGHT`/`STATELESS`. So a correlated spec can't currently resolve
to `KAFKA_STREAMS` at all; this gap is latent, not live.

## For the record: `KafkaStreamsDeliveryEngine` is real (not a missing runner)

Flagging this explicitly since it came up as a real question worth double-checking: single-source
ingestion is **not** Pekko-only. `KafkaStreamsDeliveryEngine` has its own real `main()` (same
`INGESTION_SPEC_PATH` convention as `PekkoIngestionRunner`), and branches on `ExecutionMode`:
`BOUNDED` goes through `BoundedKafkaConsumerRunner` (a real, 177-line plain consumer/producer poll
loop against `InputFrontier`, since the Kafka Streams DSL itself has no "stop at these offsets"
concept); `CONTINUOUS` builds a real `KafkaStreams` topology (`EXACTLY_ONCE_V2`, the same
`FieldMappingEngine` mapping every other engine uses) terminating in a real sink write via
`SinkRowWriter`. It just isn't named as a sibling of `PekkoIngestionRunner`, which is presumably why
it's easy to overlook at a glance.

## The real current capability matrix

| | single-source | correlated (multi-source) |
|---|---|---|
| Pekko | `PekkoIngestionRunner` — real, tested, **and reachable through `DirectIngestionLauncher`** | `PekkoCorrelationRunner` — real, tested engine, **not reachable through `DirectIngestionLauncher`** (this doc's own gap) |
| Kafka Streams | `KafkaStreamsDeliveryEngine` — real, tested (both `BOUNDED` and `CONTINUOUS`), reachable | none — engine-level deferral, not currently reachable by the compiler anyway |
| Spark | `SparkIngestionRunner` | `SparkCorrelationRunner` — real, tested, but `DirectIngestionLauncher` rejects any plan with a `sparkStage` outright (by design — Spark is a compute stage, never a delivery engine, in the unified model) |

## The fix (corrected, per the retraction at the top)

The dispatch mechanism itself is real and correct — do not touch `DirectIngestionLauncher`'s image/
command wiring. What's actually missing is proof, not capability:

1. Restore real-image coverage of correlated dispatch through `DirectIngestionLauncher` — bring back
   the shape `DirectCorrelationLauncherRealImageIntegrationTest` used to prove, retargeted at the
   real, current entrypoint (`PekkoStreamsDeliveryEngine`, which internally routes to
   `PekkoCorrelationRunner` for a correlated plan): launch a real correlated `IngestionSpec` as a
   real Kubernetes Job via `DirectIngestionLauncher`, against the real packaged Pekko image, and
   assert the merged, correlated result actually lands in the sink. This closes the real regression
   the 2026-09-21 `DirectIngestionLauncher`/`DirectCorrelationLauncher` collapse introduced: real-
   image proof of correlated dispatch existed before that refactor (as a Spark-direct test) and was
   deleted outright, not replaced with an equivalent for the engine that actually needs to prove
   this today.
2. Once that lands, sweep for the same missing-proof gap in the fowf-workflow path
   (`WorkflowIngestionLauncher`) — not checked in this pass. It doesn't select a runner image itself
   (a published fowf workflow definition does), so it isn't exposed to the same class of bug, but
   there's no workflow-definition YAML checked into this repo to confirm a published definition
   meant for a correlated spec actually resolves to `PekkoStreamsDeliveryEngine`/
   `PekkoCorrelationRunner` end to end either. Needs the same kind of real, live proof, not an
   inference from reading Java source.

Kafka Streams correlation stays a real, separately documented engine-level deferral
(`KafkaStreamsDeliveryEngine`'s own javadoc) — genuinely out of scope for this specific fix, not
something to fold in here. But it's a real capability gap in its own right, not "fine to leave
forever": if a correlated spec ever needs `STATEFUL` semantics, it has nowhere to run today.
Flagging it, not deciding it — whoever owns this should ask before treating it as permanently
out of scope, the same discipline this document itself needed to follow.
