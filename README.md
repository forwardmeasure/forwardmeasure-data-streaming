# ForwardMeasure Data Streaming

Metadata-driven ingestion: schema-described sources and targets, named transformations, Spark and
Pekko Streams execution, backpressure-controlled sinks - built on this org's proven connectors
(Apache Camel, `forwardmeasure-jpa`, `forwardmeasure-object-storage`) rather than reinventing
connectivity.

Design decisions and rationale live in
[`forwardmeasure-openworkflow/docs/forwardmeasure-data-streaming-plan.md`](https://github.com/forwardmeasure/forwardmeasure-openworkflow/blob/develop/docs/forwardmeasure-data-streaming-plan.md)
(D1-D11) - read that before changing the shape of any module here. General engineering conventions
this library follows are in
[`forwardmeasure-platform/docs/forwardmeasure-engineering-manifesto.md`](https://github.com/forwardmeasure/forwardmeasure-platform/blob/develop/docs/forwardmeasure-engineering-manifesto.md).

## Modules

| Module | Purpose |
|---|---|
| `forwardmeasure-data-streaming-bom` | Aggregate BOM for consumers |
| `forwardmeasure-data-streaming-api` | `IngestionSpec`/`SourceSpec`/`SinkSpec`/`TransformSpec` contracts (D1/D8) |
| `forwardmeasure-data-streaming-transforms` | Generic named-transform function pool, registered into a runtime `NamedTransformRegistry` - not MapStruct (D3, corrected) |
| `forwardmeasure-data-streaming-mappers` | `FieldMappingEngine`: walks an `IngestionSpec`'s `mapper.fields[]` (a `TransformSpec`/`FieldRule` list) and dispatches through `-transforms`' `NamedTransformRegistry`, producing a schema-agnostic `Map<String,Object>` per row - ported from fei's own `GenericRecordMapper`, minus its Protobuf coercion step (D1, deferred). Not MapStruct (D3, corrected) |
| `forwardmeasure-data-streaming-core` | Backpressure pipeline primitives, execution-engine selection |
| `forwardmeasure-data-streaming-connector-camel` | Generic Camel route-building + `camel-reactive-streams` bridge, parameterized by URI - not one module per protocol. Depends on `camel-core`/`camel-reactive-streams` only; consumers add whichever `camel-file`/`camel-kafka`/`camel-opensearch`/etc. jar they actually need (D5, revised - see below) |
| `forwardmeasure-data-streaming-connector-jdbc` | JPA/JPQL + Repository via `forwardmeasure-jpa` (D5, revised); the one place in this repo where MapStruct belongs, for JPA `@Entity`-to-generated-model conversion (D3, corrected) |
| `forwardmeasure-data-streaming-connector-object-storage` | Thin adapter over `forwardmeasure-object-storage` (D5, the one non-Camel connector) |
| `forwardmeasure-data-streaming-executor-pekko` | Embedded, bounded-run backpressure executor (D4) |
| `forwardmeasure-data-streaming-executor-spark` | Distributed/correlation executor (D4) |
| `forwardmeasure-data-streaming-launcher-application` | Async REST launcher orchestration - accepts an `IngestionSpec`, launches a runner image as a K8s Job, tracks status - framework-agnostic (D9-D11) |
| `forwardmeasure-data-streaming-launcher-jaxrs` | Shared JAX-RS resources (`POST`/`GET`/`cancel` on `/ingestion-runs`) over `-launcher-application`, unmodified across all three framework bindings |
| `forwardmeasure-data-streaming-framework-bindings/{quarkus,spring,micronaut}` | Per-framework CDI/DI wiring for the launcher only - mirrors fowf's own `openworkflow-framework-bindings` structure exactly. The two batch runners have no presence here; they're plain `main()` programs with no framework dependency |
| `forwardmeasure-data-streaming-deployments/launcher/{quarkus,spring,micronaut}` | The actual bootable, containerizable assemblies + `container-image` Maven profile, mirroring `forwardmeasure-entity-intelligence-deployments`' real leaf-deployment shape |

Deliberately **not** in this repo: per-schema generated model classes and the per-source YAML
field-mapping specs that reference them. Those are specific to whatever source a consumer (e.g.
`forwardmeasure-entity-intelligence`) onboards, and belong in that consumer, built on top of this
library's `-api`/`-transforms`/connector modules.

The mapping *mechanism* itself, by contrast, is generic and belongs in this repo - `-mappers`' runtime
`FieldMappingEngine`, walking an `IngestionSpec`'s `mapper.fields[]` and dispatching each field
through `-transforms`' `NamedTransformRegistry` (D3, corrected 2026-09-12; ported from fei's own
`MappingDefinition`/`FieldRule`/`GenericRecordMapper`, not modeled on `ScreeningHitTransformEngine` as
an earlier draft of this note said - see the plan doc's §2/§5). It is **not** MapStruct-generated, and
no module in this repo should build source/target payload mapping on MapStruct. MapStruct's only real
place in this architecture is JPA `@Entity`-to-model conversion inside `-connector-jdbc` - a
fixed-type, compile-time boundary, unlike this runtime, metadata-driven mapping.

A Kafka Streams executor is deliberately not yet included - see D4: reserved for a genuinely
continuous, always-on topic-sourced case, which doesn't exist in this org's ingestion pipelines
today.

**Not yet wired**: `-launcher-application`'s actual K8s-Job launch/poll/cancel logic. That belongs to
a shared library extracted from fowf's `openworkflow-operation-adapter-kubernetes-job`
(`AsyncApiKubernetesJobOperationExecutor` already implements this exact lifecycle) - `-launcher-application`
should depend on that extraction once it exists, not reimplement K8s Job lifecycle management a
second time.
