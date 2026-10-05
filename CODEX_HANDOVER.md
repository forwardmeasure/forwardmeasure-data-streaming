# FDS deployment handover — 2026-10-05

The cross-product source/release checkpoint is maintained in
[FOWF deployment implementation](../forwardmeasure-openworkflow/docs/rehabilitation/deployment-implementation-2026-10-05.md).
Use the [combined deployment runbook](../forwardmeasure-openworkflow/docs/rehabilitation/deploy-fowf-fds-fei-2026-10-05.md)
for image build/push, infrastructure prerequisites, ordered deployment and diagnostic logs.

The five identified FDS repairs are implemented: shared direct/workflow planning, versioned merge
semantics, durable Pekko correlation, neutral shared connectors and generated launcher server/client contracts.
Continuous Spark now writes mapped Kafka microbatches transactionally with source offsets; delivery
retains per-source correlation state. Direct launch persists the entire delivery-stage request for
restart, with tenant-scoped names and pre-dispatch namespace/image authorization.

Continuous launch is workflow-only; stop it explicitly through `fds-stop-ingestion` after startup.
Continuous Spark requires Kafka inputs on the handoff cluster. Nonempty TransformGraph is rejected;
source-field mapper transforms remain supported. Policies are snapshotted and file resources are
restricted to MERGE_POLICY_ROOT. These boundaries are documented in the operator runbook.

Clean selected Quarkus launcher/all-executor packaging and regression-source compilation passed in
`/tmp/fds-predeployment-clean-final-2026-10-05.log`; the final SDK-inclusive package and HTTP
regression-source compile passed in `/tmp/fds-predeployment-final-with-client-2026-10-05.log`.
No test suite was executed. The current source is
uncommitted; do not reuse old passing counts as evidence. Execute focused regressions and complete
the broader 18-cell acceptance matrix after deployment. FDE remains deferred.
