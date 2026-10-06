# FDS deployment handover — 2026-10-05

## Workflow bundle payload correction — 2026-10-05, 22:55 America/New_York

The next deployment confirmed the launcher ready and the gateway request-ID correction active.
Bundle publication then reached FOWF validation and returned HTTP 422: standard AsyncAPI
`with.message.payload` must be an object, but four tasks supplied whole-object expression strings.
Changed the two continuous apply tasks in `ingestion.yaml` to object payloads carrying the nine
planner fields consumed by the Deployment adapter (namespace, name, image, command, args, env,
replicas, resources and imagePullSecretNames). Changed both stop tasks in `stop-ingestion.yaml`
to object payloads containing action, namespace and name. Nested field expressions remain dynamic.
Custom correlated-worker command payloads and subscription filters have different DSL contracts
and are unchanged.

Both complete workflow documents passed offline JSON Schema validation against the SDK schema
whose SHA-256 matches OpenWorkflowCompiler's pinned schema:
`/tmp/fds-bundle-schema-validation-2026-10-05.log`. Added WorkflowBundleCompilationTest using the
actual shipped workflow/AsyncAPI resources and FOWF compiler, with a test-only compiler dependency.
Bounded production/test-source compilation passed in
`/tmp/fds-bundle-regression-compile-2026-10-05.log`. No test suite was executed; semantic compiler
regressions and runtime workflow execution remain unverified.

Rerun the full platform installer after the current attempt exits. The bundle Helmfile reads these
workflow files directly, so this fix requires no image rebuild or chart publication. No live cluster
resources were changed by Codex. Confirm the next bundle Job publishes both definitions successfully.

## Launcher startup correction — 2026-10-05, 22:12 America/New_York

The platform installer completed FOWF, including human-task management, then timed out waiting
for the FDS launcher. Helm's atomic install removed the failed workload. Retained GKE container
logs (00:53–00:59 UTC on October 6) repeatedly reported:
`Failed to load config value of type class java.lang.String for: datastreaming.launcher.k8s.host-aliases`.
No Kubernetes resources or events remained in the FDS namespace when inspected.

Quarkus treated the empty configuration value as absent, but LauncherQuarkusBinding injected a
required String. Changed host aliases, image pull secrets, optional Spark image/command and Kafka
bootstrap settings to Optional<String>, preserving existing empty-list/map and null semantics.
Required credentials, image allowlists and namespace configuration remain required. Spring and
Micronaut bindings were not changed by this correction.

The two Quarkus continuous-workflow smoke fixtures (Pekko and Kafka Streams) no longer supply dummy
optional values, so their real application boots exercise production defaults. Tests were not run.
Bounded production/test compilation passed in `/tmp/fds-launcher-optional-config-compile-2026-10-05.log`.
After the final fixture edits, bounded reactor packaging (all tests skipped, image operations disabled)
passed in `/tmp/fds-launcher-optional-config-package-2026-10-05.log`, including test compilation and
Quarkus augmentation. This is build evidence, not runtime proof.

Rebuild/push only `docker.io/forwardmeasure/data-streaming-launcher-quarkus:1.1.0`:

```bash
cd /home/pn/Documents/code/forwardmeasure/forwardmeasure-data-streaming
set -o pipefail
scripts/build-bounded.sh -B -pl :forwardmeasure-data-streaming-launcher-quarkus-service -am \
  package -Pcontainer-image -Dcontainer-image.push=true \
  -DskipTests -DskipITs -Dmaven.test.skip=false \
  2>&1 | tee /tmp/fds-launcher-rebuild-push-2026-10-05.log
```

Then rerun the full platform installer, which resolves the new image digest. No executor/FOWF/FEI
image or chart rebuild is required for this fix. FEI was not reached by the failed installer run.
Live read-only inspection confirmed all seven FOWF Deployments ready. No cluster changes were made
by Codex; the FDS correction remains to be validated by redeployment.

## Earlier checkpoint

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
