/**
 * JAX-RS resource classes over {@code launcher-application} - shared, unmodified, across all three
 * framework bindings. {@code jakarta.ws.rs-api} only; the actual runtime is supplied by whichever
 * binding (Quarkus/Spring/Micronaut) assembles this into a real deployable.
 *
 * <p>Three resources, one per launcher: {@link
 * com.forwardmeasure.datastreaming.launcher.jaxrs.IngestionRunResource} ({@code /ingestion-runs} -
 * direct, single-source Pekko), {@link
 * com.forwardmeasure.datastreaming.launcher.jaxrs.CorrelationRunResource} ({@code
 * /correlation-runs} - direct, multi-source Spark), and {@link
 * com.forwardmeasure.datastreaming.launcher.jaxrs.WorkflowRunResource} ({@code /workflow-runs} -
 * through fowf's own workflow engine). The {@code mapper} sub-package translates every real failure
 * these resources can throw into an HTTP response; the {@code dto} sub-package holds the handful of
 * response shapes not already covered by an existing domain/launcher-application type.
 */
package com.forwardmeasure.datastreaming.launcher.jaxrs;
