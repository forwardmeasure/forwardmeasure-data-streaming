/**
 * Framework-agnostic orchestration for the async REST launcher: accept an {@code IngestionSpec},
 * launch a runner image as a real K8s Job, translate its status, support cancellation. Knows
 * nothing about JAX-RS/CDI/HTTP - that starts one layer up, in {@code launcher-jaxrs}.
 */
package com.forwardmeasure.datastreaming.launcher.application;
