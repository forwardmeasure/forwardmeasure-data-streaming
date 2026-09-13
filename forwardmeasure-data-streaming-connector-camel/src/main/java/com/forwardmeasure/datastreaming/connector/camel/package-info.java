/**
 * Generic Camel route-building and {@code camel-reactive-streams} bridging, parameterized by
 * whatever endpoint URI an {@code IngestionSpec} declares. Depends on {@code camel-core} and {@code
 * camel-reactive-streams} only - which protocol component (file, Kafka, OpenSearch, or any other of
 * Camel's components) is actually on the classpath at runtime is the consuming application's own
 * decision, not something hardcoded per protocol here.
 */
package com.forwardmeasure.datastreaming.connector.camel;
