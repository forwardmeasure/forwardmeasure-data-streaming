/**
 * Embedded, bounded-run backpressure executor - consumes/produces the Camel connector modules'
 * Publishers/Subscribers directly, with no per-connector adapter code. Chosen over Kafka Streams
 * for this role because every real deployment shape this org runs for ingestion (a plain K8s Job,
 * or a KEDA ScaledJob) is a bounded, one-shot batch process, not a continuously-running topology.
 */
package com.forwardmeasure.datastreaming.executor.pekko;
