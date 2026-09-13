/**
 * Distributed/correlation executor - generalizes what {@code CorrelatedSourceIngestionWorker}
 * already proves in production (real SparkSession, real JavaRDD/Dataset&lt;Row&gt; operations),
 * driven by generated models/mappers instead of hand-wired Spark code specific to one source. Bulk,
 * partition-column-driven JDBC extraction lives here, not in the JDBC connector.
 */
package com.forwardmeasure.datastreaming.executor.spark;
