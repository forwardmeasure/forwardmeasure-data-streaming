/**
 * Framework-agnostic ingestion contracts: {@code IngestionSpec}, {@code SourceSpec}/{@code
 * SinkSpec}/{@code TransformSpec}. Depends on nothing that ties it to Camel, Spark, or Pekko -
 * those are executor/connector concerns, not the contract.
 *
 * <p>{@code TransformSpec} is a direct port of {@code forwardmeasure-entity-intelligence}'s real
 * {@code MappingDefinition}/{@code FieldRule}, interpreted at runtime by a mapping engine - not
 * MapStruct (D3, corrected). {@code IngestionSpec} carries it inline, structurally, because it is
 * runtime metadata, not something a compile-time mapper would generate.
 */
package com.forwardmeasure.datastreaming.api;
