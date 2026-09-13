/**
 * The runtime field-mapping engine: {@link
 * com.forwardmeasure.datastreaming.mappers.FieldMappingEngine} walks a {@code TransformSpec}/{@code
 * FieldRule} list and dispatches each field through {@code
 * forwardmeasure-data-streaming-transforms}' {@code NamedTransformRegistry} - ported directly from
 * {@code forwardmeasure-entity-intelligence}'s own {@code GenericRecordMapper}, minus its
 * Protobuf-specific coercion step (D1's separate, explicitly-deferred concern). Not MapStruct: this
 * mapping is metadata-driven, interpreted at runtime from a spec, not generated at compile time
 * (D3, corrected).
 */
package com.forwardmeasure.datastreaming.mappers;
