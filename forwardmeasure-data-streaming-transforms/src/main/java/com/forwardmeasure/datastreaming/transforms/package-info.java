/**
 * The generic, domain-agnostic named-transform function pool (date/string/number formatting,
 * country-code resolution, and similar utilities) - plain static methods, registered by name into a
 * flat runtime {@code NamedTransformRegistry} and dispatched from a {@code mapper.fields[]} spec,
 * not MapStruct-qualified. Domain-specific transforms (e.g. tied to one consumer's source schema)
 * belong downstream, not here. MapStruct has no role in this module or in source/target payload
 * mapping generally - its one real place in this architecture is JPA entity-to-model conversion, in
 * {@code forwardmeasure-data-streaming-connector-jdbc}.
 */
package com.forwardmeasure.datastreaming.transforms;
