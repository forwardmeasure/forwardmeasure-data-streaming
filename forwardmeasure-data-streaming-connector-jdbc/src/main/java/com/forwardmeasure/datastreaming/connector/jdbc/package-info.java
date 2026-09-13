/**
 * JPA/JPQL via {@code forwardmeasure-jpa}, behind a Repository, for smaller/bounded/incremental
 * polling or lookup sources - not Camel (no camel-jdbc/camel-sql precedent anywhere in this org).
 * Exposes Repository types only; any orchestration/business logic belongs in a downstream
 * application-service layer that depends on this module, never the reverse.
 *
 * <p>This is the one module in this library where MapStruct belongs (D3, corrected): converting a
 * JPA {@code @Entity} to/from a generated OpenAPI model class is a fixed-type, compile-time
 * boundary, unlike the runtime-interpreted source/target payload mapping the rest of this library
 * builds on {@code NamedTransformRegistry} for instead.
 */
package com.forwardmeasure.datastreaming.connector.jdbc;
