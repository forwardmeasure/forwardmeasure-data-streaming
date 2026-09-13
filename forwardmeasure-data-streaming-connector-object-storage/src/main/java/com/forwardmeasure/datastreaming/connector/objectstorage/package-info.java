/**
 * Thin adapter over {@code forwardmeasure-object-storage} - the one deliberate exception to "prefer
 * Camel" in this library, since no Camel S3/GCS component beats a provider-neutral interface this
 * org already built, controls, and has a real external consumer of. Not Camel-backed, so it does
 * not go through {@code camel-reactive-streams}.
 */
package com.forwardmeasure.datastreaming.connector.objectstorage;
