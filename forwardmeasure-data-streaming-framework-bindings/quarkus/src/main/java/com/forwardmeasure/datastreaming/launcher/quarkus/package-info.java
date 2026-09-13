/**
 * Quarkus CDI wiring for launcher-jaxrs/launcher-application. Carries no REST resource classes of
 * its own (shared, in launcher-jaxrs) and never boots its own bootable app - see
 * forwardmeasure-data-streaming-deployments/launcher/quarkus for the real deployable assembly.
 */
package com.forwardmeasure.datastreaming.launcher.quarkus;
