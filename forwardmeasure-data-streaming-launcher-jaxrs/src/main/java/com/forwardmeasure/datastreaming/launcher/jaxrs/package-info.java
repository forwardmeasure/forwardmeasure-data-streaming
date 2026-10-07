/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * JAX-RS resource classes over {@code launcher-application} - shared, unmodified, across all three
 * framework bindings. {@code jakarta.ws.rs-api} only; the actual runtime is supplied by whichever
 * binding (Quarkus/Spring/Micronaut) assembles this into a real deployable.
 *
 * <p>Two resources, one per launcher (collapsed from three 2026-09-21, once the unified {@code
 * IngestionSpec} removed the last real reason to split direct-mode dispatch by spec shape): {@link
 * com.forwardmeasure.datastreaming.launcher.jaxrs.IngestionRunResource} ({@code /ingestion-runs} -
 * direct, single-source or correlated, whichever engine the planner resolves), and {@link
 * com.forwardmeasure.datastreaming.launcher.jaxrs.WorkflowRunResource} ({@code /workflow-runs} -
 * through fowf's own workflow engine). The {@code mapper} sub-package translates every real failure
 * these resources can throw into an HTTP response; the {@code dto} sub-package holds the handful of
 * response shapes not already covered by an existing domain/launcher-application type.
 */
package com.forwardmeasure.datastreaming.launcher.jaxrs;
