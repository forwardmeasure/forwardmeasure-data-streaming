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
package com.forwardmeasure.datastreaming.launcher.application;

import com.forwardmeasure.authzen.AuthorizationResource;
import java.util.Map;

/**
 * FDS's own AuthZEN resource vocabulary, built 2026-09-14 alongside {@link AuthorizationAction} to
 * close the same confirmed gap (see docs/fds-authorization-remediation-guide.md). One factory per
 * real run type this launcher exposes - {@code id} names the resource collection, the specific
 * correlation/execution id lives in {@code properties}, matching this same day's real siblings
 * ({@code OpenWorkflowAuthorizationResources} in forwardmeasure-openworkflow, {@code
 * EntityIntelligenceAuthorizationResources} in forwardmeasure-entity-intelligence) in both naming
 * convention (product-prefixed, recognizable across module boundaries) and shape.
 *
 * <p>A pure static-factory utility over the shared {@link AuthorizationResource} record, not a
 * record of its own - fowf hit a real bug the first time this pattern was built (a local record
 * sharing a simple name with the shared library's own record is not interchangeable with it even
 * with an identical field shape), so this class was never named {@code AuthorizationResource} to
 * begin with.
 */
public final class DataStreamingAuthorizationResources {
  private DataStreamingAuthorizationResources() {}

  public static AuthorizationResource ingestionRun(String correlationId) {
    return new AuthorizationResource(
        "datastreaming-ingestion-run",
        "ingestion-runs",
        Map.of("correlation_id", requireText(correlationId, "correlationId")));
  }

  public static AuthorizationResource correlationRun(String correlationId) {
    return new AuthorizationResource(
        "datastreaming-correlation-run",
        "correlation-runs",
        Map.of("correlation_id", requireText(correlationId, "correlationId")));
  }

  public static AuthorizationResource workflowRun(String executionId) {
    return new AuthorizationResource(
        "datastreaming-workflow-run",
        "workflow-runs",
        Map.of("execution_id", requireText(executionId, "executionId")));
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
