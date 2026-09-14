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

import com.forwardmeasure.authzen.Action;

/**
 * FDS's own AuthZEN action vocabulary, built 2026-09-14 to close a confirmed real gap - every
 * JAX-RS endpoint in this repo ran with zero caller authorization (see
 * docs/fds-authorization-remediation-guide.md, itself confirmed by direct code inspection, not
 * inferred). One entry per privileged operation, matching the guide's own §3.2 minimum set exactly:
 * three verbs (launch/read/cancel) across the three real run types this launcher exposes.
 *
 * <p>Implements {@link Action} directly, mirroring forwardmeasure-openworkflow's and
 * forwardmeasure-entity-intelligence's own {@code AuthorizationAction} enums - both real,
 * already-proven siblings of this same shape, built the same day as part of the same fowf/fei/FDS
 * AuthZEN consolidation. {@link Enum#name()} is {@code final} and already satisfies {@link
 * Action#name()} with no extra code here; only {@link #scope()} needed a real implementation.
 */
public enum AuthorizationAction implements Action {
  INGESTION_RUN_LAUNCH("ingestion-run:launch"),
  INGESTION_RUN_READ("ingestion-run:read"),
  INGESTION_RUN_CANCEL("ingestion-run:cancel"),
  CORRELATION_RUN_LAUNCH("correlation-run:launch"),
  CORRELATION_RUN_READ("correlation-run:read"),
  CORRELATION_RUN_CANCEL("correlation-run:cancel"),
  WORKFLOW_RUN_LAUNCH("workflow-run:launch"),
  WORKFLOW_RUN_READ("workflow-run:read"),
  WORKFLOW_RUN_CANCEL("workflow-run:cancel");

  private final String scope;

  AuthorizationAction(String scope) {
    this.scope = scope;
  }

  @Override
  public String scope() {
    return scope;
  }
}
