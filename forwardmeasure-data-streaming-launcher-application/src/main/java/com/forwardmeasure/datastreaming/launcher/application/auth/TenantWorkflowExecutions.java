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
package com.forwardmeasure.datastreaming.launcher.application.auth;

import com.forwardmeasure.authzen.client.TenantClientCredentials;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.execution.client.ApiClient;
import com.forwardmeasure.openworkflow.execution.client.api.WorkflowExecutionsApi;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable bearer configuration per tenant; API clients are never retargeted between requests. */
public final class TenantWorkflowExecutions {
  private final String baseUrl;
  private final TenantClientCredentials tokens;
  private final Map<TenantId, WorkflowExecutionsApi> clients = new LinkedHashMap<>(16, 0.75f, true);

  public TenantWorkflowExecutions(String baseUrl, URI tokenUrl, String clientId, String secret) {
    this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl");
    this.tokens = new TenantClientCredentials(tokenUrl, clientId, secret);
  }

  public synchronized WorkflowExecutionsApi forTenant(TenantId tenant) {
    Objects.requireNonNull(tenant, "tenant");
    WorkflowExecutionsApi existing = clients.get(tenant);
    if (existing != null) {
      return existing;
    }
    ApiClient client = new ApiClient();
    client.setBasePath(baseUrl);
    client.setBearerToken(() -> tokens.bearerToken(tenant));
    WorkflowExecutionsApi api = new WorkflowExecutionsApi(client);
    if (clients.size() >= 256) {
      clients.remove(clients.keySet().iterator().next());
    }
    clients.put(tenant, api);
    return api;
  }
}
