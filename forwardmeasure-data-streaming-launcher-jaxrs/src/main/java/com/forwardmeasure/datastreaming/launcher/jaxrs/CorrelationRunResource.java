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
package com.forwardmeasure.datastreaming.launcher.jaxrs;

import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLauncher;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.RunAccepted;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Objects;

/**
 * Direct mode's REST surface over {@link DirectCorrelationLauncher} (multi-source, Spark) - the
 * exact sibling of {@link IngestionRunResource} for {@code CorrelationSpec} runs, kept as its own
 * resource/path ({@code /correlation-runs}) for the same reason {@code DirectCorrelationLauncher}
 * is its own class rather than a branch inside {@code DirectIngestionLauncher}: genuinely different
 * spec/runner-image pairing, not just a variant of the same one.
 */
@Path("/correlation-runs")
public class CorrelationRunResource {

  private final DirectCorrelationLauncher launcher;
  private final KubernetesClient client;

  public CorrelationRunResource(DirectCorrelationLauncher launcher, KubernetesClient client) {
    this.launcher = Objects.requireNonNull(launcher, "launcher");
    this.client = Objects.requireNonNull(client, "client");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(DirectCorrelationLaunchRequest request) {
    String jobName = launcher.launch(client, request);
    return Response.accepted()
        .header(
            "Location",
            "/correlation-runs/" + request.correlationId() + "?namespace=" + request.namespace())
        .entity(new RunAccepted(request.correlationId(), jobName))
        .build();
  }

  @GET
  @Path("/{correlationId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(
      @PathParam("correlationId") String correlationId, @QueryParam("namespace") String namespace) {
    requireNamespace(namespace);
    return launcher
        .observe(client, namespace, correlationId)
        .map(observation -> Response.ok(observation).build())
        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
  }

  @POST
  @Path("/{correlationId}/cancel")
  public Response cancel(
      @PathParam("correlationId") String correlationId, @QueryParam("namespace") String namespace) {
    requireNamespace(namespace);
    launcher.cancel(client, namespace, correlationId);
    return Response.noContent().build();
  }

  private static void requireNamespace(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new BadRequestException("the 'namespace' query parameter is required");
    }
  }
}
