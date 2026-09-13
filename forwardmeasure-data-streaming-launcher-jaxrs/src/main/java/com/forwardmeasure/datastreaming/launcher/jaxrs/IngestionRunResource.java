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

import com.forwardmeasure.datastreaming.launcher.application.DirectIngestionLauncher;
import com.forwardmeasure.datastreaming.launcher.application.DirectLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.RunAccepted;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
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
 * Direct mode's REST surface over {@link DirectIngestionLauncher} (single-source, Pekko) - {@code
 * POST /ingestion-runs} (202 Accepted + {@code Location} + {@link RunAccepted}), {@code GET
 * /ingestion-runs/{correlationId}?namespace=...} (the raw {@link KubernetesJobObservation}, or 404
 * once the underlying Job is gone or was never launched), {@code POST
 * /ingestion-runs/{correlationId}/cancel?namespace=...}.
 *
 * <p>Plain class, no framework annotations - each framework binding wires an instance of this in
 * its own idiomatic way, mirroring this org's established shared-JAX-RS-resource convention (e.g.
 * {@code ReferencePopulationResource} in forwardmeasure-entity-intelligence). Every failure this
 * class's own methods can throw ({@link SecurityException} from policy rejection, {@link
 * UnsupportedOperationException} from a non-pekko engine, {@link NullPointerException} from a
 * missing required field on the request body) is translated by this module's own {@code mapper}
 * package - nothing here builds an error {@code Response} by hand.
 *
 * <p>Both {@link #get} and {@link #cancel} require {@code namespace} as a query parameter: {@link
 * DirectIngestionLauncher#observe}/{@link DirectIngestionLauncher#cancel} need it to scope the
 * underlying Kubernetes call, and the deterministic Job name alone (derived from {@code
 * correlationId}) does not carry it.
 *
 * <p>Takes an already-constructed {@link KubernetesClient} rather than building one itself -
 * fabric8's client is thread-safe and meant to be a long-lived singleton;
 * constructing/authenticating it (kubeconfig, in-cluster config, etc.) is a framework-binding
 * concern, the same division of responsibility {@code WorkflowIngestionLauncher} already applies to
 * {@code ExecutionsApi}.
 */
@Path("/ingestion-runs")
public class IngestionRunResource {

  private final DirectIngestionLauncher launcher;
  private final KubernetesClient client;

  public IngestionRunResource(DirectIngestionLauncher launcher, KubernetesClient client) {
    this.launcher = Objects.requireNonNull(launcher, "launcher");
    this.client = Objects.requireNonNull(client, "client");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(DirectLaunchRequest request) {
    String jobName = launcher.launch(client, request);
    return Response.accepted()
        .header(
            "Location",
            "/ingestion-runs/" + request.correlationId() + "?namespace=" + request.namespace())
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
