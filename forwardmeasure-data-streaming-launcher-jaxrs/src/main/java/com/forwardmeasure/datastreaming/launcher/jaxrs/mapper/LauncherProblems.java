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
package com.forwardmeasure.datastreaming.launcher.jaxrs.mapper;

import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.platform.server.jaxrs.RequestProblems;
import jakarta.ws.rs.core.Response;
import java.util.List;

/**
 * This module's failures as the RFC 9457 problem every forwardmeasure service returns, through
 * forwardmeasure-platform-server-jaxrs's {@link RequestProblems} - so a launcher error has the same
 * shape as a request-contract error, on every framework.
 */
final class LauncherProblems {
  private LauncherProblems() {}

  /** The status's own title, with the failure's message as the detail. */
  static Response response(int status, String detail) {
    Response.Status known = Response.Status.fromStatusCode(status);
    return response(
        RequestProblems.httpError(status, known == null ? null : known.getReasonPhrase(), detail));
  }

  /** A required value the request left out, as a bean-validation failure would report it. */
  static Response required(String field) {
    return response(
        RequestProblems.badRequest(
            List.of(RequestProblems.violation(field, RequestProblems.REQUIRED, null))));
  }

  static Response response(Problem problem) {
    return Response.status(problem.getStatus())
        .type("application/problem+json")
        .entity(problem)
        .build();
  }
}
