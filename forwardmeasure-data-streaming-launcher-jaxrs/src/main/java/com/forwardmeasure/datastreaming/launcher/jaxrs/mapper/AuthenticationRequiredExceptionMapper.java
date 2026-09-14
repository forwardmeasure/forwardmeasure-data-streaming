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

import com.forwardmeasure.authzen.AuthenticationRequiredException;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates a missing/invalid JWT into 401 - each framework binding's own {@code
 * ActiveOrganizationProvider} throws this when no caller identity can be extracted (see
 * docs/fds-authorization-remediation-guide.md, added 2026-09-14 to close a confirmed real gap: this
 * repo had zero inbound authentication before).
 *
 * <p>Deliberately {@link AuthenticationRequiredException}, not {@link SecurityException} - {@link
 * SecurityExceptionMapper} in this same package is already reserved for {@code
 * IngestionJobPolicy}'s own, distinct 403 case (a request-payload allowlist rejection, not a
 * missing-identity one); reusing {@code SecurityException} here would collide two genuinely
 * different failures - a missing caller vs. a disallowed namespace/image - onto the same JAX-RS
 * exception-mapper type and the same HTTP status, exactly the mistake this shared exception type
 * exists to prevent (see its own javadoc in forwardmeasure-authzen-api).
 *
 * <p>Not {@code final} - see {@link SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class AuthenticationRequiredExceptionMapper
    implements ExceptionMapper<AuthenticationRequiredException> {

  @Override
  public Response toResponse(AuthenticationRequiredException exception) {
    return Response.status(Response.Status.UNAUTHORIZED)
        .entity(new ErrorResponse(exception.getMessage()))
        .build();
  }
}
