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

import com.forwardmeasure.authzen.AuthorizationDeniedException;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates a denied real AuthZEN decision into 403 Forbidden - thrown by {@code
 * AuthorizationService#requireAuthorized} when a real, authenticated caller is not entitled to the
 * action they asked for (see docs/fds-authorization-remediation-guide.md, added 2026-09-14 to close
 * a confirmed real gap). Distinct from {@link SecurityExceptionMapper}'s own 403 (a request-payload
 * allowlist rejection with no notion of who is calling) - both are real 403s for genuinely
 * different reasons, not a duplicate mapping.
 *
 * <p>Not {@code final} - see {@link SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class AuthorizationDeniedExceptionMapper
    implements ExceptionMapper<AuthorizationDeniedException> {

  @Override
  public Response toResponse(AuthorizationDeniedException exception) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(new ErrorResponse(exception.getMessage()))
        .build();
  }
}
