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

import com.forwardmeasure.authzen.AuthorizationUnavailableException;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates an unreachable/misbehaving AuthZEN PDP into 503 Service Unavailable - the real AuthZEN
 * client is fail-closed (any non-200 response, malformed JSON, network error, or mismatched {@code
 * X-Request-ID} correlation echo throws this rather than silently permitting), so without this
 * mapper a transient PDP outage would 500 every endpoint instead of correctly telling the caller
 * this is transient infrastructure trouble, not a bug in their request.
 *
 * <p>Not {@code final} - see {@link SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class AuthorizationUnavailableExceptionMapper
    implements ExceptionMapper<AuthorizationUnavailableException> {

  @Override
  public Response toResponse(AuthorizationUnavailableException exception) {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .entity(new ErrorResponse(exception.getMessage()))
        .build();
  }
}
