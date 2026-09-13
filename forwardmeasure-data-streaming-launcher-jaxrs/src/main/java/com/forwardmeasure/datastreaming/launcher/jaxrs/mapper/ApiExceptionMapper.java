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

import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates {@code WorkflowIngestionLauncher}'s own {@link ApiException} (fowf's generated {@code
 * ExecutionsApi} client) into a {@link Response}. {@link ApiException#getCode()} is fowf's own real
 * HTTP status when the call actually reached it and fowf rejected it (404 unknown execution, 409
 * optimistic-concurrency conflict on cancel, 422 malformed input, etc.) - passed straight through,
 * since fowf already chose the right status. A {@code code} of {@code 0} means the client never got
 * a real response at all (connection refused/timeout - see {@code ApiException}'s own
 * constructors), which this mapper reports as 502 Bad Gateway: the failure is real, but it's fowf
 * (or the network to it) that's unavailable, not this launcher.
 *
 * <p>Not {@code final} - see {@link SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class ApiExceptionMapper implements ExceptionMapper<ApiException> {

  @Override
  public Response toResponse(ApiException exception) {
    int code = exception.getCode();
    int status = code >= 100 && code < 600 ? code : Response.Status.BAD_GATEWAY.getStatusCode();
    return Response.status(status).entity(new ErrorResponse(exception.getMessage())).build();
  }
}
