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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates {@code DirectIngestionLauncher}'s own {@link UnsupportedOperationException} (a spec
 * whose {@code execution.engine()} isn't {@code pekko}, sent to the ingestion-runs endpoint) into
 * 400 Bad Request - the client asked this launcher to do something it genuinely doesn't do, not a
 * server-side failure.
 *
 * <p>Not {@code final} - see {@link SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class UnsupportedOperationExceptionMapper
    implements ExceptionMapper<UnsupportedOperationException> {

  @Override
  public Response toResponse(UnsupportedOperationException exception) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new ErrorResponse(exception.getMessage()))
        .build();
  }
}
