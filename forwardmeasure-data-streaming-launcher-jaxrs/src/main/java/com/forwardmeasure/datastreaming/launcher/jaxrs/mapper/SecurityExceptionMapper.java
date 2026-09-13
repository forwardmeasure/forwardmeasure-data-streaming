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
 * Translates {@code IngestionJobPolicy}'s own {@link SecurityException} (namespace/image not
 * allowlisted, or an unpinned image reference) into 403 Forbidden - every direct-mode resource
 * method just calls {@code launcher.launch(...)} and lets policy rejection propagate, nothing
 * builds this {@code Response} by hand.
 *
 * <p>Not {@code final} - Micronaut needs a thin {@code @Singleton} subclass wrapper for
 * compile-time discovery (confirmed necessary the hard way for this exact pattern in
 * forwardmeasure-entity-intelligence's own {@code IngestionExceptionMapper}); a plain
 * {@code @Provider} class alone is enough for Quarkus/Spring but not Micronaut.
 */
@Provider
public class SecurityExceptionMapper implements ExceptionMapper<SecurityException> {

  @Override
  public Response toResponse(SecurityException exception) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(new ErrorResponse(exception.getMessage()))
        .build();
  }
}
