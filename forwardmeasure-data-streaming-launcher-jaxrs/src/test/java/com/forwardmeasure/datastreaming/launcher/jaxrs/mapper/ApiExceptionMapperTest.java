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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import com.forwardmeasure.openworkflow.execution.client.ApiException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class ApiExceptionMapperTest {

  private final ApiExceptionMapper mapper = new ApiExceptionMapper();

  @Test
  void passesThroughFowfsOwnRealStatusCode() {
    ApiException exception = new ApiException(409, "execution has moved on: version mismatch");

    Response response = mapper.toResponse(exception);

    assertEquals(409, response.getStatus());
    assertEquals(
        "execution has moved on: version mismatch",
        ((ErrorResponse) response.getEntity()).message());
  }

  @Test
  void mapsAConnectivityFailureWithNoRealResponseToBadGateway() {
    ApiException exception = new ApiException(new java.net.ConnectException("connection refused"));

    Response response = mapper.toResponse(exception);

    assertEquals(502, response.getStatus());
  }
}
