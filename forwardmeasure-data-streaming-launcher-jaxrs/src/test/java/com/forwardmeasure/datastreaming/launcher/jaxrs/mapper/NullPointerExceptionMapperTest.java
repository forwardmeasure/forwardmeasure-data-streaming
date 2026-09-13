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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.CorrelationSpec;
import com.forwardmeasure.datastreaming.launcher.application.DirectCorrelationLaunchRequest;
import com.forwardmeasure.datastreaming.launcher.jaxrs.dto.ErrorResponse;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Proves this mapper against a real {@link NullPointerException} thrown by an actual request
 * record's own compact constructor (not a hand-built exception), the same failure a caller omitting
 * a required JSON field would trigger once deserialized.
 */
class NullPointerExceptionMapperTest {

  @Test
  void mapsAMissingRequiredRequestFieldToBadRequest() {
    NullPointerExceptionMapper mapper = new NullPointerExceptionMapper();

    NullPointerException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            NullPointerException.class,
            () ->
                new DirectCorrelationLaunchRequest(
                    "corr-1", "ns", (CorrelationSpec) null, Map.of(), Map.of(), null));

    Response response = mapper.toResponse(thrown);

    assertEquals(400, response.getStatus());
    assertTrue(((ErrorResponse) response.getEntity()).message().contains("correlationSpec"));
  }
}
