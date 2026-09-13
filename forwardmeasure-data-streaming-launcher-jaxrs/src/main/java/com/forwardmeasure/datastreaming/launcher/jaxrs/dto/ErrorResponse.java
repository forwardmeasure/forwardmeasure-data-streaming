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
package com.forwardmeasure.datastreaming.launcher.jaxrs.dto;

/**
 * A deliberately minimal error body, used by every {@code ExceptionMapper} in this module - this
 * project's org-wide RFC 9457 {@code Problem} lineage is real but not yet the shape this specific
 * module's own mappers are built around (see the design plan's common-definitions.yaml note); a
 * single {@code message} field is honest about what these mappers actually know today rather than
 * fabricating {@code type}/{@code title} values with no real meaning yet.
 */
public record ErrorResponse(String message) {}
