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
 * The 202 Accepted body for both direct-mode create endpoints ({@code POST /ingestion-runs}/{@code
 * POST /correlation-runs}) - the {@code Location} header on that same response already carries the
 * canonical way to poll status, this just spares a caller from parsing it back out of the header
 * for the common case where it already knows its own {@code correlationId}.
 */
public record RunAccepted(String correlationId, String jobName) {}
