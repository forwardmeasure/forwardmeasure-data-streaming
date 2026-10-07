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
/**
 * Framework-agnostic orchestration for the async REST launcher: accept an {@code IngestionSpec},
 * launch a runner image as a real K8s Job, translate its status, support cancellation. Knows
 * nothing about JAX-RS/CDI/HTTP - that starts one layer up, in {@code launcher-jaxrs}.
 */
package com.forwardmeasure.datastreaming.launcher.application;
