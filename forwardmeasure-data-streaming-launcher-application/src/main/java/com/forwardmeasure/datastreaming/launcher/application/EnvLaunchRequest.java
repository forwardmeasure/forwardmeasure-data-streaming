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
package com.forwardmeasure.datastreaming.launcher.application;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A direct-mode launch that skips {@link DirectLaunchRequest}'s {@code IngestionSpec}/base64-YAML
 * packaging convention entirely: {@code env} is delivered to the container as plain Kubernetes
 * environment variables, unchanged - the shape a caller whose own worker image already reads
 * configuration from named environment variables (not a reconstructed spec file) actually needs.
 * {@code image}/{@code command}/{@code args} are supplied per-request, not bound at launcher
 * construction time, since (unlike {@link DirectIngestionLauncher}'s fixed Pekko runner) this
 * launcher has no single runner image of its own - it exists precisely because a caller's image
 * isn't one of FDS's own generic runners.
 *
 * <p>{@code correlationId} is the caller's own idempotency key, same convention as {@link
 * DirectLaunchRequest} - the same id always resolves to the same Job name ({@link
 * EnvConfiguredJobLauncher#deterministicJobName}), so a caller can safely retry a launch call
 * without risking a duplicate run.
 */
public record EnvLaunchRequest(
    String correlationId,
    String namespace,
    String image,
    List<String> command,
    List<String> args,
    Map<String, String> env,
    Map<String, String> resourceRequests,
    Map<String, String> resourceLimits,
    Long activeDeadlineSeconds,
    List<String> imagePullSecretNames) {

  public EnvLaunchRequest {
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(namespace, "namespace");
    Objects.requireNonNull(image, "image");
    command = command == null ? List.of() : List.copyOf(command);
    args = args == null ? List.of() : List.copyOf(args);
    env = env == null ? Map.of() : Map.copyOf(env);
    resourceRequests = resourceRequests == null ? Map.of() : Map.copyOf(resourceRequests);
    resourceLimits = resourceLimits == null ? Map.of() : Map.copyOf(resourceLimits);
    imagePullSecretNames =
        imagePullSecretNames == null ? List.of() : List.copyOf(imagePullSecretNames);
  }
}
