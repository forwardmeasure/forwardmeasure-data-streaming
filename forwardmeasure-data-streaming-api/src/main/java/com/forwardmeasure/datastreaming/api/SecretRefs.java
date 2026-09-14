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
package com.forwardmeasure.datastreaming.api;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

/**
 * Resolves one {@link SourceSpec#options()}/{@link SinkSpec#options()} entry that names a
 * credential - added 2026-09-13, closing the gap between what those fields' own javadoc already
 * required ("a value here that authenticates anything must be a *reference*, never the secret
 * itself") and what every connector actually did until now: read {@code user}/{@code password}
 * straight out of {@code options} as literal text, in every one of them (Pekko's {@code
 * SqlDataSources}/OpenSearch sink, Spark's jdbc read/write and OpenSearch sink).
 *
 * <p>Two reference schemes, both matching how this org's own Kubernetes Jobs already deliver secret
 * material into a running process - no new delivery mechanism invented, just resolved once instead
 * of left to whatever a connector happened to do with the raw string:
 *
 * <ul>
 *   <li>{@code env:NAME} - {@link System#getenv(String)}, the same mechanism {@code
 *       SparkExecutorConfig}'s own callers already use for non-secret config (container image,
 *       namespace); a K8s {@code Secret} mounted via {@code valueFrom.secretKeyRef} lands here.
 *   <li>{@code file:/path} - reads the file's content (stripped of trailing whitespace/newline); a
 *       K8s {@code Secret} mounted as a volume lands here.
 * </ul>
 *
 * <p>A value with neither prefix is returned as-is (a literal) - not an error, since local/test
 * runs (this module's own real integration tests included, against ephemeral Testcontainers
 * credentials that are not real secrets) have no real secret to protect and no reason to require
 * one. A real production spec should always use one of the two reference forms; nothing here
 * enforces that today - see this class's own limits.
 */
public final class SecretRefs {

  private static final String ENV_PREFIX = "env:";
  private static final String FILE_PREFIX = "file:";

  private SecretRefs() {}

  /** Resolves {@code options.get(key)}, or {@code null} if that entry is absent. */
  public static String resolve(Map<String, String> options, String key) {
    return resolve(options, key, System::getenv);
  }

  /** Package-visible so tests can inject a fake environment instead of the real one. */
  static String resolve(Map<String, String> options, String key, Function<String, String> env) {
    String value = options.get(key);
    if (value == null) {
      return null;
    }
    if (value.startsWith(ENV_PREFIX)) {
      String varName = value.substring(ENV_PREFIX.length());
      String resolved = env.apply(varName);
      if (resolved == null || resolved.isBlank()) {
        throw new IllegalStateException(
            "SecretRefs: environment variable '"
                + varName
                + "' (referenced by option '"
                + key
                + "') is not set");
      }
      return resolved;
    }
    if (value.startsWith(FILE_PREFIX)) {
      Path path = Path.of(value.substring(FILE_PREFIX.length()));
      try {
        return Files.readString(path, StandardCharsets.UTF_8).stripTrailing();
      } catch (IOException e) {
        throw new UncheckedIOException(
            "SecretRefs: failed to read secret file '"
                + path
                + "' (referenced by option '"
                + key
                + "')",
            e);
      }
    }
    return value;
  }
}
