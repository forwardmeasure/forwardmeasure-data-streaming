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
package com.forwardmeasure.datastreaming.executor.spark;

import com.forwardmeasure.datastreaming.api.SecretRefs;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves the {@code user}/{@code password} entries of a spec's own {@code options} map via {@link
 * SecretRefs} before Spark ever sees them - shared by {@link SparkCorrelationEngine#readSource} and
 * {@link SparkSinks} (jdbc read/write, opensearch basic auth), added 2026-09-13 alongside {@code
 * SqlDataSources}' own identical fix on the Pekko side: Spark's own {@code DataFrameReader}/{@code
 * DataFrameWriter} have no notion of a reference like {@code env:DB_PASSWORD} - handed one
 * directly, they would use that literal string as the credential and simply fail to authenticate,
 * not resolve it. Every other option key passes through completely unchanged.
 */
final class SparkSecretOptions {

  private SparkSecretOptions() {}

  static Map<String, String> resolve(Map<String, String> options) {
    Map<String, String> resolved = new LinkedHashMap<>(options);
    String user = SecretRefs.resolve(options, "user");
    if (user != null) {
      resolved.put("user", user);
    }
    String password = SecretRefs.resolve(options, "password");
    if (password != null) {
      resolved.put("password", password);
    }
    return resolved;
  }
}
