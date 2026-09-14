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
package com.forwardmeasure.datastreaming.executor.pekko;

import com.forwardmeasure.datastreaming.api.SecretRefs;
import com.forwardmeasure.datastreaming.connector.camel.CamelBridge;
import java.util.Map;
import java.util.Objects;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Builds a plain JDBC {@code DataSource} from a {@code SourceSpec}/{@code SinkSpec}'s own {@code
 * uri()} (the JDBC connection string - the same field every other connector's own URI/location
 * lives in, not duplicated into {@code options}) plus its {@code options} map, and binds it into a
 * {@link CamelBridge}'s {@code CamelContext} registry, so a {@code sql:} endpoint's own {@code
 * ?dataSource=#name} convention can resolve it - {@code camel-sql}'s standard way of getting a real
 * connection, confirmed by reading {@code SqlComponent}'s own {@code createEndpoint} (the "endpoint
 * configured data source takes precedence" bean-binding path). {@code DriverManagerDataSource}
 * (Spring's own, already a transitive dependency of {@code camel-sql} itself) is deliberately
 * generic rather than a Postgres-specific type - the same {@code driver}/{@code url}/{@code user}/
 * {@code password} shape {@code SparkCorrelationEngine}'s own generalized jdbc source reader
 * already uses (there, {@code source.uri()}/{@code sink.uri()} is merged into the options map as
 * {@code url} via {@code options.putIfAbsent("url", ...)} - mirrored here, not duplicated by
 * accident: this class takes {@code url} as its own explicit parameter instead, since {@code
 * options} here is a plain, already-immutable {@code Map.copyOf(...)} with no mutable copy to
 * {@code putIfAbsent} into). {@code user}/{@code password} go through {@link SecretRefs#resolve} -
 * added 2026-09-13, closing a real gap: a secret-reference value (e.g. {@code env:DB_PASSWORD})
 * was, until now, handed to the {@code DataSource} as that literal string instead of being
 * resolved.
 */
final class SqlDataSources {

  private SqlDataSources() {}

  /**
   * Registers (or re-registers, harmlessly, if called again with the same {@code url}/{@code
   * options}) a {@code DataSource} and returns its bean name. The name is derived from the
   * connection target itself (not a fixed constant) so a single correlation run's one shared {@link
   * CamelBridge} can hold several sources pointed at *different* databases without one overwriting
   * another's registered bean. Deliberately hashed from the *raw* {@code url}/{@code user} values,
   * not their resolved form - the bean name is just an internal identifier, never exposed, with no
   * reason to incorporate a resolved secret anywhere, however harmless that would practically be.
   */
  static String register(CamelBridge bridge, String url, Map<String, String> options) {
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException(
          "SQL connector requires a non-blank 'uri' in the spec's own source/sink definition");
    }
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setUrl(url);
    String driver = options.get("driver");
    if (driver != null) {
      dataSource.setDriverClassName(driver);
    }
    String user = SecretRefs.resolve(options, "user");
    if (user != null) {
      dataSource.setUsername(user);
    }
    String password = SecretRefs.resolve(options, "password");
    if (password != null) {
      dataSource.setPassword(password);
    }

    String beanName =
        "fdsSqlDataSource" + Integer.toHexString(Objects.hash(url, options.get("user")));
    bridge.camelContext().getRegistry().bind(beanName, dataSource);
    return beanName;
  }
}
