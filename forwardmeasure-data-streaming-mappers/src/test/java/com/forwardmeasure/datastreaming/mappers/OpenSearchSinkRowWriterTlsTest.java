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
package com.forwardmeasure.datastreaming.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.api.OpenSearchHttpClients;
import com.forwardmeasure.datastreaming.api.SinkSpec;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchContainerConfiguration;
import com.forwardmeasure.testcontainers.opensearch.OpenSearchTestContainer;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The OpenSearch sink against a cluster with the security plugin on: https with its own private CA
 * and basic auth. The {@code caCertificate} option is what makes the connection trusted; without it
 * the JVM's default trust rejects the cluster, and a reference to an unset secret fails fast.
 */
class OpenSearchSinkRowWriterTlsTest {
  private static OpenSearchTestContainer secured;

  @BeforeAll
  static void start() {
    secured =
        new OpenSearchTestContainer(
                OpenSearchContainerConfiguration.defaults().withSecurityEnabled())
            .start();
  }

  @AfterAll
  static void stop() {
    if (secured != null) secured.close();
  }

  private static SinkSpec sink(String index, Map<String, String> options) {
    return new SinkSpec(
        "opensearch", secured.hostEndpoint().toString(), index, null, null, options);
  }

  @Test
  void writesOverTlsVerifiedAgainstTheClusterCa() throws Exception {
    Map<String, String> options =
        Map.of(
            "idField", "uid",
            "user", secured.username(),
            "password", secured.password(),
            "caCertificate", secured.certificateAuthorityPem());
    try (var writer = new OpenSearchSinkRowWriter(sink("tls-delivery", options))) {
      writer.write(Map.of("uid", "3695", "name", "JOHN PAUL MANLEY"));
    }
    String basic =
        "Basic "
            + Base64.getEncoder()
                .encodeToString(
                    (secured.username() + ":" + secured.password())
                        .getBytes(StandardCharsets.UTF_8));
    try (var client = OpenSearchHttpClients.create(options)) {
      var stored =
          client.send(
              HttpRequest.newBuilder(secured.hostEndpoint().resolve("/tls-delivery/_doc/3695"))
                  .header("Authorization", basic)
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(200, stored.statusCode(), stored.body());
      assertTrue(stored.body().contains("JOHN PAUL MANLEY"));
    }
  }

  @Test
  void withoutTheCaTheClusterIsNotTrusted() {
    try (var writer =
        new OpenSearchSinkRowWriter(
            sink(
                "tls-untrusted",
                Map.of(
                    "idField",
                    "uid",
                    "user",
                    secured.username(),
                    "password",
                    secured.password())))) {
      var failure =
          assertThrows(IllegalStateException.class, () -> writer.write(Map.of("uid", "1")));
      Throwable cause = failure;
      while (cause != null && !(cause instanceof SSLHandshakeException)) cause = cause.getCause();
      assertTrue(cause instanceof SSLHandshakeException, () -> "not a TLS failure: " + failure);
    }
  }

  @Test
  void aCaReferenceToAnUnsetSecretFailsFast() {
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                new OpenSearchSinkRowWriter(
                    sink(
                        "tls-unset",
                        Map.of(
                            "idField",
                            "uid",
                            "caCertificate",
                            "env:FDS_TLS_TEST_CA_THAT_IS_NEVER_SET"))));
    assertTrue(failure.getMessage().contains("FDS_TLS_TEST_CA_THAT_IS_NEVER_SET"));
  }

  @Test
  void anUnreadableCaIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OpenSearchHttpClients.create(Map.of("caCertificate", "not a certificate")));
  }
}
