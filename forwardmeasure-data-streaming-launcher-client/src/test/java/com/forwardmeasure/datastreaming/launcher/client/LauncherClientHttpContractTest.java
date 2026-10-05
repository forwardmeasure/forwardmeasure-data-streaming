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
package com.forwardmeasure.datastreaming.launcher.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.launcher.client.api.IngestionRunsApi;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LauncherClientHttpContractTest {
  @Test
  void preservesTenantNamespaceAuthenticationAndProblemDetails() throws Exception {
    var requestUri = new AtomicReference<URI>();
    var authorization = new AtomicReference<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try (exchange) {
            requestUri.set(exchange.getRequestURI());
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            boolean denied = exchange.getRequestURI().getPath().endsWith("/denied");
            byte[] body =
                (denied
                        ? "{\"status\":403,\"detail\":\"Wrong tenant\"}"
                        : "{\"phase\":\"RUNNING\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange
                .getResponseHeaders()
                .set("Content-Type", denied ? "application/problem+json" : "application/json");
            exchange.sendResponseHeaders(denied ? 403 : 200, body.length);
            exchange.getResponseBody().write(body);
          }
        });
    server.start();
    var client =
        new ApiClient()
            .setBasePath("http://127.0.0.1:" + server.getAddress().getPort())
            .setBearerToken("fixture-token")
            .setConnectTimeout(2000)
            .setReadTimeout(2000);
    try (var http = client.getHttpClient()) {
      var api = new IngestionRunsApi(client);
      assertEquals(
          Map.of("phase", "RUNNING"), api.getIngestionRun("key/with space", "tenant-workers"));
      assertEquals("/ingestion-runs/key%2Fwith%20space", requestUri.get().getRawPath());
      assertEquals("namespace=tenant-workers", requestUri.get().getRawQuery());
      assertEquals("Bearer fixture-token", authorization.get());
      ApiException denied =
          assertThrows(ApiException.class, () -> api.getIngestionRun("denied", "other-workers"));
      assertEquals(403, denied.getCode());
      assertTrue(denied.getResponseBody().contains("Wrong tenant"));
    } finally {
      server.stop(0);
    }
  }
}
