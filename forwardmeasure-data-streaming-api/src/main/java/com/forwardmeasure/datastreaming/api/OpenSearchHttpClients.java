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

import java.io.ByteArrayInputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * The HTTP client every OpenSearch sink path uses. A {@code caCertificate} option (PEM, resolved
 * through {@link SecretRefs} like the credentials, so {@code env:}/{@code file:} references work)
 * makes the client trust exactly that CA - the cluster's private CA - with hostname verification
 * still on. Without the option the JVM's default trust applies; like the credentials, an absent
 * option is the only way to say "none", and a reference to an empty secret fails fast.
 */
public final class OpenSearchHttpClients {

  private OpenSearchHttpClients() {}

  public static HttpClient create(Map<String, String> options) {
    HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15));
    String caCertificate = SecretRefs.resolve(options, "caCertificate");
    if (caCertificate != null) {
      builder.sslContext(trusting(caCertificate));
    }
    return builder.build();
  }

  static SSLContext trusting(String pem) {
    try {
      Collection<? extends Certificate> certificates =
          CertificateFactory.getInstance("X.509")
              .generateCertificates(
                  new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
      if (certificates.isEmpty()) {
        throw new IllegalArgumentException("caCertificate contains no certificate");
      }
      KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
      store.load(null, null);
      int index = 0;
      for (Certificate certificate : certificates) {
        store.setCertificateEntry("opensearch-ca-" + index++, certificate);
      }
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(store);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, factory.getTrustManagers(), null);
      return context;
    } catch (GeneralSecurityException | java.io.IOException failure) {
      throw new IllegalArgumentException(
          "caCertificate is not a readable PEM certificate", failure);
    }
  }
}
