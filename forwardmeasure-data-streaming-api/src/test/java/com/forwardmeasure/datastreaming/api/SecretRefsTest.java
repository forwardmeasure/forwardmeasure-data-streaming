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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecretRefsTest {

  @Test
  void resolvesALiteralValueAsIs() {
    assertEquals(
        "plaintext-for-local-test-use",
        SecretRefs.resolve(Map.of("password", "plaintext-for-local-test-use"), "password"));
  }

  @Test
  void returnsNullForAnAbsentOption() {
    assertNull(SecretRefs.resolve(Map.of(), "password"));
  }

  @Test
  void resolvesAnEnvReferenceViaTheInjectedLookup() {
    String resolved =
        SecretRefs.resolve(
            Map.of("password", "env:DB_PASSWORD"),
            "password",
            name -> "DB_PASSWORD".equals(name) ? "s3cr3t" : null);

    assertEquals("s3cr3t", resolved);
  }

  @Test
  void throwsWhenTheReferencedEnvVariableIsNotSet() {
    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                SecretRefs.resolve(
                    Map.of("password", "env:MISSING_VAR"), "password", name -> null));

    assertEquals(
        "SecretRefs: environment variable 'MISSING_VAR' (referenced by option 'password') is not"
            + " set",
        failure.getMessage());
  }

  @Test
  void resolvesAFileReferenceByReadingItsContent(@TempDir Path tempDir) throws IOException {
    Path secretFile = tempDir.resolve("password.txt");
    Files.writeString(secretFile, "s3cr3t\n", StandardCharsets.UTF_8);

    String resolved = SecretRefs.resolve(Map.of("password", "file:" + secretFile), "password");

    assertEquals(
        "s3cr3t", resolved, "trailing newline from the mounted secret file must be stripped");
  }
}
