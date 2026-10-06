/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.datastreaming.launcher.application;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Compile the shipped bundle, not a separately maintained workflow embedded in a fixture. */
class WorkflowBundleCompilationTest {
  private static final String BUNDLE = "https://bundle.example.test/fds";

  @Test
  void shippedIngestionWorkflowCompilesWithItsPinnedProtocolDocuments() throws IOException {
    compile("ingestion.yaml");
  }

  @Test
  void shippedStopWorkflowCompilesWithItsPinnedProtocolDocuments() throws IOException {
    compile("stop-ingestion.yaml");
  }

  private static void compile(String filename) throws IOException {
    var resources = List.of(protocol("ingestion-job.yaml"), protocol("ingestion-deployment.yaml"));
    byte[] source = read("/workflows/" + filename)
        .replace("@BUNDLE_DID@", BUNDLE).getBytes(StandardCharsets.UTF_8);
    assertDoesNotThrow(() -> new OpenWorkflowCompiler().compile(source, resources));
  }

  private static ResolvedWorkflowResource protocol(String filename) throws IOException {
    return ResolvedWorkflowResource.of(
        URI.create(BUNDLE + "/" + filename), "application/yaml", read("/asyncapi/" + filename));
  }

  private static String read(String path) throws IOException {
    try (var stream = WorkflowBundleCompilationTest.class.getResourceAsStream(path)) {
      if (stream == null) throw new IOException("Missing bundled resource: " + path);
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
