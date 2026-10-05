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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MergePolicyFileBoundaryTest {
  @Test
  void policyFilesCannotEscapeTheConfiguredDirectory(@TempDir Path temp) throws Exception {
    Path root = Files.createDirectory(temp.resolve("policies"));
    Path outside = Files.writeString(temp.resolve("outside.yaml"), "version: 1\nfields: {}\n");
    Path allowed = Files.writeString(root.resolve("entity.yaml"), "version: 1\nfields: {}\n");
    assertEquals(1, MergePolicy.load(allowed.toUri().toString(), root).version());
    assertThrows(
        IllegalArgumentException.class, () -> MergePolicy.load(outside.toUri().toString(), root));
    Path link = Files.createSymbolicLink(root.resolve("escape.yaml"), outside);
    assertThrows(
        IllegalArgumentException.class, () -> MergePolicy.load(link.toUri().toString(), root));
  }
}
