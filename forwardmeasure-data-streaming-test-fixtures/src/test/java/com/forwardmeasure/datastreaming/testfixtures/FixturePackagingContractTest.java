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
package com.forwardmeasure.datastreaming.testfixtures;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixturePackagingContractTest {
  @Test
  void packagedFixtureWithoutItsMappingCannotSilentlySupplyAnEmptyContract(@TempDir Path directory)
      throws Exception {
    for (Class<?> fixture : List.of(WorldCheckFixtures.class, TestCustomerMasterFixtures.class)) {
      Path jar = directory.resolve(fixture.getSimpleName() + ".jar");
      String entry = fixture.getName().replace('.', '/') + ".class";
      try (var output = new JarOutputStream(Files.newOutputStream(jar));
          var input = fixture.getClassLoader().getResourceAsStream(entry)) {
        output.putNextEntry(new JarEntry(entry));
        input.transferTo(output);
        output.closeEntry();
      }
      // Isolate the fixture artifact itself while retaining its normal API dependencies.
      try (var loader =
          new URLClassLoader(new URL[] {jar.toUri().toURL()}, fixture.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (!name.equals(fixture.getName())) return super.loadClass(name, resolve);
              Class<?> loaded = findLoadedClass(name);
              if (loaded == null) loaded = findClass(name);
              if (resolve) resolveClass(loaded);
              return loaded;
            }

            @Override
            public URL getResource(String name) {
              return name.startsWith("fixtures/") ? findResource(name) : super.getResource(name);
            }
          }) {
        Class<?> packaged = loader.loadClass(fixture.getName());
        var failure =
            assertThrows(
                InvocationTargetException.class,
                () -> packaged.getMethod("indexSettingsJson").invoke(null));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertTrue(failure.getCause().getMessage().contains("not found on classpath"));
        assertTrue(failure.getCause().getMessage().contains("opensearch-index-settings.json"));
      }
    }
  }
}
