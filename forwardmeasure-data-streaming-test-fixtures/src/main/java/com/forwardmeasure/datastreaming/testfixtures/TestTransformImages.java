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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.CodeSource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.testcontainers.DockerClientFactory;

/**
 * Builds a test variant of a production FDS executor image: the image unchanged, plus this module's
 * classes in {@code /deployments/extensions/}, which every executor jar's manifest puts on its
 * classpath. {@link TestTransformProvider} is then discovered inside the running Job exactly as a
 * product's provider would be. The production image never contains test code.
 */
public final class TestTransformImages {

  private static final Map<String, String> BUILT = new ConcurrentHashMap<>();

  private TestTransformImages() {}

  /**
   * The local name of {@code productionImage}'s test variant, building it on first use in this JVM.
   * Its repository is {@code productionImage}'s with {@code -test-transforms} appended; the tag is
   * unchanged.
   */
  public static String withTestTransforms(String productionImage) {
    return BUILT.computeIfAbsent(productionImage, TestTransformImages::build);
  }

  static String testImageName(String productionImage) {
    int slash = productionImage.lastIndexOf('/');
    int colon = productionImage.lastIndexOf(':');
    if (colon <= slash) {
      return productionImage + "-test-transforms";
    }
    return productionImage.substring(0, colon)
        + "-test-transforms"
        + productionImage.substring(colon);
  }

  private static String build(String productionImage) {
    String name = testImageName(productionImage);
    try {
      Path context = Files.createTempDirectory("fds-test-transforms-image");
      copyThisModule(context.resolve("extensions"));
      Files.writeString(
          context.resolve("Dockerfile"),
          "FROM "
              + productionImage
              + "\nCOPY --chown=10001:10001 extensions/ /deployments/extensions/\n");
      DockerClientFactory.instance()
          .client()
          .buildImageCmd(context.toFile())
          .withTags(Set.of(name))
          .withPull(false)
          .start()
          .awaitImageId();
      return name;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * This module's jar or classes directory. A Quarkus test class loader gives classes no code
   * source and serves resources under its own {@code quarkus:} scheme, so the fallback asks the
   * JVM's system class loader, which loads the test classpath itself: {@code jar:file:...!/...} for
   * a jar, {@code file:...} under the classes directory otherwise.
   */
  private static Path moduleLocation() {
    try {
      CodeSource source = TestTransformImages.class.getProtectionDomain().getCodeSource();
      if (source != null && source.getLocation() != null) {
        return Path.of(source.getLocation().toURI());
      }
      String resource = TestTransformImages.class.getName().replace('.', '/') + ".class";
      for (ClassLoader loader :
          new ClassLoader[] {
            ClassLoader.getSystemClassLoader(), TestTransformImages.class.getClassLoader()
          }) {
        URL self = loader == null ? null : loader.getResource(resource);
        if (self == null) continue;
        String url = self.toString();
        if (url.startsWith("jar:file:")) {
          return Path.of(URI.create(url.substring(4, url.indexOf("!/"))));
        }
        if (url.startsWith("file:")) {
          Path file = Path.of(self.toURI());
          for (int i = 0; i < resource.split("/").length; i++) {
            file = file.getParent();
          }
          return file;
        }
      }
      throw new IllegalStateException("Cannot locate the jar or classes directory of " + resource);
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  /** This module's classes and resources, from its jar or its classes directory. */
  private static void copyThisModule(Path target) throws IOException {
    Path location = moduleLocation();
    Files.createDirectories(target);
    if (Files.isDirectory(location)) {
      try (Stream<Path> files = Files.walk(location)) {
        for (Path file : (Iterable<Path>) files::iterator) {
          Path destination = target.resolve(location.relativize(file).toString());
          if (Files.isDirectory(file)) {
            Files.createDirectories(destination);
          } else {
            Files.copy(file, destination, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
      return;
    }
    try (InputStream jar = Files.newInputStream(location);
        ZipInputStream entries = new ZipInputStream(jar)) {
      for (ZipEntry entry; (entry = entries.getNextEntry()) != null; ) {
        Path destination = target.resolve(entry.getName()).normalize();
        if (!destination.startsWith(target)) {
          throw new IOException("jar entry outside the extension directory: " + entry.getName());
        }
        if (entry.isDirectory()) {
          Files.createDirectories(destination);
        } else {
          Files.createDirectories(destination.getParent());
          Files.copy(entries, destination, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }
}
