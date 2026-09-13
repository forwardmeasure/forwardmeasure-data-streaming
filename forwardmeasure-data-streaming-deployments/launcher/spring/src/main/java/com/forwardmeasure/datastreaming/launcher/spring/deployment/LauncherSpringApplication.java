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
package com.forwardmeasure.datastreaming.launcher.spring.deployment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The real bootable entry point - mirrors fowf's own {@code ExecutionManagementSpringApplication}.
 * {@code scanBasePackages} is required, not optional: this class lives in a sub-package of {@code
 * com.forwardmeasure.datastreaming.launcher.spring} (the actual bindings package, where {@code
 * LauncherSpringBinding} lives), and Spring Boot's default component scan only covers this class's
 * own package and below.
 */
@SpringBootApplication(scanBasePackages = "com.forwardmeasure.datastreaming")
public class LauncherSpringApplication {
  public static void main(String[] arguments) {
    SpringApplication.run(LauncherSpringApplication.class, arguments);
  }
}
