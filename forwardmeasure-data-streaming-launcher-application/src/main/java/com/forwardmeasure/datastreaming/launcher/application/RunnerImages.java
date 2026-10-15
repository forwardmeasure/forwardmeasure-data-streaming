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
package com.forwardmeasure.datastreaming.launcher.application;

import com.forwardmeasure.datastreaming.api.DeliveryEngineKind;
import java.util.Map;
import java.util.Objects;

/**
 * What a launcher dispatches on this installation: its one delivery engine (the platform {@code
 * engine} setting) and that engine's runner, plus the first-stage runners - the Kafka Streams
 * loader and the Spark stage - and the Kafka cluster their handoffs go through. Every installation
 * runs Spark, so a spec with a {@code HEAVY} transform runs on any installation.
 *
 * @param engine the installation's delivery engine
 * @param delivery the delivery engine's runner image and command
 * @param loader the bounded-load first stage; required on Kafka Streams, absent on Pekko
 * @param spark the Spark stage
 * @param kafkaBootstrapServers the cluster for loader and Spark handoffs
 * @param journal where a continuous Pekko worker keeps its state, or {@code null} when this
 *     launcher runs no continuous Pekko workers
 */
public record RunnerImages(
    DeliveryEngineKind engine,
    Runner delivery,
    Runner loader,
    Runner spark,
    String kafkaBootstrapServers,
    PekkoJournal journal) {

  /**
   * The installation's Pekko journal, as a continuous Pekko worker's environment: the platform
   * engine value, plain settings, and settings sourced from Secrets by name and key.
   */
  public record PekkoJournal(
      String platformEngine,
      Map<String, String> env,
      Map<String, SecretRef> secretEnv,
      DatabaseProxy databaseProxy) {
    public PekkoJournal {
      Objects.requireNonNull(platformEngine, "platformEngine");
      env = env == null ? Map.of() : Map.copyOf(env);
      secretEnv = secretEnv == null ? Map.of() : Map.copyOf(secretEnv);
    }
  }

  /**
   * A Cloud SQL Auth Proxy sidecar beside a continuous worker, so it reaches its tenant database's
   * journal on {@code localhost:5432} as every product's services do (GCP); absent elsewhere. The
   * worker runs as {@code serviceAccountName}, whose Workload Identity authorizes the proxy.
   */
  public record DatabaseProxy(String image, SecretRef instance, String serviceAccountName) {
    public DatabaseProxy {
      Objects.requireNonNull(image, "image");
      Objects.requireNonNull(instance, "instance");
      Objects.requireNonNull(serviceAccountName, "serviceAccountName");
    }
  }

  /** A Secret's name and key. */
  public record SecretRef(String name, String key) {
    public SecretRef {
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(key, "key");
    }
  }

  /** Runners without a journal: a launcher that runs no continuous Pekko workers. */
  public RunnerImages(
      DeliveryEngineKind engine,
      Runner delivery,
      Runner loader,
      Runner spark,
      String kafkaBootstrapServers) {
    this(engine, delivery, loader, spark, kafkaBootstrapServers, null);
  }

  /** One runner's image and the command that runs it. */
  public record Runner(String image, String command) {
    public Runner {
      Objects.requireNonNull(image, "image");
      Objects.requireNonNull(command, "command");
    }
  }

  public RunnerImages {
    Objects.requireNonNull(engine, "engine");
    Objects.requireNonNull(delivery, "delivery");
    if (engine == DeliveryEngineKind.KAFKA_STREAMS && loader == null) {
      throw new IllegalArgumentException(
          "a Kafka Streams installation needs the loader runner for bounded loads");
    }
    if (engine == DeliveryEngineKind.PEKKO_STREAMS && loader != null) {
      throw new IllegalArgumentException("a Pekko installation runs no loader stage");
    }
    Objects.requireNonNull(spark, "spark");
    if (kafkaBootstrapServers == null || kafkaBootstrapServers.isBlank()) {
      throw new IllegalArgumentException("loader and Spark handoffs need kafkaBootstrapServers");
    }
  }

  /**
   * The runners an installation configures, as the framework bindings read them: {@code
   * platformEngine} is the platform {@code engine} value and selects which delivery runner is used;
   * the other engine's settings may be absent. Blank values count as absent; the Spark runner and
   * Kafka bootstrap servers are always required.
   */
  public static RunnerImages configured(
      String platformEngine,
      String pekkoImage,
      String pekkoCommand,
      String kafkaStreamsImage,
      String kafkaStreamsCommand,
      String loaderImage,
      String loaderCommand,
      String sparkImage,
      String sparkCommand,
      String kafkaBootstrapServers,
      String journalEnv,
      String journalSecretEnv,
      String databaseProxyImage,
      String databaseProxyInstanceSecret,
      String databaseProxyServiceAccount) {
    DeliveryEngineKind engine = DeliveryEngineKind.fromPlatformEngine(platformEngine);
    Runner delivery =
        engine == DeliveryEngineKind.KAFKA_STREAMS
            ? required("kafka-streams", kafkaStreamsImage, kafkaStreamsCommand)
            : required("pekko", pekkoImage, pekkoCommand);
    Runner loader =
        engine == DeliveryEngineKind.KAFKA_STREAMS
            ? required("loader", loaderImage, loaderCommand)
            : null;
    Runner spark = required("spark", sparkImage, sparkCommand);
    PekkoJournal journal =
        engine == DeliveryEngineKind.PEKKO_STREAMS
            ? new PekkoJournal(
                platformEngine.strip(),
                assignments(journalEnv),
                secretAssignments(journalSecretEnv),
                blankToNull(databaseProxyImage) == null
                    ? null
                    : new DatabaseProxy(
                        databaseProxyImage.strip(),
                        secretAssignments("proxy=" + databaseProxyInstanceSecret).get("proxy"),
                        requiredText(
                            databaseProxyServiceAccount,
                            "datastreaming.launcher.journal.database-proxy.service-account")))
            : null;
    return new RunnerImages(
        engine, delivery, loader, spark, blankToNull(kafkaBootstrapServers), journal);
  }

  /** {@code NAME=value;NAME=value} - semicolon-separated, so a JDBC URL's commas survive. */
  static Map<String, String> assignments(String value) {
    Map<String, String> result = new java.util.LinkedHashMap<>();
    if (value == null || value.isBlank()) return result;
    for (String entry : value.split(";")) {
      if (entry.isBlank()) continue;
      int equals = entry.indexOf('=');
      if (equals <= 0)
        throw new IllegalArgumentException("Journal settings are NAME=value; entries: " + entry);
      result.put(entry.substring(0, equals).strip(), entry.substring(equals + 1).strip());
    }
    return result;
  }

  /** {@code NAME=secret/key;...} */
  static Map<String, SecretRef> secretAssignments(String value) {
    Map<String, SecretRef> result = new java.util.LinkedHashMap<>();
    assignments(value)
        .forEach(
            (name, reference) -> {
              int slash = reference.indexOf('/');
              if (slash <= 0 || slash == reference.length() - 1)
                throw new IllegalArgumentException(
                    "Journal secret settings are NAME=secret/key; entries: " + name);
              result.put(
                  name,
                  new SecretRef(reference.substring(0, slash), reference.substring(slash + 1)));
            });
    return result;
  }

  private static Runner required(String name, String image, String command) {
    Runner runner = present(name, image, command);
    if (runner == null) {
      throw new IllegalArgumentException(
          "datastreaming.launcher." + name + ".image and .command are required for this engine");
    }
    return runner;
  }

  /** Absent when no image is set (each runner's command has a default in the bindings' config). */
  private static Runner present(String name, String image, String command) {
    image = blankToNull(image);
    command = blankToNull(command);
    if (image == null) {
      return null;
    }
    if (command == null) {
      throw new IllegalArgumentException(
          "datastreaming.launcher." + name + ".command is required with its image");
    }
    return new Runner(image, command);
  }

  private static String requiredText(String value, String property) {
    String text = blankToNull(value);
    if (text == null) throw new IllegalArgumentException(property + " is required");
    return text;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.strip();
  }
}
