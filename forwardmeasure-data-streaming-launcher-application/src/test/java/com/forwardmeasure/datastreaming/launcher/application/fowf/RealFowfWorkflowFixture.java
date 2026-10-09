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
package com.forwardmeasure.datastreaming.launcher.application.fowf;

import com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture;
import com.forwardmeasure.jpa.tenancy.Did;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.testcontainers.kafka.KafkaContainerConfiguration;
import com.forwardmeasure.testcontainers.kafka.KafkaTestContainer;
import com.forwardmeasure.testcontainers.kubernetes.KubernetesTestContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlContainerConfiguration;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Phase D (2026-09-21): a real, from-scratch fowf deployment - stood up fresh by these tests, not a
 * shared/prod environment - proving {@code WorkflowIngestionLauncher} genuinely dispatches {@code
 * CONTINUOUS} mode through a real fowf execution to a real Kubernetes {@code Deployment}. This
 * exact combination (execution-management + a real engine + the operation-adapter, all running
 * together as real separate processes, driving one real execution end to end) has never been built
 * or tested anywhere before this - not in fowf's own test suite, not as a documented local-dev
 * recipe. Built incrementally, stage by stage, each stage live-verified before the next is added -
 * see this class's own stage methods.
 *
 * <p>Uses actual locally built FOWF images. Execution-management supports explicit Quarkus, Spring
 * and Micronaut selection. The framework overload selects definition, execution, engine and
 * operation-adapter images consistently; startup logs record each actual image ID.
 *
 * <p><b>Tenant identity</b>: fowf's real onboarding convention is DID-first - {@code
 * did:web:<alias>.<domain>} - never an independently-chosen UUID (see {@code
 * com.forwardmeasure.openworkflow.migration.TenantOnboarding}'s own javadoc, {@code
 * com.forwardmeasure.jpa.tenancy.TenantId#forDid}). This fixture derives the same UUID the real
 * {@code openworkflow-migrations} job will derive, so both the Keycloak Organization ({@link
 * com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture#provisionTenant}) and the real fowf
 * tenant registry agree on one identity without either being read back from the other.
 *
 * <p><b>Auth, reusing what already works</b>: fowf's own {@code QuarkusActiveOrganizationProvider}
 * reads the identical {@code com.forwardmeasure.authzen.KeycloakOrganizationClaims} shape this
 * repo's own launcher authorization already uses (confirmed by direct inspection of fowf's own
 * source) - one {@link com.forwardmeasure.authzen.testkit.AuthzenKeycloakFixture} Keycloak
 * container serves as the trusted issuer for both this launcher's own {@code AuthorizationService}
 * check and the bearer token handed to fowf's real {@code ExecutionsApi}, rather than standing up
 * two separate Keycloak realms/fixtures for what is architecturally the same claim contract.
 *
 * <p><b>Networking</b>: every long-running service container shares one real Docker {@link Network}
 * with Postgres/Kafka (both already support a caller-supplied network + alias) reachable by their
 * own in-network alias. Keycloak and the K3s cluster ({@code
 * forwardmeasure-testcontainers-keycloak}/{@code -kubernetes}) do not support attaching to a
 * caller-supplied network today, so those two are reached via {@code host.docker.internal} (real
 * Docker host-gateway DNS, {@code --add-host=host.docker.internal:host-gateway}) instead - the K3s
 * kubeconfig's own server URL is rewritten from its host-reachable form to that address before
 * being mounted into the operation-adapter container.
 */
public final class RealFowfWorkflowFixture implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealFowfWorkflowFixture.class);

  // The reactor builds these test-image dependencies before this module. Use the platform's
  // selected version (passed by Surefire), not a hard-coded or retired combined adapter image.
  private static final String FOWF_IMAGE_TAG =
      Objects.requireNonNull(
          System.getProperty("openworkflow.image.tag"), "openworkflow.image.tag");
  private static final String MIGRATIONS_IMAGE =
      "forwardmeasure/openworkflow-migrations:" + FOWF_IMAGE_TAG;
  static final String EXECUTION_MANAGEMENT_IMAGE =
      "forwardmeasure/openworkflow-execution-management-quarkus:" + FOWF_IMAGE_TAG;
  static final String ENGINE_KAFKA_STREAMS_IMAGE =
      "forwardmeasure/openworkflow-engine-kafka-streams-quarkus:" + FOWF_IMAGE_TAG;
  static final String ENGINE_PEKKO_IMAGE =
      "forwardmeasure/openworkflow-engine-pekko-quarkus:" + FOWF_IMAGE_TAG;
  static final String DEFINITION_MANAGEMENT_IMAGE =
      "forwardmeasure/openworkflow-definition-management-quarkus:" + FOWF_IMAGE_TAG;
  static final String OPERATION_ADAPTER_KAFKA_IMAGE =
      "forwardmeasure/openworkflow-operation-adapter-kafka-streams-quarkus:" + FOWF_IMAGE_TAG;
  static final String OPERATION_ADAPTER_PEKKO_IMAGE =
      "forwardmeasure/openworkflow-operation-adapter-pekko-quarkus:" + FOWF_IMAGE_TAG;

  private static final String POSTGRES_ALIAS = "postgres";
  private static final String KAFKA_ALIAS = "kafka";
  static final String EXECUTION_MANAGEMENT_ALIAS = "openworkflow-execution-management-quarkus";
  static final String ENGINE_KAFKA_STREAMS_ALIAS = "openworkflow-engine-kafka-streams-quarkus";
  static final String ENGINE_PEKKO_ALIAS = "openworkflow-engine-pekko-quarkus";
  static final String DEFINITION_MANAGEMENT_ALIAS = "openworkflow-definition-management-quarkus";
  static final String OPERATION_ADAPTER_ALIAS = "openworkflow-operation-adapter-quarkus";
  // Real, confirmed live 2026-09-24: this fixture's own startEnginePekko() previously pointed
  // OPENWORKFLOW_CLOUD_EVENTS_PUBLISH_URL (HttpCloudEventPublisher's one fixed publish:emit:
  // delivery target for this whole tenant, confirmed via that class's own source) directly at
  // execution-management's internal /internal/v1/execution-events/events - a structurally
  // different endpoint (application/json only, per its own generated @Consumes) that an earlier
  // session's own fix only patched the path segment of (a real 404), not the deeper mismatch: real
  // CloudEvents structured-mode delivery sends Content-Type: application/cloudevents+json, which
  // that endpoint correctly rejects with 415. The Pekko Projection delivering it then permanently
  // fails after 21 retries and stops, which also cancels this scenario's own correlated-worker
  // "events" leg riding the same eventing infrastructure - the execution then never reaches a
  // terminal state. This workflow genuinely does emit a real publish:emit: CloudEvent, so the fix
  // is a real receiver, not removing the config: a minimal nginx stand-in (204 for any request),
  // mirroring the identical, already-proven pattern in fowf's own
  // AbstractPekkoEngineK3sVerificationTest#executionEventsStubManifest.
  private static final String CLOUD_EVENTS_STUB_ALIAS = "openworkflow-cloud-events-stub";
  // Real, shared Pekko Cluster Bootstrap wiring between engine-pekko and operation-adapter - see
  // startEnginePekko's own comment for why this is needed. The service name is just the config
  // key pekko-discovery's "config" method groups static endpoints under
  // (pekko.discovery.config.services.<name>.endpoints) - not a real DNS name.
  private static final String CLUSTER_SERVICE_NAME = "openworkflow-pekko-cluster";
  private static final int CLUSTER_MANAGEMENT_PORT = 8558;
  private static final String KUBECONFIG_CONTAINER_PATH = "/deployments/kubeconfig.yaml";
  private static final String RUNTIME_DATABASE_USERNAME = "openworkflow_runtime";
  private static final String RUNTIME_DATABASE_PASSWORD = "openworkflow-runtime-test-only";

  /**
   * The domain every tenant onboarded by this fixture shares - a per-cluster value, not real DNS.
   */
  public static final String TENANT_DOMAIN = "fds-phase-d.test";

  private final Network network;
  private final PostgreSqlTestContainer postgres;
  private final KafkaTestContainer kafka;
  private final AuthzenKeycloakFixture keycloak;
  private final TenantId tenantId;
  private final String organizationId;
  private final List<GenericContainer<?>> services = new java.util.ArrayList<>();
  private final List<com.sun.net.httpserver.HttpServer> documentServers =
      new java.util.ArrayList<>();
  private KubernetesTestContainer kubernetes;
  private Framework framework = Framework.QUARKUS;
  private boolean networkIdentity;
  private java.util.Map<String, String> overflowEnvironment = java.util.Map.of();

  /** Configures the production storage client before any runtime service has started. */
  public void configureOverflow(String endpoint, String bucket, long threshold, long maximum) {
    if (!services.isEmpty())
      throw new IllegalStateException("Configure overflow before runtime startup");
    if (threshold < 1 || maximum <= threshold)
      throw new IllegalArgumentException("Invalid overflow bounds");
    overflowEnvironment =
        java.util.Map.of(
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_STORAGE_BACKEND",
            "gcs",
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_STORAGE_ENDPOINT",
            endpoint,
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_STORAGE_BUCKET",
            bucket,
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_STORAGE_KEY_PREFIX",
            "runtime-acceptance",
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_OFFLOAD_THRESHOLD_BYTES",
            Long.toString(threshold),
            "OPENWORKFLOW_OPERATIONS_PROTOCOL_MAX_RESPONSE_BYTES",
            Long.toString(maximum));
  }

  private com.forwardmeasure.testcontainers.cassandra.CassandraTestContainer cassandra;
  private String tenantAlias;
  private final List<String> additionalTenantAliases = new java.util.ArrayList<>();
  private PekkoPersistence pekkoPersistence =
      PekkoPersistence.valueOf(
          System.getProperty("fds.acceptance.pekko.persistence", "POSTGRESQL")
              .toUpperCase(java.util.Locale.ROOT));

  public enum PekkoPersistence {
    POSTGRESQL,
    CASSANDRA
  }

  private RealFowfWorkflowFixture(
      Network network,
      PostgreSqlTestContainer postgres,
      KafkaTestContainer kafka,
      AuthzenKeycloakFixture keycloak,
      TenantId tenantId,
      String organizationId) {
    this.network = Objects.requireNonNull(network, "network");
    this.postgres = Objects.requireNonNull(postgres, "postgres");
    this.kafka = Objects.requireNonNull(kafka, "kafka");
    this.keycloak = Objects.requireNonNull(keycloak, "keycloak");
    this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
    this.organizationId = Objects.requireNonNull(organizationId, "organizationId");
  }

  /**
   * Stage 1+2: real Postgres + the real {@code openworkflow-migrations} job (onboards {@code
   * tenantAlias} as a real row in fowf's own tenant registry, a real per-tenant database, a real
   * runtime role), plus a real Keycloak Organization carrying the identical, independently-derived
   * {@link TenantId} - {@link #tenantId()} - so a JWT minted against that Organization resolves to
   * the exact same tenant every downstream fowf service (execution-management, the engine, the
   * operation-adapter) already has registered, with no read-back between the two systems. {@code
   * role} is granted on {@value AuthzenKeycloakFixture#CLIENT_ID} for the minted actor - callers
   * still grant whatever real AuthZEN resource/scope permissions their own scenario needs via
   * {@link #keycloak()}.
   */
  /** Selects one actual framework consistently for definition, execution, engine and adapter. */
  public static RealFowfWorkflowFixture start(
      String tenantAlias, String role, Framework framework) {
    Objects.requireNonNull(framework, "framework");
    var fixture = start(tenantAlias, role);
    fixture.framework = framework;
    return fixture;
  }

  public static RealFowfWorkflowFixture start(
      String tenantAlias, String role, Framework framework, PekkoPersistence persistence) {
    Objects.requireNonNull(persistence, "persistence");
    var fixture = start(tenantAlias, role, framework);
    fixture.pekkoPersistence = persistence;
    return fixture;
  }

  public static RealFowfWorkflowFixture start(String tenantAlias, String role) {
    return start(tenantAlias, role, false);
  }

  /** Uses one network-reachable issuer for products that also call its AuthZEN endpoint. */
  public static RealFowfWorkflowFixture startWithNetworkIdentity(
      String alias, String role, Framework framework, PekkoPersistence persistence) {
    var fixture = start(alias, role, true);
    fixture.framework = Objects.requireNonNull(framework);
    fixture.pekkoPersistence = Objects.requireNonNull(persistence);
    return fixture;
  }

  private static RealFowfWorkflowFixture start(
      String tenantAlias, String role, boolean networkIdentity) {
    Objects.requireNonNull(tenantAlias, "tenantAlias");
    Objects.requireNonNull(role, "role");
    Network network = Network.newNetwork();
    List<AutoCloseable> started = new java.util.ArrayList<>();
    started.add(network);
    try {
      PostgreSqlTestContainer postgres =
          new PostgreSqlTestContainer(
                  PostgreSqlContainerConfiguration.defaults()
                      .withNetwork(network.getId(), List.of(POSTGRES_ALIAS)))
              .start();
      started.add(postgres);
      runMigrations(network, postgres, tenantAlias);
      KafkaTestContainer kafka =
          new KafkaTestContainer(
                  KafkaContainerConfiguration.defaults()
                      .withNetwork(network.getId(), List.of(KAFKA_ALIAS))
                      .withHostDockerInternalListener())
              .start();

      started.add(kafka);
      TenantId tenantId = deriveTenantId(tenantAlias);
      AuthzenKeycloakFixture keycloak =
          networkIdentity
              ? AuthzenKeycloakFixture.start(existingNetwork(network.getId()), "keycloak")
              : AuthzenKeycloakFixture.start();
      started.add(keycloak);
      String organizationId =
          keycloak.provisionTenant(
              tenantAlias, new Did("did:web:" + tenantAlias + "." + TENANT_DOMAIN), role);
      LOGGER.info(
          "RealFowfWorkflowFixture: provisioned Keycloak Organization '{}' for tenantId={}"
              + " (matches the real openworkflow-migrations tenant registry row)",
          organizationId,
          tenantId.value());
      grantRuntimePermissions(keycloak, organizationId, role);
      var fixture =
          new RealFowfWorkflowFixture(network, postgres, kafka, keycloak, tenantId, organizationId);
      fixture.tenantAlias = tenantAlias;
      fixture.networkIdentity = networkIdentity;
      return fixture;
    } catch (RuntimeException | Error failure) {
      for (AutoCloseable resource : started.reversed()) {
        try {
          resource.close();
        } catch (Exception cleanupFailure) {
          failure.addSuppressed(cleanupFailure);
        }
      }
      throw failure;
    }
  }

  /**
   * Stage 3: the real {@code openworkflow-execution-management-quarkus} image, pointed at this
   * fixture's already-running Postgres + Keycloak. Kept running until {@link #close()} - callers
   * get the container back to build a real {@code ExecutionsApi} base URL from its mapped port.
   *
   * <p>{@code OPENWORKFLOW_ORGANIZATION_CLIENT_ID} is overridden from its real production default
   * ({@code forwardmeasure-public}) to {@link AuthzenKeycloakFixture#CLIENT_ID} - this fixture's
   * Keycloak Organization grants roles on that client (see {@link #start}), not a {@code
   * forwardmeasure-public} client this fixture never creates; without this override, {@code
   * QuarkusActiveOrganizationProvider} would look for the active-Organization role claim under a
   * client that doesn't exist here and reject every request.
   *
   * <p>Runs against the Kafka-Streams engine - see {@link #startExecutionManagement(String)} for
   * the Pekko engine (real fowf parity requirement: both engines share the identical {@code
   * kubernetes-deployment} dispatch path via {@code KafkaProtocolOperationExecutors.create(...)},
   * so both need real end-to-end coverage, not just the one this fixture happened to build first).
   */
  public GenericContainer<?> startExecutionManagement() {
    return startExecutionManagement("kafka-streams");
  }

  /** Packaged service framework and its real readiness endpoint. */
  public enum Framework {
    QUARKUS("quarkus", "/q/health/ready"),
    SPRING("spring", "/actuator/health/readiness"),
    MICRONAUT("micronaut", "/health/readiness");

    private final String imageSuffix;
    private final String readinessPath;

    Framework(String imageSuffix, String readinessPath) {
      this.imageSuffix = imageSuffix;
      this.readinessPath = readinessPath;
    }
  }

  /**
   * @param engine {@code "kafka-streams"} or {@code "pekko"} - the one engine execution-management
   *     runs ({@code OPENWORKFLOW_ENGINE_ID}), reached at that engine container's own alias.
   */
  public GenericContainer<?> startExecutionManagement(String engine) {
    return startExecutionManagement(engine, framework);
  }

  /** Selects the actual packaged execution API; this alone does not select an engine framework. */
  public GenericContainer<?> startExecutionManagement(String engine, Framework framework) {
    Objects.requireNonNull(framework, "framework");
    String engineAlias = "pekko".equals(engine) ? ENGINE_PEKKO_ALIAS : ENGINE_KAFKA_STREAMS_ALIAS;
    String issuer = hostDockerInternalIssuer();
    GenericContainer<?> executionManagement =
        new GenericContainer<>(
                DockerImageName.parse(selectedImage(EXECUTION_MANAGEMENT_IMAGE, framework)))
            .withImagePullPolicy(image -> false)
            .withCreateContainerCmdModifier(
                command -> command.getHostConfig().withMemory(2L * 1024 * 1024 * 1024))
            .withEnv("JAVA_TOOL_OPTIONS", "-Xmx1g")
            .withEnv("OPENWORKFLOW_TENANT_DOMAIN", TENANT_DOMAIN)
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(EXECUTION_MANAGEMENT_ALIAS)
            .withExtraHost("host.docker.internal", "host-gateway")
            .withExposedPorts(8080)
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            // forwardmeasure.jpa.tenant-database.{host,port} - a real, SEPARATE config path from
            // the bootstrap quarkus.datasource above (TenantDataSourceRegistry's own per-tenant
            // connection routing) - defaults to localhost:5432 if left unset, which is wrong
            // inside this container (confirmed live: "Connection to localhost:5432 refused" on
            // the first real request). fowf's own real Helm template derives these from
            // OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL via a regex at render time; this fixture just
            // sets them
            // directly since it already knows the real values.
            .withEnv("OPENWORKFLOW_TENANT_DATABASE_HOST", POSTGRES_ALIAS)
            .withEnv("OPENWORKFLOW_TENANT_DATABASE_PORT", "5432")
            .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", issuer)
            // Discovery/JWKS use the container-reachable address, while the JWT must retain
            // the exact issuer used to mint the fixture's real tokens on the host.
            .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", expectedIssuer())
            .withEnv("OPENWORKFLOW_CLIENT_ID", AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID)
            .withEnv("OPENWORKFLOW_CLIENT_SECRET", AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET)
            .withEnv("OPENWORKFLOW_ORGANIZATION_CLIENT_ID", AuthzenKeycloakFixture.CLIENT_ID)
            .withEnv("OPENWORKFLOW_ENGINE_ID", engine)
            .withEnv(
                "OPENWORKFLOW_ENGINE_URL", "http://" + engineAlias + ":8080/internal/v1/engine/")
            .withLogConsumer(
                new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-execution-management"))
            .waitingFor(
                Wait.forHttp(framework.readinessPath)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    configureFramework(executionManagement, framework);
    // Register before startup so failed readiness also closes the partially started container.
    services.add(executionManagement);
    executionManagement.start();
    LOGGER.info(
        "Execution API framework={} image={} imageId={}",
        framework,
        executionManagement.getDockerImageName(),
        executionManagement.getContainerInfo().getImageId());
    return executionManagement;
  }

  /** Onboards another real tenant without seeding definitions, executions or result rows. */
  public String provisionAdditionalTenant(String alias, String role) {
    runMigrations(network, postgres, alias);
    additionalTenantAliases.add(alias);
    if (cassandra != null) migrateCassandraTenant(alias);
    String organization =
        keycloak.provisionTenant(alias, new Did("did:web:" + alias + "." + TENANT_DOMAIN), role);
    grantRuntimePermissions(keycloak, organization, role);
    return organization;
  }

  private static void grantRuntimePermissions(
      AuthzenKeycloakFixture keycloak, String organizationId, String role) {
    // fowf's own services run a SECOND, server-side AuthZEN check independent of this launcher's
    // own (AuthzenExecutionAuthorizer/WorkflowGovernanceServiceImpl, both confirmed by direct
    // source read) - real resource type/collection-id pairs from fowf's own
    // OpenWorkflowAuthorizationResources (the varying execution/definition id is only ever a
    // resource *property*, never part of the Keycloak resource's own name - same collection-
    // scoped-not-instance-scoped shape this repo's own DataStreamingAuthorizationResources uses).
    keycloak.grantResourceAuthorization(
        organizationId,
        "openworkflow-execution",
        "executions",
        "fixture-execution-permission",
        role,
        java.util.Set.of(
            AuthorizationAction.EXECUTION_START.scope(),
            AuthorizationAction.EXECUTION_READ.scope(),
            AuthorizationAction.EXECUTION_LIST.scope(),
            AuthorizationAction.EXECUTION_PAUSE.scope(),
            AuthorizationAction.EXECUTION_RESUME.scope(),
            AuthorizationAction.EXECUTION_CANCEL.scope(),
            // WorkflowIngestionLauncher (wired 2026-09-26) attaches subjectActor to every real
            // Start/Control call - fowf's server-side WorkflowExecutionManagementService.
            // resolveSubjectActor gates this on execution:assert-subject, fail-closed, regardless
            // of whether the caller already holds EXECUTION_START/etc. Without this grant every
            // real launch()/observe()/cancel() call 500s with AuthorizationDeniedException.
            AuthorizationAction.EXECUTION_ASSERT_SUBJECT.scope()));
    keycloak.grantResourceAuthorization(
        organizationId,
        "openworkflow-definition",
        "definitions",
        "fixture-definition-permission",
        role,
        java.util.Set.of(
            AuthorizationAction.DEFINITION_CREATE.scope(),
            AuthorizationAction.DEFINITION_READ.scope(),
            AuthorizationAction.DEFINITION_LIST.scope(),
            AuthorizationAction.DEFINITION_VALIDATE.scope(),
            AuthorizationAction.DEFINITION_PUBLISH.scope()));
    // A THIRD, real server-side check - independent of the two above - runs inside the
    // operation-adapter itself (AuthzenOperationSecurityResolver, confirmed by direct source
    // read), for every operation it dispatches (e.g. this fixture's own kubernetes-deployment
    // apply/watch steps): OpenWorkflowAuthorizationResources.operation(...) resolves to a
    // collection-scoped "openworkflow-operation"/"operations" resource (operation_kind is only
    // ever a resource *property*, matching the execution/definition resources' own shape) and
    // requires AuthorizationAction.OPERATION_EXECUTE. Without this, any real workflow execution
    // that reaches a call: asyncapi step fails with "Authorization denied" the moment the
    // operation-adapter picks it up off Kafka - found live 2026-09-21 driving the first real
    // end-to-end execution through this fixture.
    keycloak.grantResourceAuthorization(
        organizationId,
        "openworkflow-operation",
        "operations",
        "fixture-operation-permission",
        role,
        java.util.Set.of(AuthorizationAction.OPERATION_EXECUTE.scope()));
  }

  private void ensurePekkoPersistence() {
    if (pekkoPersistence != PekkoPersistence.CASSANDRA || cassandra != null) return;
    cassandra =
        new com.forwardmeasure.testcontainers.cassandra.CassandraTestContainer(
                existingNetwork(network.getId()), "cassandra")
            .start();
    // Provision through the same migration image as deployment; never let engine startup invent
    // schema.
    migrateCassandraTenant(tenantAlias);
    additionalTenantAliases.forEach(this::migrateCassandraTenant);
  }

  private void migrateCassandraTenant(String alias) {
    runMigrations(
        network,
        postgres,
        alias,
        java.util.Map.of(
            "OPENWORKFLOW_CASSANDRA_CONTACT_POINTS",
            "cassandra:9042",
            "OPENWORKFLOW_CASSANDRA_LOCAL_DATACENTER",
            cassandra.localDatacenter()));
  }

  private void configurePekkoPersistence(GenericContainer<?> service) {
    service.withEnv(
        "OPENWORKFLOW_PERSISTENCE_PROFILE",
        pekkoPersistence.name().toLowerCase(java.util.Locale.ROOT));
    if (pekkoPersistence == PekkoPersistence.CASSANDRA) {
      service
          .withEnv("OPENWORKFLOW_PERSISTENCE_ENDPOINT", "cassandra:9042")
          .withEnv("OPENWORKFLOW_PERSISTENCE_LOCAL_DATACENTER", cassandra.localDatacenter())
          .withEnv("OPENWORKFLOW_PERSISTENCE_USERNAME", "")
          .withEnv("OPENWORKFLOW_PERSISTENCE_PASSWORD", "");
    }
  }

  private String frameworkImage(String quarkusImage) {
    return selectedImage(quarkusImage, framework);
  }

  private static String selectedImage(String quarkusImage, Framework framework) {
    String component =
        quarkusImage.substring(
            "forwardmeasure/openworkflow-".length(), quarkusImage.indexOf("-quarkus:"));
    return System.getProperty(
        "openworkflow.acceptance." + component + "." + framework.imageSuffix + ".image",
        quarkusImage.replace("-quarkus:", "-" + framework.imageSuffix + ":"));
  }

  private void configureFramework(GenericContainer<?> service, Framework framework) {
    String issuer = hostDockerInternalIssuer();
    service
        .withImagePullPolicy(image -> false)
        .withCreateContainerCmdModifier(
            command -> command.getHostConfig().withMemory(2L * 1024 * 1024 * 1024))
        .withEnv("JAVA_TOOL_OPTIONS", "-Xmx1g")
        .withEnv("OPENWORKFLOW_TENANT_DOMAIN", TENANT_DOMAIN)
        .withEnv("OPENWORKFLOW_TENANT_DATABASE_HOST", POSTGRES_ALIAS)
        .withEnv("OPENWORKFLOW_TENANT_DATABASE_PORT", "5432")
        .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", issuer)
        .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", expectedIssuer());
    String expectedIssuer = expectedIssuer();
    if (framework == Framework.SPRING) {
      service
          .withEnv("MANAGEMENT_ENDPOINT_HEALTH_PROBES_ENABLED", "true")
          .withEnv("SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_ISSUER_URI", expectedIssuer)
          .withEnv(
              "SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWK_SET_URI",
              issuer + "/protocol/openid-connect/certs");
    } else if (framework == Framework.MICRONAUT) {
      // Keep the application's expected issuer setting while routing JWKS/AuthZEN over Docker.
      // Do not add a test-only claims validator: the packaged application must own validation.
      service
          .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", expectedIssuer)
          .withEnv(
              "JAVA_TOOL_OPTIONS",
              "-Xmx1g"
                  + " -Dopenworkflow.authorization.issuer="
                  + issuer
                  + " -Dmicronaut.security.token.jwt.signatures.jwks.keycloak.url="
                  + issuer
                  + "/protocol/openid-connect/certs");
    }
  }

  private void startFrameworkService(GenericContainer<?> service, boolean waitForHealth) {
    configureFramework(service, framework);
    service.withEnv(overflowEnvironment);
    // Pekko engine/adapter bootstrap requires both members, so do not wait for cluster readiness
    // before the other member starts. The caller must still awaitPekkoClusterReady before dispatch.
    service.waitingFor(
        waitForHealth
            ? Wait.forHttp(framework.readinessPath)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(3))
            : Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    services.add(service);
    service.start();
    LOGGER.info(
        "Runtime framework={} image={} imageId={}",
        framework,
        service.getDockerImageName(),
        service.getContainerInfo().getImageId());
  }

  /**
   * Stage 4: the real {@code openworkflow-engine-kafka-streams-quarkus} image - genuinely simpler
   * to wire than execution-management (confirmed by direct read of its own real {@code
   * application.yaml}: {@code hibernate-orm.enabled: false}, no datasource/tenant-schema config at
   * all - it talks to Postgres/tenant data only indirectly, through execution-management's own
   * {@code /internal/v1/execution-events/} callback). Needs a real Kafka broker for its own durable
   * processing (changelog/state-store topics), reachable at {@link #kafka()}'s network alias, and a
   * real path back to execution-management via {@link #EXECUTION_MANAGEMENT_ALIAS} - the two
   * services call each other's own {@code /internal/v1/...} endpoints directly.
   */
  public GenericContainer<?> startEngineKafkaStreams() {
    String issuer = hostDockerInternalIssuer();
    GenericContainer<?> engine =
        new GenericContainer<>(DockerImageName.parse(frameworkImage(ENGINE_KAFKA_STREAMS_IMAGE)))
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(ENGINE_KAFKA_STREAMS_ALIAS)
            .withExtraHost("host.docker.internal", "host-gateway")
            .withExposedPorts(8080)
            // Same real missing-"/events"-segment bug as startEnginePekko's own
            // OPENWORKFLOW_CLOUD_EVENTS_PUBLISH_URL - see that method's own comment. This engine's
            // real terminal-state reporting apparently doesn't depend on this specific publish
            // succeeding (confirmed live: this engine's own test instance reaches COMPLETED even
            // with the broken URL), but the URL itself was still wrong - fixed for correctness/
            // consistency regardless of whether anything currently depends on it.
            .withEnv(
                "OPENWORKFLOW_EXECUTION_EVENTS_URL",
                "http://"
                    + EXECUTION_MANAGEMENT_ALIAS
                    + ":8080/internal/v1/execution-events/events")
            .withEnv("OPENWORKFLOW_KAFKA_BOOTSTRAP_SERVERS", kafka.networkBootstrapServers())
            // This engine's own application.yaml declares no forwardmeasure.jpa.* section at all
            // (hibernate-orm.enabled: false, per its own javadoc - it never actually touches
            // Postgres) - but the forwardmeasure-jpa-quarkus binding it still transitively pulls
            // in for ActiveOrganizationProvider registers a mandatory (no-default)
            // forwardmeasure.jpa.tenant-database.* config mapping regardless, which fails
            // startup outright if unset (confirmed live: "Failed to load config value... for:
            // forwardmeasure.jpa.tenant-database.host/username/password,
            // forwardmeasure.jpa.functional-schema"). Real values, even though this engine never
            // functionally uses them - SmallRye Config's own direct-property-name env var form,
            // since (unlike execution-management) this app's own application.yaml never declares
            // an OPENWORKFLOW_DATABASE_* indirection for them to read.
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_HOST", POSTGRES_ALIAS)
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_PORT", "5432")
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("FORWARDMEASURE_JPA_FUNCTIONAL_SCHEMA", "OPENWORKFLOW")
            .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", issuer)
            .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", expectedIssuer())
            .withEnv("OPENWORKFLOW_CLIENT_ID", AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID)
            .withEnv("OPENWORKFLOW_CLIENT_SECRET", AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET)
            .withEnv("OPENWORKFLOW_ORGANIZATION_CLIENT_ID", AuthzenKeycloakFixture.CLIENT_ID)
            // New diagnostic logging added 2026-09-23 to WorkflowExecutionEngine (debug-level, so
            // invisible at Quarkus's default INFO root level) to actually see where the real
            // watchStreamWorker stall happens, rather than continuing to guess from indirect
            // evidence - matches this session's own established "raise the category, don't guess"
            // pattern already used for HikariCP/Pekko persistence log-volume issues.
            .withEnv(
                "QUARKUS_LOG_CATEGORY__COM_FORWARDMEASURE_OPENWORKFLOW_WORKFLOW_RUNTIME_CORE__LEVEL",
                "DEBUG")
            .withLogConsumer(
                new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-engine-kafka-streams"))
            .waitingFor(
                Wait.forLogMessage(".*started in.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    startFrameworkService(engine, false);
    return engine;
  }

  /**
   * Minimal, real stand-in for a genuine {@code publish:emit:} CloudEvents subscriber - returns 204
   * for any request on port 8080, no auth/state needed since this scenario never inspects the
   * delivered event's own content, only that delivery succeeds so the Pekko Projection driving it
   * doesn't permanently fail. Mirrors fowf's own already-proven {@code
   * AbstractPekkoEngineK3sVerificationTest#executionEventsStubManifest} nginx pattern exactly, just
   * expressed as a Testcontainers {@code GenericContainer} instead of a K8s manifest. Idempotent -
   * safe to call once per fixture even though only {@link #startEnginePekko()} currently needs it.
   */
  private void startCloudEventsStub() {
    String config = "server { listen 8080; location / { return 204; } }\n";
    GenericContainer<?> stub =
        new GenericContainer<>(DockerImageName.parse("nginx:alpine"))
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(CLOUD_EVENTS_STUB_ALIAS)
            .withExposedPorts(8080)
            .withCopyToContainer(
                Transferable.of(config.getBytes(StandardCharsets.UTF_8)),
                "/etc/nginx/conf.d/default.conf")
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));
    stub.start();
    services.add(stub);
  }

  public GenericContainer<?> startEnginePekko() {
    ensurePekkoPersistence();
    startCloudEventsStub();
    String issuer = hostDockerInternalIssuer();
    GenericContainer<?> engine =
        new GenericContainer<>(DockerImageName.parse(frameworkImage(ENGINE_PEKKO_IMAGE)))
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(ENGINE_PEKKO_ALIAS)
            .withExtraHost("host.docker.internal", "host-gateway")
            .withExposedPorts(8080)
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_PERSISTENCE_ENDPOINT", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_PERSISTENCE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_PERSISTENCE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_USERNAME", postgres.username())
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_PASSWORD", postgres.password())
            // Lifecycle events go to the real execution API. CloudEvents publication below
            // is a separate protocol and must not replace this persistence/status callback.
            .withEnv(
                "OPENWORKFLOW_EXECUTION_EVENTS_URL",
                "http://" + EXECUTION_MANAGEMENT_ALIAS + ":8080")
            .withEnv("OPENWORKFLOW_CLOUD_EVENTS_TRANSPORT", "http")
            // Real, live-caught bug (2026-09-23), corrected again 2026-09-24: an earlier fix here
            // added the missing "events" path segment to stop a real, permanent HTTP 404
            // (ExecutionEventResource's real route is POST /internal/v1/execution-events/events),
            // but that endpoint was always the wrong target regardless of path - it's
            // execution-management's own internal admission API (application/json only, per its
            // own generated @Consumes), not a real CloudEvents receiver. HttpCloudEventPublisher
            // sends real structured-mode CloudEvents (Content-Type: application/cloudevents+json),
            // which that endpoint correctly rejected with HTTP 415 once the path was fixed enough
            // to actually reach it - the Pekko Projection delivering it then permanently failed
            // after 21 retries, which also cancelled this scenario's own correlated-worker "events"
            // leg riding the same eventing infrastructure. This workflow genuinely emits a real
            // publish:emit: CloudEvent, so the fix is a real receiver (see startCloudEventsStub(),
            // called at the top of this method), not removing this config.
            .withEnv(
                "OPENWORKFLOW_CLOUD_EVENTS_PUBLISH_URL",
                "http://" + CLOUD_EVENTS_STUB_ALIAS + ":8080/")
            .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", issuer)
            .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", expectedIssuer())
            .withEnv("OPENWORKFLOW_CLIENT_ID", AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID)
            .withEnv("OPENWORKFLOW_CLIENT_SECRET", AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET)
            .withEnv("OPENWORKFLOW_ORGANIZATION_CLIENT_ID", AuthzenKeycloakFixture.CLIENT_ID)
            // Real gap found 2026-09-22: this image now transitively requires
            // forwardmeasure-jpa-quarkus's own mandatory, no-default forwardmeasure.jpa.
            // tenant-database.*/functional-schema config mapping (same real reason
            // startEngineKafkaStreams() already sets these) - fails startup outright otherwise
            // ("Failed to load config value ... for: forwardmeasure.jpa.functional-schema").
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_HOST", POSTGRES_ALIAS)
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_PORT", "5432")
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("FORWARDMEASURE_JPA_TENANT_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("FORWARDMEASURE_JPA_FUNCTIONAL_SCHEMA", "OPENWORKFLOW")
            // Real gap found 2026-09-23, live-traced via a real AskTimeoutException retrying
            // forever: without this, engine-pekko and operation-adapter each self-join their own
            // single-node Pekko cluster (PekkoClusterRuntime.Settings' own default -
            // requiredContactPoints=1, empty discoveryService - self-joins immediately), so any
            // Cluster Sharding EntityRef operation-adapter's ProtocolOperationCoordinatorEntity
            // issues against the engine's own WorkflowCommand entities can never find a real
            // recipient. Static, config-based Cluster Bootstrap (both containers listing each
            // other's own network alias:managementPort) is the real fix for a plain-Docker
            // deployment with no Kubernetes DNS SRV records - see PekkoClusterRuntime.configure's
            // own javadoc comment.
            .withEnv("OPENWORKFLOW_CLUSTER_DISCOVERY_SERVICE", CLUSTER_SERVICE_NAME)
            .withEnv("OPENWORKFLOW_CLUSTER_POD_IP", ENGINE_PEKKO_ALIAS)
            .withEnv("OPENWORKFLOW_CLUSTER_REQUIRED_CONTACT_POINTS", "2")
            .withEnv(
                "OPENWORKFLOW_CLUSTER_STATIC_CONTACT_POINTS",
                ENGINE_PEKKO_ALIAS
                    + ":"
                    + CLUSTER_MANAGEMENT_PORT
                    + ","
                    + OPERATION_ADAPTER_ALIAS
                    + ":"
                    + CLUSTER_MANAGEMENT_PORT)
            // Real, confirmed live 2026-09-22: Slick (Pekko Persistence JDBC's own SQL layer)
            // logs every single JDBC parameter/statement/query-compilation step at DEBUG by
            // default - tens of thousands of near-zero-value lines that saturated this fixture's
            // own console log capture pipe mid-run, silently truncating real diagnostic output
            // (including this container's own startup line) for the rest of the test. Suppressed
            // to WARN so the coordinator/entity's own real INFO/WARN diagnostic lines (added
            // 2026-09-21 specifically to make a silent stall like this one visible) actually reach
            // the captured log instead of being drowned out.
            .withEnv("QUARKUS_LOG_CATEGORY__SLICK__LEVEL", "WARN")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-engine-pekko"))
            // Deliberately NOT waiting for real cluster membership here (see
            // awaitPekkoClusterReady's own javadoc for why): with requiredContactPoints=2, this
            // container can only reach [Up] once operation-adapter is also discoverable, but
            // operation-adapter isn't started until this call returns (the two starts are
            // sequential, not concurrent) - waiting here would deadlock forever. Cluster
            // Bootstrap keeps retrying in the background regardless of this wait condition, so
            // this only needs to confirm the JVM itself is up before the caller starts the
            // second node.
            .waitingFor(
                Wait.forLogMessage(".*started in.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    configurePekkoPersistence(engine);
    startFrameworkService(engine, false);
    return engine;
  }

  /**
   * Stage 5: the real {@code openworkflow-definition-management-quarkus} image - needs the exact
   * same Postgres/tenant-schema/Keycloak wiring as execution-management (confirmed by direct read
   * of its own real {@code application.yaml}: identical {@code forwardmeasure.jpa.tenant-database}/
   * {@code openworkflow.authorization} shape), no engine URLs at all (it never talks to an engine
   * directly).
   */
  public GenericContainer<?> startDefinitionManagement() {
    String issuer = hostDockerInternalIssuer();
    GenericContainer<?> definitionManagement =
        new GenericContainer<>(DockerImageName.parse(frameworkImage(DEFINITION_MANAGEMENT_IMAGE)))
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(DEFINITION_MANAGEMENT_ALIAS)
            .withExtraHost("host.docker.internal", "host-gateway")
            .withExposedPorts(8080)
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_TENANT_DATABASE_HOST", POSTGRES_ALIAS)
            .withEnv("OPENWORKFLOW_TENANT_DATABASE_PORT", "5432")
            .withEnv("OPENWORKFLOW_KEYCLOAK_ISSUER", issuer)
            .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", expectedIssuer())
            .withEnv("OPENWORKFLOW_CLIENT_ID", AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID)
            .withEnv("OPENWORKFLOW_CLIENT_SECRET", AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET)
            .withEnv("OPENWORKFLOW_ORGANIZATION_CLIENT_ID", AuthzenKeycloakFixture.CLIENT_ID)
            // AllowlistedHttpWorkflowResourceLoader - definition-management's own compiler fetches
            // every call: asyncapi step's document.endpoint over real HTTP at validate/publish
            // time (exact-host egress policy, confirmed by direct source read) - this fixture
            // hosts its own AsyncAPI document on the test JVM's own host (see
            // #startAsyncApiDocumentServer), reachable from this container only via
            // host.docker.internal.
            .withEnv("OPENWORKFLOW_DEFINITION_RESOURCE_ALLOWED_HOSTS", "host.docker.internal")
            // Required by definition-management since bundle documents are named by DID URL
            // (fowf docs/did-method.md); there is no default. This fixture names none.
            .withEnv("FORWARDMEASURE_DID_METHOD", "fwmtest")
            .withLogConsumer(
                new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-definition-management"))
            .waitingFor(
                Wait.forLogMessage(".*started in.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    startFrameworkService(definitionManagement, true);
    return definitionManagement;
  }

  /**
   * Stage 6: the real {@code openworkflow-operation-adapter-quarkus} image - a plain Kafka
   * consumer, not an HTTP-inbound OIDC-secured service like execution/definition-management
   * (confirmed by direct read of {@code OperationAdapterQuarkusBinding}: it never injects a {@code
   * QuarkusActiveOrganizationProvider}/tenant-scope filter at all, only an outbound {@code
   * AuthorizationService} client for its own server-side AuthZEN checks - a different env var
   * convention too, {@code OPENWORKFLOW_AUTHORIZATION_*} rather than {@code OPENWORKFLOW_CLIENT_*}/
   * {@code OPENWORKFLOW_KEYCLOAK_ISSUER}). Dispatches real {@code kubernetes-deployment}
   * apply/watch operations against {@link #kubernetes()} - a real, disposable K3s cluster lazily
   * started here (not in {@link #start}, so stages that never call this method never pay for one).
   *
   * <p>Deny-by-default policy (see {@code KubernetesDeploymentPolicyConfiguration}) is populated
   * from {@code namespace}/{@code image}/{@code maxReplicas} for this fixture's own tenant only -
   * every other tenant stays denied, matching the real allowlist's own production posture.
   *
   * <p>The K3s kubeconfig's own {@code server:} URL is host-port-mapped, reachable from the test
   * JVM as {@code https://localhost:<port>} but not from a sibling container - rewritten onto
   * {@code host.docker.internal} (same reasoning as {@link #hostDockerInternalIssuer()}) and copied
   * into the container as a real file, with {@code KUBECONFIG} pointed at it - fabric8's own {@code
   * KubernetesClientBuilder().build()} (what {@code AsyncApiKubernetesDeploymentOperationExecutor}
   * actually calls) auto-discovers config from that env var exactly like {@code kubectl} does.
   */
  public GenericContainer<?> startOperationAdapter(
      String namespace, String image, int maxReplicas) {
    return startOperationAdapter(namespace, image, maxReplicas, false);
  }

  /**
   * @param joinPekkoCluster true only for the Pekko-engine invocation: {@code
   *     ProtocolOperationCoordinatorEntity} issues a real Cluster Sharding {@code ask} against the
   *     engine's own {@code WorkflowCommand} entities (see its {@code GetRuntimeState} call), which
   *     can only succeed if operation-adapter and engine-pekko are joined into one shared Pekko
   *     cluster - see {@link #startEnginePekko}'s own comment for the real, live-traced bug this
   *     fixes (an {@code AskTimeoutException} retrying forever otherwise). The Kafka-Streams engine
   *     has no Pekko {@code WorkflowCommand} entities at all, so operation-adapter's own cluster
   *     must stay self-joined (single-node) for that invocation, unchanged from before - passing
   *     {@code true} there would make Cluster Bootstrap wait on a contact point (engine-pekko) that
   *     this invocation never starts.
   */
  public GenericContainer<?> startOperationAdapter(
      String namespace, String image, int maxReplicas, boolean joinPekkoCluster) {
    GenericContainer<?> operationAdapter =
        buildOperationAdapter(namespace, image, maxReplicas, joinPekkoCluster);
    startFrameworkService(operationAdapter, false);
    return operationAdapter;
  }

  /**
   * Real, distinct policy from {@code kubernetes-deployment}'s own ({@code
   * KubernetesJobPolicyConfiguration}, not {@code KubernetesDeploymentPolicyConfiguration}) -
   * needed for a {@code correlated-worker}/{@code kubernetes-job} workflow step (workflow-bounded
   * mode) rather than a two-step {@code asyncapi}/{@code kubernetes-deployment} one (continuous
   * mode). A separate named method, not an overload with blank-default parameters - a real,
   * live-caught bug (2026-09-23): an earlier version of this fixture had one 7-arg {@code
   * startOperationAdapter} overload where every 4-arg caller silently passed blank {@code
   * jobNamespace}/{@code jobImage} defaults, which got set as real env vars ({@code "<tenant>="} -
   * a non-blank *string* with a blank per-tenant *value*) - {@code
   * KubernetesJobPolicyConfiguration#sets} treats a wholly-blank string as safe deny-all, but
   * throws {@code IllegalArgumentException} on that shape instead, crashing the container at boot
   * for every caller that never needed {@code kubernetes-job} allowlisting at all. This method
   * exists only for the one real caller that does; every other caller keeps using the plain 4-arg
   * {@link #startOperationAdapter(String, String, int, boolean)}, which never touches these three
   * env vars, leaving the property genuinely unset (deny-by-default, the safe case {@code sets}
   * already handles).
   */
  public GenericContainer<?> startOperationAdapterWithKubernetesJobAllowlist(
      String namespace,
      String image,
      int maxReplicas,
      boolean joinPekkoCluster,
      String jobNamespace,
      String jobImage,
      int jobMaxParallelism) {
    String tenant = tenantId.value().toString();
    GenericContainer<?> operationAdapter =
        buildOperationAdapter(namespace, image, maxReplicas, joinPekkoCluster)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_JOB_NAMESPACE_ALLOWLIST",
                tenant + "=" + jobNamespace)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_JOB_IMAGE_ALLOWLIST", tenant + "=" + jobImage)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_JOB_MAX_PARALLELISM_ALLOWLIST",
                tenant + "=" + jobMaxParallelism);
    startFrameworkService(operationAdapter, false);
    return operationAdapter;
  }

  /**
   * Runs real HTTP protocol operations against a disposable receiver, with tenant-scoped egress.
   */
  public GenericContainer<?> startHttpOperationAdapter(boolean joinPekkoCluster) {
    var adapter =
        buildOperationAdapter("unused", "unused", 1, joinPekkoCluster)
            .withEnv(
                "OPENWORKFLOW_HTTP_EGRESS_ALLOWLIST", tenantId.value() + "=host.docker.internal");
    startFrameworkService(adapter, false);
    return adapter;
  }

  /** Real secret-mounted adapter for the production FDE workflow contract. */
  public GenericContainer<?> startAuthenticatedGrpcAdapter(
      boolean pekko, String host, String token) {
    var adapter =
        buildOperationAdapter("unused", "unused", 1, pekko)
            .withEnv(
                "OPENWORKFLOW_HTTP_EGRESS_ALLOWLIST",
                tenantId.value() + "=" + host + ",host.docker.internal")
            .withEnv("OPENWORKFLOW_ADAPTER_SECRET_DIRECTORY", "/var/run/secrets/openworkflow")
            .withCopyToContainer(
                Transferable.of(token.getBytes(StandardCharsets.UTF_8), 0444),
                "/var/run/secrets/openworkflow/" + tenantAlias + "/decision-engine-token");
    startFrameworkService(adapter, false);
    return adapter;
  }

  /** Builds, but does not start, operation-adapter - shared by both public entry points above. */
  private GenericContainer<?> buildOperationAdapter(
      String namespace, String image, int maxReplicas, boolean joinPekkoCluster) {
    KubernetesTestContainer k3s = kubernetes();
    String issuer = hostDockerInternalIssuer();
    String tenant = tenantId.value().toString();
    GenericContainer<?> operationAdapter =
        new GenericContainer<>(
                DockerImageName.parse(
                    frameworkImage(
                        joinPekkoCluster
                            ? OPERATION_ADAPTER_PEKKO_IMAGE
                            : OPERATION_ADAPTER_KAFKA_IMAGE)))
            .withNetwork(existingNetwork(network.getId()))
            .withNetworkAliases(OPERATION_ADAPTER_ALIAS)
            .withExtraHost("host.docker.internal", "host-gateway")
            .withExposedPorts(8080)
            .withCopyToContainer(
                Transferable.of(hostDockerInternalKubeconfig(k3s).getBytes(StandardCharsets.UTF_8)),
                KUBECONFIG_CONTAINER_PATH)
            .withEnv("KUBECONFIG", KUBECONFIG_CONTAINER_PATH)
            .withEnv("OPENWORKFLOW_KAFKA_BOOTSTRAP_SERVERS", kafka.networkBootstrapServers())
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_USERNAME", postgres.username())
            .withEnv("OPENWORKFLOW_TENANT_REGISTRY_PASSWORD", postgres.password())
            // Used by the Pekko-specific adapter. Kafka uses its own engine-specific image.
            .withEnv("OPENWORKFLOW_PERSISTENCE_ENDPOINT", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_PERSISTENCE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_PERSISTENCE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_AUTHORIZATION_ISSUER", issuer)
            .withEnv(
                "OPENWORKFLOW_AUTHORIZATION_CLIENT_ID", AuthzenKeycloakFixture.AUTHZEN_CLIENT_ID)
            .withEnv(
                "OPENWORKFLOW_AUTHORIZATION_CLIENT_SECRET",
                AuthzenKeycloakFixture.AUTHZEN_CLIENT_SECRET)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_DEPLOYMENT_NAMESPACE_ALLOWLIST",
                tenant + "=" + namespace)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_DEPLOYMENT_IMAGE_ALLOWLIST",
                tenant + "=" + image)
            .withEnv(
                "OPENWORKFLOW_OPERATIONS_KUBERNETES_DEPLOYMENT_MAX_REPLICAS_ALLOWLIST",
                tenant + "=" + maxReplicas)
            // See startEnginePekko's own comment on this same env var - this runtime also starts
            // Pekko Persistence JDBC (its own outbox/projection wiring) and is equally subject to
            // Slick's default DEBUG-level SQL logging saturating the log capture.
            .withEnv("QUARKUS_LOG_CATEGORY__SLICK__LEVEL", "WARN")
            .withLogConsumer(
                new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-operation-adapter"))
            .waitingFor(
                Wait.forLogMessage(".*started in.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    if (joinPekkoCluster) {
      ensurePekkoPersistence();
      configurePekkoPersistence(operationAdapter);
      operationAdapter
          .withEnv("OPENWORKFLOW_CLUSTER_DISCOVERY_SERVICE", CLUSTER_SERVICE_NAME)
          .withEnv("OPENWORKFLOW_CLUSTER_POD_IP", OPERATION_ADAPTER_ALIAS)
          .withEnv("OPENWORKFLOW_CLUSTER_REQUIRED_CONTACT_POINTS", "2")
          .withEnv(
              "OPENWORKFLOW_CLUSTER_STATIC_CONTACT_POINTS",
              ENGINE_PEKKO_ALIAS
                  + ":"
                  + CLUSTER_MANAGEMENT_PORT
                  + ","
                  + OPERATION_ADAPTER_ALIAS
                  + ":"
                  + CLUSTER_MANAGEMENT_PORT);
    }
    return operationAdapter;
  }

  /**
   * Blocks until engine-pekko and operation-adapter have actually joined one shared Pekko cluster
   * (both real, confirmed live 2026-09-23) - call this only after both containers have already been
   * started via {@link #startEnginePekko} and {@link #startOperationAdapter} (with {@code
   * joinPekkoCluster=true}), never from inside either of those methods: Cluster Bootstrap needs
   * both JVMs running and mutually discoverable before it can converge, and the two starts are
   * sequential (not concurrent) - a per-container wait for cluster membership deadlocks, since the
   * second container never starts until the first one's own wait condition is satisfied. Polls each
   * container's already-buffered log output (no separate HTTP port needs exposing) rather than
   * reusing Wait.forLogMessage, since that strategy only ever inspects a container's OWN startup
   * window, not logs it continues to emit indefinitely afterward.
   */
  public void awaitPekkoClusterReady(
      GenericContainer<?> engine, GenericContainer<?> operationAdapter)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
    while (System.nanoTime() < deadline) {
      if (engine.getLogs().contains("to [Up]")
          && operationAdapter.getLogs().contains("Welcome from")) {
        return;
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException(
        "engine-pekko and operation-adapter never joined one shared Pekko cluster within 2"
            + " minutes - see their own logs for Cluster Bootstrap's real progress");
  }

  /**
   * The real, disposable K3s cluster {@link #startOperationAdapter} dispatches real Deployment
   * apply/watch operations against - lazily started so stages that never call it never pay for one,
   * and exposed here so a caller can create a real target namespace (and later assert on the real
   * applied {@code Deployment}) via its own {@link KubernetesTestContainer#createClient()} before
   * calling {@link #startOperationAdapter}.
   */
  public synchronized KubernetesTestContainer kubernetes() {
    if (kubernetes == null) {
      kubernetes = new KubernetesTestContainer().start();
    }
    return kubernetes;
  }

  /**
   * fowf's own services validate against a real Keycloak issuer reachable from inside their own
   * container - {@link AuthzenKeycloakFixture}'s container is host-port-mapped only (no
   * caller-supplied-network support today, unlike Postgres/Kafka), so this rewrites the
   * host-reachable issuer URL {@link AuthzenKeycloakFixture#issuer()} returns onto the real Docker
   * host-gateway DNS name every service container below registers via {@code
   * withExtraHost("host.docker.internal", "host-gateway")}.
   */
  private String expectedIssuer() {
    return (networkIdentity ? keycloak.networkIssuer() : keycloak.issuer()).toString();
  }

  public String hostDockerInternalIssuer() {
    if (networkIdentity) return keycloak.networkIssuer().toString();
    String issuer = expectedIssuer();
    return issuer.replaceFirst("^http://[^:/]+:", "http://host.docker.internal:");
  }

  /**
   * {@link KubernetesTestContainer}, like {@link AuthzenKeycloakFixture}, is host-port-mapped only
   * - rewrites the kubeconfig's own {@code server:} URL (host-reachable, e.g. {@code
   * https://localhost:<port>}) onto {@code host.docker.internal} so a sibling container mounting
   * this file can reach the identical cluster. Same reasoning as {@link
   * #hostDockerInternalIssuer()}, applied to a full kubeconfig YAML document instead of a single
   * bare URL.
   *
   * <p>Real, live-verified quoting detail (found 2026-09-21 - the first version of this method used
   * an unquoted pattern that silently never matched): {@code K3sContainer#getKubeConfigYaml()}
   * emits the {@code server:} value as a quoted YAML string (e.g. {@code server:
   * "https://localhost:<port>"}), not a bare scalar - the regex below matches the optional quote on
   * both sides so it fires either way.
   *
   * <p>Also sets {@code insecure-skip-tls-verify: true}: the real K3s server cert's own SAN list
   * (its container hostname/IP, {@code localhost}/{@code 127.0.0.1}, the in-cluster {@code
   * kubernetes[.default[.svc...]]} names) never includes {@code host.docker.internal}, so strict
   * TLS hostname verification against the rewritten {@code server:} URL would fail even though the
   * CA and connection are both genuinely trusted - the standard, documented kubeconfig field for
   * this disposable K3s transport. JWT issuer validation remains enabled independently.
   */
  private static String hostDockerInternalKubeconfig(KubernetesTestContainer k3s) {
    String kubeconfig = k3s.kubeConfigYaml();
    String rewritten =
        kubeconfig.replaceFirst(
            "(?m)^(\\s*server:\\s*\"?https://)[^:/\"]+(:\\d+\"?)", "$1host.docker.internal$2");
    return rewritten.replaceFirst(
        "(?m)^(\\s*server:.*)$", "$1\n    insecure-skip-tls-verify: true");
  }

  /**
   * The exact UUID {@code com.forwardmeasure.openworkflow.migration.TenantOnboarding} derives for
   * {@code tenantAlias} in this fixture's own domain ({@link #TENANT_DOMAIN}) - {@link
   * TenantId#forDid} over {@code did:web:<alias>.<domain>}, independently computed here rather than
   * read back from Postgres, so a mismatch between this fixture's own assumption and fowf's real
   * derivation would surface as a real, visible failure instead of silently agreeing with itself.
   */
  private static TenantId deriveTenantId(String tenantAlias) {
    return TenantId.forDid(new Did("did:web:" + tenantAlias + "." + TENANT_DOMAIN));
  }

  private static void runMigrations(
      Network network, PostgreSqlTestContainer postgres, String tenantAlias) {
    runMigrations(network, postgres, tenantAlias, java.util.Map.of());
  }

  private static void runMigrations(
      Network network,
      PostgreSqlTestContainer postgres,
      String tenantAlias,
      java.util.Map<String, String> persistenceEnvironment) {
    LOGGER.info(
        "RealFowfWorkflowFixture: running openworkflow-migrations for tenant '{}'", tenantAlias);
    try (GenericContainer<?> migrations =
        new GenericContainer<>(DockerImageName.parse(MIGRATIONS_IMAGE))
            .withNetwork(existingNetwork(network.getId()))
            .withEnv("OPENWORKFLOW_CONTROL_PLANE_DATABASE_URL", postgres.networkJdbcUrl())
            .withEnv("OPENWORKFLOW_ADMIN_DATABASE_USERNAME", postgres.username())
            .withEnv("OPENWORKFLOW_ADMIN_DATABASE_PASSWORD", postgres.password())
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME", RUNTIME_DATABASE_USERNAME)
            .withEnv("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD", RUNTIME_DATABASE_PASSWORD)
            .withEnv("OPENWORKFLOW_TENANT_DOMAIN", TENANT_DOMAIN)
            .withEnv("OPENWORKFLOW_TENANTS", tenantAlias + ":FDS Phase D Test Tenant")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("openworkflow-migrations"))
            // A one-shot batch Job, not a long-running service - the real, standard testcontainers
            // strategy for "start it, let it run to completion, then check its exit code" rather
            // than any readiness probe (a log-message/port wait would either hang forever or, with
            // too short a timeout, tear the container down before it can log its own real error).
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2)))) {
      migrations.withEnv(persistenceEnvironment).withImagePullPolicy(image -> false);
      migrations.start();
      awaitExit(migrations, "openworkflow-migrations");
    }
  }

  private static void awaitExit(GenericContainer<?> container, String name) {
    Long exitCode = container.getCurrentContainerInfo().getState().getExitCodeLong();
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
    while ((exitCode == null || isRunning(container)) && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for " + name + " to exit", interrupted);
      }
      exitCode = container.getCurrentContainerInfo().getState().getExitCodeLong();
    }
    if (isRunning(container)) {
      throw new IllegalStateException(name + " did not exit within the deadline");
    }
    if (exitCode == null || exitCode != 0) {
      throw new IllegalStateException(
          name + " exited with code " + exitCode + " - see its own logged output above");
    }
    LOGGER.info("RealFowfWorkflowFixture: {} exited 0", name);
  }

  private static boolean isRunning(GenericContainer<?> container) {
    try {
      return container.isRunning();
    } catch (RuntimeException notFound) {
      return false;
    }
  }

  /**
   * Refers to a caller-owned Docker network without assuming ownership of its lifecycle - same
   * shape {@code PostgreSqlTestContainer}/{@code KafkaTestContainer} already use internally for
   * this exact purpose.
   */
  private static Network existingNetwork(String networkId) {
    return new Network() {
      @Override
      public String getId() {
        return networkId;
      }

      @Override
      public void close() {
        // The caller that supplied the network ID owns the network.
      }
    };
  }

  public PostgreSqlTestContainer postgres() {
    return postgres;
  }

  public Network network() {
    return network;
  }

  public AuthzenKeycloakFixture keycloak() {
    return keycloak;
  }

  /** The real, migrated tenant's identity - see {@link #deriveTenantId}. */
  public TenantId tenantId() {
    return tenantId;
  }

  public String organizationId() {
    return organizationId;
  }

  /**
   * Hosts {@code content} (a real AsyncAPI document) on a plain JDK {@link
   * com.sun.net.httpserver.HttpServer} bound to every interface on this JVM's own host - fowf has
   * no {@code src/main/resources} convention for protocol-adapter AsyncAPI documents (confirmed by
   * direct source read - even fowf's own real dispatch tests embed one as a literal string), so a
   * real, reachable HTTP endpoint is the only way to satisfy {@code
   * AllowlistedHttpWorkflowResourceLoader}'s own real HTTP fetch. Returns the URL as reachable from
   * a container on this fixture's own network (via {@code host.docker.internal}). The fixture owns
   * the HTTP server and stops it during {@link #close()}.
   */
  public String startAsyncApiDocumentServer(String path, String content) {
    try {
      com.sun.net.httpserver.HttpServer server =
          com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("0.0.0.0", 0), 0);
      byte[] body = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      server.createContext(
          path,
          exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/yaml");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
              out.write(body);
            }
          });
      documentServers.add(server);
      server.start();
      int port = server.getAddress().getPort();
      String url = "http://host.docker.internal:" + port + path;
      LOGGER.info(
          "RealFowfWorkflowFixture: hosting AsyncAPI document at {} (real path {})", url, path);
      return url;
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  public KafkaTestContainer kafka() {
    return kafka;
  }

  @Override
  public void close() {
    documentServers.forEach(server -> server.stop(0));
    for (GenericContainer<?> service : services.reversed()) {
      service.close();
    }
    if (kubernetes != null) {
      kubernetes.close();
    }
    if (cassandra != null) cassandra.close();
    keycloak.close();
    kafka.close();
    postgres.close();
    network.close();
  }
}
