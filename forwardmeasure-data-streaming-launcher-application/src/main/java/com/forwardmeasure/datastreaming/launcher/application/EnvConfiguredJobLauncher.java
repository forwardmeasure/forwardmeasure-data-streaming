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

import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobLifecycle;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobObservation;
import com.forwardmeasure.openworkflow.kubernetes.job.KubernetesJobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Direct mode, env-var-config variant: runs an arbitrary container image as one real Kubernetes
 * Job, with configuration delivered as plain environment variables - no fowf workflow engine
 * involved, and no {@code IngestionSpec}/base64-YAML-reconstruction packaging either (see {@link
 * DirectIngestionLauncher} for that convention, which this class deliberately does not use).
 *
 * <p>Built for consumers whose own worker image already reads configuration from named environment
 * variables under its own naming convention (fei's {@code SimpleSourceIngestionWorker}/{@code
 * CorrelatedSourceIngestionWorker} are the motivating real case: {@code SOURCE_PATH}/{@code
 * MAPPING_PATH}/{@code TARGET_MODEL_CLASS}/... and {@code SOURCES_CONFIG_PATH}/{@code
 * MATCH_CRITERIA_PATH}/...) rather than a single reconstructed spec file - adapting such a worker
 * to read an {@code IngestionSpec} YAML instead would be real, unwanted rework when the worker's
 * own env-var contract already works and is already deployed. This launcher meets that contract
 * as-is: a caller supplies exactly the env var names/values its own image expects, unchanged.
 *
 * <p>Unlike {@link DirectIngestionLauncher} (bound to one fixed Pekko runner image/command at
 * construction time) or {@link DirectCorrelationLauncher}, this launcher has no runner image of its
 * own - {@code image}/{@code command}/{@code args} are supplied per {@link EnvLaunchRequest}, since
 * it exists precisely to launch images that aren't one of FDS's own generic runners. Authorization
 * still goes through the same {@link IngestionJobPolicy} contract (namespace + sha256-pinned image
 * allowlists) every other direct-mode launcher uses, and the same deterministic-Job-name-from-
 * correlation-id convention, so a caller gets the identical idempotency/authorization guarantees
 * regardless of which launcher it uses.
 */
public final class EnvConfiguredJobLauncher {

  private static final String DEFAULT_JOB_NAME_PREFIX = "fds-env-";
  private static final String CORRELATION_ID_LABEL =
      "data-streaming.forwardmeasure.com/correlation-id";
  private static final int PARALLELISM = 1;
  private static final int COMPLETIONS = 1;
  private static final int BACKOFF_LIMIT = 0;

  private final IngestionJobPolicy policy;
  private final String jobNamePrefix;

  public EnvConfiguredJobLauncher(IngestionJobPolicy policy) {
    this(policy, DEFAULT_JOB_NAME_PREFIX);
  }

  /**
   * @param jobNamePrefix overrides the default {@code fds-env-} prefix - lets a caller that already
   *     has an established naming convention of its own (e.g. {@link DirectIngestionLauncher}'s
   *     real, existing {@code fds-} prefix) delegate to this class for its actual Job mechanics
   *     without silently renaming every Job it produces. The prefix only affects the derived name
   *     string itself, never correctness: {@link KubernetesJobLifecycle#deterministicName} still
   *     guarantees the same correlation id always maps to the same name.
   */
  public EnvConfiguredJobLauncher(IngestionJobPolicy policy, String jobNamePrefix) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.jobNamePrefix = Objects.requireNonNull(jobNamePrefix, "jobNamePrefix");
  }

  /**
   * Launches {@code request.image()} as one Job with {@code request.env()} set directly as the
   * container's environment - no spec reconstruction, no wrapper shell command. Returns the
   * deterministic Job name ({@link #deterministicJobName}) a caller uses with {@link #observe}/
   * {@link #cancel}.
   */
  public String launch(KubernetesClient client, EnvLaunchRequest request) {
    policy.authorizeNamespace(request.namespace());
    policy.authorizeImage(request.image());

    String jobName = jobName(request.correlationId());
    KubernetesJobSpec jobSpec =
        new KubernetesJobSpec(
            request.namespace(),
            jobName,
            Map.of(CORRELATION_ID_LABEL, labelSafe(request.correlationId())),
            request.image(),
            request.command(),
            request.args(),
            request.env(),
            request.resourceRequests(),
            request.resourceLimits(),
            PARALLELISM,
            COMPLETIONS,
            BACKOFF_LIMIT,
            request.activeDeadlineSeconds(),
            request.imagePullSecretNames());
    KubernetesJobLifecycle.launch(client, jobSpec);
    return jobName;
  }

  public Optional<KubernetesJobObservation> observe(
      KubernetesClient client, String namespace, String correlationId) {
    return KubernetesJobLifecycle.observe(client, namespace, jobName(correlationId));
  }

  public void cancel(KubernetesClient client, String namespace, String correlationId) {
    KubernetesJobLifecycle.cancel(client, namespace, jobName(correlationId));
  }

  /** This instance's own configured prefix - see {@link #deterministicJobName} for the default. */
  private String jobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(jobNamePrefix, correlationId);
  }

  /**
   * The same correlation id always resolves to the same Job name under the *default* ({@code
   * fds-env-}) prefix - a convenience for a caller that knows it's talking to a default-constructed
   * instance (every real caller so far). A caller using the {@link #EnvConfiguredJobLauncher(
   * IngestionJobPolicy, String) prefix-overriding constructor} (e.g. {@link
   * DirectIngestionLauncher} preserving its own real, established {@code fds-} prefix) must derive
   * names the same way that instance does internally, not via this static method - {@link
   * KubernetesJobLifecycle #deterministicName} directly, with that same prefix.
   */
  public static String deterministicJobName(String correlationId) {
    return KubernetesJobLifecycle.deterministicName(DEFAULT_JOB_NAME_PREFIX, correlationId);
  }

  private static String labelSafe(String value) {
    String sanitized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "-");
    return sanitized.length() > 63 ? sanitized.substring(0, 63) : sanitized;
  }
}
