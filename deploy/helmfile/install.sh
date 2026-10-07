#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Installs or updates FDS's own launcher into a real cluster - mirrors
# forwardmeasure-openworkflow's own deploy/helmfile/install.sh shape, scaled down to this
# repo's one real release (no migrations/engines/multi-stage sequence to run).
#
# Real preconditions this script does NOT provision (see environments/gcp-platform-cluster.yaml.gotmpl's
# own comments for exactly which): the data-streaming-launcher-credentials Secret (via
# forwardmeasure-platform's own platform-secrets convention), the docker-io-credentials
# imagePullSecret, and confirming the "datastreaming" AuthZEN client exists in this cluster's own
# Keycloak realm. Applying before those are real will not fail this script outright, but the
# resulting pod will not come up healthy.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENVIRONMENT="${1:?Usage: $0 <configured-environment>}"
PLATFORM_DEPLOY="$(cd -- "${SCRIPT_DIR}/../../../forwardmeasure-platform/deploy" && pwd)"
source "${PLATFORM_DEPLOY}/scripts/product-selection.sh"
python3 "${PLATFORM_DEPLOY}/scripts/check-deployment-transition.py"

# Reconcile every selected release in Helmfile order, including unchanged failed releases.
# A separate failed-release prepass can retry a dependent before its prerequisite is updated.
# Honor needs even with stage selectors; do not implicitly include disabled or unselected releases.
# sync reruns hooks; migrations, identity reconciliation and publication must remain repeatable.

for command in kubectl helm helmfile; do
  command -v "${command}" >/dev/null || {
    echo "Required command is unavailable: ${command}" >&2
    exit 1
  }
done

helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" --environment "${ENVIRONMENT}" \
  sync --skip-needs=false --wait --wait-for-jobs
