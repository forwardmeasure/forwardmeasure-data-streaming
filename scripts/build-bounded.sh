#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the License. You may obtain
# a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
# law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
# BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
# for the specific language governing permissions and limitations under the License.
#
set -euo pipefail

# A separate scope keeps this build's memory pressure out of the editor's cgroup. Docker-created
# containers belong to the daemon's cgroups and must be limited by their own test fixtures.
if (($# == 0)); then
  echo "Usage: $0 <maven arguments> | --show-limits" >&2
  exit 2
fi
for dependency in systemd-run flock mvn; do
  command -v "$dependency" >/dev/null || {
    echo "Required command is unavailable: $dependency" >&2
    exit 1
  }
done

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repository"
lock_directory="${XDG_RUNTIME_DIR:?XDG_RUNTIME_DIR is required for a systemd user scope}"
exec 9>"$lock_directory/openworkflow-maven.lock"
flock --nonblock 9 || {
  echo "Another bounded ForwardMeasure build is running." >&2
  exit 1
}

# JAVA_TOOL_OPTIONS reaches forked test JVMs without replacing JaCoCo's argLine. The cgroup cap
# also bounds native memory and descendants, even if a plugin chooses another explicit heap.
# The last -Xms/-Xmx wins: an inherited MAVEN_OPTS with a larger initial heap would otherwise stop
# the JVM at startup ("Initial heap size set to a larger value than the maximum heap size").
export MAVEN_OPTS="${MAVEN_OPTS:+$MAVEN_OPTS }-Xms256m -Xmx2g"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-Xmx2g"
scope=(systemd-run --user --scope --quiet
  -p MemoryAccounting=yes -p MemoryHigh=6G -p MemoryMax=8G -p MemorySwapMax=1G)

if [[ "$1" == --show-limits ]]; then
  "${scope[@]}" bash -c '
    group=$(awk -F: '\''$1 == "0" { print $3 }'\'' /proc/self/cgroup)
    for name in memory.high memory.max memory.swap.max; do
      printf "%s=" "$name"
      cat "/sys/fs/cgroup$group/$name"
    done
  '
else
  # Keep this small shell alive to own the lock even if systemd-run closes inherited descriptors.
  "${scope[@]}" mvn "$@"
fi
