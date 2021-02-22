#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script tests upgrade from a previous release to the current
# binaries.  Docker image with Ozone binaries is required for the
# initial version, while the snapshot version uses Ozone runner image.

set -e -o pipefail

# Prepare OMs before upgrade unless this variable is unset.
: "${OZONE_PREPARE_OMS:='true'}"

# Fail if required vars are not set.
set -u
: "${OZONE_UPGRADE_FROM}"
: "${OZONE_UPGRADE_TO}"
: "${OZONE_UPGRADE_CALLBACK}"
: "${COMPOSE_DIR}"
set +u

source "$COMPOSE_DIR"/testlib.sh
source "$OZONE_UPGRADE_CALLBACK"

prepare_oms() {
  if [[ "$OZONE_PREPARE_OMS" = 'true' ]]; then
    execute_robot_test scm upgrade/om-prepare.robot
  fi
}

cancel_prepare_oms() {
  if [[ "$OZONE_PREPARE_OMS" = 'true' ]]; then
    execute_robot_test scm upgrade/om-cancel-prepare.robot
  fi
}

prepare_for_image "$OZONE_UPGRADE_FROM"
start_docker_env
callback with_old_version
prepare_oms

prepare_for_image "$OZONE_CURRENT_VERSION"
restart_docker_env
cancel_prepare_oms
callback with_new_version_pre_finalized
prepare_oms

prepare_for_image "$OZONE_UPGRADE_FROM"
restart_docker_env
cancel_prepare_oms
callback with_old_version_rollback
prepare_oms

prepare_for_image "$OZONE_CURRENT_VERSION"
restart_docker_env
cancel_prepare_oms
execute_robot_test scm upgrade/finalize-scm.robot
execute_robot_test scm upgrade/finalize-om.robot
callback with_new_version_finalized

stop_docker_env
generate_report
