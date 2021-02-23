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

# Prepare OMs before upgrade unless this variable is false.
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

export OM_HA_ARGS='--'

prepare_for_image '1.1.0'
start_docker_env
execute_robot_test scm omha/om-prepare.robot
