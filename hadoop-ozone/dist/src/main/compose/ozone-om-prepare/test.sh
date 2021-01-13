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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

# Clean up saved internal state from each container's volume for the next run.
rm -rf "${OZONE_VOLUME}"
mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}

if [[ -n "${OZONE_VOLUME_OWNER}" ]]; then
  current_user=$(whoami)
  if [[ "${OZONE_VOLUME_OWNER}" != "${current_user}" ]]; then
    chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}" \
      || sudo chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}"
  fi
fi

export OZONE_DIR=/opt/hadoop
# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

# Write data and prepare cluster.
start_docker_env
execute_robot_test scm omha/om-prepare.robot
KEEP_RUNNING=false stop_docker_env

# re-start cluster with --upgrade flag to take it out of prepare.
export OM_HA_ARGS='--upgrade'
export OZONE_KEEP_RESULTS=true
start_docker_env
# Writes should now succeed.
execute_robot_test scm topology/loaddata.robot
stop_docker_env

generate_report
