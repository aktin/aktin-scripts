#!/bin/bash
#--------------------------------------
# Script Name:  remote_update_EAR.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Copies local EAR to wildfly deployments folder of remote AKTIN DWH
#               and restarts DWH
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -ne 3 ]] && {
  echo "Usage: $0 <dwh_ip> <server_user> <ear_path>"
  exit 1
}

readonly dwh_ip=$1
readonly server_user=$2
readonly ear_path=$3

# copy the .ear file to the azure dwh
scp "$ear_path" "$server_user@$dwh_ip:/opt/wildfly/standalone/deployments"

# restart the wildfly service
ssh "$server_user@$dwh_ip" "sudo service wildfly restart"
