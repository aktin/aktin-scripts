#!/bin/bash
#--------------------------------------
# Script Name:  remote_reset_dwh_requests.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Connects to remote AKTIN DWH and deletes all Broker requests from database
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -ne 2 ]] && {
  echo "Usage: $0 <dwh_ip> <server_user>"
  exit 1
}

readonly dwh_ip=$1
readonly server_user=$2

# execute the commands on the azure dwh
ssh "$server_user@$dwh_ip" <<EOF
    sudo -u postgres psql -d aktin -c "TRUNCATE aktin.broker_requests;"
    sudo -u postgres psql -d aktin -c "TRUNCATE aktin.broker_query_rules;"
    sudo service wildfly restart
EOF
