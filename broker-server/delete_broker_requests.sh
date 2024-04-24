#!/bin/bash
#--------------------------------------
# Script Name:  delete_broker_requests.sh
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Deletes all requests from AKTIN Broker in
#               given id range
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -ne 4 ]] && {
  echo "Usage: $0 <broker_url> <admin_api_key> <start_id> <end_id>"
  exit 1
}

readonly broker_url="$1/broker"
readonly admin_api_key=$2
readonly request_id_start=$3
readonly request_id_end=$4

# retrieve a list of requests from the broker
readonly requests=$(curl -s --request GET "$broker_url/request" --header "Authorization: Bearer $admin_api_key")

# extract individual request ids from the retrieved list
readonly request_ids=$(grep -Po '<request id="\K[^"]+' <<<"${requests}")

delete_request() {
  local request_id=$1
  curl -s --request DELETE "${broker_url}/request/${request_id}" --header "Authorization: Bearer ${admin_api_key}"
}

# iterate and download request status within the specified id range
for request_id in ${request_ids[@]}; do
  if [[ $request_id -ge $request_id_start && $request_id -le $request_id_end ]]; then
    delete_request "$request_id"
  fi
done
