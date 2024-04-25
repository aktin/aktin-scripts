#! /bin/bash
#--------------------------------------
# Script Name:  submit_dummy_result_for_request.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Imitates a broker-client to submit dummy results to a given Broker request and
#               set its status to "completed"
#--------------------------------------

# ensure the correct number of arguments is provided
[[ $# -ne 3 ]] && {
  echo "Usage: $0 <broker_url> <client_api_key> <request_id>"
  exit 1
}

readonly broker_url=$1
readonly client_api_key=$2
readonly request_id=$3

# submit a dummy file content as a result
curl -s --request PUT -d "a;b\n1;2\n3;4\n" "$broker_url/aggregator/my/request/$request_id/result" --header "Authorization: Bearer $client_api_key" --header "Content-Type: text/csv"

# update status of given request of client to completed
curl -s --request POST "$broker_url/broker/my/request/$request_id/status/completed" --header "Authorization: Bearer $client_api_key"
