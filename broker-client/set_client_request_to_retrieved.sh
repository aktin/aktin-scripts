#! /bin/bash
#--------------------------------------
# Script Name:  set_client_request_to_retrieved.sh
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Imitates a broker-client to set the status for
#               a given Broker request to "retrieved"
#--------------------------------------

# ensure the correct number of arguments is provided
[[ $# -ne 3 ]] && {
  echo "Usage: $0 <broker_url> <client_api_key> <request_id>"
  exit 1
}

readonly broker_url=$1
readonly client_api_key=$2
readonly request_id=$3

# set request status on given request for this api key on "retrieved"
curl -s --request POST "$broker_url/broker/my/request/$request_id/status/retrieved" --header "Authorization: Bearer $client_api_key"
