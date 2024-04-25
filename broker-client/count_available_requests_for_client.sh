#! /bin/bash
#--------------------------------------
# Script Name:  count_available_requests_for_client.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Imitates a broker-client to count the available requests for that client on a given AKTIN Broker
#--------------------------------------

# ensure the correct number of arguments is provided
[[ $# -ne 2 ]] && {
  echo "Usage: $0 <broker_url> <client_api_key>"
  exit 1
}

readonly broker_url=$1
readonly client_api_key=$2

# get xml with all published requests for this api key
resp=$(curl -s --request GET "$broker_url/broker/my/request" --header "Authorization: Bearer $client_api_key")

# count how often tag named "id" appears in response
expr $(echo $resp | grep -o "id" | wc -l)
