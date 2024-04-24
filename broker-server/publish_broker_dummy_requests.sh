#!/bin/bash
#--------------------------------------
# Script Name:  publish_broker_dummy_requests.sh
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Unzips given zip with dummy requests and publishes
#               the included requests to given nodes via given AKTIN
#               Broker
# ATTENTION:    This script is still static. The zip file must contain
#               exactly the requests specified in the script
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -lt 4 ]] && {
  echo "Usage: $0 <broker_url> <admin_api_key> <dummy_requests_zip> <node_1> ..."
  exit 1
}

readonly broker_url="$1/broker"
readonly admin_api_key=$2
readonly dummy_requests_zip=$3

readonly node_ids=(${@:3})
if [ ${#node_ids[@]} -eq 0 ]; then
    echo "At least one node id is required as input"
    exit 1
fi

extract_zip_and_get_extracted_folder() {
  local zip_file="$1"
  # get folder name and parent directory
  local folder_name=$(basename "$zip_file" .zip)
  local parent_dir=$(dirname "$zip_file")
  # extract the zip file into the same directory
  unzip -q "$zip_file" -d "$parent_dir/$folder_name" || {
    echo "Error: Failed to extract zip file"
    return 1
  }
  echo "$parent_dir/$folder_name"
}

allocate_and_submit_request() {
  local filename=$1
  local requests_dir=$2
  response=$(curl -is --request POST "$broker_url/request" --data-binary "@$requests_dir/0_empty_request.xml" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/vnd.aktin.query.request+xml")
  request_id=$(echo "$response" | grep -oP "broker/request/\d*" | grep -oP "\d*")
  sed -i "1 s/<id>.*<\/id>/<id>$request_id<\/id>/" "$requests_dir/$filename"
  curl -s --request PUT "$broker_url/request/$request_id" --data-binary "@$requests_dir/$filename" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/vnd.aktin.query.request+xml"
  echo "$request_id"
}

publish_request() {
  local request_id=${1}
  local target_ids=(${@:2})
  local target_nodes=""
  for target_id in "${target_ids[@]}"; do
    target_nodes+="<node>${target_id}</node>"
  done
  curl -s --request PUT "$broker_url/request/$request_id/nodes" --data-raw "<nodes xmlns='http://aktin.org/ns/exchange'>${target_nodes}</nodes>" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/xml"
  curl -s --request POST "$broker_url/request/$request_id/publish" --header "Authorization: Bearer $admin_api_key"
}

main() {
  local requests_dir=$(extract_zip_and_get_extracted_folder $dummy_requests_zip)
  local xml_requests=("1_single_quality_assurance_pseudonym_future.xml" "2_single_quality_assurance_anonynm.xml" "3_query_public_health_surveillance_pseudonym.xml" "4_query_public_health_surveillance_pseudonym.xml" "5_single_research_anonynm_failure.xml" "6_single_research_pseudonym.xml")
  for xml_request in "${xml_requests[@]}"; do
    local request_id
    request_id=$(allocate_and_submit_request "$xml_request" "$requests_dir")
    publish_request "$request_id" "${node_ids[@]}"
  done
  rm -r $requests_dir
}

main
