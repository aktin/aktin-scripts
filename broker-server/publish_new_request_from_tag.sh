#!/bin/bash
#--------------------------------------
# Script Name:  publish_new_request_from_tag.sh
# Version:      1.0
# Author:       whoy@ukaachen.de, akombeiz@ukaachen.de
# Purpose:      Automates the duplication of a tagged request by retrieving the latest matching
#               request, updating its dates, and publishing it to the broker.
#--------------------------------------

set -euo pipefail

[[ $# -lt 3 ]] && {
  echo "Usage: $0 <broker_url> <admin_api_key> <tag> [days_to_increase]" >&2
  echo "  <broker-url>       Required: The base URL of the broker" >&2
  echo "  <admin-api-key>    Required: The admin API key for authentication" >&2
  echo "  <tag>              Required: The tag to filter requests" >&2
  echo "  [days-to-increase] Optional: Number of days to increase dates (default: 7)" >&2
  exit 1
}

readonly broker_url="$1/broker"
readonly admin_api_key="$2"
readonly tag="$3"
readonly days_to_increase="${4:-7}"

# retrieve list of requests containing given tag
readonly tagged_requests=$(curl -s --request GET "$broker_url/request/filtered?type=application/vnd.aktin.query.request%2Bxml&predicate=//queryRequest/query/principal/tags/tag='$tag'" --header "Authorization: Bearer $admin_api_key")

# Exit script if no matching requests found
if ! echo "$tagged_requests" | grep -q "<request "; then
  echo "No requests found with tag '$tag'. Exiting."
  exit 0
fi

# get id of last request with given tag
readonly max_tagged_id=$(echo "$tagged_requests" | grep -oP '(?<=request id=")[0-9]+' | sort -nr | head -n1)

# allocate new request on broker
readonly allocate_res=$(curl -s -i --request POST "$broker_url/request" --header "Authorization: Bearer $admin_api_key")
readonly new_request_id=$(echo "$allocate_res" | grep -oE 'Location: .*/[0-9]+' | grep -oE '[0-9]+$')

increase_date_in_request_content() {
  local content="$1"
  local element="$2"
  local old_date=$(echo "$content" | sed -n "s/.*<$element>\(.*\)<\/$element>.*/\1/p")
  local new_date=$(date -u -d "$old_date + $days_to_increase days" +"%Y-%m-%dT%H:%M:%S.000Z")
  echo "$content" | sed "s|<$element>$old_date</$element>|<$element>$new_date</$element>|"
}

# get content of request with max id and increment id and update dates
response=$(curl -s --request GET "$broker_url/request/$max_tagged_id" --header "Authorization: Bearer $admin_api_key")
updated_response=$(echo "$response" | sed "s|<id>[0-9]*</id>|<id>$new_request_id</id>|")
updated_response=$(increase_date_in_request_content "$updated_response" "reference")
updated_response=$(increase_date_in_request_content "$updated_response" "scheduled")

# put updated content to allocated request
curl -s --request PUT "$broker_url/request/$new_request_id" --data-raw "$updated_response" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/vnd.aktin.query.request+xml"

# get targeted nodes
target_nodes_response=$(curl -s -w "%{http_code}" --request GET "$broker_url/request/$max_tagged_id/nodes" --header "Authorization: Bearer $admin_api_key")
target_nodes="${target_nodes_response::-3}"
http_code="${target_nodes_response: -3}"

# if GET request is successful, add target nodes in new request
# if no target nodes are set, request is global
if [[ "$http_code" -ne 404 ]]; then
  curl -s --request PUT "$broker_url/request/$new_request_id/nodes" --data-raw "$target_nodes" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/xml"
fi

# publish new request
curl -s --request POST "$broker_url/request/$new_request_id/publish" --header "Authorization: Bearer $admin_api_key"
