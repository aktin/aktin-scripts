#!/bin/bash
#--------------------------------------
# Script Name:  publish_new_request_from_tag.sh
# Version:      1.1
# Author:       whoy@ukaachen.de, akombeiz@ukaachen.de
# Purpose:      Automates the duplication of a tagged request by retrieving the latest matching
#               request, updating its dates, and publishing it to the broker.
#--------------------------------------

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <broker_url> <admin_api_key> <tag> [time_to_increase]" >&2
  echo "  <broker_url>       Required: The base URL of the broker" >&2
  echo "  <admin_api_key>    Required: The admin API key for authentication" >&2
  echo "  <tag>              Required: The tag to filter requests" >&2
  echo "  [time_to_increase] Optional: Time increment (e.g., '+7 days', '-1 month', '+1 year', default: '+7 days')" >&2
  exit 1
fi

readonly BROKER_URL="$1/broker"
readonly AUTH_HEADER="Authorization: Bearer $2"
readonly TAG="$3"
readonly TIME_INCREMENT="${4:-"7 days"}"

get_max_id_of_tagged_requests() {
  local requests
  requests=$(curl -s --request GET "$BROKER_URL/request/filtered?type=application/vnd.aktin.query.request%2Bxml&predicate=//queryRequest/query/principal/tags/tag='$TAG'" --header "$AUTH_HEADER")
  if ! echo "$requests" | grep -q "<request "; then
    echo "No requests found with tag '$TAG'. Exiting."
    exit 0
  fi
  echo "$requests" | grep -oP '(?<=request id=")[0-9]+' | sort -nr | head -n1
}

allocate_new_request() {
  local response
  response=$(curl -s -i --request POST "$BROKER_URL/request" --header "$AUTH_HEADER")
  echo "$response" | grep -oE 'Location: .*/[0-9]+' | grep -oE '[0-9]+$'
}

increase_date_in_request_content() {
  local content="$1"
  local element="$2"
  local old_date
  old_date=$(echo "$content" | sed -n "s/.*<$element>\(.*\)<\/$element>.*/\1/p")
  local new_date
  new_date=$(date -u -d "$old_date + $TIME_INCREMENT" +"%Y-%m-%dT%H:%M:%S.000Z")
  echo "$content" | sed "s|<$element>$old_date</$element>|<$element>$new_date</$element>|"
}

clone_and_update_request() {
  local original_request_id="$1"
  local new_request_id="$2"
  local response
  response=$(curl -s --request GET "$BROKER_URL/request/$original_request_id" --header "$AUTH_HEADER")
  response=$(echo "$response" | sed "0,/<id>[0-9]*<\/id>/s|<id>[0-9]*</id>|<id>$new_request_id</id>|")
  response=$(increase_date_in_request_content "$response" "reference")
  response=$(increase_date_in_request_content "$response" "scheduled")
  echo "$response"
}

add_request_definition() {
  local request_id="$1"
  local content="$2"
  curl -s --request PUT "$BROKER_URL/request/$request_id" --data-raw "$content" --header "$AUTH_HEADER" --header "Content-Type: application/vnd.aktin.query.request+xml"
}

copy_target_nodes_from_request_to_another() {
  local old_request_id="$1"
  local new_request_id="$2"
  local response
  response=$(curl -s -w "%{http_code}" --request GET "$BROKER_URL/request/$old_request_id/nodes" --header "$AUTH_HEADER")
  target_nodes="${response::-3}"
  http_code="${response: -3}"
  # if GET request is successful, add target nodes in new request
  # if no target nodes are set, request is global
  if [[ "$http_code" -ne 404 ]]; then
    curl -s --request PUT "$BROKER_URL/request/$new_request_id/nodes" --data-raw "$target_nodes" --header "$AUTH_HEADER" --header "Content-Type: application/xml"
  fi
}

publish_request() {
  local request_id="$1"
  curl -s --request POST "$BROKER_URL/request/$request_id/publish" --header "$AUTH_HEADER"
}

main() {
  max_id=$(get_max_id_of_tagged_requests)
  new_id=$(allocate_new_request)
  updated_request=$(clone_and_update_request "$max_id" "$new_id")
  add_request_definition "$new_id" "$updated_request"
  copy_target_nodes_from_request_to_another "$max_id" "$new_id"
  publish_request "$new_id"
}

main "$@"
