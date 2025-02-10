#!/bin/bash
#--------------------------------------
# Script Name:  publish_new_request_from_tag.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         05 Feb 25
# Purpose:      Uploads a new request of given tag, using the last requests' data and new timestamps
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -lt 3 ]] && {
  echo "Usage: $0 <broker_url> <admin_api_key> <tag>"
  exit 1
}

readonly broker_url="$1/broker"
readonly admin_api_key="$2"
readonly tag="$3"

# retireve list of requests containing given tag
readonly tagged_requests=$(curl -s --request GET "$broker_url/request/filtered?type=application/vnd.aktin.query.request%2Bxml&predicate=//queryRequest/query/principal/tags/tag='$tag'" --header "Authorization: Bearer $admin_api_key")

# get id of last request with given tag
readonly max_tagged_id=$(echo "$tagged_requests" | grep -oP '(?<=request id=")[0-9]+' | sort -nr | head -n1)

# created new request on broker
allocate_res=$(curl -s -i --request POST "$broker_url/request" --header "Authorization: Bearer $admin_api_key")
new_request_id=$(echo "$allocate_res" | grep -oE 'Location: .*/[0-9]+' | grep -oE '[0-9]+$')

# get last request content
response=$(curl -s --request GET "$broker_url/request/$max_tagged_id" --header "Authorization: Bearer $admin_api_key")

increase_ref_date_in_request_content() {
  request_content="$1"

  # Extract the 'reference' date from the content
  reference_date=$(echo "$request_content" | sed -n 's/.*<reference>\(.*\)<\/reference>.*/\1/p')

  # Add 7 days to the extracted reference date using the date command
  new_reference_date=$(date -u -d "$reference_date + 7 days" +"%Y-%m-%dT%H:%M:%S.000Z")

  # Replace the old reference date with the new one in the content using sed
  new_response=$(echo "$request_content" | sed "s|<reference>$reference_date</reference>|<reference>$new_reference_date</reference>|")

  echo "$new_response"
}

increase_scheduled_date_in_request_content() {
  request_content="$1"

  # Extract the 'reference' date from the content
  old_date=$(echo "$request_content" | sed -n 's/.*<scheduled>\(.*\)<\/scheduled>.*/\1/p')

  # Add 7 days to the extracted reference date using the date command
  new_date=$(date -u -d "$old_date + 7 days" +"%Y-%m-%dT%H:%M:%S.000Z")

  # Replace the old reference date with the new one in the content using sed
  new_response=$(echo "$request_content" | sed "s|<scheduled>$old_date</scheduled>|<scheduled>$new_date</scheduled>|")

  echo "$new_response"
}

# id, reference und scheduled ändern
response_id_inc=$(echo "$response" | sed "s|<id>[0-9]*</id>|<id>$new_request_id</id>|")
response_ref_inc=$(increase_ref_date_in_request_content "$response_id_inc")
response_sched_inc=$(increase_scheduled_date_in_request_content "$response_ref_inc")

# add content to new allocated request
curl -s --request PUT "$broker_url/request/$new_request_id" --data-raw "$response_sched_inc" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/vnd.aktin.query.request+xml"

# get targeted nodes
readonly nodes=$(curl -s --request GET "$broker_url/request/$max_tagged_id/nodes" --header "Authorization: Bearer $admin_api_key")

# add target nodes in new request
curl -s --request PUT "$broker_url/request/$new_request_id/nodes" --data-raw "$nodes" --header "Authorization: Bearer $admin_api_key" --header "Content-Type: application/xml"

#publish new request
curl -s --request POST "$broker_url/request/$new_request_id/publish" --header "Authorization: Bearer $admin_api_key"




