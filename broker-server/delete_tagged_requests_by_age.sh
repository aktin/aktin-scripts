#!/bin/bash
#--------------------------------------
# Script Name:  delete_tagged_requests_by_age.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Purpose:      Deletes requests that match a specific tag AND are older than a specific time interval.
#--------------------------------------

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <broker_url> <admin_api_key> <tag> <min_age>" >&2
  echo "  <broker_url>    Required: The base URL of the broker" >&2
  echo "  <admin_api_key> Required: The admin API key for authentication" >&2
  echo "  <tag>           Required: The tag to filter requests" >&2
  echo "  [min_age]       Optional: The minimum age to delete (default: '1 month')" >&2
  exit 1
fi

readonly BROKER_URL="$1/broker"
readonly AUTH_HEADER="Authorization: Bearer $2"
readonly TAG="$3"
readonly MIN_AGE="${4:-"1 month"}"
readonly LOG_FILE="./cleanup_requests_${TAG}_${MIN_AGE// /_}.log"

# Buffer startup messages instead of writing them immediately
STARTUP_LOG_BUFFER=""
HAS_WRITTEN_HEADER=false

buffer_log() {
  local msg="$1"
  local timestamp
  timestamp=$(date "+%Y-%m-%d %H:%M:%S")
  # Append to our buffer variable with a newline
  STARTUP_LOG_BUFFER+="${STARTUP_LOG_BUFFER:+$'\n'}[$timestamp] $msg"
}

write_to_file() {
  local msg="$1"
  local timestamp
  timestamp=$(date "+%Y-%m-%d %H:%M:%S")

  if [[ "$HAS_WRITTEN_HEADER" == "false" ]]; then
    touch "$LOG_FILE"
    echo "$STARTUP_LOG_BUFFER" >> "$LOG_FILE"
    HAS_WRITTEN_HEADER=true
  fi
  echo "[$timestamp] $msg" >> "$LOG_FILE"
}

get_tagged_request_ids() {
  local requests
  requests=$(curl -s --request GET "$BROKER_URL/request/filtered?type=application/vnd.aktin.query.request%2Bxml&predicate=//queryRequest/query/principal/tags/tag='$TAG'" --header "$AUTH_HEADER")
  if ! echo "$requests" | grep -q "<request "; then
    # Return empty string if nothing found
    return
  fi
  # Extract all IDs (using Perl-compatible grep regex as in your original scripts)
  echo "$requests" | grep -oP '(?<=request id=")[0-9]+'
}

get_request_timestamp() {
  local request_id="$1"
  local response
  local date_str
  response=$(curl -s --request OPTIONS "$BROKER_URL/request/$request_id" --header "$AUTH_HEADER")
  date_str=$(echo "$response" | grep -oP '(?<=<published>)[^<]+')
  if [[ -n "$date_str" ]]; then
    date -d "$date_str" +%s # Convert published date to timestamp
  else
    echo "0" # Return 0 if date not found
  fi
}

delete_request() {
  local request_id="$1"
  local response_code
  response_code=$(curl -s -o /dev/null -w "%{http_code}" --request DELETE "$BROKER_URL/request/$request_id" --header "$AUTH_HEADER")
  if [[ "$response_code" -eq 204 ]] || [[ "$response_code" -eq 200 ]]; then
    write_to_file "[SUCCESS] Deleted request ID $request_id"
  else
    write_to_file "[ERROR] Failed to delete request ID $request_id (HTTP $response_code)"
  fi
}

check_single_request() {
  local id="$1"
  local cutoff_ts="$2"
  local published_ts
  published_ts=$(get_request_timestamp "$id")
  if [[ "$published_ts" -eq 0 ]]; then
    write_to_file "[WARN] ID $id: Could not determine published date. Skipping."
    return
  fi
  if [[ "$published_ts" -lt "$cutoff_ts" ]]; then
    delete_request "$id"
  fi
}

main() {
  local cutoff_ts
  local readable_cutoff
  local ids

  cutoff_ts=$(date -d "now - $MIN_AGE" +%s)
  readable_cutoff=$(date -d @$cutoff_ts)
  buffer_log "Config: Tag='$TAG', Age='$MIN_AGE', Cutoff='$readable_cutoff'"

  ids=$(get_tagged_request_ids)
  if [[ -z "$ids" ]]; then
    exit 0
  fi
  for id in $ids; do
    check_single_request "$id" "$cutoff_ts"
  done
}

main "$@"
