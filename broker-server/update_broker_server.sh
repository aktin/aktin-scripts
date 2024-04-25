#!/bin/bash
#--------------------------------------
# Script Name:  update_broker_server.sh
# Author:       akombeiz@ukaachen.de
# Date:         24 Apr 24
# Purpose:      Downloads given version of AKTIN Broker
#               and updates given local Broker to this
#               version
# ATTENTION:    Only useable on production server with
#               "broker" service 
#--------------------------------------

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

# ensure the correct number of arguments is provided
[[ $# -ne 2 ]] && {
  echo "Usage: $0 <broker_path> <version_to_update_to>"
  exit 1
}

readonly broker_path="$1"
readonly broker_lib_path="$1/lib/"
readonly new_version="$2"

stop_and_wait_for_service() {
  local service_name="$1"
  local timeout=30
  # attempt to stop the service
  sudo service "$service_name" stop || {
    echo "Failed to stop service: $service_name"
    return 1
  }
  # wait until the service is stopped or the timeout is reached
  for (( i=0; i<$timeout; i++ )); do
    sudo service "$service_name" status | grep -q 'not running' && break
    sleep 1
  done
  # check if the service stopped successfully
  if sudo service "$service_name" status | grep -q 'not running'; then
    echo "Service stopped: $service_name"
  else
    echo "Timeout waiting for service to stop: $service_name"
    return 1
  fi
}

start_and_wait_for_service() {
  local service_name="$1"
  local timeout=30
  # attempt to start the service
  sudo service "$service_name" start || {
    echo "Failed to start service: $service_name"
    return 1
  }
  # wait until the service is running or the timeout is reached
  for (( i=0; i<$timeout; i++ )); do
    sudo service "$service_name" status | grep -q 'running' && break
    sleep 1
  done
  # check if the service started successfully
  if sudo service "$service_name" status | grep -q 'running'; then
    echo "Service started: $service_name"
  else
    echo "Timeout waiting for service to start: $service_name"
    return 1
  fi
}

zip_folder_as_backup() {
  local folder_path="$1"
  # get folder name and parent directory
  local folder_name=$(basename "$folder_path")
  local parent_dir=$(dirname "$folder_path")
  # construct the zip file name
  local zip_file="${parent_dir}/${folder_name}_$(date +%Y%m%d).zip"
  # create the zip file (overwriting if exists)
  cd "$parent_dir" && zip -q "$zip_file" "$folder_name/*" || {
    echo "Error: Failed to create zip backup"
    return 1
  }
  echo "Zip backup created (overwritten): $zip_file"
}

download_broker_admin_dist() {
  local version="$1"
  echo "Downloading broker admin distribution version: $version"
  local download_url="https://github.com/aktin/broker/releases/download/v$version/broker-admin-dist-$version.zip"
  local target_file="/tmp/broker-admin-dist-$version.zip"
  echo "Download URL: $download_url"
  # use curl to download with progress indication (-#)
  curl -s -L -# "$download_url" -o "$target_file" || {
    echo "Error: Failed to download broker admin distribution"
    return 1
  }
  echo "$target_file"
}

extract_zip_and_get_extracted_folder() {
  local zip_file="$1"
  # get folder name and parent directory
  local folder_name=$(basename "$zip_file" .zip)
  local parent_dir=$(dirname "$zip_file")
  echo "Extracting $zip_file to $parent_dir/$folder_name"
  # extract the zip file into the same directory
  unzip -q "$zip_file" -d "$parent_dir/$folder_name" || {
    echo "Error: Failed to extract zip file"
    return 1
  }
  echo "$parent_dir/$folder_name"
}

main() {
  stop_and_wait_for_service "broker"
  zip_folder_as_backup "$broker_lib_path"
  new_version_zip_path=$(download_broker_admin_dist "$new_version")
  new_version_folder_path=$(extract_zip_and_get_extracted_folder "$new_version_zip_path")
  new_version_lib_path="$new_version_folder_path/lib/"
  # replace old lib with new downloaded lib
  rm -rf "$broker_lib_path"
  cp -r "$new_version_lib_path" "$broker_path"
  # remove downloaded broker admin dist
  rm $new_version_zip_path
  rm -r $new_version_folder_path
  start_and_wait_for_service "broker"
}

main
