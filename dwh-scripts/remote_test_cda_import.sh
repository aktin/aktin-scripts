#!/bin/bash
#--------------------------------------
# Script Name:  remote_test_cda_import.sh
# Version:      1.1
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Imports included XML files of a given zip or folder into remote
#               AKTIN DWH via endpoint
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -lt 2 ]] && {
  echo "Usage: $0 <dwh_ip> <storyboards_zip_or_folder>"
  exit 1
}

readonly dwh_ip=$1
readonly input_path=$2
storyboards_dir=""

# Clean up the storyboards_dir when the script exits (only for zip files)
function cleanup {
  if [[ -n "$storyboards_dir" && -d "$storyboards_dir" ]]; then
    rm -rf "${storyboards_dir}"
  fi
}
trap cleanup EXIT

function run_curl_command {
    local xml_file=$1
    curl -v -H "Content-Type: application/xml" -d "@${xml_file}" "http://$dwh_ip/aktin/cda/fhir/Binary/"
}

function process_folder {
  for xml in $(find "$1" -type f -name "*.xml"); do
    run_curl_command "$xml"
  done
}

if [[ -f "$input_path" && "$input_path" == *.zip ]]; then
  readonly storyboards_dir="${input_path%.*}"
  unzip -q -d "$storyboards_dir" "$input_path"
  process_folder "$storyboards_dir"
elif [[ -d "$input_path" ]]; then
  process_folder "$input_path"
else
  echo "Error: $input_path is not a valid zip file or folder."
  exit 1
fi

