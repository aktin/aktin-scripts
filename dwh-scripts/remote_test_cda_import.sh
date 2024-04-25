#!/bin/bash
#--------------------------------------
# Script Name:  remote_test_cda_import.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Imports included XML files of a given zip into remote AKTIN DWH via
#               endpoint
#--------------------------------------

set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -ne 2 ]] && {
  echo "Usage: $0 <dwh_ip> <storyboards_zip>"
  exit 1
}

readonly dwh_ip=$1
readonly storyboards_zip=$2

# storyboards_zip without .zip at the end
readonly storyboards_dir="${storyboards_zip%.*}"

# Clean up the storyboards_dir when the script exits
trap 'rm -rf "${storyboards_dir}"' EXIT

unzip -q -d "$storyboards_dir" "$storyboards_zip"

function run_curl_command {
    local xml_file=$1
    curl -v -H "Content-Type: application/xml" -d "@${xml_file}" "http://$dwh_ip/aktin/cda/fhir/Binary/"
}

for xml in $(find "${storyboards_dir}" -type f -name "*.xml"); do
    run_curl_command "$xml"
done
