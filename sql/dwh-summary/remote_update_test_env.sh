#!/bin/bash
#--------------------------------------
# Script Name:  remote_update_test_env.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         22 Apr 25
# Purpose:      Copies summarise_cases_in_dwh_by_month.sql and all setup and test scripts
# to a VM running a CI/CD pipeline for testing the script.
#--------------------------------------

# If pipe of commmands in this file fails, it displays the failed stage and not just the last one.
# "-e" -exit immediatly on non-zero status, "-u" - treat unset variable as error
set -euo pipefail

# ensure the correct number of arguments is provided
[[ $# -ne 0 ]] && {
  echo "No argument expected"
  exit 1
}


readonly remote_ip=192.168.122.214  # ip of VM with CI/CD pipeline
readonly source_dir=/home/wiliam/PycharmProjects/aktin-scripts/sql/dwh-summary  # path of main script and test/init files
readonly zip_name=dwh-summary.zip
readonly zip_dir=/home/wiliam/PycharmProjects/aktin-scripts/sql/$zip_name
readonly target_path=/var/lib/jenkins/docker
readonly target_zip=$target_path/$zip_name

cd "$(dirname "$source_dir")"
zip -r dwh-summary.zip "$(basename "$source_dir")"

ssh root@$remote_ip "rm -r $target_zip"
scp $zip_dir root@$remote_ip:$target_path
ssh root@$remote_ip "unzip $target_zip"

# Clean-up
rm -r $zip_dir