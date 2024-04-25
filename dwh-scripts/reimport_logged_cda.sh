#!/bin/bash
#--------------------------------------
# Script Name:  reimport_logged_cda.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Reimports stored CDAs back in to the AKTIN DWH
#--------------------------------------

properties_path="/etc/aktin/aktin.properties"
url="http://localhost:80/aktin/cda/fhir/Binary/"
namespace="urn:hl7-org:v3"

# Check if script is run as root
if [[ "$EUID" -ne 0 ]]; then
    echo "Please run as root"
    exit 1
fi

# Check if path to folder with CDAs is provided
if [ $# -eq 0 ]; then
    echo "Please provide the path to the folder with CDAs to import (default should be /tmp/)"
    exit 1
else
    path=$1
fi

# Check if CDA debugging is activated
key_value=$(grep "^import.cda.debug.level=" $properties_path | cut -d '=' -f2)
if [ "$key_value" != "all" ]; then
    echo "Key 'import.cda.debug.level' is not set to 'all'. Exiting script."
    exit 1
fi

# Install xmlstarlet for processing XML-files
if ! which xmlstarlet >/dev/null; then
    echo "xmlstarlet not found, installing..."
    sudo apt-get update
    sudo apt-get install -y xmlstarlet
else
    echo "xmlstarlet is already installed"
fi

# Check and handle services, postgres must be running, wildlfy must be stopped
if systemctl is-active --quiet postgresql; then
    echo "Postgresql service is running"
else
    systemctl start postgresql
    while ! systemctl is-active --quiet postgresql; do
        echo "Waiting for Postgresql to start..."
        sleep 3
    done
    echo "Postgresql service has started"
fi
if systemctl is-active --quiet wildfly; then
    echo "Wildfly service is running, stopping it now"
    systemctl stop wildfly
    while systemctl is-active --quiet wildfly; do
        echo "Waiting for Wildfly to stop..."
        sleep 3
    done
    echo "Wildfly has stopped"
else
    echo "Wildfly is not running"
fi

# Get the current path to debug dir
old_debug_dir=$(grep "^import.cda.debug.dir=" "$properties_path" | cut -d'=' -f2)

# Create new folder /tmp/reimport_cda_<CURRENT_DATE>
new_debug_dir="/tmp/reimport_cda_$(date +"%d-%m-%Y+%H:%M")"
mkdir -p "$new_debug_dir"
chown wildfly: $new_debug_dir
echo "Created new folder: $new_debug_dir"

# Change the value of import.cda.debug.dir to the newly created folder
# Make a backup before inplace change
cp "$properties_path" "${properties_path}.bak"
sed -i "s|^import.cda.debug.dir=.*$|import.cda.debug.dir=${new_debug_dir}|g" "$properties_path"
echo "Updated value of import.cda.debug.dir in $properties_path to $new_debug_dir"

# Delete all patients and encounters from database
echo "Clearing AKTIN database"
tables=("encounter_mapping" "visit_dimension" "observation_fact" "patient_mapping" "patient_dimension")
for table in "${tables[@]}"; do
    echo "Truncating $table"
    sudo -u postgres psql -d i2b2 -c "TRUNCATE TABLE i2b2crcdata.$table;"
done

# Restart database to clear cache and avoid inconsistencies
echo "Restaring Postgresql service"
systemctl restart postgresql
while ! systemctl is-active --quiet postgresql; do
    echo "Waiting for Postgresql to start..."
    sleep 3
done
echo "Postgresql service has started"

# Ensure wildfly has started and deployments are done
deployments_dir="/path/to/wildfly/standalone/deployments"
while ! systemctl is-active --quiet wildfly; do
    sudo systemctl start wildfly
    echo "Waiting for wildfly to start..."
    sleep 10
done
while ls $deployments_dir/*.isdeploying >/dev/null 2>&1 || ls $deployments_dir/*.war >/dev/null 2>&1; do
    echo "Waiting for all applications to finish deploying..."
    sleep 3
done
if ls $deployments_dir/*.failed >/dev/null 2>&1; then
    echo "Warning: Some applications failed to deploy"
    exit 1
fi
echo "Widfly has started and all applications deployed"

echo "Reimporting encounter from $path"
# for each XML file in a given directory, stores metadata in an array along with the file name
patient_root=$(grep "^cda.patient.root.preset=" "/etc/aktin/aktin.properties" | cut -d '=' -f2)
encounter_root=$(grep "^cda.encounter.root.preset=" "/etc/aktin/aktin.properties" | cut -d '=' -f2)
billing_root=$(grep "^cda.billing.root.preset=" "/etc/aktin/aktin.properties" | cut -d '=' -f2)
xml_files=("$path"/*.xml)
declare -a import_log_list
for index in "${!xml_files[@]}"; do
    xml_path=${xml_files[$index]}
    creation_date=$(xmlstarlet sel -N x=$namespace -t -v "/x:ClinicalDocument/x:effectiveTime/@value" $xml_path)
    patient_contact_start=$(xmlstarlet sel -N x=$namespace -t -v "/x:ClinicalDocument/x:componentOf/x:encompassingEncounter/x:effectiveTime/x:low/@value" $xml_path)
    patient_id=$(xmlstarlet sel -N x=$namespace -t -v "/x:ClinicalDocument/x:recordTarget/x:patientRole/x:id[@root='$patient_root']/@extension" $xml_path)
    encounter_id=$(xmlstarlet sel -N x=$namespace -t -v "/x:ClinicalDocument/x:componentOf/x:encompassingEncounter/x:id[@root='$encounter_root']/@extension" $xml_path)
    billing_id=$(xmlstarlet sel -N x=$namespace -t -v "/x:ClinicalDocument/x:componentOf/x:encompassingEncounter/x:id[@root='$billing_id']/@extension" $xml_path)
    import_log_list[$index]="$xml_path|$creation_date|$patient_contact_start|$patient_id|$encounter_id|$billing_id"
done

# Sort import_log_list by $creation_date (index 2)
sorted_import_log_list=()
printf '%s\n' "${import_log_list[@]}" >temp_file.txt
sort -t '|' -k2 -n temp_file.txt >sorted_file.txt # assuming '|' as a delimiter
while IFS= read -r line; do
    sorted_import_log_list+=("$line")
done <sorted_file.txt
rm temp_file.txt sorted_file.txt

# Reimport sorted CDA files and print metadata and http status code
echo "cda path | creation date | start of patient contact | patient id | encounter id | billing id | http code"
IFS='|' # Setting IFS to pipe character
for index in "${!sorted_import_log_list[@]}"; do
    read -ra ADDR <<<"${sorted_import_log_list[$index]}"
    cda=${ADDR[0]}
    http_code=$(curl -o /dev/null -s -w "%{http_code}" -X POST -H "Content-Type: application/xml" -d @$cda $url)
    echo "${sorted_import_log_list[$index]}|$http_code"
done
unset IFS # Reset IFS to its original value

# Set debug dir to value before script was run
sed -i "s|^import.cda.debug.dir=.*$|import.cda.debug.dir=${old_debug_dir}|g" "$properties_path"
echo "Set value of import.cda.debug.dir in $properties_path back to $old_debug_dir"

# Restart Wildfly to apply changes
echo "Restart Wildfly to apply changes"
sudo systemctl restart wildfly
sleep 10
while ! systemctl is-active --quiet wildfly; do
    echo "Waiting for wildfly to restart..."
    sleep 3
done
while ls $deployments_dir/*.isdeploying >/dev/null 2>&1 || ls $deployments_dir/*.war >/dev/null 2>&1; do
    echo "Waiting for all applications to finish deploying..."
    sleep 3
done
if ls $deployments_dir/*.failed >/dev/null 2>&1; then
    echo "Warning: Some applications failed to deploy"
    exit 1
fi
echo "Widfly has started and all applications deployed"
