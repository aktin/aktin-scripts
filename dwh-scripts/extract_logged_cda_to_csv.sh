#!/bin/bash
#--------------------------------------
# Script Name:  extract_logged_cda_to_csv.sh
# Version:      1.2
# Author:       jbienzeisler@ukaachen.de, akombeiz@ukaachen.de, jkramer@ukaachen.de
# Date:         10 Oct 24
# Purpose:      This script processes CDA (Clinical Document Architecture) XML files in a specified directory, extracts
#               specific medical data fields, and saves the results into a timestamped CSV file while logging errors.
#--------------------------------------

# pre-checks
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <path_to_aktin_properties> <path_to_cda_folder>"
  exit 1
fi

if ! command -v xmllint &> /dev/null; then
  echo "Error: libxml2-utils is required but not installed" >&2
  exit 1
fi

# set to "ALL" to disable zipcode filtering
ALLOWED_POSTAL_CODES=("ALL")

# setup paths
AKTIN_PROPERTIES_PATH="$1"
CDA_DIR_PATH="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

current_datetime=$(TZ=Europe/Berlin date +'%Y-%m-%d_%H-%M-%S')
CSV_FILE="$SCRIPT_DIR/cda_data_$current_datetime.csv"
LOG_FILE="$SCRIPT_DIR/cda_log_$current_datetime.log"

# enable global logging
exec >> "$LOG_FILE" 2>&1

extract_roots_from_aktin_properties() {
  local aktin_properties="$1"
  if [ ! -f "$aktin_properties" ]; then
    echo "Error: AKTIN properties file not found at $aktin_properties" >&2
    exit 1
  fi
  encounter_root=$(grep -oP '(?<=cda.encounter.root.preset=)[^\r\n]+' "$aktin_properties")
}

initialize_csv_file_if_nonexisting() {
  local csv_file="$1"
  if [ ! -f "$csv_file" ]; then
    printf "encounter_id\tzipcode\tstreet_address\tbirth_date\tgender\tadmission_date\tdischarge_date\tdischarge_code\tcedis_code\tdiagnosis\tfile\n" > "$csv_file"
  fi
}

get_xml_val() {
  local xpath="$1"
  local file="$2"
  local result
  result=$(xmllint --xpath "$xpath" "$file" 2>/dev/null)
  if [ -z "$result" ]; then echo "NA"; else echo "$result"; fi
}

is_postal_code_allowed() {
  local postal_code="$1"
  for allowed_code in "${ALLOWED_POSTAL_CODES[@]}"; do
    if [[ "$allowed_code" == "ALL" ]] || [[ "$postal_code" == "$allowed_code" ]]; then
      return 0
    fi
  done
  return 1 # Postal code is not allowed
}

parse_cda_file() {
  local cda_file="$1"
  if ! grep -q "<ClinicalDocument" "$cda_file"; then
    echo "Error: File $(basename "$cda_file") is not a valid CDA file." >&2
    return 1
  fi
  # extract zipcode first to filter immediately
  local zipcode=$(get_xml_val "string(//*[local-name()='postalCode']/text())" "$cda_file")
  if ! is_postal_code_allowed "$zipcode"; then
    return
  fi
  local encounter_id=$(get_xml_val "string(//*[local-name()='id'][@root='$encounter_root']/@extension)" "$cda_file")
  local street_address=$(get_xml_val "string(//*[local-name()='streetAddressLine']/text())" "$cda_file")
  local birth_date=$(get_xml_val "string(//*[local-name()='birthTime']/@value)" "$cda_file")
  local gender=$(get_xml_val "string(//*[local-name()='administrativeGenderCode']/@code)" "$cda_file")
  local admission_date=$(get_xml_val "string(//*[local-name()='encompassingEncounter']/*[local-name()='effectiveTime']/*[local-name()='low']/@value)" "$cda_file")
  local discharge_date=$(get_xml_val "string(//*[local-name()='encompassingEncounter']/*[local-name()='effectiveTime']/*[local-name()='high']/@value)" "$cda_file")
  local discharge_code=$(get_xml_val "string(//*[local-name()='dischargeDispositionCode']/@code)" "$cda_file")
  local cedis_code=$(get_xml_val "string(//*[local-name()='value'][@codeSystem='1.2.276.0.76.5.439']/@code)" "$cda_file")
  # get all diagnosis fields
  local diagnosis=$(xmllint --xpath "//*[local-name()='value'][@codeSystem='1.2.276.0.76.5.424']/@code" "$cda_file" 2>/dev/null | grep -oP '(?<=code=")[^"]*' | paste -sd "," -)
  if [ -z "$diagnosis" ]; then diagnosis="NA"; fi
  local file_name=$(basename "$cda_file")
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$encounter_id" "$zipcode" "$street_address" "$birth_date" "$gender" "$admission_date" "$discharge_date" "$discharge_code" "$cedis_code" "$diagnosis" "$file_name" >> "$CSV_FILE"
}

process_all_cda_files_in_dir() {
  local cda_folder="$1"
  for cda_file in "$cda_folder"/*.xml; do
    if [[ -f "$cda_file" ]]; then
      parse_cda_file "$cda_file"
    fi
  done
}

main() {
  extract_roots_from_aktin_properties "$AKTIN_PROPERTIES_PATH"
  initialize_csv_file_if_nonexisting "$CSV_FILE"
  process_all_cda_files_in_dir "$CDA_DIR_PATH"
  if [ ! -s "$LOG_FILE" ]; then
    rm "$LOG_FILE"
  fi
}

main "$@"
