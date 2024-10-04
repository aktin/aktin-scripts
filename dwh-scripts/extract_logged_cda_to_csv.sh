#!/bin/bash
#--------------------------------------
# Script Name:  extract_logged_cda_to_csv.sh
# Version:      1.1
# Author:       jbienzeisler@ukaachen.de & akombeiz@ukaachen.de
# Date:         10 Oct 24
# Purpose:      This script processes CDA (Clinical Document Architecture) XML files in a specified directory, extracts
#               specific medical data fields, and saves the results into a timestamped CSV file while logging errors.
#--------------------------------------

# ensure the correct number of arguments is provided
[[ $# -lt 2 ]] && {
  echo "Usage: $0 <path_to_aktin_properties> <path_to_cda_folder>"
  exit 1
}

# Config file and CDA directory
AKTIN_PROPERTIES_PATH="$1"
CDA_DIR_PATH="$2"

# Generate the CSV filename based on the current date and time (YYYY-MM-DD_HH-MM-SS)
current_datetime=$(TZ=Europe/Berlin date +'%Y-%m-%d_%H-%M-%S')
CSV_FILE="$CDA_DIR_PATH/cda_data_$current_datetime.csv"

# Log file for errors in the same directory as the CDA files
LOG_FILE="$CDA_DIR_PATH/cda_errors_$current_datetime.log"

extract_roots_from_aktin_properties() {
    local aktin_properties="$1"
    encounter_root=$(grep -oP '(?<=cda.encounter.root.preset=)[^\r\n]+' "$aktin_properties")
    billing_root=$(grep -oP '(?<=cda.billing.root.preset=)[^\r\n]+' "$aktin_properties")
}

initialize_csv_file_if_nonexisting() {
    local csv_file="$1"
    if [ ! -f "$csv_file" ]; then
        echo "\"Birth Date\",\"Gender\",\"Admission Date\",\"Discharge Date\",\"CEDIS Code\",\"MTS Score\",\"Transfer\",\"Postal Code\",\"Street Address\",\"Discharge Code\",\"Document ID\",\"Internal Case Number\",\"Diagnosis\",\"File\"" > "$csv_file"
    fi
}

is_postal_code_allowed() {
    local postal_code="$1"
    for allowed_code in "${ALLOWED_POSTAL_CODES[@]}"; do
        if [[ "$postal_code" == "$allowed_code" ]]; then
            return 0
        fi
    done
    return 1  # Postal code is not allowed
}

# Parse a CDA XML file and extract the necessary data
parse_cda_file() {
    local cda_file="$1"

    # Check if the file is a valid CDA document
    if ! grep -q "<ClinicalDocument" "$cda_file"; then
        echo "Error in file $cda_file: Not a valid CDA file." >> "$LOG_FILE"
        return 1
    fi

    local birth_date=$(grep -oP '(?<=<birthTime value=")[^"]*(?=")' "$cda_file" | head -1)
    [ -z "$birth_date" ] && birth_date="NA"

    local gender=$(grep -oP '(?<=<administrativeGenderCode code=")[^"]*(?=")' "$cda_file" | head -1)
    [ -z "$gender" ] && gender="NA"

    local admission_date=$(grep -oP '(?<=<low value=")[^"]*(?=")' "$cda_file" | head -1)
    [ -z "$admission_date" ] && admission_date="NA"

    local discharge_date=$(grep -oP '(?<=<high value=")[^"]*(?=")' "$cda_file" | head -1)
    [ -z "$discharge_date" ] && discharge_date="NA"

    local cedis_code=$(grep -oP '(?<=<value xsi:type="CE" code=")[^"]*(?=" codeSystem="1.2.276.0.76.5.439")' "$cda_file" | head -1)
    [ -z "$cedis_code" ] && cedis_code="NA"

    local mts_score=$(grep -oP '(?<=<value xsi:type="CE" code=")[^"]*(?=" codeSystem="1.2.276.0.76.5.438")' "$cda_file" | head -1)
    [ -z "$mts_score" ] && mts_score="NA"

    local transfer=$(grep -oP '(?<=<value xsi:type="CE" code=")[^"]*(?=" codeSystem="1.2.276.0.76.3.1.195.5.53")' "$cda_file" | head -1)
    [ -z "$transfer" ] && transfer="NA"

    local postal_code=$(grep -oP '(?<=<postalCode>)[^<]*(?=</postalCode>)' "$cda_file" | head -1)
    [ -z "$postal_code" ] && postal_code="NA"

    local street_address=$(grep -oP '(?<=<streetAddressLine>)[^<]*(?=</streetAddressLine>)' "$cda_file" | head -1)
    [ -z "$street_address" ] && street_address="NA"

    local discharge_code=$(grep -oP '(?<=<dischargeDispositionCode code=")[^"]*(?=" codeSystem="1.2.276.0.76.3.1.195.5.56")' "$cda_file" | head -1)
    [ -z "$discharge_code" ] && discharge_code="NA"

    local document_id=$(grep -oP "(?<=<id root=\"$encounter_root\" extension=\")[^\"]*(?=\")" "$cda_file" | head -1)
    [ -z "$document_id" ] && document_id="NA"

    local internal_case_number=$(grep -oP "(?<=<id root=\"$billing_root\" extension=\")[^\"]*(?=\")" "$cda_file" | head -1)
    [ -z "$internal_case_number" ] && internal_case_number="NA"

    local diagnosis=$(grep -oP '(?<=<value xsi:type="CD" code=")[^"]*(?=" codeSystem="1.2.276.0.76.5.424")' "$cda_file" | head -1)
    [ -z "$diagnosis" ] && diagnosis="NA"

    local file_name=$(basename "$cda_file")

    # Append the extracted data to the CSV file
    echo "\"$birth_date\",\"$gender\",\"$admission_date\",\"$discharge_date\",\"$cedis_code\",\"$mts_score\",\"$transfer\",\"$postal_code\",\"$street_address\",\"$discharge_code\",\"$document_id\",\"$internal_case_number\",\"$diagnosis\",\"$file_name\"" >> "$CSV_FILE"
}

# Process all CDA XML files in the specified directory
process_all_cda_files() {
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
  process_all_cda_files "$CDA_DIR_PATH"
}

main "$@"
