#!/usr/bin/env bash
#--------------------------------------
# Script Name:  check_p21_files_matching.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Verifies given zip file with p21 data and checks if corresponding encounter appear in the database
#               of AKTIN DWH. Outputs files list each encounter of corresponding csv files with invalid encounters
#               marked with !!. Additionally, an output file gives information, if an encounter can be linked
#               between csv files and matched with encounter in database.
#--------------------------------------

# Set locale settings to avoid Perl warnings
export LANGUAGE="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"
export LANG="en_US.UTF-8"

# Define color codes for terminal text formatting
readonly whi="\e[0m"    # white (reset)
readonly ora="\e[1;33m" # orange (bold)
readonly red="\e[1;31m" # red (bold)

readonly current_date=$(date +%Y-%m-%d_%H:%M)
readonly output_fall="output_fall_$current_date.csv"
readonly output_fab="output_fab_$current_date.csv"
readonly output_icd="output_icd_$current_date.csv"
readonly output_ops="output_ops_$current_date.csv"
readonly output_merged="merged_encounter_$current_date.csv"

# Check if the script is being run as the root user (EUID 0)
if [[ $EUID -ne 0 ]]; then
    echo -e "${red}script must be executed as root!${whi}"
    exit 1
fi
# Check if the path to p21.zip is provided and if the file exists.
readonly path_p21_zip="${1}"
if [[ -z "$path_p21_zip" ]]; then
    echo -e "${red}path to p21.zip is missing.${whi}"
    exit 1
elif [[ ! -f "$path_p21_zip" ]]; then
    echo -e "${red}file \"$path_p21_zip\" does not exist.${whi}"
    exit 1
fi
# Check if the aktin.properties file exists at the given path.
readonly path_aktin_properties=${2:-"/etc/aktin/aktin.properties"}
if [[ ! -f $path_aktin_properties ]]; then
    echo -e "${red}aktin.properties does not exist at the given path!${whi}"
    exit 1
fi

# Create a temporary directory to unzip files and perform file operations.
# Unzip the p21.zip file to the temporary directory.
# Rename files in the temporary directory to lowercase.
# Initialize variables to store the paths of specific CSV files.
readonly tmp_dir=$(mktemp -d)
unzip -q "$path_p21_zip" -d "$tmp_dir"
find "$tmp_dir" -type f -exec sh -c 'mv "$0" "${0%/*}/$(basename "${0}" | tr "[:upper:]" "[:lower:]")"' {} \;
for file in $(ls "$tmp_dir"); do
    case "$file" in
    "fall.csv")
        readonly path_fall="$tmp_dir/$file"
        ;;
    "fab.csv")
        readonly path_fab="$tmp_dir/$file"
        ;;
    "icd.csv")
        readonly path_icd="$tmp_dir/$file"
        ;;
    "ops.csv")
        readonly path_ops="$tmp_dir/$file"
        ;;
    esac
done

# This function checks the existence of a given file and creates an output file if the input file exists.
#
# Parameters:
# - filename: Name of the file to check.
# - path_csv: Path to the CSV file to check.
# - path_output: Path where the output file should be created.
# - is_mandatory: Flag to indicate if the file is mandatory.
#
# Returns:
# - No return value. The function exits with a code of 1 if the mandatory file does not exist.
check_and_touch_file() {
    local filename=$1
    local path_csv=$2
    local path_output=$3
    local is_mandatory=$4
    if [ -z "$path_csv" ]; then
        if [ "$is_mandatory" == "true" ]; then
            echo -e "${red}path to $filename is missing${whi}"
            exit 1
        else
            echo -e "${ora}path to $filename not provided${whi}"
        fi
    elif [ ! -f $path_csv ]; then
        echo -e "${red}file \"$path_csv\" does not exist${whi}"
        exit 1
    else
        touch $path_output
    fi
}

check_and_touch_file "fall.csv" "$path_fall" "$output_fall" "true"
check_and_touch_file "fab.csv" "$path_fab" "$output_fab" "false"
check_and_touch_file "icd.csv" "$path_icd" "$output_icd" "false"
check_and_touch_file "ops.csv" "$path_ops" "$output_ops" "false"

# This function creates a temporary file with the headers of the source file converted to lowercase.
#
# Parameters:
# - src_file: Source file with the original headers.
# - tmp_file: Temporary file to store the headers with lowercase.
#
# Returns:
# - No return value. A new file with lowercase headers is created at the location specified by tmp_file.
create_tmp_file_with_lowercase_headers() {
    local src_file=$1
    local tmp_file=$2
    head -1 "$src_file" | tr "[:upper:]" "[:lower:]" | tr -d ' ' | tr -d '-' >"$tmp_file"
    tail -n +2 "$src_file" >>"$tmp_file"
}

# This function checks that a CSV file contains all mandatory headers.
#
# Parameters:
# - tmp_file: Temporary file with the headers to check.
# - headers_mandatory: Array containing the mandatory headers to check.
#
# Returns:
# - No return value. The function exits with a code of 1 if any mandatory header is missing.
check_mandatory_headers() {
    local tmp_file=$1
    local headers_mandatory=("$@")
    unset headers_mandatory[0]
    local headers=$(head -1 "$tmp_file")
    for header in "${headers_mandatory[@]}"; do
        if ! echo "$headers" | grep -q "$header"; then
            echo -e "${red}header \"$header\" is missing in $tmp_file${whi}"
            exit 1
        fi
    done
}

# This function trims special characters from a given string.
#
# Parameters:
# - The string from which to trim special characters.
#
# Returns:
# - The trimmed string.
trim_special_chars() {
    echo "$1" | sed 's/[^a-zA-Z0-9]*//g'
}

# This function checks the validity of CSV columns based on given regex patterns.
#
# Parameters:
#   - tmp_file: The temporary file containing the CSV data.
#   - output_file: The file to store the valid CSV data.
#   - headers_mandatory: A space-separated string containing the mandatory headers.
#   - regex_patterns: A semicolon-separated string containing the regex patterns for validation.
#
# Returns:
#   - No return value. A new file containing the valid data is created at the location specified by output_file.
check_csv_columns_for_validity() {
    local tmp_file="$1"
    local output_file="$2"
    local -a headers_mandatory
    IFS=' ' read -ra headers_mandatory <<<"$3"
    local -a regex_patterns
    IFS=';' read -ra regex_patterns <<<"$4"

    # Initialize local_index_map with empty strings
    declare -a local_index_map
    for header in "${headers_mandatory[@]}"; do
        local_index_map+=("")
    done

    # Find the indeces of the mandatory headers in the CSV data and store them in local_index_map
    header=($(head -1 "$tmp_file" | tr ';' ' '))
    for i in "${!header[@]}"; do
        trimmed_header=$(trim_special_chars "${header[$i]}")
        for idx in "${!headers_mandatory[@]}"; do
            if [[ "$trimmed_header" == "${headers_mandatory[$idx]}" ]]; then
                local_index_map[$idx]=$i
            fi
        done
    done

    # Write the mandatory headers to the output_file
    echo "$(
        IFS=';'
        echo "${headers_mandatory[*]}"
    )" >"$output_file"

    # Start processing file line by line
    while IFS= read -r line; do
        IFS=';' read -ra values <<<"$line"
        output_line=""
        flag_invalid=0

        # Iterate through the mandatory headers and check the values for the given regex pattern
        # Store the values of the mandatory columns in a new CSV file
        # Mark invalid values with !!
        for idx in "${!headers_mandatory[@]}"; do
            original_val="${values[${local_index_map[$idx]}]}"
            val=$(trim_special_chars "$original_val")
            regex="${regex_patterns[$idx]}"
            if [[ ! $val =~ $regex ]]; then
                flag_invalid=1
                val="!!$val"
            fi
            output_line="$output_line$val;"
        done

        # Mark khinterneskennzeichen with !! if any value was invalid
        if [[ $flag_invalid -eq 1 ]]; then
            IFS=';' read -ra output_values <<<"$output_line"
            output_values[0]="!!${output_values[0]}"
            output_line=$(
                IFS=';'
                echo "${output_values[*]}"
            )
        fi

        echo "$output_line" >>"$output_file"
    done < <(tail -n +2 "$tmp_file")
}

readonly tmp_fall="temp_fall.csv"
declare -a fall_headers_mandatory=("khinterneskennzeichen" "aufnahmedatum" "aufnahmegrund" "aufnahmeanlass")
readonly fall_regex_patterns="^.*$;^[0-9]{12}$;^(0[1-9]|10)[0-9]{2}$;^[EZNRVAGB]$"
create_tmp_file_with_lowercase_headers "$path_fall" "$tmp_fall"
check_mandatory_headers "$tmp_fall" "${fall_headers_mandatory[@]}"
check_csv_columns_for_validity "$tmp_fall" "$output_fall" "${fall_headers_mandatory[*]}" "$fall_regex_patterns"

readonly tmp_fab="temp_fab.csv"
declare -a fab_headers_mandatory=("khinterneskennzeichen" "fachabteilung" "fabaufnahmedatum" "kennungintensivbett")
readonly fab_regex_patterns="^.*$;^(HA|BA|BE)[0-9]{4}$;^[0-9]{12}$;^(J|N)$"
if [ -n "$path_fab" ]; then
    create_tmp_file_with_lowercase_headers "$path_fab" "$tmp_fab"
    check_mandatory_headers "$tmp_fab" "${fab_headers_mandatory[@]}"
    check_csv_columns_for_validity "$tmp_fab" "$output_fab" "${fab_headers_mandatory[*]}" "$fab_regex_patterns"
fi

readonly tmp_icd="temp_icd.csv"
declare -a icd_headers_mandatory=("khinterneskennzeichen" "diagnoseart" "icdversion" "icdkode")
readonly icd_regex_patterns="^.*$;^(HD|ND|SD)$;^20[0-9]{2}$;^[A-Z][0-9]{2}(\.)?.{0,5}$"
if [ -n "$path_icd" ]; then
    create_tmp_file_with_lowercase_headers "$path_icd" "$tmp_icd"
    check_mandatory_headers "$tmp_icd" "${icd_headers_mandatory[@]}"
    check_csv_columns_for_validity "$tmp_icd" "$output_icd" "${icd_headers_mandatory[*]}" "$icd_regex_patterns"
fi

readonly tmp_ops="temp_ops.csv"
declare -a ops_headers_mandatory=("khinterneskennzeichen" "opsversion" "opskode" "opsdatum")
readonly ops_regex_patterns="^.*$;^20[0-9]{2}$;^[0-9]{1}(\-)?[0-9]{2}(.{1})?(\.)?.{0,3}$;^[0-9]{12}$"
if [ -n "$path_ops" ]; then
    create_tmp_file_with_lowercase_headers "$path_ops" "$tmp_ops"
    check_mandatory_headers "$tmp_ops" "${ops_headers_mandatory[@]}"
    check_csv_columns_for_validity "$tmp_ops" "$output_ops" "${ops_headers_mandatory[*]}" "$ops_regex_patterns"
fi

# This function extracts the ID column values from a CSV file and stores them in a new file.
#
# Parameters:
# - input_file: The CSV file to extract IDs from.
# - output_file: The file to store the extracted IDs.
#
# Returns:
# - No return value. A new file containing the IDs is created at the location specified by output_file.
function extract_ids {
    local input_file=$1
    local output_file=$2
    if [[ -f $input_file ]]; then
        awk -F';' 'NR>1 {print $1}' "$input_file" >"$output_file"
    else
        touch "$output_file"
    fi
}

extract_ids $output_fall $tmp_fall
extract_ids $output_fab $tmp_fab
extract_ids $output_icd $tmp_icd
extract_ids $output_ops $tmp_ops

# Checks if any of the temporary files ($tmp_fab, $tmp_icd, $tmp_ops) contain data.
# If at least one of them has content, it creates a merged output file ($output_merged) with a header and merges corresponding data from these temporary files, separating values with semicolons (;).
# If none of the temporary files have content, it copies the output fall file ($output_fall) to the merged output file.
# The merged data is sorted by the first column (fall) in ascending order and saved to the final merged output file.
# Temporary files are removed after processing.
if [[ -s $tmp_fab ]] || [[ -s $tmp_icd ]] || [[ -s $tmp_ops ]]; then
    echo "fall;fab;icd;ops" >$output_merged
    while IFS= read -r line; do
        fall_value=$line
        fab_value=$(grep -w -m 1 "$line" $tmp_fab)
        icd_value=$(grep -w -m 1 "$line" $tmp_icd)
        ops_value=$(grep -w -m 1 "$line" $tmp_ops)
        echo "$fall_value;$fab_value;$icd_value;$ops_value" >>$output_merged
    done <$tmp_fall
    {
        echo "fall;fab;icd;ops"
        tail -n +2 $output_merged | sort -t';' -k1,1n
    } >sorted_$output_merged
    mv sorted_$output_merged $output_merged
else
    cp $output_fall $output_merged
fi
rm $tmp_fall $tmp_fab $tmp_icd $tmp_ops

# Calculates hash values for the first column of merged output file ($output_merged).
# Root values for the hashes are extracted from the aktin.properties file.
# Hash algorithm is in accordance with the used DWH process.
# Hashed values are append as the columns 'encounter_ide' and 'billing_ide'.
readonly enc_root=$(grep "cda.encounter.root=*" "$path_aktin_properties" | awk -F'=' '{print $2}')
readonly bill_root=$(grep "cda.billing.root=*" "$path_aktin_properties" | awk -F'=' '{print $2}')
awk -F';' -v enc_root="$enc_root" -v bill_root="$bill_root" '
    function calculate_hash(root, value) {
        cmd = "echo -n " root "/" value " | openssl sha1 -binary | base64"
        cmd | getline result
        close(cmd)
        return result
    }
    NR == 1 {
        # Process the header row
        $0 = $0 ";encounter_ide;billing_ide"
        OFS=";"
        print
    }
    NR > 1 {
        gsub(/^!!/, "", $1)
        sub(/[[:space:]]*$/, "", $1)
        encounter_ide = calculate_hash(enc_root, $1)
        billing_ide = calculate_hash(bill_root, $1)
        OFS=";"
        print $1, $2, $3, $4, encounter_ide, billing_ide
    }
' "$output_merged" >tmp_$output_merged
mv tmp_$output_merged $output_merged

# Retrieves the encounter ids and billing ids from the i2b2 database.
# Checks for links between the retrieved ids and the hashed ids in the merged output file.
# Existing links are marked in the corresponding row by an "X" in new columns.
# For a linkage in the encounter id, the column "linked_encounter" is appended.
# For a linkage in the billing id, the column "linked_billing" is appended.
sql_encounter="SELECT enc.encounter_ide FROM i2b2crcdata.encounter_mapping enc JOIN i2b2crcdata.patient_mapping pat ON enc.patient_ide = pat.patient_ide LEFT JOIN i2b2crcdata.optinout_patients opt ON pat.patient_ide = opt.pat_psn WHERE opt.study_id != 'AKTIN' OR opt.pat_psn IS NULL;"
sql_billing="SELECT obs.tval_char FROM i2b2crcdata.observation_fact obs JOIN i2b2crcdata.patient_mapping pat ON obs.patient_num = pat.patient_num LEFT JOIN i2b2crcdata.optinout_patients opt ON pat.patient_ide = opt.pat_psn WHERE (opt.study_id != 'AKTIN' OR opt.pat_psn IS NULL) AND obs.concept_cd = 'AKTIN:Fallkennzeichen';"
sql_encouter_result_file="sql_result_encounter.txt"
sql_billing_result_file="sql_result_billing.txt"
sudo -u postgres psql -d i2b2 -c "$sql_encounter" >"$sql_encouter_result_file"
sudo -u postgres psql -d i2b2 -c "$sql_billing" >"$sql_billing_result_file"
awk -F';' -v OFS=';' -v sql_encounter_file="$sql_encouter_result_file" -v sql_billing_file="$sql_billing_result_file" '
    function trim(s) {
        sub(/^[[:space:]]+/, "", s)
        sub(/[[:space:]]+$/, "", s)
        return s
    }
    BEGIN {
        getline header < "'"$output_merged"'"
        print header ";linked_encounter;linked_billing"
        while ((getline < sql_encounter_file) > 0) {
            sql_encounter[trim($1)] = 1
        }
        close(sql_encounter_file)
        while ((getline < sql_billing_file) > 0) {
            sql_billing[trim($1)] = 1
        }
        close(sql_billing_file)
    }
    NR > 1 {
        linked_encounter = ($5 in sql_encounter) ? "X" : ""
        linked_billing = ($6 in sql_billing) ? "X" : ""
        print $0, linked_encounter, linked_billing
    }
' "$output_merged" >tmp_$output_merged
mv tmp_$output_merged "$output_merged"
rm $sql_encouter_result_file $sql_billing_result_file
