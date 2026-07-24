#!/usr/bin/env bash
# Imports patient consents (opt-in/opt-out) for a study directly via the
# REST API, bypassing the Angular frontend. Reads the list of extensions
# (patient/encounter/billing ids) from a CSV or Excel file.
#
# Calls: POST {BASE_URL}/studies/{studyId}/patients/batch  (OptInEndpoint#createEntries)
#
# Usage:
#   BASE_URL=http://localhost:80/aktin/admin/rest \
#   AKTIN_USER=i2b2 AKTIN_PASSWORD=secret \
#   ./import-consent.sh <studyId> <reference:Patient|Encounter|Billing> <participation:OptIn|OptOut> \
#                        <generateSic:true|false> <filePath> <columnName> [separator=","] [sic] [comment]

set -euo pipefail

# Reads one column from a CSV file into the global array REFERENCE_VALUES,
# for later functions to iterate over (patient/encounter/billing ids).
#
# Args: <csv_file> <column_name> [separator=","]
read_csv() {
    local csv_file="$1"
    local column_name="$2"
    local separator="${3:-,}"

    if [[ ! -f "${csv_file}" ]]; then
        echo "CSV file not found: ${csv_file}" >&2
        return 1
    fi

    local header
    header=$(head -n 1 "${csv_file}")

    local col_index
    col_index=$(awk -F"${separator}" -v col="${column_name}" '
        { for (i = 1; i <= NF; i++) if ($i == col) { print i; exit } }
    ' <<< "${header}")

    if [[ -z "${col_index}" ]]; then
        echo "Column '${column_name}' not found in ${csv_file}" >&2
        return 1
    fi

    REFERENCE_VALUES=()
    while IFS= read -r line; do
        REFERENCE_VALUES+=("$(cut -d"${separator}" -f"${col_index}" <<< "${line}")")
    done < <(tail -n +2 "${csv_file}")
}

# Reads one column from an Excel file (.xlsx/.xls) into the global array
# REFERENCE_VALUES. Requires python3 with the openpyxl module.
#
# Args: <excel_file> <column_name>
read_excel() {
    local excel_file="$1"
    local column_name="$2"

    if [[ ! -f "${excel_file}" ]]; then
        echo "Excel file not found: ${excel_file}" >&2
        return 1
    fi

    if ! command -v python3 &> /dev/null; then
        echo "python3 is required to read Excel files" >&2
        return 1
    fi

    local values
    values=$(python3 - "${excel_file}" "${column_name}" <<'PYEOF'
import sys

try:
    import openpyxl
except ImportError:
    print("openpyxl module not installed (pip install openpyxl)", file=sys.stderr)
    sys.exit(1)

excel_file, column_name = sys.argv[1], sys.argv[2]
sheet = openpyxl.load_workbook(excel_file, read_only=True, data_only=True).active

header = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]
if column_name not in header:
    print(f"Column '{column_name}' not found in {excel_file}", file=sys.stderr)
    sys.exit(1)

col_index = header.index(column_name)
for row in sheet.iter_rows(min_row=2):
    value = row[col_index].value
    if value is not None:
        print(value)
PYEOF
    ) || return 1

    REFERENCE_VALUES=()
    while IFS= read -r line; do
        REFERENCE_VALUES+=("${line}")
    done <<< "${values}"
}

# Dispatches to read_csv or read_excel based on the file's extension, filling
# the global array REFERENCE_VALUES.
#
# Args: <file_path> <column_name> [separator=","]
read_file() {
    local file_path="$1"
    local column_name="$2"
    local separator="${3:-,}"

    if [[ ! -f "${file_path}" ]]; then
        echo "File not found: ${file_path}" >&2
        return 1
    fi

    case "${file_path,,}" in
        *.csv)
            read_csv "${file_path}" "${column_name}" "${separator}"
            ;;
        *.xlsx|*.xls)
            read_excel "${file_path}" "${column_name}"
            ;;
        *)
            echo "Unsupported file format: ${file_path}" >&2
            return 1
            ;;
    esac
}

# Logs in against the REST API and sets the global TOKEN.
login() {
    TOKEN=$(curl -sf -X POST "${BASE_URL}/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"${AKTIN_USER}\",\"password\":\"${AKTIN_PASSWORD}\"}")

    if [[ -z "${TOKEN}" ]]; then
        echo "Login failed" >&2
        exit 1
    fi
}

# Imports a single consent entry for the given extension (patient/encounter/
# billing id), using the already-acquired TOKEN and the shared
# STUDY_ID/REFERENCE/PARTICIPATION/GENERATE_SIC/SIC/COMMENT settings.
#
# Args: <extension>
import_single_consent() {
    local extension="$1"

    local payload
    payload=$(cat <<EOF
[
  {
    "participation": "${PARTICIPATION}",
    "reference": "${REFERENCE}",
    "extension": "${extension}",
    "sic": "${SIC}",
    "comment": "${COMMENT}",
    "generateSic": ${GENERATE_SIC}
  }
]
EOF
)

    curl -sf -X POST "${BASE_URL}/studies/${STUDY_ID}/patients/batch" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "${payload}"
}

# Reads a column of extensions from a CSV/Excel file via read_file, then
# imports a consent for each value.
#
# Args: <file_path> <column_name> [separator=","]
import_consents_from_file() {
    local file_path="$1"
    local column_name="$2"
    local separator="${3:-,}"

    read_file "${file_path}" "${column_name}" "${separator}"

    local extension
    for extension in "${REFERENCE_VALUES[@]}"; do
        import_single_consent "${extension}"
    done
}

# Entry point: parses CLI args, logs in, then imports all consents found in
# the given file. This is the only function invoked at top level; every
# other function is reached through this call tree.
main() {
    BASE_URL="${BASE_URL:-http://localhost:80/aktin/admin/rest}"
    AKTIN_USER="${AKTIN_USER:-i2b2}"
    AKTIN_PASSWORD="${AKTIN_PASSWORD:?AKTIN_PASSWORD must be set}"

    STUDY_ID="${1:?studyId required}"
    REFERENCE="${2:?reference required (Patient|Encounter|Billing)}"
    PARTICIPATION="${3:?participation required (OptIn|OptOut)}"
    GENERATE_SIC="${4:?generateSic required (true|false)}"
    FILE_PATH="${5:?file path required}"
    COLUMN_NAME="${6:?column name required}"
    SEPARATOR="${7:-,}"
    SIC="${8:-}"
    COMMENT="${9:-}"

    if [[ "${GENERATE_SIC}" != "true" && "${GENERATE_SIC}" != "false" ]]; then
        echo "generateSic must be 'true' or 'false'" >&2
        exit 1
    fi

    login
    import_consents_from_file "${FILE_PATH}" "${COLUMN_NAME}" "${SEPARATOR}"
}

main "$@"
