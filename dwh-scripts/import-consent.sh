#!/usr/bin/env bash
# Imports a single patient consent (opt-in/opt-out) for a study directly via
# the REST API, bypassing the Angular frontend.
#
# Calls: POST {BASE_URL}/studies/{studyId}/patients/batch  (OptInEndpoint#createEntries)
#
# Usage:
#   BASE_URL=http://localhost:80/aktin/admin/rest \
#   AKTIN_USER=admin AKTIN_PASSWORD=secret \
#   ./import-consent.sh <studyId> <reference:Patient|Encounter|Billing> <extension> <participation:OptIn|OptOut> [sic] [comment]

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:80/aktin/admin/rest}"
AKTIN_USER="${AKTIN_USER:-i2b2}"
AKTIN_PASSWORD="${AKTIN_PASSWORD:?AKTIN_PASSWORD must be set}"

STUDY_ID="${1:?studyId required}"
REFERENCE="${2:?reference required (Patient|Encounter|Billing)}"
EXTENSION="${3:?extension required}"
PARTICIPATION="${4:?participation required (OptIn|OptOut)}"
SIC="${5:-}"
COMMENT="${6:-}"

TOKEN=$(curl -sf -X POST "${BASE_URL}/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${AKTIN_USER}\",\"password\":\"${AKTIN_PASSWORD}\"}")

if [[ -z "${TOKEN}" ]]; then
    echo "Login failed" >&2
    exit 1
fi

PAYLOAD=$(cat <<EOF
[
  {
    "participation": "${PARTICIPATION}",
    "reference": "${REFERENCE}",
    "extension": "${EXTENSION}",
    "sic": "${SIC}",
    "comment": "${COMMENT}",
    "generateSic": false
  }
]
EOF
)

curl -sf -X POST "${BASE_URL}/studies/${STUDY_ID}/patients/batch" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${PAYLOAD}"
