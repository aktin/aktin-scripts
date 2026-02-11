#!/usr/bin/env bash
#--------------------------------------
# Script Name:  check_connection_closed_and_send_email
# Version:      1.0
# Author:       aivanets@ukaachen.de
# Date:         18 Dec 25
# Purpose:      Script checks import-summary if it has an error "connection has been closed". 
# If so, sends an email to check this manually.
#--------------------------------------

set -euo pipefail

for cmd in curl jq; do
  command -v $cmd >/dev/null || {
    echo "Error: $cmd is required but not installed." >&2
    exit 1
  }
done

# Email Settings (SMTP)
SMTP_SERVER='CHANGEME'
SMTP_PORT='CHANGEME'
SMTP_USER='CHANGEME'
SMTP_PASS='CHANGEME'
MAIL_FROM='CHANGEME'
RECIPIENTS='CHANGEME' # comma-separated
AKTIN_DWH_URL='http://localhost:80'

IMPORT_ENDPOINT="${AKTIN_DWH_URL}/aktin/admin/rest/import-summary"

result=$(curl -s -k "$IMPORT_ENDPOINT")
import_summary_errors=$(echo "$result" | jq -r '.error | join(", ")' || echo "")

if [[ "$import_summary_errors" != *"error: This connection has been closed"* ]]; then
    echo "Connection error found. Sending bulk alert..."

    # Prepare recipient arguments for curl
    MAIL_ARGS=()
    for rec in "${RECIPIENTS[@]}"; do
        MAIL_ARGS+=("--mail-rcpt" "$rec")
    done

    # We use --fail to ensure curl exits with an error code if Auth fails
    if ! curl --fail --ssl-reqd \
      --url "smtp://${SMTP_SERVER}:${SMTP_PORT}" \
      --user "${SMTP_USER}:${SMTP_PASS}" \
      --mail-from "$MAIL_FROM" \
      "${MAIL_ARGS[@]}" \
      --upload-file - <<EOF
From: Aktin Monitoring <$MAIL_FROM>
To: ${RECIPIENTS[*]}
Subject: CRITICAL: Aktin DWH Connection Closed

The Aktin DWH import summary reported a connection failure.
Error Details: $import_summary_errors
EOF
    then
        echo "ERROR: SMTP Authentication failed or Server unreachable. Stopping immediately."
        exit 1
    fi
fi
