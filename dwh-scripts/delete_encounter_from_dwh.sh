#!/bin/bash
#--------------------------------------
# Script Name:  delete_encounter_from_dwh.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Stops AKTIN DWH, deletes encounter with admission dates in given time span
#               and restarts DWH afterwards
#--------------------------------------

check_root() {
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root"
        exit 1
    fi
}

validate_date() {
    local date=$1
    if [[ $date =~ ^[0-9]{8}$ ]] && date -d "${date:0:4}-${date:4:2}-${date:6:2}" "+%Y%m%d" >/dev/null 2>&1; then
        return 0
    else
        echo "Invalid date: $date. Expected format: yyyymmdd."
        return 1
    fi
}

check_service() {
    local service=$1
    if systemctl is-active --quiet $service; then
        echo "$service service is running."
        return 0
    else
        echo "$service service is not running."
        return 1
    fi
}

execute_sql() {
    local start_date=$1
    local end_date=$2
    sudo -u postgres psql -d i2b2 -c "DELETE FROM i2b2crcdata.observation_fact WHERE (start_date BETWEEN '$start_date' AND '$end_date'); WITH Lookuptable AS (DELETE from i2b2crcdata.visit_dimension where (start_date BETWEEN '$start_date' AND '$end_date') RETURNING encounter_num) DELETE FROM i2b2crcdata.encounter_mapping WHERE encounter_num IN (SELECT encounter_num FROM Lookuptable);"
}

main() {
    check_root

    if [ $# -ne 2 ]; then
        echo "Two arguments are required. Usage: $0 yyyymmdd yyyymmdd"
        exit 1
    fi

    local start_date=$1
    local end_date=$2

    validate_date $start_date
    local valid_start_date=$?

    validate_date $end_date
    local valid_end_date=$?

    if [ $valid_start_date -eq 0 ] && [ $valid_end_date -eq 0 ]; then
        echo "Start Date: $start_date and End Date: $end_date are valid."

        if check_service postgresql; then
            local wildfly_was_running=0
            if check_service wildfly; then
                echo "Wildfly service is running, stopping it now."
                systemctl stop wildfly
                wildfly_was_running=1

                while check_service wildfly; do
                    echo "Waiting for Wildfly to stop..."
                    sleep 1
                done
                echo "Wildfly has stopped."
            fi

            echo "Executing SQL query..."
            execute_sql $start_date $end_date

            if [ $wildfly_was_running -eq 1 ]; then
                echo "Restarting Wildfly service..."
                systemctl start wildfly
            fi
        else
            echo "Postgresql service is not running. Please start it and try again."
            exit 1
        fi
    else
        echo "Either start date or end date is invalid."
        exit 1
    fi
}

main "$@"
