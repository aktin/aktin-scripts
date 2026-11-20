#!/bin/bash
#--------------------------------------
# Script Name:  apply_backup_to_deb.sh
# Version:      2.1
# Author:       akombeiz@ukaachen.de, whoy@ukaachen.de
# Date:         28 OKT 26
# Purpose:      Installs a new AKTIN DWH from repository and fills it with backed up data from a
#               given tar.gz file
#--------------------------------------

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# ensure the correct number of arguments is provided
[[ $# -ne 1 ]] && {
    echo "Usage: $0 <path_to_backup_tarfile>"
    exit 1
}

readonly tarfile="$1"

# create log file
readonly log=apply_aktin_backup_$(date +%Y_%h_%d_%H%M).log

# get wildfly home directory
if [[ -d /opt/wildfly/ ]]; then
    readonly wildfly_home=/opt/wildfly
elif [[ -n $(ls /opt/wildfly-*) ]]; then
    readonly wildfly_home=/opt/wildfly-*
fi

extract_tarfile_and_get_folder_path() {
    local tar="$1"
    # create a temporary directory for extraction
    local temp_dir=$(mktemp -d -p "$PWD")
    # extract the archive into the temporary directory
    tar -xf "$tar" -C "$temp_dir"
    # check if exactly one folder exists in the extracted content
    local folder=$(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d)
    if [[ $(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d | wc -l) -ne 1 ]]; then
        echo "Error: Archive contains multiple folders or no folders"
        rm -rf "$temp_dir"  # Clean up temp directory
        return 1
    fi
    # return the path to the extracted folder
    echo "$folder"
}

remove_dir() {
    rm -rf $1
}

install_aktin_packages() {
    echo "registering aktin repository"
    apt-get update && apt-get install -y ca-certificates
    # echo  "Acquire { https::Verify-Peer false }" > /etc/apt/apt.conf.d/99verify-peer.conf
    wget -O - https://www.aktin.org/software/repo/org/apt/conf/aktin.gpg.key | sudo apt-key add -
    echo "deb https://www.aktin.org/software/repo/org/apt jammy main" >/etc/apt/sources.list.d/aktin.list
    echo "installing aktin packages"
    apt-get update
    apt-get install -y aktin-notaufnahme-i2b2
    apt-get install -y aktin-notaufnahme-dwh
    apt-get install -y aktin-notaufnahme-updateagent
}

stop_aktin_services() {
    echo "stoping aktin services"
    service apache2 stop
    service wildfly stop
    service postgresql start
}

patch_aktin_properties() {
    local backup_folder=$1
    echo "patching aktin.properties with backup"
    while read -r line1; do
        if [[ ! $line1 = \#* && ! -z $line1 ]]; then
            key=${line1%=*}
            value=${line1#*=}
            while read -r line2; do
                if [[ ! $line2 = \#* && ! -z $line2 ]]; then
                    if [[ ${line2%=*} == $key ]]; then
                        sed -i "s|${key}=.*|${key}=${value}|" /etc/aktin/aktin.properties
                        break
                    fi
                fi
            done </etc/aktin/aktin.properties
        fi
    done <${backup_folder}/backup_aktin.properties
    chown wildfly:wildfly /etc/aktin/aktin.properties
}

patch_wilfly_conf() {
    local backup_folder=$1
    echo "patching wilfly configuration with backup"
    cp $backup_folder/backup_standalone.xml $wildfly_home/standalone/configuration/standalone.xml
    chown wildfly:wildfly $wildfly_home/standalone/configuration/standalone.xml
    cp $backup_folder/backup_standalone.conf $wildfly_home/bin/standalone.conf
    chown wildfly:wildfly $wildfly_home/bin/standalone.conf
}

merge_aktin_lib_dir() {
    local backup_folder=$1
    # Merge /var/lib/aktin payload and fix ownership.
    # Drop import-scripts first to safely ignore it during merge
    echo "merging /var/lib/aktin with backup"
    rm -rf "$backup_folder/var/lib/aktin/import-scripts"
    cp -r "$backup_folder/var/lib/aktin/." /var/lib/aktin
    chown -R wildfly:wildfly /var/lib/aktin
}

# Drop a database and create it again empty, keeping its owner.
# Ensures the restored database is an exact copy of the backup, without objects only the target had
recreate_database() {
    local db=$1
    local owner
    owner=$(sudo -u postgres psql -tA -v ON_ERROR_STOP=1 -c "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '$db';")

    echo "recreating empty database $db (owner: $owner)"
    sudo -u postgres psql -q -v ON_ERROR_STOP=1 -c "DROP DATABASE \"$db\";"
    sudo -u postgres psql -q -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$db\" OWNER \"$owner\" TEMPLATE template0;"
}

import_databases_backup() {
    local backup_folder=$1      # Path to the extracted backup folder
    local path_to_pmcell_backup="/tmp/pmbackup.sql"  # Temp file for pm_cell_data snapshot

    # Snapshot i2b2pm.pm_cell_data before full import
    sudo -u postgres pg_dump -t i2b2pm.pm_cell_data i2b2 --data-only > "$path_to_pmcell_backup"

    recreate_database "i2b2"
    recreate_database "aktin"

    echo "importing the backup of aktin and i2b2 databases"
    # Import i2b2 and aktin into the freshly recreated databases.
    # Feed the dumps via stdin: the root shell opens the files before sudo drops privileges,
    # so postgres doesn't need read access to root's 0700 mktemp dir
    # ON_ERROR_STOP makes psql exit non-zero on the first failed statement, so set -e aborts the migration
    sudo -u postgres psql -q -v ON_ERROR_STOP=1 -d i2b2 < "$backup_folder/backup_i2b2.sql" > /dev/null
    sudo -u postgres psql -q -v ON_ERROR_STOP=1 -d aktin < "$backup_folder/backup_aktin.sql" > /dev/null

    # Reapply pm_cell_data
    echo "importing pm cells"
    sudo -u postgres psql -d i2b2 -c "TRUNCATE Table i2b2pm.pm_cell_data;" > /dev/null
    sudo -u postgres psql -d i2b2 -q -v ON_ERROR_STOP=1 -f "$path_to_pmcell_backup"
    rm -f "$path_to_pmcell_backup"
}

run_info_service() {
    nc -w 2 127.0.0.1 1002
}

run_update_service() {
    nc -w 2 127.0.0.1 1003
}

restart_aktin_services() {
    echo "starting aktin services"
    service apache2 start
    service postgresql restart
    service wildfly start
}

main() {
    local backup_folder=$(extract_tarfile_and_get_folder_path $tarfile)
    install_aktin_packages
    stop_aktin_services
    patch_aktin_properties "$backup_folder"
    patch_wilfly_conf "$backup_folder"
    merge_aktin_lib_dir "$backup_folder"
    import_databases_backup "$backup_folder"
    run_info_service
    run_update_service
    restart_aktin_services
    remove_dir "$backup_folder"
    echo "migration completed"
}

main | tee -a $log
