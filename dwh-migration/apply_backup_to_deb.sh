#!/bin/bash
#--------------------------------------
# Script Name:  apply_backup_to_deb.sh
# Version:      2.0
# Author:       akombeiz@ukaachen.de, whoy@ukaachen.de
# Date:         08 SEP 25
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
    local temp_dir=$(mktemp -d)
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

import_folder_backup() {
    local backup_folder=$1
    local source_folder=$2
    echo "importing backup of $source_folder"
    cp -r $backup_folder$source_folder/* $source_folder
    chown -R wildfly:wildfly $source_folder
}

import_databases_backup() {
    local backup_folder=$1
    echo "deleting aktin and i2b2 databases"
    sudo -u postgres dropdb --if-exists aktin
    sudo -u postgres dropuser --if-exists aktin
    sudo -u postgres dropdb --if-exists i2b2
    sudo -u postgres dropuser --if-exists i2b2crcdata
    sudo -u postgres dropuser --if-exists i2b2hive
    sudo -u postgres dropuser --if-exists i2b2imdata
    sudo -u postgres dropuser --if-exists i2b2metadata
    sudo -u postgres dropuser --if-exists i2b2pm
    sudo -u postgres dropuser --if-exists i2b2workdata
    echo "reinitialising aktin and i2b2 databases"
    sudo -u postgres psql -c "CREATE DATABASE aktin;"
    sudo -u postgres psql -d aktin -c "CREATE USER aktin with PASSWORD 'aktin'; CREATE SCHEMA AUTHORIZATION aktin; GRANT ALL ON SCHEMA aktin to aktin; ALTER ROLE aktin WITH LOGIN;"
    sudo -u postgres psql -c "CREATE DATABASE i2b2;"
    sudo -u postgres psql -d i2b2 -c "CREATE USER i2b2crcdata WITH PASSWORD 'demouser'; CREATE USER i2b2hive WITH PASSWORD 'demouser'; CREATE USER i2b2imdata WITH PASSWORD 'demouser'; CREATE USER i2b2metadata WITH PASSWORD 'demouser'; CREATE USER i2b2pm WITH PASSWORD 'demouser'; CREATE USER i2b2workdata WITH PASSWORD 'demouser'; CREATE SCHEMA AUTHORIZATION i2b2crcdata; CREATE SCHEMA AUTHORIZATION i2b2hive; CREATE SCHEMA AUTHORIZATION i2b2imdata; CREATE SCHEMA AUTHORIZATION i2b2metadata; CREATE SCHEMA AUTHORIZATION i2b2pm; CREATE SCHEMA AUTHORIZATION i2b2workdata;"
    echo "copy database backups to /tmp/"
    cp "$backup_folder/backup_i2b2.sql" /tmp/
    cp "$backup_folder/backup_aktin.sql" /tmp/
    echo "importing the backup of aktin and i2b2 databases"
    sudo -u postgres psql -d i2b2 -f "/tmp/backup_i2b2.sql"
    sudo -u postgres psql -d aktin -f "/tmp/backup_aktin.sql"
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
    import_folder_backup "$backup_folder" "/var/lib/aktin"
    import_databases_backup "$backup_folder"
    run_info_service
    run_update_service
    restart_aktin_services
    remove_dir "$backup_folder"
    echo "migration completed"
}

main | tee -a $log
