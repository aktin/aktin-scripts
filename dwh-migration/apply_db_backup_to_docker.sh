#!/bin/bash
#--------------------------------------
# Script Name:  apply_backup_to_docker.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         4 Jun 25
# Purpose:      Applies the database backup from dwh backup tar-file and applies it to the docker dwh postgres container
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

readonly postgres_container="build-database-1"
readonly apache_container="build-httpd-1"
readonly wildyfly_container="build-wildfly-1"

stop_aktin_services() {
    echo "stopping aktin services"
    sudo docker container stop "$apache_container"
    sudo docker container stop "$wildyfly_container"
    sudo docker container start "$postgres_container"
}

restart_aktin_services() {
    echo "starting aktin services"
    sudo docker container restart "$postgres_container"
    sudo docker container start "$apache_container"
    sudo docker container start "$wildyfly_container"
}

extract_and_copy_to_docker() {
    local tar="$1"
    local container="$2"
    local dest_path="$3"

    local temp_dir
    temp_dir=$(mktemp -d -p "$PWD")

    # Extract and copy in one go
    tar -xf "$tar" -C "$temp_dir"

    # Get the single extracted folder name
    local folder
    folder=$(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d)

    # Check if there's exactly one directory
    if [[ $(echo "$folder" | wc -l) -ne 1 ]]; then
        echo "Error: Archive contains multiple folders or no folders" >&2
        rm -rf "$temp_dir"
        return 1
    fi

    # Get just the folder name (basename)
    local folder_name
    folder_name=$(basename "$folder")

    # Copy entire temp directory contents to Docker
    docker cp "$temp_dir/." "$container:$dest_path"

    # Clean up
    rm -rf "$temp_dir"

    # Return the Docker path to the extracted folder
    echo "$dest_path/$folder_name"
}

remove_dir() {
    rm -rf $1
}


import_folder_backup() {
    local backup_folder=$1
    local backup_source_folder=$backup_folder$2
    local docker_source_folder=$3
    local container=$(container_name_postgresql)

    echo -e "extract backup tar to: $backup_folder\n"
    echo -e "locate backup database data: $backup_source_folder\n"
    echo -e "Copy backup files $backup_source_folder/. to $container:$docker_source_folder"

    sudo docker cp "$backup_source_folder/." "$container:$docker_source_folder"
    chown -R wildfly:wildfly $backup_source_folder
}

drop_db() {
    local database_name=$1
    sudo docker exec -i $container psql -U postgres -q -c "DROP DATABASE IF EXISTS $database_name;" > /dev/null
}

drop_user() {
    local user_name=$1
    sudo docker exec -i $container psql -U postgres -q -c "DROP USER IF EXISTS $user_name;" > /dev/null
}

import_databases_backup() {
    local backup_folder=$1
    local container=$2

    # Arrays for cleaner iteration
    local databases=("aktin" "i2b2")
    local i2b2_users=("i2b2crcdata" "i2b2hive" "i2b2imdata" "i2b2metadata" "i2b2pm" "i2b2workdata")

    echo "Cleaning up existing databases and users"

    # Drop databases and users
    for db in "${databases[@]}"; do
        drop_db "$db"
    done

    sudo docker exec $container psql -U postgres -c "DROP USER IF EXISTS aktin;"
    for user in "${i2b2_users[@]}"; do
       drop_user "$user"
    done

    echo "reinitialising aktin and i2b2 databases"
    sudo docker exec -i $container psql -U postgres -c "CREATE DATABASE aktin;"
    sudo docker exec -i $container psql -U postgres -d aktin -c "CREATE USER aktin with PASSWORD 'aktin'; CREATE SCHEMA AUTHORIZATION aktin; GRANT ALL ON SCHEMA aktin to aktin; ALTER ROLE aktin WITH LOGIN;"
    sudo docker exec -i $container psql -U postgres -c "CREATE DATABASE i2b2;"
    sudo docker exec -i $container psql -U postgres -d i2b2 -c "CREATE USER i2b2crcdata WITH PASSWORD 'demouser'; CREATE USER i2b2hive WITH PASSWORD 'demouser'; CREATE USER i2b2imdata WITH PASSWORD 'demouser'; CREATE USER i2b2metadata WITH PASSWORD 'demouser'; CREATE USER i2b2pm WITH PASSWORD 'demouser'; CREATE USER i2b2workdata WITH PASSWORD 'demouser'; CREATE SCHEMA AUTHORIZATION i2b2crcdata; CREATE SCHEMA AUTHORIZATION i2b2hive; CREATE SCHEMA AUTHORIZATION i2b2imdata; CREATE SCHEMA AUTHORIZATION i2b2metadata; CREATE SCHEMA AUTHORIZATION i2b2pm; CREATE SCHEMA AUTHORIZATION i2b2workdata;"


    echo "importing the backup of aktin and i2b2 databases"
    sudo docker exec $container psql -U postgres -d i2b2 -q -f "$backup_folder/backup_i2b2.sql" > /dev/null
    sudo docker exec $container psql -U postgres -d aktin -q -f "$backup_folder/backup_aktin.sql" > /dev/null
}

restore_pg_dump() {
    local dumpfile=$1
    local db_name=$2
    # import backup, stop when error occurs and roll back all changes
    psql -X --set ON_ERROR_STOP=on --single-transaction "$db_name" < "$dumpfile"
}

main() {
    local postgres_container_name="$postgres_container"
    local postgres_backup_path="/var/tmp"
    local docker_db_path="/var/lib/postgresql/data"
    local backup_folder=$(extract_and_copy_to_docker "$tarfile" "$postgres_container_name" "$postgres_backup_path")
    echo "$backup_folder"

    stop_aktin_services
    import_databases_backup "$backup_folder" "$postgres_container_name"
    restart_aktin_services
    remove_dir "$backup_folder"
    echo "migration completed"
}

main | tee -a $log