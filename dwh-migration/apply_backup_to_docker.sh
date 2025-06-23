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
[[ $# -ne 4 ]] && {
    echo "Usage: $0 <path_to_backup_tarfile> <wildfly_container_name> <postgres_container_name> <httpd_container_name>"
    exit 1
}

readonly tarfile="$1"
readonly wildfly_container="$2"
readonly postgres_container="$3"
readonly apache_container="$4"


# create log file
readonly log=apply_aktin_backup_$(date +%Y_%h_%d_%H%M).log

check_containers_same_dwh() {
  project_name_postgres="${postgres_container%%-*}"
  project_name_apache="${apache_container%%-*}"
  project_name_wildfly="${wildfly_container%%-*}"

  # Compare prefixes
  if [[ "$project_name_postgres" == "$project_name_wildfly" && "$project_name_wildfly" == "$project_name_wildfly" ]]; then
      echo "All variables share the same prefix: $project_name_postgres"
  else
      echo "pname_postgres: $project_name_postgres, pname_apache: $project_name_apache, pname_wildfly: $project_name_wildfly"
      read -r -p "Container prefix does not match, possibly selected containers from different data warehouses. Do you still want to proceed? [y/n]: " proceed
      if [ "$proceed" == "n" ] || [ "$proceed" = "N" ]; then
        echo "Operation terminated"
        exit 1
      fi
  fi
}

check_container_running() {
  local container_name="$1"

  if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
      echo "Container '$container_name' is running."
  else
      echo "Container '$container_name' is NOT running."
      echo "Terminating process"
      exit 1
  fi
}


stop_aktin_services() {
    echo "stopping aktin services"
    sudo docker container stop "$apache_container"
    sudo docker container stop "$wildfly_container"
    sudo docker container start "$postgres_container"
}

start_wildfly() {
    sudo docker container start "$wildfly_container"
}



restart_aktin_services() {
    echo "starting aktin services"
    sudo docker container restart "$postgres_container"
    sudo docker container restart "$apache_container"
    sudo docker container restart "$wildfly_container"
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

extract_tar_to_tmp() {
    local tar="$1"
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

    echo "$temp_dir/$folder_name"
}

copy_to_container() {
    local container_name="$1"
    local source_dir="$2"
    local dest_path="$3"
    local source_name=$(basename "$source_dir")

    docker cp "$source_dir" "$container_name:$dest_path"
    echo "$dest_path/$source_name"
}

remove_dir() {
    rm -rf "$1"
}

drop_db() {
    local database_name="$1"
    sudo docker exec -i "$container" psql -U postgres -q -c "DROP DATABASE IF EXISTS $database_name;" > /dev/null
}

drop_user() {
    local user_name=$1
    sudo docker exec -i "$container" psql -U postgres -q -c "DROP USER IF EXISTS $user_name;" > /dev/null
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

    sudo docker exec "$container" psql -U postgres -c "DROP USER IF EXISTS aktin;"
    for user in "${i2b2_users[@]}"; do
       drop_user "$user"
    done

    echo "reinitialising aktin and i2b2 databases"
    sudo docker exec -i "$container" psql -U postgres -c "CREATE DATABASE aktin;"
    sudo docker exec -i "$container" psql -U postgres -d aktin -c "CREATE USER aktin with PASSWORD 'aktin'; CREATE SCHEMA AUTHORIZATION aktin; GRANT ALL ON SCHEMA aktin to aktin; ALTER ROLE aktin WITH LOGIN;"
    sudo docker exec -i "$container" psql -U postgres -c "CREATE DATABASE i2b2;"
    sudo docker exec -i "$container" psql -U postgres -d i2b2 -c "CREATE USER i2b2crcdata WITH PASSWORD 'demouser'; CREATE USER i2b2hive WITH PASSWORD 'demouser'; CREATE USER i2b2imdata WITH PASSWORD 'demouser'; CREATE USER i2b2metadata WITH PASSWORD 'demouser'; CREATE USER i2b2pm WITH PASSWORD 'demouser'; CREATE USER i2b2workdata WITH PASSWORD 'demouser'; CREATE SCHEMA AUTHORIZATION i2b2crcdata; CREATE SCHEMA AUTHORIZATION i2b2hive; CREATE SCHEMA AUTHORIZATION i2b2imdata; CREATE SCHEMA AUTHORIZATION i2b2metadata; CREATE SCHEMA AUTHORIZATION i2b2pm; CREATE SCHEMA AUTHORIZATION i2b2workdata;"


    echo "importing the backup of aktin and i2b2 databases"
    sudo docker exec "$container" psql -U postgres -d i2b2 -q -f "$backup_folder/backup_i2b2.sql" > /dev/null
    sudo docker exec "$container" psql -U postgres -d aktin -q -f "$backup_folder/backup_aktin.sql" > /dev/null
}

import_aktin_properties() {
    local backup_folder="$1"
    local backup_file_name="backup_aktin.properties" # TODO set dynamically
    local target_file_name="aktin.properties"
    local target_path="/etc/aktin"

    echo "replace aktin.properties with properties from backup"
    copy_to_container "$wildfly_container" "$backup_folder/$backup_file_name" "$target_path/$target_file_name"
}

import_config() {
  local src_path="$1"
  local target_path="$2"

  echo "importing config from: $src_path to: $target_path"
  sudo docker cp "$src_path" "$target_path"
}

main() {
    # check if containers originate from same data warehouse
    check_containers_same_dwh
    check_container_running "$postgres_container"
    check_container_running "$apache_container"
    check_container_running "$wildfly_container"

    local container_backup_path="/var/tmp"
    local backup_folder
    echo "extract backup-tar on host"
    local backup_folder_host=$(extract_tar_to_tmp "$tarfile")
    backup_folder=$(copy_to_container "$postgres_container" "$backup_folder_host" "$container_backup_path")

    stop_aktin_services
    import_databases_backup "$backup_folder" "$postgres_container"
    start_wildfly

    # import standalone.xml
    import_config "$backup_folder_host/backup_standalone.xml" "$wildfly_container:/opt/wildfly/standalone/configuration/backup_standalone.xml"
    # import standalone.conf
    import_config "$backup_folder_host/backup_standalone.conf" "$wildfly_container:/opt/wildfly/bin/backup_standalone.conf"
    # import aktin.properties
    import_config "$backup_folder_host/backup_aktin.properties" "$wildfly_container:/etc/aktin/aktin.properties"

#    import_aktin_properties "$backup_folder_host"
    restart_aktin_services
    remove_dir "$(dirname "$backup_folder_host")"
    echo "migration completed"


}

main | tee -a "$log"