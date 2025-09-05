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
    echo "Usage: $0 <path_to_backup_tarfile> <wildfly_container_name> <postgres_container_name> <httpd_container>"
    exit 1
}

readonly tarfile="$1"
readonly wildfly_container="$2"
readonly postgres_container="$3"
readonly apache_container="$4"
readonly log=apply_aktin_backup_$(date +%Y_%h_%d_%H%M).log
cleanup=()


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

# Exits script if given container is not running
exit_if_container_down() {
  local container_name="$1"

  if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
      echo "Container '$container_name' is running."
  else
      echo "Container '$container_name' is NOT running."
      echo "Terminating process"
      exit 1
  fi
}

# Enable database migration by stopping all containers, depending on- and thereby locking the database for changes
stop_db_users() {
    echo "stop container $apache_container"
    sudo docker container stop "$apache_container"
    echo "stop container $wildfly_container"
    sudo docker container stop "$wildfly_container"
}

start_wildfly() {
    echo "starting wildfly container: $wildfly_container"
    sudo docker container start "$wildfly_container"
}

restart_aktin_services() {
    echo "starting aktin services"
    sudo docker container restart "$postgres_container"
    sudo docker container restart "$apache_container"
    sudo docker container restart "$wildfly_container"
}


# Extracts a given tar file inside a temporary directory
extract_tar() {
    local tar="$1"
    local temp_dir

    temp_dir=$(mktemp -d -p "$PWD")
    cleanup+=("$temp_dir")
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

remove_dir() {
    rm -rf "$1"
}

import_databases_backup() {
    local backup_folder=$1      # Path to backup folder inside container
    local container=$2          # PostgreSQL container name
    local path_to_pmcell_backup="/tmp/pmbackup.sql"  # Temporary backup file for pm_cell_data table

    # Create a backup of pm_cell_data inside the container
    sudo docker exec -it "$container" sh -c "pg_dump -U postgres -t i2b2pm.pm_cell_data i2b2 --data-only > $path_to_pmcell_backup"
    cleanup+=("$path_to_pmcell_backup")

    echo "importing the backup of aktin and i2b2 databases"
    sudo docker exec -i "$container" psql -U postgres -q -d i2b2 < "$backup_folder/backup_i2b2.sql" > /dev/null 2>&1  # is path on host
    sudo docker exec -i "$container" psql -U postgres -q -d aktin < "$backup_folder/backup_aktin.sql" > /dev/null 2>&1

    # Restore the pm_cell_data table from the temporary backup
    echo "importing pm cells"
    sudo docker exec "$container" psql -U postgres -d i2b2 -c "TRUNCATE Table i2b2pm.pm_cell_data;" > /dev/null 2>&1
    sudo docker exec "$container" psql -U postgres -d i2b2 -q -f "$path_to_pmcell_backup"
}

import_config() {
  local src_path="$1"
  local target_path="$2"

  echo "importing config from: $src_path to: $target_path"
  sudo docker cp "$src_path" "$target_path"
}

clean() {
    for path in "${cleanup[@]}"; do
        sudo rm -rf "$path"
    done
}

main() {
    # Validate script variables
    check_containers_same_dwh   # check if test-containers originate from same data warehouse
    exit_if_container_down "$postgres_container"
    exit_if_container_down "$apache_container"
    exit_if_container_down "$wildfly_container"

    # Copy backup to container
    local backup_dir_docker="/var/tmp"  # target path inside the container
    echo "extract backup-tar on host"
    local backup_dir_host=$(extract_tar "$tarfile")

    stop_db_users   # remove database lock
    import_databases_backup "$backup_dir_host" "$postgres_container"
    start_wildfly

    # import static config files standalone.xml
    import_config "$backup_dir_host/backup_standalone.xml" "$wildfly_container:/opt/wildfly/standalone/configuration/backup_standalone.xml"
    import_config "$backup_dir_host/backup_standalone.conf" "$wildfly_container:/opt/wildfly/bin/backup_standalone.conf"
    import_config "$backup_dir_host/backup_aktin.properties" "$wildfly_container:/etc/aktin/aktin.properties"

    # import /var/lib/aktin
    sudo docker cp "$backup_dir_host/var/lib/aktin" "$wildfly_container:/var/lib"
    # give wildfly permission for /var/lib/aktin
    sudo docker exec -u 0 "$wildfly_container" chown -R wildfly:wildfly /var/lib/aktin

    restart_aktin_services
    clean
    echo "migration completed"
}

main | tee -a "$log"
