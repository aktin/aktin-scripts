#!/bin/bash
#--------------------------------------
# Script: apply_backup_to_docker.sh
# Version: 1.0
# Author:  whoy@ukaachen.de
# Date:    2025-06-04
# Purpose: Restore i2b2/aktin DBs from a given tar.gz into the target DWH
#          Docker stack (PostgreSQL + WildFly + Apache HTTPD).
#--------------------------------------

set -euo pipefail

# Require root to manage containers and copy files
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# Args: <backup.tar.gz> <wildfly_container> <postgres_container> <httpd_container>
[[ $# -ne 4 ]] && {
    echo "Usage: $0 <path_to_backup_tarfile> <wildfly_container_name> <postgres_container_name> <httpd_container>"
    exit 1
}

readonly backup_tar="$1"
readonly wildfly_container="$2"
readonly postgres_container="$3"
readonly apache_container="$4"
readonly log=apply_aktin_backup_$(date +%Y_%h_%d_%H%M).log
host_tmp_paths=()

# Verify the three container names look like the same stack by prefix
assert_same_prefix() {
  project_name_postgres="${postgres_container%%-*}"
  project_name_apache="${apache_container%%-*}"
  project_name_wildfly="${wildfly_container%%-*}"

  # Validate containers are part of the same data warehouse instance, by matching container-name prefixes.
  # Important test for setups composed of multiple docker data warehouses
  if [[ "$project_name_postgres" == "$project_name_wildfly" && "$project_name_wildfly" == "$project_name_apache" ]]; then
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

# Fail fast if a required container isn’t running
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

# Stop frontend/app containers to open DB for changes
stop_db_users() {
    echo "stop container $apache_container"
    sudo docker container stop "$apache_container"
    echo "stop container $wildfly_container"
    sudo docker container stop "$wildfly_container"
}

# Bring WildFly back early (needed after DB restore for config copy)
start_wildfly() {
    echo "starting wildfly container: $wildfly_container"
    sudo docker container start "$wildfly_container"
}

# Restart full stack after import and file copies
restart_stack() {
    echo "starting aktin services"
    sudo docker container restart "$postgres_container"
    sudo docker container restart "$apache_container"
    sudo docker container restart "$wildfly_container"
}

# Un-tar backup into a temp dir and return the single top-level folder path
extract_tar() {
    local tar="$1"
    local temp_dir

    temp_dir=$(mktemp -d -p "$PWD")
    host_tmp_paths+=("$temp_dir")
    tar -xf "$tar" -C "$temp_dir"

    # Expect exactly one folder in the archive
    local folder
    folder=$(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d)
    if [[ $(echo "$folder" | wc -l) -ne 1 ]]; then
        echo "Error: Archive contains multiple folders or no folders" >&2
        rm -rf "$temp_dir"
        return 1
    fi

    local folder_name
    folder_name=$(basename "$folder")
    echo "$temp_dir/$folder_name"
}

# Simple helper (currently unused)
remove_dir() {
    rm -rf "$1"
}

# Restore DBs from SQL dumps, preserving pm_cell_data then reapplying it
restore_databases() {
    local backup_folder=$1      # Host path to extracted backup folder
    local container=$2          # PostgreSQL container name
    local path_to_pmcell_backup="/tmp/pmbackup.sql"  # Temp file inside container

    # Snapshot i2b2pm.pm_cell_data before full import
    sudo docker exec -it "$container" sh -c "pg_dump -U postgres -t i2b2pm.pm_cell_data i2b2 --data-only > $path_to_pmcell_backup"
    host_tmp_paths+=("$path_to_pmcell_backup")

    echo "importing the backup of aktin and i2b2 databases"
    # Import i2b2 and aktin from host-side SQL dumps
    sudo docker exec -i "$container" psql -U postgres -q -d i2b2 < "$backup_folder/backup_i2b2.sql" > /dev/null 2>&1
    sudo docker exec -i "$container" psql -U postgres -q -d aktin < "$backup_folder/backup_aktin.sql" > /dev/null 2>&1

    # Reapply pm_cell_data
    echo "importing pm cells"
    sudo docker exec "$container" psql -U postgres -d i2b2 -c "TRUNCATE Table i2b2pm.pm_cell_data;" > /dev/null 2>&1
    sudo docker exec "$container" psql -U postgres -d i2b2 -q -f "$path_to_pmcell_backup"
}

# Copy a config file (host → container target path)
copy_config_into_container() {
  local src_path="$1"
  local target_path="$2"

  echo "importing config from: $src_path to: $target_path"
  sudo docker cp "$src_path" "$target_path"
}

# Remove any host-side temp artifacts tracked in host_tmp_paths[]
cleanup_host_temp() {
    for path in "${host_tmp_paths[@]}"; do
        sudo rm -rf "$path"
    done
}

main() {
    # Sanity checks
    assert_same_prefix
    exit_if_container_down "$postgres_container"
    exit_if_container_down "$apache_container"
    exit_if_container_down "$wildfly_container"

    # Extract backup on host; use extracted folder for file/db imports
    echo "extract backup-tar on host"
    local backup_dir_host=$(extract_tar "$backup_tar")

    # Remove DB locks and import new DB
    stop_db_users
    restore_databases "$backup_dir_host" "$postgres_container"
    start_wildfly

    # Push WildFly/i2b2/aktin config files
    copy_config_into_container "$backup_dir_host/backup_standalone.xml" "$wildfly_container:/opt/wildfly/standalone/configuration/backup_standalone.xml"
    copy_config_into_container "$backup_dir_host/backup_standalone.conf" "$wildfly_container:/opt/wildfly/bin/backup_standalone.conf"
    copy_config_into_container "$backup_dir_host/backup_aktin.properties" "$wildfly_container:/etc/aktin/aktin.properties"

    # Push /var/lib/aktin payload and fix ownership
    sudo docker cp "$backup_dir_host/var/lib/aktin" "$wildfly_container:/var/lib"
    sudo docker exec -u 0 "$wildfly_container" chown -R wildfly:wildfly /var/lib/aktin

    # Bring everything back up and clean host temp files
    restart_stack
    cleanup_host_temp
    echo "migration completed"
}

main | tee -a "$log"
