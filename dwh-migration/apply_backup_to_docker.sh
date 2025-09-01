#!/bin/bash
#--------------------------------------
# Script Name:  apply_backup_to_docker.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         4 Jun 25
# Purpose:      Applies the database backup from dwh backup tar-file and applies it to the docker dwh postgres container
#               given tar.gz file
#--------------------------------------

# Enable strict error handling: exit on error, undefined variables, and pipe failures
set -euo pipefail

# Check if script is running as root (required for docker operations)
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# Validate command line arguments - script requires exactly 4 parameters
[[ $# -ne 4 ]] && {
    echo "Usage: $0 <path_to_backup_tarfile> <wildfly_container_name> <postgres_container_name> <httpd_container>"
    exit 1
}

# Store command line arguments in readable variable names
readonly tarfile="$1"           # Path to the backup tar file
readonly wildfly_container="$2" # Name of the WildFly application server container
readonly postgres_container="$3" # Name of the PostgreSQL database container
readonly apache_container="$4"   # Name of the Apache HTTP server container
readonly log=apply_aktin_backup_$(date +%Y_%h_%d_%H%M).log  # Generate timestamped log file name


#---------------------
# DOCKER Paths (commented out - legacy code)
#---------------------
#readonly DB_BACKUP=       "$postgres_container:/var/tmp/"
#readonly STANDALONE_XML=  "$wildfly_container:/opt/wildfly/standalone/configuration/backup_standalone.xml"
#readonly STANDALONE_CONF= "$wildfly_container:/opt/wildfly/bin/backup_standalone.conf"
#readonly PROPERTIES=      "$wildfly_container:/etc/aktin/aktin.properties"
#readonly TMP_DIR=         "$(mktemp -d -p "$PWD")"
#readonly EXTRACTED=       "$temp_dir/$folder_name"



# Function to verify that all three containers belong to the same Docker Compose project
# This ensures we're working with related containers from the same deployment
do_containers_share_prefix() {
  # Extract project prefix from each container name (everything before the first hyphen)
  project_name_postgres="${postgres_container%%-*}"
  project_name_apache="${apache_container%%-*}"
  project_name_wildfly="${wildfly_container%%-*}"

  # Compare prefixes to ensure all containers are from the same project
  if [[ "$project_name_postgres" == "$project_name_wildfly" && "$project_name_wildfly" == "$project_name_wildfly" ]]; then
      echo "All variables share the same prefix: $project_name_postgres"
  else
      echo "Container prefixes do not match, cancel migration"
      echo "->postgres: $project_name_postgres, apache: $project_name_apache, wildfly: $project_name_wildfly"
      exit 1
  fi
}

# Function to check if a specified container is currently running
# Exits the script if the container is not running (safety check)
exit_if_container_down() {
  local container_name="$1"

  # Check if container is in the list of running containers
  if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
      echo "Container '$container_name' is running."
  else
      echo "Container '$container_name' is NOT running."
      echo "Terminating process"
      exit 1
  fi
}

# Function to stop containers that use the database
# This prevents database locks during the migration process
stop_db_users() {
    echo "stop container $apache_container"
    sudo docker container stop "$apache_container"
    echo "stop container $wildfly_container"
    sudo docker container stop "$wildfly_container"
}

# Function to start only the WildFly container
# Used after database migration but before full service restart
start_wildfly() {
    echo "starting wildfly container: $wildfly_container"
    sudo docker container start "$wildfly_container"
}

# Function to restart all AKTIN services in proper order
# Ensures all components are running with the new configuration
restart_aktin_services() {
    echo "starting aktin services"
    sudo docker container restart "$postgres_container"
    sudo docker container restart "$apache_container"
    sudo docker container restart "$wildfly_container"
}

# Function to extract a tar file to a temporary directory
# Returns the path to the extracted folder
extract_tar() {
    local tar="$1"
    local temp_dir

    # Create temporary directory in current working directory
    temp_dir=$(mktemp -d -p "$PWD")
    # Extract tar file contents to temporary directory
    tar -xf "$tar" -C "$temp_dir"
    # Find the extracted directory (should be exactly one)
    local folder
    folder=$(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d)
    # Validate that exactly one directory was extracted
    if [[ $(echo "$folder" | wc -l) -ne 1 ]]; then
        echo "Error: Archive contains multiple folders or no folders" >&2
        rm -rf "$temp_dir"
        return 1
    fi
    # Get the basename of the extracted folder
    local folder_name
    folder_name=$(basename "$folder")

    # Return the full path to the extracted content
    echo "$temp_dir/$folder_name"
}

# Function to copy a directory from host to a Docker container
# Returns the destination path inside the container
copy_to_container() {
    local container_name="$1"  # Target container name
    local source_dir="$2"      # Source directory on host
    local dest_path="$3"       # Destination path in container
    local source_name=$(basename "$source_dir")

    echo "Copy $source_dir to $container_name:$dest_path"
    docker cp "$source_dir" "$container_name:$dest_path"
    # Return the full destination path inside container
    echo "$dest_path/$source_name"
}

# Function to remove a directory and its contents
# Used for cleanup of temporary files
remove_dir() {
    local target="$1"
    echo "removing directory $target"
    rm -rf "$target"
}

# Function to drop a PostgreSQL database if it exists
# Part of the database cleanup process before restoration
drop_db() {
    local database_name="$1"
    echo "dropping database $database_name"
    sudo docker exec -i "$container" psql -U postgres -q -c "DROP DATABASE IF EXISTS $database_name;" > /dev/null
}

# Function to drop a PostgreSQL user if it exists
# Part of the user cleanup process before restoration
drop_user() {
    local user_name=$1
    echo "dropping user $user_name"
    sudo docker exec -i "$container" psql -U postgres -q -c "DROP USER IF EXISTS $user_name;" > /dev/null
}

# Function to install the 'pv' package for progress display
# pv (pipe viewer) shows progress bars during database import
install_package_pv() {
  echo "installing package pv"
  sudo docker exec "$postgres_container" apt update && apt install -y pv
}


# Function to import database backups from the extracted backup folder
# Handles both AKTIN and i2b2 database restoration with proper user management
import_databases_backup() {
    local backup_folder=$1      # Path to backup folder inside container
    local container=$2          # PostgreSQL container name
    local path_to_pmcell_backup="pmbackup.sql"  # Temporary backup file for pm_cell_data table

    # Create backup of existing pm_cell_data table before dropping databases
    sudo docker exec -it "$container" pg_dump -U postgres -t i2b2pm.pm_cell_data i2b2 > "$path_to_pmcell_backup"
    sudo docker cp "$path_to_pmcell_backup" "$container:/tmp/"

    # Define databases and users that need to be recreated
    local databases=("aktin" "i2b2")
    local i2b2_users=("i2b2crcdata" "i2b2hive" "i2b2imdata" "i2b2metadata" "i2b2pm" "i2b2workdata")

    echo "Cleaning up existing databases and users"

    # Drop existing databases to start fresh
    for db in "${databases[@]}"; do
        drop_db "$db"
    done

    # Drop existing users to avoid conflicts
    sudo docker exec "$container" psql -U postgres -c "DROP USER IF EXISTS aktin;"
    for user in "${i2b2_users[@]}"; do
       drop_user "$user"
    done

    # Recreate AKTIN database with proper user and schema
    echo "reinitialising aktin and i2b2 databases"
    sudo docker exec -i "$container" psql -U postgres -c "CREATE DATABASE aktin;"
    sudo docker exec -i "$container" psql -U postgres -d aktin -c "CREATE USER aktin with PASSWORD 'aktin'; CREATE SCHEMA AUTHORIZATION aktin; GRANT ALL ON SCHEMA aktin to aktin; ALTER ROLE aktin WITH LOGIN;"

    # Recreate i2b2 database with all required users and schemas
    sudo docker exec -i "$container" psql -U postgres -c "CREATE DATABASE i2b2;"
    sudo docker exec -i "$container" psql -U postgres -d i2b2 -c "CREATE USER i2b2crcdata WITH PASSWORD 'demouser'; CREATE USER i2b2hive WITH PASSWORD 'demouser'; CREATE USER i2b2imdata WITH PASSWORD 'demouser'; CREATE USER i2b2metadata WITH PASSWORD 'demouser'; CREATE USER i2b2pm WITH PASSWORD 'demouser'; CREATE USER i2b2workdata WITH PASSWORD 'demouser'; CREATE SCHEMA AUTHORIZATION i2b2crcdata; CREATE SCHEMA AUTHORIZATION i2b2hive; CREATE SCHEMA AUTHORIZATION i2b2imdata; CREATE SCHEMA AUTHORIZATION i2b2metadata; CREATE SCHEMA AUTHORIZATION i2b2pm; CREATE SCHEMA AUTHORIZATION i2b2workdata;"

    # Import database backups with progress indicators
    echo "importing the backup of aktin and i2b2 databases"
    pv "$backup_folder/backup_i2b2.sql" | sudo docker exec -i "$container" psql -U postgres -d i2b2 -q > /dev/null
    pv "$backup_folder/backup_aktin.sql" | sudo docker exec -i "$container" psql -U postgres -d aktin -q > /dev/null

    # Restore the pm_cell_data table from the temporary backup
    sudo docker exec "$container" psql -U postgres -d i2b2 -q -c "DROP TABLE IF EXISTS i2b2pm.pm_cell_data;" > /dev/null
    sudo docker exec "$container" psql -U postgres -d i2b2 -q -f "/tmp/$path_to_pmcell_backup" > /dev/null
    # Clean up temporary backup file
    sudo rm "$path_to_pmcell_backup"
}

# Function to import AKTIN properties file from backup
# Replaces the current configuration with backed-up settings
import_aktin_properties() {
    local backup_folder="$1"
    local backup_file_name="backup_aktin.properties" # TODO set dynamically
    local target_file_name="aktin.properties"
    local target_path="/etc/aktin"

    echo "replace aktin.properties with properties from backup"

    copy_to_container "$wildfly_container" "$backup_folder/$backup_file_name" "$target_path/$target_file_name"
}

# Generic function to import configuration files from backup to container
# Used for copying various config files to their proper locations
import_config() {
  local src_path="$1"    # Source path on host
  local target_path="$2" # Target path in container

  echo "importing config from: $src_path to: $target_path"
  sudo docker cp "$src_path" "$target_path"
}



# Main function that orchestrates the entire backup restoration process
main() {
    # Pre-flight checks: validate containers and ensure they're from the same project
    do_containers_share_prefix   # check if test-containers originate from same data warehouse
    exit_if_container_down "$postgres_container"
    exit_if_container_down "$apache_container"
    exit_if_container_down "$wildfly_container"

    # Install required tools for progress display
    install_package_pv

    # Extract and copy backup files to the database container
    local backup_dir_docker="/var/tmp"  # target path inside the container
    echo "extract backup-tar on host"
    local backup_dir_host=$(extract_tar "$tarfile")
    local backup_folder=$(copy_to_container "$postgres_container" "$backup_dir_host" "$backup_dir_docker")

    # Stop services that could interfere with database operations
    stop_db_users   # remove database lock
    # Import database backups (main restoration process)
    import_databases_backup "$backup_folder" "$postgres_container"
    # Start WildFly to prepare for configuration import
    start_wildfly

    # Import WildFly configuration files from backup
    import_config "$backup_dir_host/backup_standalone.xml" "$wildfly_container:/opt/wildfly/standalone/configuration/backup_standalone.xml"
    import_config "$backup_dir_host/backup_standalone.conf" "$wildfly_container:/opt/wildfly/bin/backup_standalone.conf"
    import_config "$backup_dir_host/backup_aktin.properties" "$wildfly_container:/etc/aktin/aktin.properties"

    # Restart all services with new configuration and data
    restart_aktin_services
    # Clean up temporary files created during the process
    remove_dir "$(dirname "$backup_dir_host")"
    echo "migration completed"
}

# Execute main function and log all output to timestamped log file
main | tee -a "$log"