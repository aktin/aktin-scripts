#!/bin/bash
#--------------------------------------
# Script Name:  create_docker_backup.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         4 Jun 25
# Purpose:      Creates a backup file of a AKTIN dockerized data warehouse,
#               containing data and configurations of the original
#--------------------------------------

# Require root to avoid permission errors
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

[[ $# -ne 2 ]] && {
    echo "Usage: $0 <wildfly_container_name> <postgres_container_name>"
    exit 1
}

readonly wildfly_container="$1"
readonly postgres_container="$2"

# create timestamp and log file
readonly current=$(date +%Y_%h_%d_%H%M)
readonly log=create_aktin_backup_$current.log


# Fail fast if a required container isn't running
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

# Copy a file/dir from container to host staging area
backup_docker_resource() {
    local resource="$1"
    local destination="$2"
    echo -e "backing up $resource"
    docker cp "$resource" "$destination"
}

create_dir() {
    local dir=$1
    mkdir -p $dir
    echo $dir
}

# Dump a PostgreSQL database from inside the container to a host file
backup_database() {
    local db="$1"
    local destination="$2"
    local container_dest="/tmp/backup_$db.sql"

    echo -e "create backup of database $db"
    docker exec -u postgres "$postgres_container" pg_dump -U postgres $db > $destination
}

# compresses the backup files into athe final tar.gz archive
tar_dir()  {
    tar -czf aktin_backup_"$current".tar.gz --absolute-names --warning=no-file-changed "$1"/*
}


main() {
  check_container_running "$wildfly_container"
  check_container_running "$postgres_container"

  # create backup dir and fill with backup files
  local tmp_dir=$(create_dir "backup_$current")
  # backup configuration files from container
  backup_docker_resource "$wildfly_container:/etc/aktin/aktin.properties" "$tmp_dir/backup_aktin.properties"
  backup_docker_resource "$wildfly_container:/opt/wildfly/standalone/configuration/standalone.xml" "$tmp_dir/backup_standalone.xml"
  backup_docker_resource "$wildfly_container:/opt/wildfly/bin/standalone.conf" "$tmp_dir/backup_standalone.conf"

  create_dir "$tmp_dir/var/lib/"
  backup_docker_resource "$wildfly_container:/var/lib/aktin" "$tmp_dir/var/lib"

  backup_database "i2b2" "$tmp_dir/backup_i2b2.sql"
  backup_database "aktin" "$tmp_dir/backup_aktin.sql"

  # create final compressed backup file
  tar_dir "$tmp_dir"
  rm -rf "$tmp_dir"
  echo "backup completed"
}

main | tee -a "$log"
