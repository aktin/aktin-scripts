#!/bin/bash

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

backup_docker_file() {
  local src="$1"
  local dest="$2"
  sudo docker cp "$src" "$dest"
}

backup_docker_folder() {
    local folder="$1"
    local destination="$2"
    echo -e "backing up $folder"
    docker cp "$folder" "$destination"
}

create_dir() {
    local dir=$1
    if [ ! -d $dir ]; then
        mkdir $dir
    fi
    echo $dir
}

backup_database() {
    local db="$1"
    local destination="$2"
    local container_dest="/tmp/backup_$db.sql"

    echo -e "create backup of database $db"
#    sudo docker exec -it "$postgres_container" "pg_dump -U postgres $db > $container_dest"
    docker exec -u postgres "$postgres_container" bash -c "pg_dump -U postgres $db --data-only > $container_dest"

    # opy backup file to host
    sudo docker cp "$postgres_container:$container_dest" "$destination"
}

tar_dir()  {
    tar -czf aktin_backup_"$current".tar.gz --absolute-names --warning=no-file-changed "$1"/*
}

remove_dir() {
    rm -rf $1
}


main() {
  check_container_running "$wildfly_container"
  check_container_running "$postgres_container"

  # prepare
  local tmp_dir=$(create_dir "backup_$current")
  # backup "aktin.properties" from container
  backup_docker_file "$wildfly_container:/etc/aktin/aktin.properties" "$tmp_dir/backup_aktin.properties"
  backup_docker_file "$wildfly_container:/opt/wildfly/standalone/configuration/standalone.xml" "$tmp_dir/backup_standalone.xml"
  backup_docker_file "$wildfly_container:/opt/wildfly/bin/standalone.conf" "$tmp_dir/backup_standalone.conf"

  create_dir "$tmp_dir/var"
  create_dir "$tmp_dir/var/lib"
  create_dir "$tmp_dir/var/lib/aktin"
  backup_docker_folder "$wildfly_container:/var/lib/aktin" "$tmp_dir/var/lib/aktin"

  backup_database "i2b2" "$tmp_dir/backup_i2b2.sql"
  backup_database "aktin" "$tmp_dir/backup_aktin.sql"

   tar_dir "$tmp_dir"
    remove_dir "$tmp_dir"
    echo "backup completed"

}

main | tee -a "$log"