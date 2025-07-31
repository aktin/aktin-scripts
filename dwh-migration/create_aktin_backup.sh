#!/bin/bash
#--------------------------------------
# Script Name:  create_aktin_backup.sh
# Version:      1.0
# Author:       akombeiz@ukaachen.de
# Date:         25 Apr 24
# Purpose:      Backups database and configuration files of an AKTIN DWH as a tar.gz. Used for the
#               migration from version 1.4 (install-script) to version 1.5 (debian package)
#--------------------------------------

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# create timestamp and log file
readonly current=$(date +%Y_%h_%d_%H%M)
readonly log=create_aktin_backup_$current.log

# get wildfly home directory
if [[ -d /opt/wildfly/ ]]; then
    readonly wildfly_home=/opt/wildfly
elif [[ -n $(ls /opt/wildfly-*) ]]; then
    readonly wildfly_home=/opt/wildfly-*
fi

create_dir() {
    local dir=$1
    if [ ! -d $dir ]; then
        mkdir $dir
    fi
    echo $dir
}

remove_dir() {
    rm -rf $1
}

tar_dir()  {
    tar -czf aktin_backup_$current.tar.gz --absolute-names --warning=no-file-changed $1/*
}

backup_file() {
    local file=$1
    local destination=$2
    echo -e "backing up $file"
    cp $file $destination
}

backup_folder() {
    local folder=$1
    local destination=$2
    echo -e "backing up $folder"
    cp -r $folder/* $destination
}

backup_aktin() {
    local db=$1
    local destination=$2
    echo -e "backing up $db"
    sudo -u postgres pg_dump $db \
  --no-owner \
  --no-privileges \
  --clean \
  --if-exists \
  > "$destination"
}

backup_i2b2() {
    local db="i2b2"
    local destination=$1
    echo -e "backing up $db"
    sudo -u postgres pg_dump $db \
    --exclude-table=i2b2pm.pm_cell_data \
  --no-owner \
  --no-privileges \
  --clean \
  --if-exists \
  > "$destination"
}

backup_globals() {
  local destination=$1
  sudo -u postgres pg_dumpall --globals-only > $destination
}

main() {
    local tmp_dir=$(create_dir "backup_$current")
	
    backup_file "/etc/aktin/aktin.properties" "$tmp_dir/backup_aktin.properties"
    backup_file "$wildfly_home/standalone/configuration/standalone.xml" "$tmp_dir/backup_standalone.xml"
    backup_file "$wildfly_home/bin/standalone.conf" "$tmp_dir/backup_standalone.conf"
    create_dir "$tmp_dir/var"
    create_dir "$tmp_dir/var/lib"
    create_dir "$tmp_dir/var/lib/aktin"
    backup_folder "/var/lib/aktin" "$tmp_dir/var/lib/aktin"
    backup_globals "$tmp_dir/globals_backup.sql"
    backup_i2b2 "$tmp_dir/backup_i2b2.sql"
    backup_aktin "aktin" "$tmp_dir/backup_aktin.sql"

    tar_dir "$tmp_dir"
    remove_dir "$tmp_dir"
    echo "backup completed"
}

main | tee -a $log
