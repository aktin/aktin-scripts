#! /bin/bash

# script to diagnose errors and bundle them with metadata for mail service
# maintainer: manissen@ukaachen.de
set -euo pipefail

#detect deployment type (Docker or Debian)
if [ -f /.dockerenv ]; then
    DEPLOYMENT_TYPE="Docker"

    WILDFLY_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=wildfly" --format '{{.Names}}' | head -n 1)
    APACHE_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=httpd" --format '{{.Names}}' | head -n 1)
    POSTGRES_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=database" --format '{{.Names}}' | head -n 1)

    WILDFLY_HOME="${WILDFLY_CONTAINER}:/opt/wildfly"
    APACHE_LOG="${APACHE_CONTAINER}:/var/log/apache2"
else
    DEPLOYMENT_TYPE="Debian"
    WILDFLY_HOME="/opt/wildfly"
    APACHE_LOG="/var/log/apache2"
    POSTGRES_LOG="/var/log/postgresql"
fi

# create a log folder for this diagnosis
CURRENT=$(date +%Y_%h_%d_%H%M)
readonly LOGFOLDER="/tmp/aktin_diag_$CURRENT"
if [[ ! -d "$LOGFOLDER" ]]; then
    mkdir "$LOGFOLDER"
fi

# check for root privileges
if [[ $EUID -ne 0 ]]; then
   echo -e "This script must be executed with root privileges!"
   exit 1
fi

copy() {
  local src=$1
  local dest=$2
  if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    docker cp $src $dest
  else
    cp -R $src $dest
  fi
}

run_cmd() {
  local target=$1
  shift
  if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    local container=${target%%:*}
    docker exec $container "$@"
  else
    "$@"
  fi
}

# copy wildfly log into log folder
copy $WILDFLY_HOME/standalone/log $LOGFOLDER/wildfly_log

# copy apache2 logs into log folder
if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    copy $APACHE_LOG $LOGFOLDER/apache_log
    docker logs --tail 2000 "$APACHE_CONTAINER" > "$LOGFOLDER/apache_container_log.txt" 2>&1
else
    copy "$APACHE_LOG" "$LOGFOLDER/apache_log"
fi

# Copy postgresql logs into log folder
if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    docker logs --tail 2000 "$POSTGRES_CONTAINER" > "$LOGFOLDER/database_container_log.txt" 2>&1
else
    copy $POSTGRES_LOG $LOGFOLDER/postgresql_log
fi

# list deployments of wildfly
local_path=${WILDFLY_HOME#*:}
run_cmd $WILDFLY_HOME ls -l $local_path/standalone/deployments/ > $LOGFOLDER/deployments.txt

# check usage of hard drive space
df -h > $LOGFOLDER/diskspace.txt

# check running process
echo -e "+++++ WILDFLY SERVICE STATUS +++++$" > $LOGFOLDER/services.txt
if [[ $DEPLOYMENT_TYPE == "Docker" ]]; then
    docker ps -a --filter "name=${WILDFLY_HOME%%:*}" >> $LOGFOLDER/services.txt
else
  echo $(service wildfly status) >> $LOGFOLDER/services.txt
fi

echo -e "+++++ WILDFLY PS +++++" >> $LOGFOLDER/services.txt
echo $(ps -ef | grep wildfly) >> $LOGFOLDER/services.txt

echo -e "+++++ POSTGRES SERVICE STATUS +++++" >> $LOGFOLDER/services.txt
if [[ $DEPLOYMENT_TYPE == "Docker" ]]; then
    docker ps -a --filter "name=${POSTGRES_CONTAINER}" >> $LOGFOLDER/services.txt
else
  echo $(service postgresql status) >> $LOGFOLDER/services.txt
fi

echo -e "+++++ POSTGRES PS +++++" >> $LOGFOLDER/services.txt
echo $(ps -ef | grep postgresql) >> $LOGFOLDER/services.txt

echo -e "+++++ JAVA PS +++++" >> $LOGFOLDER/services.txt
echo $(ps -ef | grep java) >> $LOGFOLDER/services.txt

echo -e "+++++ OPERATING SYSTEM (HOST) +++++" > $LOGFOLDER/version.txt
cat /etc/os-release >> $LOGFOLDER/version.txt

if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    echo -e "+++++ DOCKER ENGINE VERSION +++++" >> "$LOGFOLDER/version.txt"
    docker --version >> "$LOGFOLDER/version.txt"

    echo -e "+++++ DOCKER COMPOSE VERSION +++++" >> "$LOGFOLDER/version.txt"
    docker compose version >> "$LOGFOLDER/version.txt" 2>/dev/null || docker-compose --version >> "$LOGFOLDER/version.txt" 2>/dev/null || echo "Not found" >> "$LOGFOLDER/version.txt"

    echo -e "+++++ OPERATING SYSTEM (WILDFLY CONTAINER) +++++" >> "$LOGFOLDER/version.txt"
    run_cmd "$WILDFLY_HOME" cat /etc/os-release >> "$LOGFOLDER/version.txt"
fi

echo -e "+++++ POSTGRES VERSION +++++" >> $LOGFOLDER/version.txt
if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    run_cmd $POSTGRES_CONTAINER psql --version >> $LOGFOLDER/version.txt
else
    psql --version >> $LOGFOLDER/version.txt
fi

echo -e "+++++ JAVA VERSION +++++" >> $LOGFOLDER/version.txt
echo $(java --version) >> $LOGFOLDER/version.txt

# check internal memory
top -b -n 1 > $LOGFOLDER/ram.txt

#collect metadate
echo -e "+++++ INSTITUTION ID +++++" >> $LOGFOLDER/metadata.txt
if [[ $DEPLOYMENT_TYPE == "Docker" ]]; then
  docker exec ${WILDFLY_HOME%%:*} grep '^local.o=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
else
  grep '^local.o=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
fi

echo -e "+++++ SITE ID +++++" >> $LOGFOLDER/metadata.txt
if [[ $DEPLOYMENT_TYPE == "Docker" ]]; then
  docker exec ${WILDFLY_HOME%%:*} grep '^local.ou=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
else
  grep '^local.ou=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
fi

echo -e "+++++ DWH VERSION +++++" >> $LOGFOLDER/metadata.txt
if [[ $DEPLOYMENT_TYPE == "Docker" ]]; then
  docker exec ${WILDFLY_HOME%%:*} grep '^local.cn=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
else
  grep '^local.cn=' /etc/aktin/aktin.properties | cut -d '=' -f2 >> $LOGFOLDER/metadata.txt
fi

echo -e "+++++ DEPLOYMENT TYPE +++++" >> $LOGFOLDER/metadata.txt
echo "$DEPLOYMENT_TYPE" >> $LOGFOLDER/metadata.txt

# check permission and existence of folders
if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/ ; then
    run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/ >> $LOGFOLDER/permissions.txt
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/reports ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/reports >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/reports DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/tmp/report-temp ; then
            run_cmd $WILDFLY_HOME ls -ld /var/tmp/report-temp >> $LOGFOLDER/permissions.txt
        else
           echo -e "/var/tmp/report-temp DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/report-archive ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/report-archive >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/report-archive DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/broker ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/broker >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/broker DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/broker-archive ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/broker-archive >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/broker-archive DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/import ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/import >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/import DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
        if run_cmd $WILDFLY_HOME test -d /var/lib/aktin/import-scripts ; then
            run_cmd $WILDFLY_HOME ls -ld /var/lib/aktin/import-scripts >> $LOGFOLDER/permissions.txt
        else
            echo -e "/var/lib/aktin/import-scripts DOES NOT EXIST" >> $LOGFOLDER/permissions.txt
        fi
else
    echo -e "FOLDER /var/lib/aktin DOES NOT EXIST" >>  $LOGFOLDER/permissions.txt
fi

# zip all logs, delete logfolder and return path to zip
ARCHIVE_PATH="/tmp/aktin_diag_$CURRENT.tar.gz"
tar -czf $ARCHIVE_PATH --absolute-names --warning=no-file-changed "$LOGFOLDER/"
rm -rf $LOGFOLDER
echo $ARCHIVE_PATH