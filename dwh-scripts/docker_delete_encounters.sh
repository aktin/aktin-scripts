#!/usr/bin/env bash
#--------------------------------------
# Script Name:  docker_delete_encounter.sh
# Version:      1.0
# Author:       whoy@ukaachen.de
# Date:         25 Nov 25
# Purpose:      Delete encounter window from database of a Docker DWH (from yyyymmdd inclusive to yyyymmdd exclusive)
#--------------------------------------
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }
note() { echo "==> $*" >&2; }   # status messages to stderr

check_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    die "Please run as root"
  fi
}

validate_date() {
  local date="$1"
  if [[ $date =~ ^[0-9]{8}$ ]] && date -d "${date:0:4}-${date:4:2}-${date:6:2}" +%Y%m%d >/dev/null 2>&1; then
    return 0
  else
    echo "Invalid date: $date. Expected format: yyyymmdd." >&2
    return 1
  fi
}

check_date_order() {
  local start="$1" end="$2"
  local s e
  s=$(date -d "${start:0:4}-${start:4:2}-${start:6:2}" +%s) || return 1
  e=$(date -d "${end:0:4}-${end:4:2}-${end:6:2}" +%s) || return 1
  (( s <= e ))
}

 check_dwh_prefix() {
   local prefix="$1"
   local postgres="${prefix}-database-1"
   local wildfly="${prefix}-wildfly-1"

   local running
   running=$(docker ps --format '{{.Names}}' || true)
   [[ -n "$running" ]] || { echo "No running containers found." >&2; return 1; }

   echo "$running" | grep -Fqx "$postgres" || {
     echo "ERROR: PostgreSQL container '$postgres' with prefix '$prefix' is not running." >&2
     return 1
   }

   echo "$running" | grep -Fqx "$wildfly" || {
     echo "ERROR: WildFly container '$wildfly' with prefix '$prefix' is not running." >&2
     return 1
   }

   return 0
 }

get_container_id() {
  local name="$1"
  if docker inspect --type container "$name" &>/dev/null; then
    docker inspect --type container --format '{{.Id}}' "$name"
    return 0
  fi
  docker ps -aqf "name=^${name}$" || true
}

wait_until_stopped() {
  local name="$1" timeout="${2:-60}" t=0
  while service_running "$name"; do
    (( t >= timeout )) && { echo "Timed out waiting for $name to stop." >&2; return 1; }
    sleep 1
    ((t++))
  done
}

execute_sql() {
  local start_date="$1"  # yyyymmdd
  local end_date="$2"    # yyyymmdd
  local container="$3"

  docker exec "$container" psql -U postgres -d i2b2 -v ON_ERROR_STOP=1 -c "
    DELETE FROM i2b2crcdata.observation_fact
    WHERE start_date BETWEEN to_date('$start_date','YYYYMMDD') AND to_date('$end_date','YYYYMMDD');

    WITH Lookuptable AS (
      DELETE FROM i2b2crcdata.visit_dimension
      WHERE start_date BETWEEN to_date('$start_date','YYYYMMDD') AND to_date('$end_date','YYYYMMDD')
      RETURNING encounter_num
    )
    DELETE FROM i2b2crcdata.encounter_mapping
    WHERE encounter_num IN (SELECT encounter_num FROM Lookuptable);
  "
}

main() {
  check_root

  if [[ $# -ne 3 ]]; then
    die "Three arguments are required. Usage: $0 yyyymmdd yyyymmdd prefix"
  fi

  local start_date="$1" end_date="$2" DWH_PREFIX="$3"
  validate_date "$start_date" || die "Invalid start date"
  validate_date "$end_date"   || die "Invalid end date"
  check_date_order "$start_date" "$end_date" || die "Start date must be <= end date"

  note "Start Date: $start_date and End Date: $end_date are valid."
  note "Using DWH prefix: $DWH_PREFIX"

  check_dwh_prefix "$DWH_PREFIX" || die "Required container with prefix "$DWH_PREFIX" does not exist."

  local postgres="${DWH_PREFIX}-database-1"
  local wildfly="${DWH_PREFIX}-wildfly-1"

  local wildfly_was_running=1
  note "Stopping WildFly ($wildfly)…"
  docker stop "$wildfly" >/dev/null || wildfly_was_running=0
  if (( wildfly_was_running == 1 )); then
    wait_until_stopped "$wildfly" 90
    note "WildFly stopped."
  else
    note "WildFly was not running."
  fi

  note "Executing SQL query on $postgres…"
  execute_sql "$start_date" "$end_date" "$postgres"
  note "SQL execution finished."

  if (( wildfly_was_running == 1 )); then
    note "Restarting WildFly service…"
    docker start "$wildfly" >/dev/null
    note "WildFly restarted."
  fi

  note "Done."
}

main "$@"