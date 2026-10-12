#! /bin/bash
#--------------------------------------
# Script Name:  extract_logged_technical_and_organizational_metadata
# Version:      1.0
# Authors:      manissen@ukaachen.de
# Date:         12 Aug 26
# Purpose:      script to diagnose errors and bundle them with metadata for mail service
#--------------------------------------
set -euo pipefail

#global variables
readonly WILDFLY_HOME=/opt/wildfly
readonly CURRENT=$(date +%Y_%m_%d_%H%M)
APACHE_LOG=""
POSTGRES_LOG=""

# deployment detection
if [[ -f /.dockerenv ]]; then
    DEPLOYMENT_TYPE="Docker"
    POSTGRES_HOST="database"
else
    DEPLOYMENT_TYPE="Debian"
    APACHE_LOG="/var/log/apache2"
    POSTGRES_LOG="/var/log/postgresql"
fi

# create log directory
readonly LOGFOLDER="/tmp/aktin_diag_$CURRENT"
if [[ ! -d "$LOGFOLDER" ]]; then
    rm -rf "$LOGFOLDER"
    mkdir "$LOGFOLDER"
fi

# wildfly logs
cp -R "$WILDFLY_HOME/standalone/log" "$LOGFOLDER/wildfly_log" \
    || echo "Failed to copy wildfly logs" > "$LOGFOLDER/error_log.txt"

# apache und postgres logs
if [[ ! (-n $APACHE_LOG && -n $POSTGRES_LOG) ]]; then
    echo "Apache logs are NOT accessible from inside the Wildfly container." > "$LOGFOLDER/apache_log_unavailable.txt"
    echo "PostgreSQL logs are NOT accessible from inside the Wildfly container." > "$LOGFOLDER/postgresql_log_unavailable.txt"
else
    cp -R "$APACHE_LOG" "$LOGFOLDER/apache_log" \
        || echo "Failed to copy apache logs" > "$LOGFOLDER/error_log.txt"
    cp -R "$POSTGRES_LOG" "$LOGFOLDER/postgresql_log" \
        || echo "Failed to copy postgresql logs" > "$LOGFOLDER/error_log.txt"
fi

# wildfly deployments
ls -l "$WILDFLY_HOME/standalone/deployments/" > "$LOGFOLDER/deployments.txt" || true

#  disk space
df -hT > "$LOGFOLDER/diskspace.txt"

log_processes() {
    local header="$1"
    local process="$2"

    echo -e "+++++ $header PS +++++" >> "$LOGFOLDER/services.txt"
    ps -ef | grep "$process" >> "$LOGFOLDER/services.txt" || true
}

# services and processes
echo -e "+++++ WILDFLY SERVICE STATUS +++++" > "$LOGFOLDER/services.txt"
if [ -x "$WILDFLY_HOME/bin/jboss-cli.sh" ]; then
    "$WILDFLY_HOME/bin/jboss-cli.sh" --connect --commands=":read-attribute(name=server-state)" < /dev/null >> "$LOGFOLDER/services.txt" 2>&1 || true
else
    echo "jboss-cli.sh not found under $WILDFLY_HOME/bin/" >> "$LOGFOLDER/services.txt"
fi
if [[ "$DEPLOYMENT_TYPE" = "Debian" ]]; then
    echo -e "\n+++++ WILDFLY SYSTEMD SERVICE STATUS +++++" >> "$LOGFOLDER/services.txt"
    service wildfly status >> "$LOGFOLDER/services.txt" 2>&1 || true
fi
echo -e "\n+++++ DEPLOYMENT STATUS MARKERS +++++" >> "$LOGFOLDER/services.txt"
ls -1 "$WILDFLY_HOME/standalone/deployments"/*.{deployed,failed,isdeploying} >> "$LOGFOLDER/services.txt" 2>&1 || true


log_processes "WILDFLY" "wildfly"

echo -e "+++++ POSTGRES SERVICE STATUS +++++" >> "$LOGFOLDER/services.txt"
if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    if timeout 2 bash -c "</dev/tcp/$POSTGRES_HOST/5432" 2>/dev/null; then
        echo "PostgreSQL on '$POSTGRES_HOST:5432' is accepting connections." >> "$LOGFOLDER/services.txt"
    else
        echo "PostgreSQL on '$POSTGRES_HOST:5432' is NOT reachable." >> "$LOGFOLDER/services.txt"
    fi
else
    service postgresql status >> "$LOGFOLDER/services.txt" || true
fi

log_processes "POSTGRES" "postgresql"
log_processes "JAVA" "java"

# versions
echo -e "+++++ OPERATING SYSTEM +++++" > "$LOGFOLDER/version.txt"
cat /etc/os-release >> "$LOGFOLDER/version.txt" 2>/dev/null || true

echo -e "+++++ POSTGRES VERSION +++++" >> "$LOGFOLDER/version.txt"
if [[ "$DEPLOYMENT_TYPE" == "Docker" ]]; then
    if command -v psql &>/dev/null; then
        psql --version >> "$LOGFOLDER/version.txt"
    else
        echo "psql client not installed in this container/environment." >> "$LOGFOLDER/version.txt"
    fi
else
    psql --version >> "$LOGFOLDER/version.txt" 2>/dev/null || true
fi

echo -e "+++++ JAVA VERSION +++++" >> "$LOGFOLDER/version.txt"
java --version >> "$LOGFOLDER/version.txt" 2>&1 || true

# memory
top -b -n 1 | head -n 30 > "$LOGFOLDER/ram.txt" || true

# aktin metadata
echo -e "+++++ INSTITUTION ID +++++" > "$LOGFOLDER/metadata.txt"
grep "^local\.o=" /etc/aktin/aktin.properties 2>/dev/null | cut -d '=' -f2 >> "$LOGFOLDER/metadata.txt" || true

echo -e "+++++ SITE ID +++++" >> "$LOGFOLDER/metadata.txt"
grep "^local\.ou=" /etc/aktin/aktin.properties 2>/dev/null | cut -d '=' -f2 >> "$LOGFOLDER/metadata.txt" || true

echo -e "+++++ DWH VERSION +++++" >> "$LOGFOLDER/metadata.txt"
grep "^local\.cn=" /etc/aktin/aktin.properties 2>/dev/null | cut -d '=' -f2 >> "$LOGFOLDER/metadata.txt" || true

echo -e "+++++ DEPLOYMENT TYPE +++++" >> "$LOGFOLDER/metadata.txt"
echo "$DEPLOYMENT_TYPE" >> "$LOGFOLDER/metadata.txt"

# archive
tar -czf "${LOGFOLDER}.tar.gz" --absolute-names --warning=no-file-changed "$LOGFOLDER/" || true
rm -rf "$LOGFOLDER"
echo "${LOGFOLDER}.tar.gz"