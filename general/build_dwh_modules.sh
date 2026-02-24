#!/bin/bash
#--------------------------------------
# Script Name:  build_dwh_modules.sh
# Version:      2.0
# Author:       whoy@ukaachen.de
# Date:         11 Feb 26
# Purpose:      Build DWH Maven modules, manages maven artifact versioning, creates and relocates EAR to target,
#               manages EAR integration on target.
# Usages:       ./build_dwh_modules.sh -p <target_ip> -d <parent_dir_of_projects>
#               ./build_dwh_modules.sh -p <target_ip> -d <parent_dir_of_projects> -r true
#               ./build_dwh_modules.sh -p <target_ip> -d <parent_dir_of_projects> -b <project_name>
#--------------------------------------

set -euo pipefail

# check: running as root
if [ "$EUID" = 0 ]; then
  echo "Pleas do not run as root"
  exit 1
fi


BASE_GROUP_ID="org.aktin"


print_help() {
  local script
  script="$(basename "${BASH_SOURCE[0]}")"

  # Use a heredoc and substitute only SCRIPT_NAME
  cat <<EOF
Usage:
  ${script} --server-ip <IP> [options]

Recommended Uses:
  # Minimal
  ${script} -p <IP> -d

Description:
  Builds and installs configured Maven modules (offline by default) and can optionally
  clean local Maven artifacts before building. Uses a config file (projects.conf by default)
  that is copied to script-config.conf for stable re-runs.

Required:
  -p, --server-ip <IP>         Target server IP / host used by later deployment steps.
  --set-java-8                 Path to installed Java SDK 8
  --set-java-11                Path to installed Java SDK 11

Options:
  -b, --build-from <NAME>      Start building from the given project name (skip earlier entries).
                               Default: build all configured projects.

  -d, --project-dir <DIR>      Root directory that contains the project folders.
                               Default: current working directory.

  -c, --conf <FILE>            Path to config file to load projects array from.
                               Default: ./projects.conf (will be copied to ./script-config.conf)

  -r, --remove-packages        Remove local Maven artifacts under ~/.m2/repository/org/aktin
                               before building.
                               Default: false

  -i, --instance <NAME>        Instance/environment selector.
                               Default: debian, Options: debian, docker

  -w, --wildfly <NAME>         Name of Wildfly container. Required if instance is set to "docker".

  -h, --help                   Show this help and exit.

Examples:
  ${script} -p 10.0.0.12
  ${script} -p 10.0.0.12 -d /home/user/IdeaProjects -c ./projects.conf
  ${script} -p dwh.example.org --remove-packages
  ${script} -p 10.0.0.12 --build-from dwh-j2ee --instance debian

Exit codes:
  0  Success
  1  Invalid arguments / missing required parameters
EOF
}

log_debug() { echo "[DEBUG] $*" >&2; }
log_info()  { echo "[INFO]  $*" >&2; }
log_warn()  { echo "[WARN]  $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }

#----------------------------------
# START - Read Script parameters
#----------------------------------
ORIG_ARGS=("$@")

: "${BASE_PATH:=$PATH}"
export BASE_PATH

SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
readonly SCRIPT_PATH


# check and init script parameters
# set project to start building from if not all should be build
server_ip=""
build_from=""
CONFIG_FILE=""
rm_packages="false"
tmp_dir=""
instance="debian"
SDK8=""
SDK11=""
wildfly_container=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
       print_help
       exit 0
       ;;
    -p|--server-ip)
      server_ip="$2"
      shift 2
      ;;

    -b|--build-from)
      build_from="$2"
      shift 2
      ;;

    -d|--project-dir)
      ROOT_DIR="$2"
      shift 2
      ;;

    -c|--conf)
      CONFIG_FILE="$2"
      shift 2
      ;;

    -r|--remove-packages)
      rm_packages="true"
      shift 1
      ;;

    -i|--instance)
      instance="$2"
      shift 2
      ;;

    -w|--wildfly)
      wildfly_container="$2"
      shift 2
      ;;

    --set-java-8)
      SDK8="$2"
      shift 2
      ;;

    --set-java-11)
      SDK11="$2"
      shift 2
      ;;

    *)
      echo "Unknown parameter: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$server_ip" ]]; then
  echo "Missing required parameter: --server-ip" >&2
  exit 1
fi

if [[ -z "$ROOT_DIR" ]]; then
  ROOT_DIR="$PWD"
fi

if [[ "$instance" == "docker" ]] && [[ -z "$wildfly_container" ]]; then
  log_error "docker instance requires wildfly container. Add using \"-w\" or \"--wildfly\""
  exit 1
fi

[[ -z "$SDK8" ]] && log_error "SDK-8 path not set"
[[ -z "$SDK11" ]] && log_error "SDK-11 path not set"

readonly rm_packages
if [[ "$rm_packages" == "true" ]]; then
  echo "[WARN] Removing all packages from local Maven installations"
  if [[ -d "$HOME/.m2/repository/org/aktin" ]]; then
    rm -rf ~/.m2/repository/org/aktin
    echo "[INFO] Removed artifacts from local maven store"
  else
    echo "[INFO] No Local maven store found, skipping."
  fi
fi

if [[ -z "$CONFIG_FILE" ]]; then
  CONFIG_FILE="$PWD/projects.conf"
fi

# Copy config to script-config.conf so that reruns are stable and do not change original config file.
if [[ "$(basename $CONFIG_FILE)" != "script-config.conf" ]];then
  cp "$CONFIG_FILE" "$(dirname $CONFIG_FILE)/script-config.conf"
  CONFIG_FILE="$(dirname $CONFIG_FILE)/script-config.conf"
fi

readonly CONFIG_FILE

source "$CONFIG_FILE" # Load project confs from .conf file
if [[ "${#projects[@]}" -gt 100 ]]; then
  read -p "Warning. Current config size: "$(expr ${#projects[@]} / "$projects_entry_width")" (Press any key to continue, ctrl+c to stop)"
fi

# if a config exists, remove it from orig_args, to make it possible to insert internal script-config as parameter
for ((i=0;i<"${#ORIG_ARGS[@]}";i+=1)); do
  if [[ "${ORIG_ARGS[i]}" == "-c" ]] || [[ "${ORIG_ARGS[i]}" == "--config" ]]; then
    ORIG_ARGS=("${ORIG_ARGS[@]:0:i}" "${ORIG_ARGS[@]:i+2}")
    break
  fi
done
ORIG_ARGS=("${ORIG_ARGS[@]}" "-c" "$CONFIG_FILE")

# remove 'remove-package' tag, to make further executions run faster
for ((i=0;i<"${#ORIG_ARGS[@]}";i+=1)); do
  if [[ "${ORIG_ARGS[i]}" == "-r" ]] || [[ "${ORIG_ARGS[i]}" == "--remove-packages" ]]; then
    ORIG_ARGS=("${ORIG_ARGS[@]:0:i}" "${ORIG_ARGS[@]:i+2}")
    break
  fi
done

ORIG_ARGS=("${ORIG_ARGS[@]}" "--instance" "$instance")
readonly -a ORIG_ARGS

# end script if build from artifact is not in project configs
[[ -n "$build_from" ]] && ! project_exists "$build_from" && die "Given Project '$build_from' not found in config."
if [[ -z "$build_from" ]]; then
  build_from=${projects[0]}   # root project default
fi

readonly server_ip
readonly build_from
readonly ROOT_DIR
readonly server_user=root
readonly SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"

#----------------------------------
# START - General helper functions
#----------------------------------
die() {
  local message="$1"
  local code="${2:-1}"; shift || true
  log_error "$message" "Exiting with error code: $code"
  exit "$code"
}

# does given project name exist in project list
project_exists() {
  local project="$1"
  local project_name=""

  log_debug "Checking if project exists in config: $project"

  for ((i=0; i<"${#projects[@]}"; i+="$projects_entry_width")); do
    project_name="${projects[i]}"
    if [[ "$project_name" == "$project" ]]; then
      printf '%s\n' "$project"
      return 0
    fi
  done

  return 1
}

add_new_project_conf(){
  # Adds a new config for $projects. Inserts the config after existing entry of the same project to keep build order intact.
  local project="$1"
  local commit="$2"

  [[ -n "$commit" ]] || die "[ERROR] Commit ID was empty, but is required. $commit"

  for ((i=0; i<${#projects[@]}; i+=$projects_entry_width)); do
    if [[ "${projects[i]}" == "$project" ]]; then
       # Insert new config set at index i
      projects=(
        "${projects[@]:0:i}"
        "${projects[i]}" "${projects[i+1]}" "" "$commit"
        "${projects[@]:i}"
      )
      declare -p projects > "$CONFIG_FILE" # save configs with newly added conf
      log_info "Added alt config for '$project' with commit '$commit' into $CONFIG_FILE"
      return 0
    fi
  done
  return 1
}

get_configured_projects() {
  projects_listed=()
  for ((i=0; i<${#projects[@]}; i+=$projects_entry_width)); do
      [[ -n "${projects[i]}" ]] && projects_listed+=("${projects[i]}")
  done
}

open_tmp_worktree() {
  local target_commit="$1"
  [[ -d "$tmp_dir" ]] && die "A temporary workingtree already exists."
  tmp_dir="$(mktemp -d -t build-wt-XXXXXXXX)"
  git worktree add --detach "$tmp_dir" "$target_commit"
  cd "$tmp_dir" # move to temporary worktree
}

close_and_flush_tmp_worktree() {
  # Save EAR files from wortree before removal, to make it possible to be deployed later.
  local ear=$(find "$tmp_dir" -type f -name "dwh-j2ee-*.ear")
  [[ -n "$ear" ]] && cp "$ear" "/tmp/$(basename $ear)"  # save ear file from worktree

  # delete temporary worktree and navigate to main working directory
  git worktree remove --force "$tmp_dir"
  cd "$(dirname "$SCRIPT_PATH")"
  rm -rf "$tmp_dir"
}

# get commit id from branch, tag, commit etc.
git_resolve_ref_to_commit() {
  local ref="$1"
  git rev-parse "${ref}^{commit}" 2>/dev/null
}

xml_reactor_artifactId() {
  local pom="$1"
  if [[ -f "$pom" ]]; then
    xmlstarlet sel -t -v "/*[local-name()='project']/*[local-name()='artifactId']" -n "$pom" 2>/dev/null | head -n 1
  else
    xmlstarlet sel -t -v "/*[local-name()='project']/*[local-name()='artifactId']" -n - 2>/dev/null <<<"$pom" | head -n 1
  fi
}

xml_reactor_version() {
  local pom="$1"
  if [[ -f "$pom" ]]; then
    xmlstarlet sel -t -v "/*[local-name()='project']/*[local-name()='version']" -n "$pom" 2>/dev/null | head -n 1
  else
    xmlstarlet sel -t -v "/*[local-name()='project']/*[local-name()='version']" -n - 2>/dev/null <<<"$pom" | head -n 1
  fi
}

# Debugging helper, not used in current script pipeline
pom_get() {
  local pom="$1" e_parent="$2" e_target="$3" matches=""
  log_debug "pom_get: $pom:$e_parent:$e_target"
  if [[ -f "$pom" ]]; then
    matches="$(xmlstarlet sel -t -v "/*[local-name()='$e_parent']/*[local-name()='$e_target']" -n "$pom" 2>/dev/null)"
  else
    matches="$(xmlstarlet sel -t -v "/*[local-name()='$e_parent']/*[local-name()='$e_target']" -n - 2>/dev/null <<<"$pom")"
  fi

  printf '%s\n' "$matches"
}

get_all_poms() {
  # Find all pom files in whitelisted projects. Ignores target directories
  local -a poms=()

  log_debug "Collect pom files from whitelisted directories: ${projects_listed[*]}"
  mapfile -t poms < <(
    for dir in "${projects_listed[@]}"; do
      find "$ROOT_DIR/$dir" -type f -name "pom.xml" \
        -not -path '*/target/*' 2>/dev/null
    done
  )

  log_debug "Found ${#poms[@]} poms"
  printf '%s\n' "${poms[@]}"
}

resolve_artifact_to_pom() {
  # Find a pom file used for building the given artifact. Ignoring artifact version.
  # Searches only pom files of projects inside whitelist.
  # Returns empty string if no pom files were found containing the artifact.
  local reactor="$1"
  local target_path=""
  local -a poms

  mapfile -t poms < <(get_all_poms)

  for pom in "${poms[@]}"; do
    log_debug "pom: $pom"
    local found="$(xml_reactor_artifactId "$pom")"
    log_debug "reactor: $found"
    if [[ "$found" == "$reactor" ]]; then
      target_path="$pom"
      break
    fi
  done
  echo "$target_path"
}

resolve_to_commit() {
  # Search the commit history of project containing the target pom file,
  # for the last tagged commit containing the given pom file with target version.
  # Returns empty string if no match was found.
  local pom_dir="$1"
  local version="$2"
  local relative_path="$3"
  local commits commit_found=""

  # Find all tagged commits of project containing the target pom
  local commits=$(git -C "$pom_dir" for-each-ref --sort=-creatordate --format='%(objectname) %(refname:short)' refs/tags)

  local cache_found_versions="" content ver commit tag
  while read -r commit tag; do
    content="$(git -C "$pom_dir" show "$commit:$relative_path" 2>/dev/null)"
    ver="$(xml_reactor_version "$content")"
    cache_found_versions="$cache_found_versions $ver"

    if [[ "$ver" == "$version" ]]; then
      log_debug "Module version found in commit '$commit' (tag '$tag')."
      commit_found="$commit"
      break
    fi
  done <<< "$commits"
  log_debug "Artifact versions found: '$cache_found_versions'"
  echo "$commit_found"
}



git_resolve_module_to_commit() {
  # This function returns the last commit containing the artifact-version pair
  local artifact="$1"
  local version="$2"

  local target_path="$(resolve_artifact_to_pom $artifact)"

  if [[ -z "$target_path" ]]; then
    log_error "No artifact '$artifact' found inside project pom-files"
    return 1
  fi
  log_info "Found artifact '$artifact' inside '$target_path'"

  local target_dir relative_path
  target_dir="$(dirname "$target_path")"
  relative_path="$(git -C "$target_dir" rev-parse --show-prefix)pom.xml"

  # Search commit containing the requested artifact version
  local commit="$(resolve_to_commit $target_dir $version $relative_path)"

  if [[ -z "$commit" ]]; then
    log_error "Did not find '$module:$version'"
  fi

  printf '%s:%s\n' "$target_path" "$commit"
}

set_java_path() {
  export JAVA_HOME="$1"
  export PATH="$JAVA_HOME/bin:$PATH"
  log_debug "JAVA_HOME set to: $JAVA_HOME"
}

parse_failure_context() {
  local out="$1"
  local -n _failed_ref="$2"
  local -n _group_ref="$3"
  local -n _artifact_ref="$4"
  local -n _version_ref="$5"

  local required_str

  _failed_ref="$(grep -oP 'project \K[^ ]+' <<<"$out" | tail -n 1 || true)"
  required_str="$(grep -oPm1 'artifact \K[^ ]+' <<<"$out" || true)"

  local -a required=()
  IFS=':' read -r -a required <<<"$required_str"

  _group_ref="${required[0]}"
  _artifact_ref="${required[1]}"
  _version_ref="${required[-1]}"
}

attempt_online_artifact_recovery() {
  # Install Maven artifact from online repository, if required module
  # has different group id than $BASE_GROUP_ID or is specifically called.
    local req_group="$1"
    local req_artifact="$2"
    local req_version="$3"
    local failed="$4"
    local sdk_dir="$5"

    # todo: add a whitelist/blacklist for these hard-coded modules
    # todo: add functionality to "git_resolve_module_to_commit", that checks past pom versions (these modules were removed in newer releases but still depended on them)
    log_debug "try non aktin install of '$req_artifact'"
    if { [[ -z "$failed" ]] && [[ "$req_artifact" != "$BASE_GROUP_ID"* ]]; } \
       || [[ "$req_artifact" == *"query-i2b2-sql"* ]] \
       || [[ "$req_artifact" == *"query-aggregate-rscript"* ]] \
       || [[ "$req_artifact" == *"query-model"* ]]; then

      log_debug "Missing artifact not in config projects; trying dependency:get for '${req_group}:${req_artifact}:${req_version}'"

      set +e
      mvn dependency:get \
        -DgroupId="$req_group" \
        -DartifactId="$req_artifact" \
        -Dversion="$req_version"
      local rc=$?
      set -e

      if (( rc != 0 )); then
        return 1
      fi

      log_debug "Artifact '$req_artifact' was not part of parent group '$BASE_GROUP_ID' or was whitelisted therefore loaded in online Mode."
      return 0
    fi

    return 1
}

resolve_required_module() {
  local artifact="$1"
  local version="$2"
  local -n _required_pom_dir="$3"
  local -n _required_commit_id="$4"
  req_full="$(git_resolve_module_to_commit "$artifact" "$version")"
  req="${req_full##*$'\n'}"
  IFS=':' read -r _required_pom_dir _required_commit_id <<<"$req"
}

attempt_local_artifact_recovery() {
  # Search each configured projects git history for maven reactors.
  # Requires the target reactor-file to exist in one of the projects.
  # When reactor found, update projects config and execute this entire script in a subshell, ending this scripts execution.
  local req_artifact="$1"
  local req_version="$2"
  local req_full req required_pom_dir required_commit_id project_name
  resolve_required_module "$req_artifact" "$req_version" required_pom_dir required_commit_id

  project_name="$(basename "$(git -C "$(dirname "$required_pom_dir")" rev-parse --show-toplevel)")"
  log_error "Another module is required; add: \"$project_name\" \"<sdk>\" \"\" \"$required_commit_id\" to config and re-run"
  add_new_project_conf "$project_name" "$required_commit_id"
  close_and_flush_tmp_worktree
  exec env -i \
    PATH="$BASE_PATH" \
    HOME="${HOME:-/home/$USER}" \
    USER="${USER:-}" \
    "$SCRIPT_PATH" "${ORIG_ARGS[@]}"
}

mvn_clean_install_module() {
  # Main installation logic, implementing local package building by default and adds failure handling.
  local sdk_dir="$1"
  local out="" rc=0

  # Try maven offline install
  log_info "Building module using $sdk_dir"
  set_java_path "$sdk_dir"
  set +e
  out="$(mvn -o -B -Dstyle.color=always clean install -DskipTests 2>&1)"
  rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    return 0  # Maven build successful
  fi

  log_error "Build failed"
  local failed req_group req_artifact req_version
  parse_failure_context "$out" failed req_group req_artifact req_version
  log_debug "failed=$failed, artifact=$req_group:$req_artifact:$req_version"

  # load and clean install package from remote repo if allowed, if not attempt to install it from a local projects history
  if attempt_online_artifact_recovery "$req_group" "$req_artifact" "$req_version" "$failed" "$sdk_dir"; then
    mvn_clean_install_module "$sdk_dir" || return 1
  else
    attempt_local_artifact_recovery "$req_artifact" "$req_version"
  fi
}

build_all_projects() {
  # For all projects:
  # -check if target branches exist
  # -safely pull from remote if needed / cancel if local branch is behind or diverged from remote
  # -switch to target local branch
  # -build maven artifact
  # -switch to old branch and restore working tree
  log_debug "Executing script in ROOT_DIR=$ROOT_DIR"
  log_info "Start handling Git branches"

  for ((i=0; i<${#projects[@]}; i+=projects_entry_width)); do
    local project_name="${projects[i]}"
    local project_dir="$ROOT_DIR/$project_name"
    local sdk_dir="${projects[i+1]}"
    local target_ref="${projects[i+2]}"
    local target_commit="${projects[i+3]}"

    if [[ -z "$target_ref" && -z "$target_commit" ]]; then
      log_info "No config found for '$project_name', skipping maven build..."
      continue
    fi

    cd "$project_dir"
    echo ""

    if [[ -z "$(git branch --show-current)" ]]; then
      die 1 "'$project_name' is in detached HEAD; switch to a branch manually."
    fi

    if [[ -z "$target_commit" ]]; then
      [[ -n "$target_ref" ]] || die 1 "No reference/commit config for '$project_name'"
      log_debug "Resolving reference '$target_ref' → commit"
      target_commit="$(git_resolve_ref_to_commit "$target_ref")"
      log_debug "Resolved commit: $target_commit"
    fi

    # if current HEAD is target, local untracked changes will be built too
    if [[ "$(git rev-parse HEAD)" == "$target_commit" ]]; then
      log_debug "Already on target HEAD for '$project_name'"
      mvn_clean_install_module "$sdk_dir"
    else
      log_debug "Switching '$project_name' to target commit '$target_commit'"
      open_tmp_worktree "$target_commit"
      mvn_clean_install_module "$sdk_dir"
      close_and_flush_tmp_worktree
    fi
  done
}

do_all_projects_exist() {
  cd "$ROOT_DIR"
  log_info "Validating project directories..."

  for ((i=0; i<${#projects[@]}; i+=projects_entry_width)); do
    local project_name="${projects[i]}"
    local project_dir="$ROOT_DIR/$project_name"

    if [[ ! -d "$project_dir" ]]; then
      log_error "Project does not exist: $project_name"
      return 1
    fi
  done
}


notify() {
  printf "\a"; sleep 0.15
  printf "\a"
}

#----------------------------------
# END - General helper functions
#----------------------------------


#----------------------------------
# START - Script Execution Workflow
#----------------------------------


get_configured_projects
echo "System Java version: $(which java)"


# VALIDATE - projects exist
if ! do_all_projects_exist; then
  die "Some projects could not be found."
else
  log_info "All projects found."
fi

# Safely build each maven project iteratively
build_all_projects
notify
log_info "copy the .ear file to the dwh"

mapfile -t dwh_ears < <(
    for dir in "${projects_listed[@]}" "tmp"; do
        if [[ $dir == "tmp" ]]; then
          find "/tmp" -type f -name "dwh-j2ee-*.ear" 2>/dev/null
        else
          find "$ROOT_DIR/$dir" -type f -name "dwh-j2ee-*.ear" 2>/dev/null
        fi
    done
)

readonly ear_path=$(
  stat -c '%Y %n' "${dwh_ears[@]}" \
    | sort -nr \
    | head -n1 \
    | cut -d' ' -f2-
)
ear_name="$(basename $ear_path)"
log_debug "Newest EAR file: $ear_path"

# push new ear to target
debian_deploy() {
  remote_cmd=""
  remote_cmd+="sudo service wildfly stop;"
  remote_cmd+="sudo rm /opt/wildfly/standalone/deployments/dwh*;"
  remote_cmd+="mv /tmp/$ear_name /opt/wildfly/standalone/deployments;"
  remote_cmd+="sudo service wildfly restart;"

  host="$server_user@$server_ip"
  ctl="$HOME/.ssh/cm-%r@%h:%p"


  ssh -o ControlMaster=auto -o ControlPersist=5m -o ControlPath="$ctl" -Nf "$host"
  scp -o ControlPath="$ctl" "$ear_path" "$host:/tmp/$ear_name"
  ssh -o ControlPath="$ctl" "$host" "$remote_cmd"
  ssh -O exit -o ControlPath="$ctl" "$host"
}


docker_deploy() {
  log_debug "Starting Docker Deployment"
  remote_cmd=""
  # Undeploy current .ear
  remote_cmd+="sudo docker exec $wildfly_container bash -lc '
set -e
name=\"$ear_name\"

# If the deployment already exists, undeploy it
if ./bin/jboss-cli.sh --connect --output-json --command=\"/deployment=${ear_name}:read-resource\" >/dev/null 2>&1; then
  echo \"Undeploying existing ${ear_name}\"
  ./bin/jboss-cli.sh --connect --command=\"undeploy ${ear_name}\"
fi

# Clean up filesystem deployment artifacts
rm -f /opt/wildfly/standalone/deployments/dwh* || true
' ; "

  # Currently Wildfly seems to automatically deploy the ear when copied to this directory.
  remote_cmd+="sudo docker cp /tmp/$ear_name $wildfly_container:/opt/wildfly/standalone/deployments/ ; "
  # todo: add a command to remote_cmd that manually deploys the ear if it is not deployed automatically.

  host="$server_user@$server_ip"
  ctl="$HOME/.ssh/cm-%r@%h:%p"

  # start a new ssh session
  ssh -o ControlMaster=auto -o ControlPersist=5m -o ControlPath="$ctl" -Nf "$host"
  # copy ear to target host
  scp -o ControlPath="$ctl" "$ear_path" "$host:/tmp/$ear_name"
  ssh -o ControlPath="$ctl" "$host" "$remote_cmd"
  ssh -O exit -o ControlPath="$ctl" "$host"
}

if [[ "$instance" == "docker" ]]; then
  docker_deploy
elif [[ "$instance" == "debian" ]]; then
  debian_deploy
fi


