#!/bin/bash
#--------------------------------------
# Script Name:  build_dwh_modules.sh.sh
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

# SET you Java paths here
SDK8="$HOME/.jdks/corretto-1.8.0_432"
SDK11="$HOME/.jdks/corretto-11.0.25/"

BASE_GROUP_ID="org.aktin"

#----------------------------------
# START - General helper functions
#----------------------------------
log_debug() { echo "[DEBUG] $*" >&2; }
log_info()  { echo "[INFO]  $*" >&2; }
log_warn()  { echo "[WARN]  $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }

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

  for ((i=0; i<"${#projects[@]}"; i+="$p_array_width")); do
    project_name="${projects[i]}"
    if [[ "$project_name" == "$project" ]]; then
      printf '%s\n' "$project"
      return 0
    fi
  done

  return 1
}

add_alt_conf_commit(){
  # duplicate target projects config and alter it's commit id
  local project="$1"
  local commit="$2"

  [[ -n "$commit" ]] || die "[ERROR] Commit ID was empty, but is required. $commit"

  for ((i=0; i<${#projects[@]}; i+=$p_array_width)); do
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

generate_project_whitelist() {
  p_whitelist_dirs=()
  for ((i=0; i<${#projects[@]}; i+=$p_array_width)); do
      [[ -n "${projects[i]}" ]] && p_whitelist_dirs+=("${projects[i]}")
  done
}

create_tmp_worktree() {
  local target_commit="$1"
  [[ -d "$tmp_dir" ]] && die "A temporary workingtree already exists."
  tmp_dir="$(mktemp -d -t build-wt-XXXXXXXX)"
  git worktree add --detach "$tmp_dir" "$target_commit"
}

delete_tmp_worktree() {
  # delete temporary worktree and navigate to main working directory
  git worktree remove --force "$tmp_dir"
  cd "$(dirname "$SCRIPT_DIR")"
  rm -rf "$tmp_dir"
}

backup_worktree_with_stash() {
  if [ -z "$(git status --porcelain)" ]; then
    log_info "Worktree clean, no stash created."
    return 0
  fi

  log_info "Stashing worktree..."
  git stash push -u -m "automated worktree backup" >/dev/null \
    || die "Error while stashing worktree."
}

git_stash_exists() {
  git stash list | grep -q .
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




git_resolve_module_to_commit() {
  # This function returns the last commit containing the artifact-version pair
  local artifact="$1"
  local version="$2"

  local target_path=""
  local -a poms

  # get all poms from all projects
  mapfile -t poms < <(
      for dir in "${p_whitelist_dirs[@]}"; do
          find "$ROOT_DIR/$dir" -type f -name "pom.xml" \
              -not -path '*/target/*' 2>/dev/null
      done
  )

  log_debug "Whitelist: ${p_whitelist_dirs[*]}"
  log_debug "Found ${#poms[@]} poms"

  # store reactor pom of target artifact, if one is found
  for pom in "${poms[@]}"; do
    local found="$(xml_reactor_artifactId "$pom")"
#    log_debug "$pom"
    log_debug "reactor: $found"
    if [[ "$found" == "$artifact" ]]; then
      target_path="$pom"
      break
    fi
  done

  if [[ -z "$target_path" ]]; then
    log_error "No artifact '$artifact' found inside project pom-files"
    return 1
  fi
  log_info "Found artifact '$artifact' inside '$target_path'"

  local target_dir relative_path
  target_dir="$(dirname "$target_path")"
  relative_path="$(git -C "$target_dir" rev-parse --show-prefix)pom.xml"
  echo "$target_dir $relative_path"

  # Search commit containing the requested artifact version
  local commits commit_found=""
  local commits=$(git -C "$target_dir" for-each-ref --sort=-creatordate --format='%(objectname) %(refname:short)' refs/tags)

  local cache_found_versions="" content ver commit tag
  while read -r commit tag; do
    content="$(git -C "$target_dir" show "$commit:$relative_path" 2>/dev/null)"
    ver="$(xml_reactor_version "$content")"
    cache_found_versions="$cache_found_versions $ver"

    if [[ "$ver" == "$version" ]]; then
      log_debug "Module version found in commit '$commit' (tag '$tag')."
      commit_found="$commit"
      break
    fi
  done <<< "$commits"

  if [[ -z "$commit_found" ]]; then
    log_error "Did not find '$module:$version' in$cache_found_versions"
  fi

  printf '%s:%s\n' "$target_path" "$commit_found"
}

mvn_clean_install_module() {
  local sdk_dir="$1"
  local out="" rc=0

  log_info "Building module using $sdk_dir"

  export JAVA_HOME="$sdk_dir"
  export PATH="$JAVA_HOME/bin:$PATH"
  log_debug "JAVA_HOME set to: $JAVA_HOME"

  cd "$tmp_dir" # move to temporary worktree
  set +e
  out="$(mvn -o -B -Dstyle.color=always clean install -DskipTests 2>&1)"
  rc=$?
  set -e

  if [[ $rc -eq 0 ]]; then
    return 0
  fi

  log_error "Build failed"

  local failed required_str
  failed="$(grep -oP 'project \K[^ ]+' <<<"$out" | tail -n 1 || true)"
  required_str="$(grep -oPm1 'artifact \K[^ ]+' <<<"$out" || true)"

  log_debug "Module that failed: '$failed', requires: '$required_str'"

  local -a required=()
  IFS=':' read -r -a required <<<"$required_str"

  # if dependency is not a org.aktin dependency ($failed is empty), permit online mode and loading from maven store
  # todo: add a whitelist/blacklist for these hard-coded modules
  # todo: add functionality to "git_resolve_module_to_commit", that checks past pom versions (these modules were removed in newer releases but still depended on them)
  if { [[ -z "$failed" ]] && [[ "${required[1]}" != "$BASE_GROUP_ID"* ]]; } \
     || [[ "${required[1]}" == *"query-i2b2-sql"* ]] \
     || [[ "${required[1]}" == *"query-aggregate-rscript"* ]] \
     || [[ "${required[1]}" == *"query-model"* ]]; then


    log_debug "Missing artifact not in config projects; trying dependency:get for '${required[0]}:${required[1]}:${required[-1]}'"

    set +e
    mvn dependency:get \
      -DgroupId="${required[0]}" \
      -DartifactId="${required[1]}" \
      -Dversion="${required[-1]}"
    set -e

    mvn_clean_install_module "$sdk_dir"
    return 0
  fi

  local req_full req required_pom_dir required_commit_id project_name
  req_full="$(git_resolve_module_to_commit "${required[1]}" "${required[-1]}")"
  req="${req_full##*$'\n'}"
  IFS=':' read -r required_pom_dir required_commit_id <<<"$req"

  project_name="$(basename "$(git -C "$(dirname "$required_pom_dir")" rev-parse --show-toplevel)")"
  log_error "Another module is required; add: \"$project_name\" \"<sdk>\" \"\" \"$required_commit_id\" to config and re-run"
  add_alt_conf_commit "$project_name" "$required_commit_id"

  delete_tmp_worktree
  exec env -i \
    PATH="$BASE_PATH" \
    HOME="${HOME:-/home/$USER}" \
    USER="${USER:-}" \
    "$SCRIPT_PATH" "${ORIG_ARGS[@]}"
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

  for ((i=0; i<${#projects[@]}; i+=p_array_width)); do
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
    else
      log_debug "Switching '$project_name' to target commit '$target_commit'"
    fi
    create_tmp_worktree "$target_commit"
    mvn_clean_install_module "$sdk_dir"
    delete_tmp_worktree
  done
}

do_all_projects_exist() {
  cd "$ROOT_DIR"
  log_info "Validating project directories..."

  for ((i=0; i<${#projects[@]}; i+=p_array_width)); do
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

check_for_conflicting_dwh_files() {
  ssh "$server_user@$dwh_ip" 'find /opt/wildfly/standalone/deployments -type f -name "dwh*.ear"'
}

#----------------------------------
# END - General helper functions
#----------------------------------


#----------------------------------
# START - Script Execution Workflow
#----------------------------------
ORIG_ARGS=("$@")

: "${BASE_PATH:=$PATH}"
export BASE_PATH

SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
readonly SCRIPT_PATH


# check and init script parameters
# set project to start building from if not all should be build
dwh_ip=""
build_from=""
CONFIG_FILE=""
rm_packages="false"
tmp_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--server-ip)
      dwh_ip="$2"
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
      rm_packages="$2"
      shift 2
      ;;

    *)
      echo "Unknown parameter: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$dwh_ip" ]]; then
  echo "Missing required parameter: --server-ip" >&2
  exit 1
fi

if [[ -z "$ROOT_DIR" ]]; then
  ROOT_DIR="$PWD"
fi

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
if [[ "$(basename $CONFIG_FILE)" != "script-config.conf" ]];then
  cp "$CONFIG_FILE" "$(dirname $CONFIG_FILE)/script-config.conf"
  CONFIG_FILE="$(dirname $CONFIG_FILE)/script-config.conf"
fi

readonly CONFIG_FILE

source "$CONFIG_FILE"
p_array_width=4
if [[ "${#projects[@]}" -gt 100 ]]; then
  read -p "Warning. Current config size: "$(expr ${#projects[@]} / "$p_array_width")" (Press any key to continue, ctrl+c to stop)"
fi

# if a config exists, remove it from orig_args
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
readonly -a ORIG_ARGS

# end script if build from artifact is not in project configs
[[ -n "$build_from" ]] && ! project_exists "$build_from" && die "Given Project '$build_from' not found in config."
if [[ -z "$build_from" ]]; then
  build_from=${projects[0]}   # root project default
fi

readonly dwh_ip
readonly build_from
readonly ROOT_DIR
readonly server_user=root
readonly SCRIPT_DIR="$(realpath "${BASH_SOURCE[0]}")"

generate_project_whitelist
echo "System Java version: $(which java)"


# VALIDATE - projects exist
do_all_projects_exist
all_do_exist=$?
if ! do_all_projects_exist; then
  die "Some projects could not be found."
else
  log_info "All projects found."
fi

# Safely build each maven project iteratively
build_all_projects
notify
echo "copy the .ear file to the dwh"
mapfile -t dwh_ears < <(
    for dir in "${p_whitelist_dirs[@]}"; do
        find "$ROOT_DIR/$dir" -type f -name "dwh-j2ee-*.ear" 2>/dev/null
    done
)

# todo: add multiple targets like docker instances
# push new ear to target
echo "${dwh_ears[@]}"
readonly ear_path="${dwh_ears[0]}"
echo "$ear_path"
ear_name="$(basename $ear_path)"
scp "$ear_path" "$server_user@$dwh_ip:/tmp/$ear_name"

remote_cmd=""

if [ -n "$(check_for_conflicting_dwh_files)" ]; then
  remote_cmd="sudo service wildfly stop; sudo rm /opt/wildfly/standalone/deployments/dwh*;"
fi
remote_cmd="$remote_cmd mv /tmp/$ear_name /opt/wildfly/standalone/deployments; sudo service wildfly restart;"

ssh $server_user@$dwh_ip "$remote_cmd"

