# What removing weft from this machine takes, shared by setup.sh
# (--uninstall, --purge) and scripts/scrub-old-install.sh, so the two
# can never disagree about what weft left behind.
#
# The caller defines the reporting it wants before sourcing:
#   ok <msg>    something was removed
#   warn <msg>  something could not be, with how to do it by hand
#   hint <msg>  nothing to do (absent already)
# Safe under `set -euo pipefail`: a failed step reports and returns.
# Every function assumes the docker daemon answers when it touches
# docker; the callers check that once, up front.

# SYNC: the install label <-> crates/weft-core/src/infra/instance.rs (INSTALL_LABEL)
weft_install_label=weft-install

# The data directory every install lives under: the default install at
# its root, a named one under installs/<name>.
# SYNC: <-> crates/weft-cli/src/commands/daemon.rs (data_dir, Install::from_env)
weft_state_dir="${HOME}/.local/share/weft"

# Repositories of the images weft builds or runs, matched with any
# registry prefix stripped. The system images (weft-dispatcher and the
# rest before weft-runtime) are an older weft's; nothing builds them now.
# SYNC: weft-infra-<name> repo <-> crates/weft-compiler/src/image_set.rs (infra_image_repo)
weft_system_repos_re='^(weft-runtime|weft-dispatcher|weft-listener|weft-broker|weft-infra-supervisor)$'
weft_built_repos_re='^(weft-worker|weft-builder-base|weft-node-tests)$|^weft-infra-'

# IDs of host docker images whose repo, with any registry prefix
# stripped, matches the anchored regex. Content-addressed tags mean the
# repo is the only stable part of a ref. Returns non-zero when `docker
# images` itself fails (pipefail carries it through): an unanswerable
# daemon must never read as "no images".
weft_image_ids() {
  docker images --format '{{.Repository}} {{.ID}}' 2>/dev/null \
    | awk -v re="$1" '{repo=$1; sub(/^.*\//,"",repo); if (repo ~ re) print $2}' \
    | sort -u
}

# Remove one docker object (image | container | volume | network) if it
# is there. Absence reads as absence, a stuck removal as a warning.
remove_docker_object() { # kind ref [note]
  local kind="$1" ref="$2" note="${3:-}"
  if ! docker "${kind}" inspect "${ref}" >/dev/null 2>&1; then
    hint "${ref}: not present${note}"
    return 0
  fi
  # -v on a container: an image that declares a VOLUME (postgres, the
  # object store) leaves an anonymous volume behind on a plain rm.
  local -a rm_flags=()
  case "${kind}" in
    container) rm_flags=(-f -v) ;;
    network) ;;
    *) rm_flags=(-f) ;;
  esac
  if docker "${kind}" rm ${rm_flags[@]+"${rm_flags[@]}"} "${ref}" >/dev/null 2>&1; then
    ok "removed ${ref}${note}"
  else
    warn "could not remove ${ref} (still in use?)"
  fi
}

# Remove a set of image ids and say what happened.
remove_docker_images_by_id() { # label ids [note] [warn_reason]
  local label="$1" ids="$2" note="${3:-}" reason="${4:-still referenced by a container?}"
  if [[ -z "${ids}" ]]; then
    hint "no ${label} to remove"
  elif echo "${ids}" | xargs docker rmi -f >/dev/null 2>&1; then
    ok "removed ${label}${note}"
  else
    warn "some ${label} could not be removed (${reason})"
  fi
}

# Stop every install's runtime and take it off the service manager, so
# nothing writes while its data goes and nothing comes back at the next
# login. Covers the default install (`weft-runtime`) and every named one
# (`weft-<name>-runtime`), and a runtime `weft daemon start` detached
# without a service manager, which only its pid file names. A pid is
# signalled only while its command line is that install's runtime (the
# pid may have been reused by anything since).
# SYNC: service names, pid file, command line <-> crates/weft-cli/src/commands/daemon.rs
#       (Install::service_name, systemd_unit_path, launchd_plist_path,
#       pid_path, runtime_command, is_runtime_command_line)
weft_stop_runtimes() {
  local unit name plist pidfile dir pid
  if command -v systemctl >/dev/null 2>&1; then
    for unit in "${HOME}"/.config/systemd/user/weft*-runtime.service; do
      [[ -e "${unit}" ]] || continue
      name="$(basename "${unit}")"
      systemctl --user disable --now "${name}" >/dev/null 2>&1 || true
      rm -f "${unit}"
      ok "stopped and removed ${name}"
    done
    systemctl --user daemon-reload >/dev/null 2>&1 || true
  fi
  if command -v launchctl >/dev/null 2>&1; then
    for plist in "${HOME}"/Library/LaunchAgents/ai.weavemind.weft*-runtime.plist; do
      [[ -e "${plist}" ]] || continue
      launchctl bootout "gui/$(id -u)" "${plist}" >/dev/null 2>&1 || true
      rm -f "${plist}"
      ok "stopped and removed $(basename "${plist}" .plist)"
    done
  fi
  for pidfile in "${weft_state_dir}/runtime.pid" "${weft_state_dir}"/installs/*/runtime.pid; do
    [[ -e "${pidfile}" ]] || continue
    dir="$(dirname "${pidfile}")"
    pid="$(tr -d '[:space:]' < "${pidfile}")" || pid=""
    if [[ -n "${pid}" ]] && weft_pid_is_runtime "${pid}" "${dir}"; then
      kill "${pid}" 2>/dev/null || true
      local waited=0
      while weft_pid_is_runtime "${pid}" "${dir}" && [[ ${waited} -lt 60 ]]; do
        sleep 1
        waited=$((waited + 1))
      done
      if weft_pid_is_runtime "${pid}" "${dir}"; then
        warn "weft's runtime (pid ${pid}) did not stop within a minute of being asked to; end it with: kill -9 ${pid}"
        continue
      fi
      ok "stopped the runtime of ${dir} (pid ${pid})"
    fi
    rm -f "${pidfile}"
  done
}

# Whether pid runs the runtime of the install whose directory is dir.
weft_pid_is_runtime() { # pid dir
  local line
  line="$(ps -p "$1" -o command= 2>/dev/null)" || return 1
  [[ "${line}" == *" serve --config $2/config.json "* ]]
}

# Whether an older weft (the one that ran on a local Kubernetes cluster)
# is still on this machine, found by the cluster's node container, so a
# machine whose `kind` binary is gone is still caught.
# SYNC: <-> crates/weft-cli/src/commands/daemon.rs (refuse_an_older_install)
weft_old_install_present() {
  [[ -n "$(docker ps -aq --filter name=^weft-local-control-plane$ 2>/dev/null)" ]]
}

# The named installs on this machine (test cells, say), comma separated,
# empty when there are none. A wipe takes them too, so its question
# names them.
# SYNC: installs/<name> <-> crates/weft-core/src/infra/instance.rs (Instance::dir)
weft_named_installs() {
  local d names=()
  for d in "${weft_state_dir}"/installs/*/; do
    [[ -d "${d}" ]] && names+=("$(basename "${d}")")
  done
  local IFS=,
  printf '%s' "${names[*]}" | sed 's/,/, /g'
}

# Delete the Kubernetes cluster an older weft ran, and the shared `kind`
# docker network once no cluster is left (`kind delete` leaves it).
weft_delete_old_cluster() {
  if ! command -v kind >/dev/null 2>&1; then
    # Without kind, the cluster is its one node container.
    if [[ -n "$(docker ps -aq --filter name=^weft-local-control-plane$ 2>/dev/null)" ]]; then
      if docker rm -f -v weft-local-control-plane >/dev/null 2>&1; then
        ok "removed an older weft's Kubernetes node container 'weft-local-control-plane'"
      else
        warn "could not remove the container weft-local-control-plane; remove it by hand: docker rm -f -v weft-local-control-plane"
      fi
    fi
    return 0
  fi
  if kind get clusters 2>/dev/null | grep -qx weft-local; then
    if kind delete cluster --name weft-local >/dev/null 2>&1; then
      ok "deleted an older weft's kind cluster 'weft-local'"
    else
      warn "could not delete the kind cluster; delete it by hand: kind delete cluster --name weft-local"
    fi
  else
    hint "kind cluster 'weft-local': not present"
  fi
  if [[ -z "$(kind get clusters 2>/dev/null)" ]] && docker network inspect kind >/dev/null 2>&1; then
    if docker network rm kind >/dev/null 2>&1; then
      ok "removed the kind docker network"
    else
      warn "could not remove the kind docker network (another tool's container is still on it)"
    fi
  fi
}

# Every container and volume weft started, for every install: what
# carries the install label (workers, infra units, each install's
# Postgres, the tunnel), and the fixed names an older weft used before
# it labelled them, with the object store's data volume.
# SYNC: weft-object-store <-> crates/weft-cli/src/commands/daemon.rs (OBJECT_STORE_CONTAINER)
weft_remove_containers_and_volumes() {
  local ids name
  ids="$(docker ps -aq --filter "label=${weft_install_label}" 2>/dev/null)" || true
  if [[ -n "${ids}" ]]; then
    # A container the stopped runtime was already removing answers
    # "removal already in progress"; only one still there afterwards is
    # a failure.
    # shellcheck disable=SC2086
    docker rm -f -v ${ids} >/dev/null 2>&1 || true
    local waited=0
    while [[ -n "$(docker ps -aq --filter "label=${weft_install_label}" 2>/dev/null)" && ${waited} -lt 10 ]]; do
      sleep 1
      waited=$((waited + 1))
    done
    if [[ -z "$(docker ps -aq --filter "label=${weft_install_label}" 2>/dev/null)" ]]; then
      ok "removed weft's containers (workers, infra units, databases, the tunnel)"
    else
      warn "some of weft's containers could not be removed; list them with: docker ps -a --filter label=${weft_install_label}"
    fi
  fi
  for name in weft-postgres weft-tunnel weft-object-store weft-registry; do
    docker container inspect "${name}" >/dev/null 2>&1 && remove_docker_object container "${name}"
  done
  ids="$(docker volume ls -q --filter "label=${weft_install_label}" 2>/dev/null)" || true
  if [[ -n "${ids}" ]]; then
    # shellcheck disable=SC2086
    if docker volume rm -f ${ids} >/dev/null 2>&1; then
      ok "removed weft's volumes"
    else
      warn "some of weft's volumes could not be removed; list them with: docker volume ls --filter label=${weft_install_label}"
    fi
  fi
  remove_docker_object volume weft-object-store-data
  docker network inspect weft >/dev/null 2>&1 && remove_docker_object network weft
  return 0
}

# Every image weft built or ran. Returns non-zero when docker cannot list
# its images, which the caller must not read as "nothing to remove".
weft_remove_images() {
  local ids
  ids="$(weft_image_ids "${weft_system_repos_re}")" || return 1
  remove_docker_images_by_id "weft-runtime images (and an older weft's system images)" "${ids}"
  ids="$(weft_image_ids "${weft_built_repos_re}")" || return 1
  remove_docker_images_by_id "weft-worker / weft-builder-base / weft-node-tests / weft-infra-* images" "${ids}"
}

# The kind node image. Shared with any other kind user on the machine,
# so only on request. Returns non-zero when docker cannot list it.
weft_remove_kind_images() { # [note]
  local ids
  ids="$(docker images kindest/node -q 2>/dev/null | sort -u)" || return 1
  remove_docker_images_by_id "kindest/node images" "${ids}" "${1:-}" "a kind cluster still running?"
}

# Remove the data directory, the database's files among them. Docker may
# have written root-owned entries there (Postgres runs as another user;
# a missing bind-mount source becomes a root directory), so docker
# itself clears what a plain rm cannot. Returns non-zero when anything
# survived; the caller reports.
weft_remove_state_dir() {
  [[ -d "${weft_state_dir}" ]] || return 0
  if ! rm -rf "${weft_state_dir}" 2>/dev/null; then
    docker run --rm -v "${weft_state_dir}:/wipe" alpine:3 \
      sh -c 'rm -rf /wipe/* /wipe/.[!.]*' >/dev/null 2>&1 || true
    rm -rf "${weft_state_dir}" 2>/dev/null || true
  fi
  [[ ! -d "${weft_state_dir}" ]]
}
