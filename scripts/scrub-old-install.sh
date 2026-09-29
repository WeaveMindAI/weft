#!/usr/bin/env bash
# Wipe what an older weft left on this machine, so ./setup.sh starts clean.
#
# Weft used to run on a local Kubernetes cluster (kind). The version that
# replaced it runs as one process beside Docker, and its database starts a
# new schema history: a database from before cannot be carried forward, and
# the new runtime refuses to boot on one. This script removes all of it:
#
#   - weft's runtime for every install, stopped and taken off the service
#     manager (or, when a detached start left it running, stopped by its
#     pid file);
#   - the kind cluster `weft-local` (and the `kind` docker network once no
#     cluster is left);
#   - every container and volume weft started: workers, infra units and
#     their data, the Postgres containers, the object store and its data,
#     the tunnel, the old local image registry;
#   - every image weft built or ran (the runtime, an older weft's system
#     images, workers, the builder base, infra images); with --kind, the
#     kindest/node image too, which other kind clusters on the machine
#     share;
#   - ~/.local/share/weft: THE DATABASE (your projects' run history,
#     versions and stored connections), the install's config and keys,
#     and every named install under installs/ (the question lists them).
#
# What it keeps: your project folders (the source is the record of your
# projects; `weft run` registers them again), the `--public-url` choice,
# the assistant you picked for `weft new`, cargo's target/ and the
# BuildKit cache. A named tunnel needs nothing kept: its token is read
# from WEFT_PUBLIC_TUNNEL_TOKEN at every start.
#
# Usage:
#   scripts/scrub-old-install.sh [--yes] [--kind]
#     --yes   no question before deleting
#     --kind  also remove the kindest/node image
# Then: ./setup.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

yes=0
kind_images=0
for arg in "$@"; do
  case "${arg}" in
    --yes) yes=1 ;;
    --kind) kind_images=1 ;;
    *) echo "usage: scripts/scrub-old-install.sh [--yes] [--kind]" >&2; exit 1 ;;
  esac
done

ok() { printf '%s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
# Something already absent is not worth a line here.
hint() { :; }

# shellcheck source=scripts/lib/weft-cleanup.sh
. "${here}/lib/weft-cleanup.sh"

# The files under the data directory that are the person's own choices,
# not the install's state: whether the tunnel is on, and the assistant
# `weft new` sets up.
# SYNC: <-> crates/weft-cli/src/commands/daemon.rs (public_url_marker),
#       crates/weft-cli/src/commands/new.rs (default-assistants)
keep=(public-url-enabled default-assistants)

if ! command -v docker >/dev/null 2>&1 || ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  echo "docker is not reachable, so nothing weft left in it can be removed; start docker and run this again" >&2
  exit 1
fi

if [[ $yes -eq 0 ]]; then
  ok "This deletes weft's database (every project's run history, versions and stored"
  ok "connections), its containers, volumes and images, and an older weft's Kubernetes"
  ok "cluster. Your project folders are not touched."
  named="$(weft_named_installs)"
  [[ -n "${named}" ]] && ok "These named installs go too, with their databases: ${named}."
  read -r -p "Wipe it? [y/N] " answer
  [[ "${answer}" == "y" || "${answer}" == "Y" ]] || { ok "nothing deleted"; exit 0; }
fi

# The runtime first, so nothing writes while its database goes.
weft_stop_runtimes
weft_delete_old_cluster
weft_remove_containers_and_volumes
weft_remove_images || warn "docker could not list its images, so weft's are still there; run this again"
if [[ $kind_images -eq 1 ]]; then
  weft_remove_kind_images || warn "docker could not list the kindest/node images; run this again"
fi

# The install's files, the database's among them, keeping the person's
# own choices.
if [[ -d "${weft_state_dir}" ]]; then
  held="$(mktemp -d)"
  for f in "${keep[@]}"; do
    [[ -e "${weft_state_dir}/${f}" ]] && cp -a "${weft_state_dir}/${f}" "${held}/"
  done
  if weft_remove_state_dir; then
    ok "removed ${weft_state_dir} (the database, config and keys)"
    mkdir -p "${weft_state_dir}"
    for f in "${keep[@]}"; do
      [[ -e "${held}/${f}" ]] && cp -a "${held}/${f}" "${weft_state_dir}/"
    done
    rm -rf "${held}"
  else
    warn "could not remove ${weft_state_dir}; remove it by hand: sudo rm -rf ${weft_state_dir} (your kept choices are in ${held})"
  fi
fi

ok ""
ok "Done. Install the current weft with: ./setup.sh"
