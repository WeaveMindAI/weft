#!/usr/bin/env bash
# setup.sh: build and install everything weft-related on the local
# machine.
#
# By default, runs the install pipeline for the everyday loop:
#   - CLI                  (cargo build, symlink into ~/.local/bin)
#   - Daemon               (rebuild dispatcher/listener images and
#                          restart the kind pod if it's running;
#                          if it's not, start it fresh)
#   - VS Code extension    (compile, package .vsix, install into VS Code)
#
# The browser extension is OPT-IN via `--browser` since rebuilding
# it signs Firefox and builds every target, which is heavier than
# most rebuild loops need.
#
# Prebuilt artifacts: on a CLEAN checkout of a commit CI has already
# built (the rolling `mvp-latest` release), the CLI binary and the
# .vsix are downloaded instead of compiled and the daemon images are
# pulled from the registry, so a fresh install needs no Rust or Node
# toolchain and takes minutes, not tens of minutes. Any local change
# at all, tracked or not, takes the build path for everything (the
# published artifacts are keyed to one exact commit).
#
# Component flags pick a subset (multiple combine):
#   --cli         build CLI only
#   --daemon      refresh daemon only
#   --vscode      build/install VS Code extension only
#   --browser     build browser extension only (signs, zips)
#   (e.g. --cli --daemon does both, skips the VS Code extension)
#
# Extension release gesture:
#   --bump        bump the version of the extensions this run builds
#                 (the VS Code extension on the default install and
#                 --vscode; the browser extension with --browser). The
#                 version is what makes CI publish the pushed build to
#                 the stores (VS Code Marketplace, Open VSX, Chrome,
#                 Firefox, Edge), so this is the deliberate "release
#                 the extension" switch; a plain rebuild never bumps.
#
# Default-on knobs you opt OUT of:
#   --no-sign     skip the Firefox AMO signing step
#   --no-daemon   skip the daemon refresh on a default install (it does
#                 not combine with the component flags, which already
#                 pick what runs)
#
# Escape hatch:
#   --from-source compile the CLI and the VS Code extension locally
#                 even when a published build matches this checkout
#                 (for when a published artifact turns out broken).
#                 The daemon images still pull from the registry when
#                 their content matches; add --rebuild to rebuild them
#                 locally too.
#
# Diagnostics:
#   An install writes a START line and a DONE/FAIL line (with the
#   section it died in) to ~/.local/share/weft/setup-runs.log. A run
#   refused on its arguments records only the FAIL; a --purge deletes
#   the journal along with the rest of the directory, so the next
#   install starts a fresh one.
#
# Public trigger surface (event triggers delivered BY providers):
#   --public-url    expose /events/... and /signal/... to the internet
#                   through an outbound tunnel + a filtering proxy, so
#                   provider event pushes reach this local install.
#                   Nothing else is exposed. Persisted across runs.
#   --no-public-url close it again.
#
# Browser-target flags (default: every browser):
#   --chrome      Chrome / Brave / Vivaldi / Arc / Edge / Opera unpacked
#   --firefox
#   --edge
#   --opera
#   --safari
#   (multiple combine)
#
# CLI knobs:
#   --rebuild     force the daemon images to be rebuilt even when nothing
#                 they are built from has changed. For when an image is
#                 corrupt or hand-modified; a plain run already rebuilds
#                 whatever actually moved.
#   --rebuild-cluster  allow the daemon to delete and recreate the kind
#                 cluster when its shape changed. Every project's own
#                 database lives inside the node and is destroyed with it,
#                 so this is never done without the flag.
#   --debug       build CLI with the debug profile
#   --prefix PATH install CLI binary into PATH/bin (default ~/.local)
#
# Changing a table:
#   --migration NAME  Write the migration for whatever you changed in a
#                     CREATE TABLE, then install as usual, which applies it.
#                     Compares the schema your database has against the one
#                     your code now declares and writes the difference. By
#                     itself it writes a DRAFT: gitignored, yours alone, and
#                     applied to your database like any other migration, so
#                     nothing you have stored is lost while you are still
#                     deciding the shape. Ask as often as you change the
#                     table.
#   --release         With --migration: collapse every draft into one
#                     released migration, the one that goes in the PR, and
#                     tell your database it is already in it. Nothing is
#                     re-run and nothing is lost.
#
# Removal:
#   --uninstall   Remove user-facing pieces but preserve work. Stops
#                 the daemon (graceful: leases released on the Pod's
#                 SIGTERM), uninstalls the VS Code extension, drops
#                 the CLI symlink. PRESERVES: kind cluster,
#                 postgres data, the object-store container + its
#                 data volume, docker images, BuildKit cache,
#                 cargo target/, manifest stamps, browser
#                 extensions. Reinstall via ./setup.sh and your
#                 projects + history come back instantly.
#   --purge       TRUE clean slate. Deletes the kind cluster (and
#                 the `kind` docker network once no clusters
#                 remain), every weft-built docker image
#                 (dispatcher, listener, every weft-worker),
#                 every weft infra image, the object-store
#                 container + data volume + seaweedfs image, the
#                 BuildKit cache, the workspace target/ cargo
#                 cache, ~/.local/share/weft (THE DATABASE'S FILES
#                 under postgres-data/, manifest stamps, prebuilt
#                 binaries, port-forward state), and the untracked
#                 extension build artifacts. The next install pays a
#                 full cold-rebuild cost. Can combine with --uninstall.
#
#                 SHARED base images (commonly reused by other
#                 docker projects on the host) are kept by default.
#                 Add the matching flag to remove them too:
#                   --postgres   remove postgres:18-alpine (and the
#                                postgres:18 image --migration pulls)
#                   --kind       remove kindest/node images
#                   --debian     remove debian:bookworm-slim
#
# Examples:
#   ./setup.sh                          # full install
#   ./setup.sh --browser                # rebuild browser ext only
#   ./setup.sh --browser --no-sign      # browser ext, skip AMO signing
#   ./setup.sh --browser --chrome       # browser ext, Chrome only
#   ./setup.sh --cli --daemon           # CLI + daemon, no extensions
#   ./setup.sh --uninstall              # remove installed pieces
#   ./setup.sh --uninstall --purge      # remove + nuke local state
#   ./setup.sh --uninstall --purge --postgres --kind --debian
#                                       # purge including shared base images

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The invocation's args, captured before the parse loop consumes them,
# for the run journal.
orig_args="$*"

# The VS Code extension id (publisher.name from extension-vscode/package.json).
# Single source so install, version-probe, and uninstall can never drift (a
# stale `weavemindai.` typo made --uninstall silently match nothing).
ext_id="weavemind.weft-vscode"

# ---- visual library --------------------------------------------------
#
# Colors + symbols. Disabled when stdout isn't a TTY (CI pipelines,
# `setup.sh > log.txt`) or when NO_COLOR is set, so the output stays
# clean and grep-able.

if [[ -t 1 && "${NO_COLOR:-}" == "" ]]; then
  C_RESET=$'\033[0m'
  C_DIM=$'\033[2m'
  C_BOLD=$'\033[1m'
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_CYAN=$'\033[36m'
  HAS_TTY=1
else
  C_RESET=""
  C_DIM=""
  C_BOLD=""
  C_RED=""
  C_GREEN=""
  C_YELLOW=""
  C_BLUE=""
  C_CYAN=""
  HAS_TTY=0
fi

# Symbols: braille spinner frames + status glyphs. Plain ASCII
# fallbacks aren't worth the branching; every modern terminal
# (kitty, alacritty, iTerm2, Windows Terminal, vscode terminal,
# gnome-terminal) renders these correctly.
SP_FRAMES=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
SYM_OK="✓"
SYM_FAIL="✗"
SYM_STEP="▸"
SYM_INFO="ℹ"
SYM_WARN="⚠"
SYM_ARROW="→"

section() {
  # Recorded so a failed run's journal line can name where it died.
  current_section="$*"
  printf '\n%s%s%s %s%s%s\n' "${C_BOLD}" "${C_BLUE}" "▶" "${C_BOLD}" "$*" "${C_RESET}"
}
ok()   { printf '  %s%s%s %s\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "$*"; }
fail() { printf '  %s%s%s %s\n' "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "$*" >&2; }
step() { printf '  %s%s%s %s\n' "${C_CYAN}" "${SYM_STEP}" "${C_RESET}" "$*"; }
hint() { printf '  %s%s %s%s\n' "${C_DIM}" "${SYM_INFO}" "$*" "${C_RESET}"; }
warn() { printf '  %s%s%s %s\n' "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "$*"; }

# spin "label" cmd args... -- run cmd while showing a spinner. On
# success replace the spinner with `✓ label`. On failure, replace
# with `✗ label` and forward the captured output to stderr so the
# user sees what went wrong. Plain echo fallback when no TTY.
spin() {
  local label="$1"; shift
  # One per-run mktemp log on BOTH branches: a fixed /tmp name would
  # let two concurrent runs interleave and delete each other's output
  # (and hand any local user a symlink-overwrite seat).
  local logfile
  logfile="$(mktemp -t weft-setup.XXXXXX.log)"
  if [[ $HAS_TTY -eq 0 ]]; then
    printf '  %s %s\n' "${SYM_STEP}" "${label}"
    if "$@" >"${logfile}" 2>&1; then
      ok "${label}"
      rm -f "${logfile}"
      return 0
    else
      local rc=$?
      fail "${label}"
      cat "${logfile}" >&2 || true
      rm -f "${logfile}"
      return $rc
    fi
  fi
  "$@" >"${logfile}" 2>&1 &
  local pid=$!
  local i=0
  printf '  '
  while kill -0 "${pid}" 2>/dev/null; do
    local frame="${SP_FRAMES[$((i % ${#SP_FRAMES[@]}))]}"
    printf '\r  %s%s%s %s' "${C_CYAN}" "${frame}" "${C_RESET}" "${label}"
    sleep 0.08
    i=$((i + 1))
  done
  if wait "${pid}"; then
    printf '\r  %s%s%s %s\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${label}"
    rm -f "${logfile}"
    return 0
  else
    local rc=$?
    printf '\r  %s%s%s %s\n' "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "${label}"
    cat "${logfile}" >&2 || true
    rm -f "${logfile}"
    return $rc
  fi
}

# spin_passthrough "label" cmd args... -- like spin but lets the
# command print its OWN output live. Used for cargo/docker where
# users want to watch progress. Just runs the command directly,
# bracketed by a step header and a final ✓ / ✗.
spin_passthrough() {
  local label="$1"; shift
  step "${label}"
  if "$@"; then
    ok "${label}"
    return 0
  else
    local rc=$?
    fail "${label}"
    return $rc
  fi
}

# Portable sha256 (GNU sha256sum on Linux, shasum on macOS), same
# output format either way. `sha256_bin` is the underlying COMMAND for
# the call sites that go through xargs, which cannot invoke a shell
# function; expand it unquoted there so the shasum form keeps its args.
if command -v sha256sum >/dev/null 2>&1; then
  sha256_bin="sha256sum"
else
  sha256_bin="shasum -a 256"
fi
sha256() {
  ${sha256_bin} "$@"
}

# IDs of host docker images whose repo, with any registry prefix
# stripped, matches the anchored regex. Content-addressed tags mean
# the repo is the only stable part of a ref, and the system images +
# builder base are registry-qualified (ghcr.io/...) while workers and
# node-test images stay bare, so every sweep matches through this one
# helper instead of hand-rolling the strip.
weft_image_ids() {
  # Returns non-zero when `docker images` itself fails (pipefail
  # carries it through the pipeline): an unanswerable daemon must
  # never read as "no images", so every caller decides what a failed
  # listing means instead of receiving a silent empty.
  docker images --format '{{.Repository}} {{.ID}}' 2>/dev/null \
    | awk -v re="$1" '{repo=$1; sub(/^.*\//,"",repo); if (repo ~ re) print $2}' \
    | sort -u
}
# The images an ENGINE change strands (workers + builder base + the
# node-test image bake the engine in).
build_plane_repos_re='^(weft-worker|weft-builder-base|weft-node-tests)$'

# Remove one docker object (image | container | volume) if it is
# there. THE one removal shape, so absence always reads as absence, a
# stuck removal as a warning, and neither ever prints as success.
# Callers gate on the daemon being reachable; this assumes it is.
remove_docker_object() { # kind ref [note]
  local kind="$1" ref="$2" note="${3:-}"
  if ! docker "${kind}" inspect "${ref}" >/dev/null 2>&1; then
    hint "${ref}: not present${note}"
    return 0
  fi
  if docker "${kind}" rm -f "${ref}" >/dev/null 2>&1; then
    ok "removed ${ref}${note}"
  else
    warn "could not remove ${ref} (still in use?)"
  fi
}

# Remove a directory tree and report: ok exactly when it is gone,
# warn when something survived. ONE shape for every best-effort tree
# removal, so partial removals never print as success.
remove_dir_reporting() { # path label [ok_note] [fail_note]
  local path="$1" label="$2" ok_note="${3:-}" fail_note="${4:-}"
  if [[ ! -e "${path}" ]]; then
    hint "no ${label} to remove"
    return 0
  fi
  rm -rf "${path}" 2>/dev/null || true
  if [[ -e "${path}" ]]; then
    warn "could not remove ${label} (permissions?)${fail_note}"
  else
    ok "removed ${label}${ok_note}"
  fi
}

# Its sibling for a SET of image ids (from `weft_image_ids` or a
# `docker images -q` listing): one shape for "remove these, say what
# happened", so every multi-image sweep reports the same way.
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

# Drop everything an engine change strands: the tagged build-plane
# images on host docker and inside the kind node, the BuildKit cache
# (the cargo layers of worker/infra/test builds all bake the engine, so
# a bounded prune would keep 20GB of dead weight), and the node-test
# cargo cache under target/tmp. Best effort throughout: a --cli install
# must finish even with the docker daemon down; the loud bounded prune
# earlier in the CLI section is the one that fails visibly. Best-effort
# still SPEAKS: a removal that could not happen warns instead of
# printing success over it.
sweep_stale_build_plane() {
  # One probe up front: with docker down, every docker step below
  # would either lie ("no X to remove" over an unanswerable daemon) or
  # repeat the same warn. Say it once and keep the non-docker cleanup.
  local docker_up=1
  docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || docker_up=0
  if [[ ${docker_up} -eq 0 ]]; then
    warn "docker unreachable; the stale build-plane images (host docker and the kind node's containerd) and the BuildKit cache stay until the next install with docker up"
    remove_dir_reporting "${here}/target/tmp" "${C_DIM}target/tmp${C_RESET}" \
      " ${C_DIM}(node-test sweep cache, engine-keyed)${C_RESET}" "; it goes on a later install"
    return 0
  fi
  local ids
  if ids="$(weft_image_ids "${build_plane_repos_re}")"; then
    remove_docker_images_by_id "stale build-plane images" "${ids}" \
      " ${C_DIM}(weft-worker / weft-builder-base / weft-node-tests, host docker)${C_RESET}" \
      "still referenced by a container? they go on a later install"
  else
    # The daemon answered the probe above but not this listing: never
    # turn that into "nothing to remove".
    warn "could not list the stale build-plane images; they go on a later install"
  fi
  if docker builder prune --force >/dev/null 2>&1; then
    ok "dropped the BuildKit cache (stale against the new engine)"
  else
    warn "could not prune the BuildKit cache; it still holds entries built against the old engine (prune it by hand: docker builder prune --force)"
  fi
  remove_dir_reporting "${here}/target/tmp" "${C_DIM}target/tmp${C_RESET}" \
    " ${C_DIM}(node-test sweep cache, engine-keyed)${C_RESET}" "; it goes on a later install"
  if command -v kind >/dev/null 2>&1; then
    local kind_node="" cluster node tags
    for cluster in $(kind get clusters 2>/dev/null); do
      node="${cluster}-control-plane"
      if docker inspect "${node}" >/dev/null 2>&1; then
        kind_node="${node}"
        break
      fi
    done
    if [[ -n "${kind_node}" ]]; then
      # `crictl images` lists repo and tag as separate columns; the
      # repo may carry a registry prefix, stripped the same way
      # `weft_image_ids` strips it.
      tags="$(
        docker exec "${kind_node}" crictl images 2>/dev/null \
          | awk -v re="${build_plane_repos_re}" \
              'NR>1 {repo=$1; sub(/^.*\//,"",repo); if (repo ~ re) print $1":"$2}' \
          | sort -u || true
      )"
      if [[ -n "${tags}" ]]; then
        # shellcheck disable=SC2086
        if docker exec "${kind_node}" crictl rmi ${tags} >/dev/null 2>&1; then
          ok "removed cached weft-worker + weft-builder-base + weft-node-tests images in kind containerd"
        else
          warn "some cached build-plane images in kind containerd could not be removed (a pod still runs one?); they go on a later install"
        fi
      fi
    fi
  fi
}

# The installed version of the weft extension, queried through a live
# IPC socket (so it reflects that VS Code window, not some global
# registry). Empty if not installed. Shared by the install and the
# uninstall side, so neither can claim work the other can disprove.
installed_ext_version() {
  local sock="$1"
  VSCODE_IPC_HOOK_CLI="${sock}" code --list-extensions --show-versions 2>/dev/null \
    | sed -n "s/^${ext_id//./\\.}@//p" | head -n1
}

# Print the path of a LIVE VS Code IPC socket under /run/user/$UID (closed
# terminals leave dead sockets behind, so probe for one that answers). Used by
# both the install side and the uninstall/reinstall side; defined once here so
# the two can't drift. Returns non-zero if no live socket is found.
pick_live_vscode_socket() {
  local run_dir="/run/user/${UID}"
  [[ -d "${run_dir}" ]] || return 1
  local sock
  for sock in $(ls -1t "${run_dir}"/vscode-ipc-*.sock 2>/dev/null); do
    if command -v socat >/dev/null 2>&1; then
      if timeout 0.3 socat -u /dev/null UNIX-CONNECT:"${sock}" >/dev/null 2>&1; then
        printf '%s' "${sock}"; return 0
      fi
    elif command -v nc >/dev/null 2>&1; then
      if timeout 0.3 nc -U -z "${sock}" >/dev/null 2>&1; then
        printf '%s' "${sock}"; return 0
      fi
    else
      printf '%s' "${sock}"; return 0
    fi
  done
  return 1
}

# ---- defaults --------------------------------------------------------

profile="release"
prefix="${HOME}/.local"
do_uninstall=0
do_purge=0
write_migration=""
do_release=0
rebuild_flag=""
rebuild_cluster_flag=""
purge_debian=0
purge_kind=0
purge_postgres=0

# Components: 0 = excluded, 1 = included. The default install set
# is CLI + daemon + VS Code extension. The browser extension is
# OPT-IN via --browser since it's heavier (extension store sign,
# full per-target build) and most rebuild loops don't need it.
#
# When the user passes any --<component> we flip every component
# to 0 first so the listed flags act as opt-ins.
build_cli=1
refresh_daemon=1
public_url_flag=""
build_vscode=1
build_browser=0
component_flag_seen=0
no_daemon_seen=0

# Browser-target subset: same logic as components.
target_chrome=1
target_firefox=1
target_edge=1
target_opera=1
target_safari=1
target_flag_seen=0

# The extension versions in package.json are what gate the STORE
# publishes in CI (a pushed commit whose version moved gets submitted
# to the VS Code Marketplace / Open VSX / Chrome / Firefox / Edge), so
# bumping is the deliberate release gesture (--bump), never a side
# effect of a rebuild.
do_bump=0
do_sign=1
# --from-source: refuse the prebuilt CLI/.vsix fast path and compile
# locally (the escape hatch when a published binary is broken).
from_source=0

# Called the first time a --<component> flag is seen. Zeroes every
# component so subsequent flags act as opt-ins. No-op after the
# first call.
components_flip() {
  if [[ $component_flag_seen -eq 0 ]]; then
    build_cli=0
    refresh_daemon=0
    build_vscode=0
    build_browser=0
    component_flag_seen=1
  fi
}
targets_flip() {
  if [[ $target_flag_seen -eq 0 ]]; then
    target_chrome=0
    target_firefox=0
    target_edge=0
    target_opera=0
    target_safari=0
    target_flag_seen=1
  fi
}

# ---- argv ------------------------------------------------------------

# Run journal: one line per run at start, one at exit, in
# ~/.local/share/weft/setup-runs.log. A run that dies mid-pipeline
# (set -e) scrolls its failure past and leaves the machine half
# installed with no trace; the journal makes "what did my last install
# actually do, and did it finish" a lookup instead of a debate.
# Installed BEFORE argument parsing so even a bad-flag exit is
# recorded; the EXIT trap logs the exit code and the LAST section
# entered, so a partial run names where it stopped.
run_log_dir="${HOME}/.local/share/weft"
run_log="${run_log_dir}/setup-runs.log"
# Best-effort at both ends: the journal is a diagnostic aid, and a
# root-owned ~/.local/share/weft (docker can auto-create such entries)
# must not hard-kill the very install that would explain it.
if ! mkdir -p "${run_log_dir}" 2>/dev/null || ! { : >> "${run_log}"; } 2>/dev/null; then
  run_log=/dev/null
  warn "cannot write the run journal under ${run_log_dir}; continuing without it"
fi
current_section="(argument parsing)"
# The exit code arrives as $1: a composite trap (`cleanup; log_run_exit`)
# would otherwise have $? read the CLEANUP's status and journal a died
# run as `DONE ok`.
# One entry per tracked package.json --bump has already moved this
# run (--vscode --browser --bump moves two): a failure AFTER a bump
# must name every version that moved, or a re-run with the same flags
# bumps a second time (and the version is what CI turns into a store
# release).
bump_pending_notes=()
log_run_exit() {
  local code="${1:-$?}"
  if [[ ${code} -eq 0 ]]; then
    printf '%s DONE  ok\n' "$(date '+%Y-%m-%d %H:%M:%S')" >> "${run_log}"
  else
    printf '%s FAIL  exit=%s in section %s\n' \
      "$(date '+%Y-%m-%d %H:%M:%S')" "${code}" "${current_section}" >> "${run_log}"
    local journal_note=""
    [[ "${run_log}" != /dev/null ]] && journal_note=" (journal: ${run_log})"
    printf '  %s%s%s this run FAILED in %s%s%s; the install is incomplete%s\n' \
      "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "${C_BOLD}" "${current_section}" "${C_RESET}" \
      "${journal_note}" >&2
    if [[ ${#bump_pending_notes[@]} -gt 0 ]]; then
      local n
      for n in "${bump_pending_notes[@]}"; do
        printf '  %s%s%s %s\n' "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "${n}" >&2
      done
    fi
  fi
}
trap 'log_run_exit $?' EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uninstall) do_uninstall=1 ;;
    --purge)     do_purge=1 ;;
    --debian)    purge_debian=1 ;;
    --kind)      purge_kind=1 ;;
    --postgres)  purge_postgres=1 ;;

    --cli)       components_flip; build_cli=1 ;;
    --daemon)    components_flip; refresh_daemon=1 ;;
    --vscode)    components_flip; build_vscode=1 ;;
    --browser)   components_flip; build_browser=1 ;;

    --bump)      do_bump=1 ;;
    --no-sign)   do_sign=0 ;;
    --no-daemon) refresh_daemon=0; no_daemon_seen=1 ;;

    --public-url)    public_url_flag="--public-url" ;;
    --no-public-url) public_url_flag="--no-public-url" ;;

    --chrome)    targets_flip; target_chrome=1 ;;
    --firefox)   targets_flip; target_firefox=1 ;;
    --edge)      targets_flip; target_edge=1 ;;
    --opera)     targets_flip; target_opera=1 ;;
    --safari)    targets_flip; target_safari=1 ;;

    --rebuild)   rebuild_flag="--rebuild" ;;
    --rebuild-cluster) rebuild_cluster_flag="--rebuild-cluster" ;;
    --debug)     profile="dev" ;;
    --prefix)
      shift
      [[ $# -gt 0 ]] || { fail "--prefix needs a path"; exit 1; }
      prefix="$1" ;;
    --migration)
      shift
      [[ $# -gt 0 ]] || { fail "--migration needs a name: ./setup.sh --migration add_owner"; exit 1; }
      write_migration="$1" ;;
    --release)   do_release=1 ;;

    --from-source) from_source=1 ;;

    -h|--help)
      # The whole leading comment block, however long it grows: from
      # line 2, print `#` lines with the marker stripped, stop at the
      # first non-comment line. Never journaled: a help read is not a
      # run, and a DONE line for it would dilute the journal's answer
      # to "did my last install finish".
      trap - EXIT
      awk 'NR>1 && /^#/ {sub(/^# ?/,""); print; next} NR>1 {exit}' "$0"
      exit 0
      ;;
    *)
      fail "unknown flag: $1 (run --help for the list)"
      exit 1
      ;;
  esac
  shift
done

# A flag that would silently apply to nothing is a mistyped invocation,
# not a preference: fail naming the shape that works. An uninstall or
# purge builds nothing, so every build-shaping flag is inapplicable
# there; the purge-image pickers only mean something WITH a purge; and
# --public-url / --migration both ride the daemon refresh, so a run
# that skips it must not accept them (--public-url opens an
# internet-facing surface: a silent no-op there is the worst outcome,
# and --migration's draft is only APPLIED by the refresh).
if [[ $do_uninstall -eq 1 || $do_purge -eq 1 ]]; then
  if [[ $do_bump -eq 1 || $do_sign -eq 0 || $target_flag_seen -eq 1 || -n "${write_migration}" \
    || -n "${public_url_flag}" || -n "${rebuild_flag}" || -n "${rebuild_cluster_flag}" \
    || $from_source -eq 1 || "${profile}" != "release" \
    || $component_flag_seen -eq 1 || $no_daemon_seen -eq 1 ]]; then
    fail "an --uninstall / --purge run builds and refreshes nothing, so --bump, --no-sign, --migration, --public-url/--no-public-url, --rebuild, --rebuild-cluster, --debug, --from-source, the component flags (--cli/--daemon/--vscode/--browser), --no-daemon and the browser-target flags do not apply here"
    exit 1
  fi
else
  if [[ $purge_postgres -eq 1 || $purge_kind -eq 1 || $purge_debian -eq 1 ]]; then
    fail "--postgres / --kind / --debian only pick which SHARED base images a purge also removes; pass them with --purge"
    exit 1
  fi
  if [[ $build_cli -eq 0 && $refresh_daemon -eq 0 && $build_vscode -eq 0 && $build_browser -eq 0 ]]; then
    # Reachable via e.g. `--daemon --no-daemon`: a run that does
    # nothing must say so, never journal a DONE over it.
    fail "this combination selects nothing to build or refresh"
    exit 1
  fi
  if [[ -n "${public_url_flag}" && $refresh_daemon -eq 0 ]]; then
    fail "${public_url_flag} is applied by the daemon refresh, which this run skips; drop --no-daemon or the component flags that exclude --daemon"
    exit 1
  fi
  if [[ -n "${write_migration}" && $refresh_daemon -eq 0 ]]; then
    fail "--migration writes the migration and the daemon refresh applies it, and this run skips the daemon; include --daemon (or drop --no-daemon)"
    exit 1
  fi
  if [[ $no_daemon_seen -eq 1 && $component_flag_seen -eq 1 ]]; then
    fail "--no-daemon opts out of the daemon refresh, which this component selection does not include anyway; drop it"
    exit 1
  fi
  if [[ $do_bump -eq 1 && $build_vscode -eq 0 && $build_browser -eq 0 ]]; then
    fail "--bump releases the extensions this run builds, and this run builds neither; pass it with --vscode and/or --browser (the default install covers --vscode)"
    exit 1
  fi
  if [[ $build_browser -eq 0 ]]; then
    if [[ $do_sign -eq 0 ]]; then
      fail "--no-sign shapes the browser-extension build, which this run does not do; pass it with --browser"
      exit 1
    fi
    if [[ $target_flag_seen -eq 1 ]]; then
      fail "the browser-target flags (--chrome/--firefox/--edge/--opera/--safari) shape the browser-extension build, which this run does not do; pass them with --browser"
      exit 1
    fi
  fi
fi

# The full default install (CLI + daemon + VS Code, no browser), the
# one shape that gets the post-install summary. Computed ONCE, here,
# where every component flag is final; an uninstall/purge run is never
# an install, whatever the component defaults say.
is_default_install=0
if [[ $do_uninstall -eq 0 && $do_purge -eq 0 \
  && $build_cli -eq 1 && $refresh_daemon -eq 1 && $build_vscode -eq 1 && $build_browser -eq 0 ]]; then
  is_default_install=1
fi

# The journal's START line, after parsing so a --help read never
# journals; a bad flag above still lands its FAIL line via the trap.
printf '%s START tree=%s args=[%s]\n' \
  "$(date '+%Y-%m-%d %H:%M:%S')" "${here}" "${orig_args}" >> "${run_log}"

# Worktree banner: this script builds/installs from its OWN directory (`here`).
# With multiple git worktrees of this repo checked out, it is easy to run the
# wrong one and rebuild stale source; print the tree up front so a wrong-tree
# run is obvious before anything is built.
hint "installing from ${C_BOLD}${here}${C_RESET}"

# ---- prebuilt artifacts ----------------------------------------------
#
# CI publishes a rolling release on every push to the release branch:
# CLI binaries, the .vsix, and a manifest.json naming the exact commit
# they were built from plus a sha256 per asset. When this checkout IS
# that commit with a clean tree, downloading is equivalent to building,
# so the install skips the compilers entirely (no Rust or Node
# toolchain needed). Any local change, a different commit, or no
# network takes the build path instead, and every branch says so.
#
# SYNC: release tag + asset names + manifest.json keys (commit,
#       vscode_version, sha256_<asset>) <->
#       .github/workflows/release.yml (cli matrix `asset` values, the
#       vsix `mv` target, the release job's manifest generation)
release_assets_url="https://github.com/WeaveMindAI/weft/releases/download/mvp-latest"
prebuilt_dir="${HOME}/.local/share/weft/prebuilt"
prebuilt_commit=""
prebuilt_vscode_version=""
use_prebuilt_cli=0
use_prebuilt_vsix=0
cli_sha256=""
vsix_sha256=""
cli_asset=""
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)   cli_asset="weft-x86_64-linux" ;;
  Linux-aarch64)  cli_asset="weft-aarch64-linux" ;;
  Darwin-arm64)   cli_asset="weft-aarch64-macos" ;;
  Darwin-x86_64)  cli_asset="weft-x86_64-macos" ;;
esac
# One string value out of the fetched manifest, empty when the key is
# absent (an asset key missing means "not published; build locally
# there"). THE one JSON-string extractor, `[^"]*` so no value alphabet
# assumption can silently zero a field (a prerelease version once
# parsed empty and cost every user a full compile).
manifest_value() {
  sed -n 's/.*"'"$1"'": *"\([^"]*\)".*/\1/p' <<<"${release_manifest}"
}

# Decide, per component this run installs, whether the published build
# can stand in for a local one. Sets use_prebuilt_cli / use_prebuilt_vsix
# plus the shas + version the install sections consume; every path that
# declines says why.
decide_prebuilt_use() {
  if ! git -C "${here}" rev-parse HEAD >/dev/null 2>&1; then
    hint "not a git checkout, so no published build can be matched; building locally"
    return 0
  fi
  if [[ -n "$(git -C "${here}" status --porcelain 2>/dev/null)" ]]; then
    hint "local changes present; building locally"
    return 0
  fi
  # Interrupted downloads leave per-PID temps; reap only STALE ones so
  # a concurrent run's in-flight temp survives.
  find "${prebuilt_dir}" -name '*.download' -mmin +60 -delete 2>/dev/null || true
  release_manifest="$(curl -fsSL --max-time 10 "${release_assets_url}/manifest.json" 2>/dev/null || true)"
  if [[ -z "${release_manifest}" ]]; then
    hint "could not reach the published build manifest; building locally"
    return 0
  fi
  prebuilt_commit="$(manifest_value "commit")"
  prebuilt_vscode_version="$(manifest_value "vscode_version")"
  if [[ -z "${prebuilt_commit}" ]]; then
    hint "the published manifest names no commit; building locally"
    return 0
  fi
  if [[ "${prebuilt_commit}" != "$(git -C "${here}" rev-parse HEAD)" ]]; then
    hint "no prebuilt artifacts for this commit yet (latest published: ${prebuilt_commit:0:12}); building locally. If you just pulled, CI is likely still building; re-run in a while to download instead."
    return 0
  fi
  if [[ $build_cli -eq 1 ]]; then
    # A debug CLI (--debug) is always a local build; the published
    # binary is a release build.
    if [[ "${profile}" != "release" ]]; then
      hint "a --debug CLI is always built locally"
    elif [[ -z "${cli_asset}" ]]; then
      hint "no published CLI for $(uname -s)-$(uname -m); building the CLI locally"
    else
      cli_sha256="$(manifest_value "sha256_${cli_asset}")"
      if [[ -n "${cli_sha256}" ]]; then
        use_prebuilt_cli=1
      else
        hint "this commit's ${cli_asset} was not published (its build failed in CI); building the CLI locally"
      fi
    fi
  fi
  if [[ $build_vscode -eq 1 ]]; then
    if [[ $do_bump -eq 1 ]]; then
      # The bump moves the version past the published package by
      # definition; the release must ship the bumped build.
      hint "--bump: building the VS Code extension locally at the new version"
    else
      vsix_sha256="$(manifest_value "sha256_weft-vscode.vsix")"
      if [[ -n "${prebuilt_vscode_version}" && -n "${vsix_sha256}" ]]; then
        use_prebuilt_vsix=1
      else
        hint "this commit's .vsix was not published (its build failed in CI); building the extension locally"
      fi
    fi
  fi
}

# Probe only when this run can actually consume a prebuilt (installing
# the CLI or the extension): an uninstall/purge or a --browser-only run
# must neither wait on the network nor print build-path hints. --bump
# also forces the build path for the bumped extension: the bump makes
# the tree diverge from the published commit by definition.
if [[ $do_uninstall -eq 0 && $do_purge -eq 0 ]] \
  && [[ $build_cli -eq 1 || $build_vscode -eq 1 ]]; then
  if [[ $from_source -eq 0 ]]; then
    decide_prebuilt_use
  else
    hint "--from-source: compiling the CLI/.vsix locally, published artifacts ignored"
  fi
fi

# Download one release asset to its final path, verified against the
# sha256 the manifest recorded for it: a truncated transfer, a caching
# proxy, or an asset mid-replacement (the release updates
# non-atomically) all fail the compare instead of installing wrong
# bytes. `mode` (optional) is applied to the temp BEFORE the atomic
# move, so a binary is never observable half-installed as
# non-executable. Per-PID temp name so two concurrent runs never race
# the mv. Returns non-zero (having removed the temp) on any miss; the
# caller owns the fallback.
download_verified() {
  local asset="$1" want_sha="$2" dest="$3" mode="${4:-}"
  local tmp="${dest}.$$.download"
  mkdir -p "$(dirname "${dest}")"
  # --max-time: a stalled connection must fail (and fall back to the
  # local build) instead of hanging the install with no output.
  if ! curl -fsSL --retry 2 --max-time 300 -o "${tmp}" "${release_assets_url}/${asset}"; then
    rm -f "${tmp}"
    return 1
  fi
  local got_sha
  got_sha="$(sha256 "${tmp}" | cut -d' ' -f1)"
  if [[ "${got_sha}" != "${want_sha}" ]]; then
    rm -f "${tmp}"
    warn "downloaded ${asset} does not match the manifest's checksum (got ${got_sha:0:12}, want ${want_sha:0:12})"
    return 1
  fi
  if [[ -n "${mode}" ]]; then
    chmod "${mode}" "${tmp}"
  fi
  # Every failure path drops the temp, this one included (an
  # unmovable temp would otherwise linger as junk nobody names).
  mv -f "${tmp}" "${dest}" || { rm -f "${tmp}"; return 1; }
}

# Make one prebuilt asset exist at `dest`: a byte-matching copy from an
# earlier run is reused, anything else is downloaded and verified. One
# ladder for the CLI and the .vsix, so "already downloaded" can never
# mean two things. Returns non-zero when neither worked; the caller
# owns the build-locally fallback and its toolchain messaging.
acquire_prebuilt() {
  local asset="$1" want_sha="$2" dest="$3" mode="${4:-}"
  if [[ -f "${dest}" && "$(sha256 "${dest}" | cut -d' ' -f1)" == "${want_sha}" ]]; then
    ok "prebuilt $(basename "${dest}") already downloaded ${C_DIM}(commit ${prebuilt_commit:0:12})${C_RESET}"
    return 0
  fi
  if download_verified "${asset}" "${want_sha}" "${dest}" ${mode:+"${mode}"}; then
    ok "downloaded prebuilt $(basename "${dest}") ${C_DIM}(${asset}, commit ${prebuilt_commit:0:12})${C_RESET}"
    return 0
  fi
  return 1
}

# ---- --migration: write the migration, then carry on installing -------
#
# Writing it needs a Postgres to build two schemas in and compare them, and the
# one weft runs lives inside the cluster where nothing outside can reach it, so
# this runs a throwaway container and takes it away again. Releasing also needs
# to reach the real one, to swap its drafts for the released file, which it
# does through a port-forward the way the tests do.
if [[ $do_release -eq 1 && -z "${write_migration}" ]]; then
  fail "--release only means something with --migration <name>: it collapses that run's drafts"
  exit 1
fi

if [[ -n "${write_migration}" ]]; then
  section "migration"
  # Everything this run's shape needs, named up front instead of a
  # bare command-not-found mid-run: cargo (the schema generators
  # compile even when the CLI came prebuilt), docker (the throwaway
  # scratch postgres), and kubectl when releasing (the release records
  # itself on the live database through a port-forward).
  migration_missing=""
  command -v cargo >/dev/null 2>&1 || migration_missing="cargo (install Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh)"
  command -v docker >/dev/null 2>&1 \
    || migration_missing="${migration_missing:+${migration_missing}; }docker (runs the scratch postgres)"
  if [[ $do_release -eq 1 ]] && ! command -v kubectl >/dev/null 2>&1; then
    migration_missing="${migration_missing:+${migration_missing}; }kubectl (--release reaches the live database)"
  fi
  if [[ -n "${migration_missing}" ]]; then
    fail "--migration needs: ${migration_missing}"
    exit 1
  fi
  # shellcheck source=scripts/lib/throwaway-postgres.sh
  . "${here}/scripts/lib/throwaway-postgres.sh"
  # Chain onto the script's existing exit logger so a Ctrl-C mid-run
  # leaves neither the container nor the port-forward behind.
  pf_pid=""
  migration_cleanup() {
    [[ -n "${pf_pid}" ]] && { kill "${pf_pid}" 2>/dev/null || true; }
    [[ -n "${THROWAWAY_PG_CONTAINER:-}" ]] \
      && docker rm -f "${THROWAWAY_PG_CONTAINER}" >/dev/null 2>&1 || true
  }
  trap 'rc=$?; migration_cleanup; log_run_exit "${rc}"' EXIT
  if ! start_throwaway_postgres weft-migration-scratch; then
    fail "could not start a scratch postgres in docker"
    exit 1
  fi
  export DATABASE_URL="$THROWAWAY_DATABASE_URL"

  release_flag=""
  if [[ $do_release -eq 1 ]]; then
    release_flag="--release"
    # SYNC: weft-db <-> crates/weft-core/src/infra/mod.rs (DB_NAMESPACE)
    if ! kubectl get namespace weft-db >/dev/null 2>&1; then
      fail "releasing has to reach the database you have been working against, and no cluster is up"
      exit 1
    fi
    kubectl -n weft-db port-forward svc/weft-postgres 15433:5432 >/dev/null 2>&1 &
    pf_pid=$!
    # SYNC: local-dev PG credentials <-> deploy/k8s/postgres.yaml (WEFT_DATABASE_URL secret),
    #       crates/weft-e2e/src/platform.rs (PG_USER/PG_PASSWORD/PG_DBNAME),
    #       scripts/run-e2e.sh (WEFT_E2E_DATABASE_URL)
    export WEFT_LIVE_DATABASE_URL="postgres://weft:weft-local-dev@127.0.0.1:15433/weft"
    pf_up=0
    for _ in $(seq 1 30); do
      (exec 3<>/dev/tcp/127.0.0.1/15433) 2>/dev/null && { pf_up=1; break; }
      sleep 1
    done
    if [[ $pf_up -ne 1 ]]; then
      fail "the port-forward to weft-postgres (127.0.0.1:15433) never came up"
      exit 1
    fi
  fi

  # A failure in one crate leaves the other's release intact and this
  # command re-runnable: the generator only deletes a crate's drafts
  # after its released file is recorded on the live database, and a
  # released crate answers "nothing changed" on the re-run.
  for pkg in weft-dispatcher weft-broker; do
    if ! cargo run -q -p "${pkg}" --features db-tests --example schema_migration \
        -- "${write_migration}" ${release_flag}; then
      fail "writing the migration failed in ${pkg}; fix the cause and re-run the same command"
      exit 1
    fi
  done
  migration_cleanup
  trap 'log_run_exit $?' EXIT
  hint "carrying on with the install, which applies it"
fi

bin_dir="${prefix}/bin"
weft_bin="${bin_dir}/weft"

# ---- pre-flight: required binaries ----------------------------------
#
# Check up front so we fail fast instead of midway through. Each
# component lists what it needs. Missing pieces get listed all at
# once (not one at a time) so the user installs everything in one
# round trip.

if [[ $do_uninstall -eq 0 && $do_purge -eq 0 ]]; then
  missing=()
  install_hints=()

  # Prebuilt CLI: no Rust toolchain needed; the binary is downloaded.
  if [[ $build_cli -eq 1 && $use_prebuilt_cli -eq 0 ]]; then
    if ! command -v cargo >/dev/null 2>&1; then
      missing+=("cargo (Rust toolchain)")
      install_hints+=("  Rust:    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh")
    fi
  fi

  if [[ $refresh_daemon -eq 1 ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      missing+=("docker")
      install_hints+=("  Docker:  https://docs.docker.com/get-docker/")
    fi
    if ! command -v kubectl >/dev/null 2>&1; then
      missing+=("kubectl")
      install_hints+=("  kubectl: https://kubernetes.io/docs/tasks/tools/")
    fi
    if ! command -v kind >/dev/null 2>&1; then
      missing+=("kind")
      install_hints+=("  kind:    https://kind.sigs.k8s.io/docs/user/quick-start/#installation")
    fi
  fi

  # Prebuilt .vsix: no Node toolchain needed; the package is downloaded.
  if [[ $build_vscode -eq 1 && $use_prebuilt_vsix -eq 0 ]]; then
    node_major="$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
    if [[ "${node_major}" -lt 20 ]]; then
      missing+=("node 20+")
      install_hints+=("  Node:    https://nodejs.org/en/download (or 'nvm install 20')")
    fi
    if ! command -v pnpm >/dev/null 2>&1; then
      missing+=("pnpm")
      install_hints+=("  pnpm:    npm i -g pnpm")
    fi
    if ! command -v code >/dev/null 2>&1; then
      # Not fatal: we can still build the .vsix; the user just
      # needs to install it manually. Surface as a soft warning
      # later in the VS Code section, not a pre-flight failure.
      :
    fi
  fi

  if [[ $build_browser -eq 1 ]]; then
    if ! command -v node >/dev/null 2>&1; then
      missing+=("node 20+ (browser extension)")
      install_hints+=("  Node:    https://nodejs.org/en/download")
    fi
    if ! command -v pnpm >/dev/null 2>&1; then
      missing+=("pnpm (browser extension)")
      install_hints+=("  pnpm:    npm i -g pnpm")
    fi
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    # Dedupe (node + pnpm may be requested by multiple components).
    seen=()
    unique_missing=()
    for item in "${missing[@]}"; do
      already=0
      for s in "${seen[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
      if [[ $already -eq 0 ]]; then
        unique_missing+=("$item")
        seen+=("$item")
      fi
    done
    seen_hints=()
    unique_hints=()
    for item in "${install_hints[@]}"; do
      already=0
      for s in "${seen_hints[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
      if [[ $already -eq 0 ]]; then
        unique_hints+=("$item")
        seen_hints+=("$item")
      fi
    done

    section "Pre-flight"
    fail "missing required tools:"
    for m in "${unique_missing[@]}"; do
      printf '    %s%s%s %s\n' "${C_RED}" "${SYM_ARROW}" "${C_RESET}" "${m}" >&2
    done
    printf '\n'
    hint "install:"
    for h in "${unique_hints[@]}"; do
      printf '    %s%s%s\n' "${C_DIM}" "${h#  }" "${C_RESET}" >&2
    done
    printf '\n'
    hint "re-run ${C_BOLD}./setup.sh${C_RESET}${C_DIM} once everything above is on PATH${C_RESET}"
    exit 1
  fi
fi

# ---- uninstall / purge ----------------------------------------------

if [[ $do_uninstall -eq 1 || $do_purge -eq 1 ]]; then
  if [[ $do_uninstall -eq 1 ]]; then
    section "Uninstall"
    hint "${C_DIM}preserving cluster + data; pass --purge to wipe everything${C_RESET}"

    # 1. Stop the daemon. Scales the StatefulSet to 0 which fires
    #    SIGTERM on the Pod, which triggers the dispatcher's
    #    graceful_shutdown to release leases cleanly. Best-effort:
    #    a missing CLI or already-stopped daemon doesn't fail.
    if command -v "${weft_bin}" >/dev/null 2>&1 || [[ -L "${weft_bin}" ]]; then
      if "${weft_bin}" daemon stop >/dev/null 2>&1; then
        ok "daemon stopped (cluster + data kept)"
      else
        hint "daemon: already stopped, or its cluster is unreachable; nothing to stop"
      fi
    else
      hint "daemon: no weft binary on disk; nothing to stop"
    fi

    # 2. Remove the VS Code extension if `code` is on PATH. Uses
    #    the same live-IPC-socket discovery as the install side
    #    so an open VS Code window picks up the change without a
    #    manual reload.
    if command -v code >/dev/null 2>&1; then
      if live_sock="$(pick_live_vscode_socket)"; then
        if [[ -z "$(installed_ext_version "${live_sock}")" ]]; then
          hint "VS Code extension: not installed; nothing to remove"
        elif VSCODE_IPC_HOOK_CLI="${live_sock}" \
            code --uninstall-extension "${ext_id}" >/dev/null 2>&1; then
          ok "VS Code extension uninstalled"
        else
          warn "could not uninstall the VS Code extension; remove it by hand: code --uninstall-extension ${ext_id}"
        fi
      elif code --uninstall-extension "${ext_id}" >/dev/null 2>&1; then
        ok "VS Code extension uninstalled"
      else
        # With no live window this path also fires for "not
        # installed"; the hedge is honest here.
        hint "VS Code extension: not installed, or no window to reach; if it is still there: code --uninstall-extension ${ext_id}"
      fi
    else
      hint "VS Code: 'code' not on PATH; skipping extension removal"
    fi

    # 3. Drop the CLI symlink.
    if [[ -L "${weft_bin}" || -f "${weft_bin}" ]]; then
      rm -f "${weft_bin}"
      ok "removed ${C_DIM}${weft_bin}${C_RESET}"
    else
      hint "CLI symlink at ${weft_bin} already absent"
    fi

    # 4. Hints for the manual cleanup we deliberately don't do.
    #    Only relevant when uninstall runs without --purge; if the
    #    user chained --purge, those things are already gone.
    if [[ $do_purge -eq 0 ]]; then
      printf '\n%s%sWhat is preserved:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
      printf "  %skind cluster%s %s'%s' (the running pods)%s\n" \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${WEFT_CLUSTER_NAME:-weft-local}" "${C_RESET}"
      printf '  %s~/.local/share/weft%s %s(the database: postgres, history, projects; manifest stamps, prebuilt binaries, port-forward state)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sobject store%s %s(weft-object-store container + data volume)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sdocker images%s %s(dispatcher, listener, weft-worker)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sworkspace target/%s %s(cargo cache)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sbrowser extensions%s %s(remove manually from each browser if installed)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '\n%sTo wipe everything too:%s ./setup.sh --uninstall --purge\n' \
        "${C_DIM}" "${C_RESET}"
    fi
  fi

  if [[ $do_purge -eq 1 ]]; then
    section "Purge"
    hint "${C_DIM}true clean slate; next install pays a full rebuild cost${C_RESET}"

    # A purge with the docker CLI present but the daemon down cannot do
    # most of its job, and every "is it there?" probe below would read
    # absence into the silence: refuse up front instead of printing a
    # wall of green over untouched state.
    if command -v docker >/dev/null 2>&1 \
      && ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
      fail "the docker daemon is not reachable, so nothing docker holds can be purged: the kind cluster, weft images, the object-store container + volume, and the BuildKit cache all remain"
      hint "start docker and re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
      exit 1
    fi

    # 1. Delete the kind cluster. `kind delete` leaves the shared
    #    `kind` docker network behind; once no clusters remain it is
    #    pure leftover, so remove it too (best-effort: another tool's
    #    running container on it just keeps it alive).
    if command -v kind >/dev/null 2>&1; then
      cluster="${WEFT_CLUSTER_NAME:-weft-local}"
      if kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
        if kind delete cluster --name "${cluster}" >/dev/null 2>&1; then
          ok "kind cluster ${C_DIM}'${cluster}'${C_RESET} deleted"
        else
          warn "could not delete the kind cluster '${cluster}'; delete it by hand: kind delete cluster --name ${cluster}"
        fi
      else
        hint "kind cluster ${C_DIM}'${cluster}'${C_RESET}: not present"
      fi
      if [[ -z "$(kind get clusters 2>/dev/null)" ]] \
        && docker network inspect kind >/dev/null 2>&1; then
        if docker network rm kind >/dev/null 2>&1; then
          ok "removed the ${C_DIM}kind${C_RESET} docker network"
        else
          warn "could not remove the kind docker network (another tool's container still on it?)"
        fi
      fi
    fi

    # 2. Reclaim every weft-related host docker image. The system images
    # carry content-addressed tags under any registry prefix, so match
    # the repo with the prefix stripped. (The daemon answered the gate
    # above, so "not present" below really means absent.)
    if ! command -v docker >/dev/null 2>&1; then
      warn "docker not on PATH; everything docker holds (images, the object store, the BuildKit cache) is untouched"
    else
      system_ids="$(weft_image_ids '^(weft-dispatcher|weft-listener|weft-broker|weft-infra-supervisor)$')" || {
        fail "could not list the weft system images (docker stopped answering?); re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
        exit 1
      }
      remove_docker_images_by_id "dispatcher + listener + broker + supervisor images" "${system_ids}"

      if docker image prune --force \
        --filter "label=weft.dev/project" >/dev/null 2>&1; then
        ok "pruned dangling project-labelled images"
      else
        warn "could not prune the dangling project-labelled images"
      fi
      # Remove every CONTENT-ADDRESSED image the build builds, by REPOSITORY (the
      # `<repo>:<hash>` form has no hyphen, so a `<repo>-*` glob silently matches
      # nothing). These accumulate with no implicit GC, so a purge must clear ALL:
      #   - `weft-worker`        : one per project build.
      #   - `weft-builder-base`  : one ~1.4GB image per engine version.
      #   - `weft-infra-<name>`  : one per infra node, built locally by the CLI as
      #                            `weft-infra-<name>:<hash>` (see weft-compiler
      #                            `infra_image_repo`). A tagged image is NOT
      #                            dangling, so the `image prune --filter label`
      #                            above does NOT remove these; match the repo.
      #     SYNC: weft-infra-<name> repo <-> crates/weft-compiler/src/image_set.rs
      #           (infra_image_repo). Sidecar images are a REMOVED concept; the
      #           current code builds no `weft-sidecar-*`, so none is matched here.
      bp_ids="$(weft_image_ids '^(weft-worker|weft-builder-base|weft-node-tests)$|^weft-infra-')" || {
        fail "could not list the weft build-plane images (docker stopped answering?); re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
        exit 1
      }
      remove_docker_images_by_id "weft-worker / weft-builder-base / weft-node-tests / weft-infra-* images" "${bp_ids}"

      # Shared base images, gated. `--postgres` covers BOTH postgres
      # images weft pulls: the in-cluster one and the --migration
      # scratch one (leaving the scratch image behind would break the
      # clean-slate floor with a ~450MB orphan nobody is told about).
      # SYNC: postgres image tags <-> deploy/k8s/postgres.yaml (the
      #       postgres container image, 18-alpine),
      #       scripts/lib/throwaway-postgres.sh (the scratch image, 18)
      if [[ $purge_postgres -eq 1 ]]; then
        remove_docker_object image postgres:18-alpine " ${C_DIM}(--postgres)${C_RESET}"
        remove_docker_object image postgres:18 " ${C_DIM}(--postgres, the --migration scratch image)${C_RESET}"
      fi
      if [[ $purge_kind -eq 1 ]]; then
        kind_ids="$(docker images kindest/node -q 2>/dev/null | sort -u)" || {
          fail "could not list the kindest/node images (docker stopped answering?); re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
          exit 1
        }
        remove_docker_images_by_id "kindest/node images" "${kind_ids}" \
          " ${C_DIM}(--kind)${C_RESET}" "a kind cluster still running?"
      fi
      if [[ $purge_debian -eq 1 ]]; then
        remove_docker_object image debian:bookworm-slim " ${C_DIM}(--debian)${C_RESET}"
      fi

      # The daemon's host-side object store: a docker container, its
      # named data volume, and the pulled seaweedfs image. All three
      # are weft-created (the image is niche enough that nothing else
      # on the host wants it), so a purge removes them unconditionally.
      # SYNC: weft-object-store <-> crates/weft-cli/src/commands/daemon.rs
      #       (OBJECT_STORE_CONTAINER; the volume is "<container>-data",
      #       the image is the `docker run` line below the constant)
      remove_docker_object container weft-object-store
      remove_docker_object volume weft-object-store-data
      remove_docker_object image chrislusf/seaweedfs:3.80

      if docker buildx prune --force >/dev/null 2>&1; then
        ok "pruned BuildKit cache"
      else
        warn "could not prune the BuildKit cache; prune it by hand: docker buildx prune --force"
      fi
    fi

    # 3. Workspace cargo target/ + the staged builder-base docker context
    # (both derived artifacts, regenerated on the next build). A failed
    # removal warns and the purge keeps going: dying here would abandon
    # every later step over one stubborn directory.
    remove_dir_reporting "${here}/target" "${C_DIM}target/${C_RESET}" \
      "" "; remove it by hand: rm -rf ${here}/target"
    # The staging lock lives BESIDE the dir (see
    # weft-cli images.rs ensure_worker_builder_base), so it needs its
    # own removal or it survives the purge.
    remove_dir_reporting "${here}/.weft-base-context" "${C_DIM}.weft-base-context/${C_RESET}" \
      " ${C_DIM}(builder-base staging context)${C_RESET}" "; remove it by hand: rm -rf ${here}/.weft-base-context"
    remove_dir_reporting "${here}/.weft-base-context.lock" "${C_DIM}.weft-base-context.lock${C_RESET}"

    # 4. Daemon-local state. Docker may have auto-created root-owned
    # entries under it (a missing bind-mount source becomes a root
    # directory); the plain rm fails on those, so docker itself (root)
    # cleans them first.
    if [[ -d "${HOME}/.local/share/weft" ]]; then
      if ! rm -rf "${HOME}/.local/share/weft" 2>/dev/null; then
        docker run --rm -v "${HOME}/.local/share/weft:/heal" alpine:3 \
          sh -c "rm -rf /heal/* /heal/.[!.]*" >/dev/null 2>&1 || true
        rm -rf "${HOME}/.local/share/weft" 2>/dev/null || true
      fi
      # Both attempts can fail (root-owned entries with docker itself
      # unavailable); only claim success when the directory is gone.
      if [[ -d "${HOME}/.local/share/weft" ]]; then
        warn "could not remove ${C_DIM}~/.local/share/weft/${C_RESET}; remove it manually: sudo rm -rf ~/.local/share/weft"
      else
        ok "removed ${C_DIM}~/.local/share/weft/${C_RESET}"
        # The run journal lived in the directory we just purged.
        # Recreating it to append the exit line would undo the clean
        # slate, so the journal ends here for this run.
        run_log=/dev/null
      fi
    fi

    # 5. Browser-extension build artifacts. WXT writes everything
    # into extension-browser/build/ (configured via wxt.config.ts).
    # The per-target zips and the signed .xpi are TRACKED files (the
    # latest distributables live in git); deleting them would leave the
    # user's working tree showing deletions they never made, so the
    # purge takes only the untracked scratch: the unpacked per-target
    # dirs and the AMO sources zip.
    for ext_scratch in "${here}/extension-browser/build/"*-mv2 \
                       "${here}/extension-browser/build/"*-mv3 \
                       "${here}/extension-browser/build/weft-extension-sources.zip"; do
      # Unexpanded globs pass through literally; the helper's absent
      # hint on those would print the pattern, so skip them quietly.
      [[ -e "${ext_scratch}" ]] || continue
      remove_dir_reporting "${ext_scratch}" \
        "${C_DIM}${ext_scratch#"${here}"/}${C_RESET}" " ${C_DIM}(tracked zips kept)${C_RESET}"
    done

    # 6. VS Code extension's pre-packaged .vsix.
    for ext_scratch in "${here}/extension-vscode/"weft-vscode-*.vsix; do
      [[ -e "${ext_scratch}" ]] || continue
      remove_dir_reporting "${ext_scratch}" "${C_DIM}${ext_scratch#"${here}"/}${C_RESET}"
    done

    # Shared-base hint footer.
    skipped_lines=()
    if [[ $purge_postgres -eq 0 ]]; then
      if docker image inspect postgres:18-alpine >/dev/null 2>&1; then
        skipped_lines+=("postgres:18-alpine|--postgres")
      fi
      if docker image inspect postgres:18 >/dev/null 2>&1; then
        skipped_lines+=("postgres:18|--postgres")
      fi
    fi
    if [[ $purge_kind -eq 0 ]]; then
      # A failed listing must not read as "no images skipped": say so
      # instead (only a footer line, so a warn is enough).
      if kind_footer_ids="$(docker images kindest/node -q 2>/dev/null)"; then
        [[ -n "${kind_footer_ids}" ]] && skipped_lines+=("kindest/node|--kind")
      else
        warn "could not list kindest/node images for the skipped-images footer"
      fi
    fi
    if [[ $purge_debian -eq 0 ]]; then
      if docker image inspect debian:bookworm-slim >/dev/null 2>&1; then
        skipped_lines+=("debian:bookworm-slim|--debian")
      fi
    fi
    if [[ ${#skipped_lines[@]} -gt 0 ]]; then
      printf '\n  %s%sShared base images kept%s %s(reused by other docker projects)%s\n' \
        "${C_BOLD}" "${C_BLUE}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      for entry in "${skipped_lines[@]}"; do
        skipped_image="${entry%%|*}"
        skipped_flag="${entry##*|}"
        printf '    %s%s%s %-22s %s%s%s\n' \
          "${C_DIM}" "${SYM_ARROW}" "${C_RESET}" "${skipped_image}" \
          "${C_DIM}" "(${skipped_flag})" "${C_RESET}"
      done
      printf '\n  %sopt in:%s ./setup.sh --uninstall --purge --postgres --kind --debian\n' \
        "${C_DIM}" "${C_RESET}"
    fi
  fi

  printf '\n%s%s%s %sDone.%s\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
  exit 0
fi

# ---- CLI -------------------------------------------------------------

if [[ $build_cli -eq 1 ]]; then
  section "CLI"
  mkdir -p "${bin_dir}"

  # ---- disk hygiene, BEFORE the build -------------------------------
  #
  # Two unbounded caches live on this machine and both once grew to
  # ~100GB each before anyone noticed:
  #   - the cargo target/ dir (incremental artifacts accumulate across
  #     dep bumps and crate renames and are never evicted), and
  #   - docker's BuildKit layer cache (every worker/infra/test image
  #     build adds entries; nothing prunes them).
  # Bound both on every install. The target/ bound is a cap, not a
  # wipe: under the cap the incremental cache is untouched and rebuilds
  # stay fast; over it we clean and pay one cold build now instead of
  # filling the disk later. Override with WEFT_TARGET_CAP_GB.
  target_cap_gb="${WEFT_TARGET_CAP_GB:-60}"
  if [[ ! "${target_cap_gb}" =~ ^[0-9]+$ ]]; then
    fail "WEFT_TARGET_CAP_GB must be a whole number of gigabytes, got '${target_cap_gb}'"
    exit 1
  fi
  if [[ -d "${here}/target" ]]; then
    # -L: a target/ symlinked onto a scratch disk (a normal cargo
    # setup, and the case where the cap matters most) measures as 0
    # without it. -sk (POSIX kilobytes; GNU-only -BG never works on
    # macOS, which would silently disable the cap there forever). A
    # non-numeric measurement skips the cap (wiping on an unreadable
    # measurement is the dangerous direction).
    target_kb="$(du -sk -L "${here}/target" 2>/dev/null | tail -n1 | cut -f1)"
    target_gb=""
    [[ "${target_kb}" =~ ^[0-9]+$ ]] && target_gb="$((target_kb / 1024 / 1024))"
    if [[ ! "${target_gb}" =~ ^[0-9]+$ ]]; then
      warn "could not measure target/; skipping the size cap"
    elif [[ "${target_gb}" -gt "${target_cap_gb}" ]]; then
      warn "target/ is ${target_gb}G (cap ${target_cap_gb}G); cleaning before the build"
      remove_dir_reporting "${here}/target" "${C_DIM}target/${C_RESET}" \
        " ${C_DIM}(this build runs cold; the cap is WEFT_TARGET_CAP_GB)${C_RESET}" \
        "; the cap stays exceeded"
    fi
  fi
  # Gate on daemon REACHABILITY, not binary presence: a --cli install
  # on a machine whose docker isn't running must still produce a
  # binary. With a live daemon, a failed prune fails loud.
  if docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
    spin "bound BuildKit cache to 20GB (LRU)" \
      docker builder prune --force --max-used-space 20GB
  fi

  # Prebuilt path: this checkout matches the commit CI built, so the
  # published binary IS this source compiled. A failed download drops
  # to the local build when a toolchain is there (the same
  # present -> pull -> build ladder the daemon images use).
  if [[ $use_prebuilt_cli -eq 1 ]]; then
    # Byte-identity lives in `acquire_prebuilt` (keyed on the PREBUILT
    # FILE, not on what the symlink points at: the link and the
    # recorded repo location must refresh even when the bytes are
    # already right, since this checkout may be a different clone than
    # the one that downloaded them).
    if ! acquire_prebuilt "${cli_asset}" "${cli_sha256}" "${prebuilt_dir}/weft" 0755; then
      use_prebuilt_cli=0
      if command -v cargo >/dev/null 2>&1; then
        warn "prebuilt CLI download failed; building locally instead"
      else
        fail "the prebuilt CLI could not be downloaded and no Rust toolchain is on PATH"
        hint "install Rust (${C_BOLD}curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh${C_RESET}${C_DIM}) or re-run once the network is back${C_RESET}"
        exit 1
      fi
    fi
  fi
  if [[ $use_prebuilt_cli -eq 1 ]]; then
    # A prebuilt binary bakes its BUILDER's checkout path as its
    # compile-time repo location, which does not exist here; record
    # where the repo actually lives (THIS tree, every run, so a moved
    # or re-cloned checkout re-points it) so the binary can find its
    # own catalog / manifests / build context. Written BEFORE the
    # symlink lands on PATH: the other order could publish a weft that
    # cannot find its checkout. Loud on failure WITH the recovery.
    # SYNC: repo-root file <-> crates/weft-catalog/src/lib.rs (weft_repo_root)
    repo_root_file="${HOME}/.local/share/weft/repo-root"
    if ! { mkdir -p "$(dirname "${repo_root_file}")" \
        && printf '%s' "${here}" > "${repo_root_file}"; } 2>/dev/null; then
      fail "the prebuilt CLI needs ${repo_root_file} to find this checkout, and that path is not writable"
      hint "fix the ownership (docker can leave root-owned entries there): ${C_BOLD}sudo chown -R \"\$(id -u):\$(id -g)\" ~/.local/share/weft${C_RESET}${C_DIM}, then re-run${C_RESET}"
      exit 1
    fi
    chmod 0755 "${prebuilt_dir}/weft"
    ln -sfn "${prebuilt_dir}/weft" "${weft_bin}"
    ok "linked ${C_DIM}${weft_bin}${C_RESET} ${SYM_ARROW} ${C_DIM}${prebuilt_dir}/weft${C_RESET}"
  fi

  if [[ $use_prebuilt_cli -eq 0 ]]; then
    hint "${C_DIM}cargo's incremental cache makes re-runs near-instant${C_RESET}"
    if [[ "${profile}" == "release" ]]; then
      target_dir="${here}/target/release"
    else
      target_dir="${here}/target/debug"
    fi
    src="${target_dir}/weft"
    # Sources can change WHILE this script runs (an AI session editing in
    # parallel with a reinstall is the everyday case here). cargo builds
    # what existed when it started, so a source edit landing mid-build
    # ships a binary that is ALREADY stale, silently. Build, then check
    # whether any workspace source is newer than the produced binary; if
    # so, build again (once more is always enough: the recheck is
    # instant), and if it STILL moves, say so loudly instead of
    # pretending the install is current.
    for build_pass in 1 2 3; do
      if [[ "${profile}" == "release" ]]; then
        spin_passthrough "cargo build --release -p weft-cli" \
          cargo build --release -p weft-cli
      else
        spin_passthrough "cargo build -p weft-cli" cargo build -p weft-cli
      fi
      if [[ ! -x "${src}" ]]; then
        fail "build output missing: ${src}"
        exit 1
      fi
      # cargo's own freshness check is the authority on whether the
      # binary matches the sources (mtime scans over the tree false-alarm
      # on files that are not build inputs). A repeated build doing zero
      # work == current; it doing work == a source edit landed mid-build
      # and the first binary shipped stale, so loop.
      # A bare word, not an array: expanding an EMPTY array under
      # `set -u` is fatal on bash 3.2 (macOS's /bin/bash), same idiom
      # as ${rebuild_flag} elsewhere. The `||` keeps a recheck FAILURE
      # from dying silently under `set -e` (the assignment's status is
      # cargo's), with the captured output shown instead of discarded.
      recheck_release_flag=""
      [[ "${profile}" == "release" ]] && recheck_release_flag="--release"
      recheck_out="$(cargo build ${recheck_release_flag} -p weft-cli 2>&1)" || {
        fail "the freshness recheck build failed (a source edit landed mid-build?)"
        printf '%s\n' "${recheck_out}" >&2
        exit 1
      }
      # [[:space:]], never \s: BSD grep (macOS) has no \s and would
      # read the pattern as a literal, silently disabling this check.
      if ! grep -q '^[[:space:]]*Compiling' <<<"${recheck_out}"; then
        break
      fi
      if [[ "${build_pass}" -eq 3 ]]; then
        fail "sources are still changing under the build; the installed binary does NOT include them. Re-run setup.sh once the edits settle."
        exit 1
      fi
      hint "source changed during the build; building again"
    done
    ln -sfn "${src}" "${weft_bin}"
    ok "linked ${C_DIM}${weft_bin}${C_RESET} ${SYM_ARROW} ${C_DIM}${src}${C_RESET}"
  fi

  # Engine-change sweep, on either path: worker images, the builder
  # base and the node-test image all bake the engine crates in, so an
  # engine change strands every cached build-plane image (host docker +
  # kind containerd), the BuildKit cache and the node-test cargo cache.
  # Keyed on the ENGINE'S OWN identity (the builder-base ref the
  # freshly installed CLI computes from source content), which is the
  # same on the prebuilt and the compiled path and does not move when
  # only the binary does (a --debug/--release switch, a prebuilt/local
  # switch), so those never cost a sweep.
  engine_ref_file="${HOME}/.local/share/weft/builder-base-ref"
  # Only the TAG (the content hash) is the identity; the registry
  # prefix is naming, and keying on it would fire a full sweep on a
  # WEFT_IMAGE_REGISTRY change with the engine untouched. Streams kept
  # apart: only STDOUT is grepped for the ref (a diagnostic line on
  # stderr that happened to mention the base would otherwise feed the
  # parse a wrong hash), and stderr is kept so the warn below can show
  # the resolver's own diagnostics (they name the fix), never a bare
  # "could not compute".
  new_engine_ref=""
  engine_err="$(mktemp -t weft-engineref.XXXXXX)"
  if engine_print_out="$(env WEFT_REPO_ROOT="${here}" "${weft_bin}" build-images --print 2>"${engine_err}")"; then
    new_engine_ref="$(grep -E '(^|/)weft-builder-base:' <<<"${engine_print_out}" || true)"
    new_engine_ref="${new_engine_ref##*:}"
  fi
  if [[ -z "${new_engine_ref}" ]]; then
    warn "could not compute the engine identity (weft build-images --print); skipping the stale-image sweep this run"
    [[ -s "${engine_err}" ]] && cat "${engine_err}" >&2
  elif [[ "${new_engine_ref}" != "$(cat "${engine_ref_file}" 2>/dev/null || true)" ]]; then
    sweep_stale_build_plane
    # Braced so a redirect failure is silenced too (a bare 2>/dev/null
    # does not cover the redirect itself) and only the warn speaks.
    if ! { printf '%s' "${new_engine_ref}" > "${engine_ref_file}"; } 2>/dev/null; then
      warn "could not record the engine identity at ${engine_ref_file}; the sweep re-runs next install"
    fi
  fi
  rm -f "${engine_err}"
fi

# ---- daemon refresh / start ------------------------------------------
#
# Two cases share this block:
#   1. Daemon already up: rebuild dispatcher/listener images and roll
#      the pod so it picks up the new code.
#   2. Daemon not up: start it. The CLI's `daemon start` builds the
#      fresh images itself, so a separate --rebuild isn't needed.
#
# Either way the user ends up with a running daemon on the latest
# source. Pre-setup.sh behavior was to skip when the daemon was down,
# which forced a manual `weft daemon start` afterwards.

if [[ $refresh_daemon -eq 1 ]]; then
  section "Daemon"
  if [[ ! -x "${weft_bin}" && ! -L "${weft_bin}" ]]; then
    warn "no weft binary at ${weft_bin}; skipping"
  elif ! command -v docker >/dev/null 2>&1; then
    warn "docker not on PATH; skipping"
  elif ! command -v kubectl >/dev/null 2>&1; then
    warn "kubectl not on PATH; skipping"
  else
    dispatcher_url="${WEFT_DISPATCHER_URL:-http://127.0.0.1:9999}"
    if curl --silent --max-time 2 "${dispatcher_url}/health" >/dev/null 2>&1; then
      hint "daemon running at ${C_DIM}${dispatcher_url}${C_RESET}; refreshing (no-op if nothing changed)"
    else
      hint "daemon not running; first install pulls images and creates the kind cluster (~2-3 min)"
    fi
    # One verb either way: `daemon start` is an idempotent reconcile
    # (boot and refresh are the same operation; `restart` is its alias).
    spin_passthrough "weft daemon start" \
      env WEFT_REPO_ROOT="${here}" "${weft_bin}" daemon start ${rebuild_flag} ${rebuild_cluster_flag} ${public_url_flag}
  fi
fi

# The address the daemon recorded for the public trigger surface, when
# it is open. Read from the daemon's own state file so setup.sh never
# re-derives it (one authority, and `weft daemon status` reads the same).
public_url=""
if [[ -r "${HOME}/.local/share/weft/public-url-enabled" \
   && -r "${HOME}/.local/share/weft/public-url" ]]; then
  public_url="$(tr -d '[:space:]' < "${HOME}/.local/share/weft/public-url")"
fi
if [[ -n "${public_url}" && $is_default_install -eq 0 ]]; then
  # Non-default installs print no summary block, so say it here.
  hint "public surface: ${C_BOLD}${public_url}${C_RESET}${C_DIM} (event pushes, signal fire links, file share links, OAuth callback)"
fi

# ---- VS Code extension -----------------------------------------------

if [[ $build_vscode -eq 1 ]]; then
  section "VS Code extension"
  ext_dir="${here}/extension-vscode"
  if [[ ! -d "${ext_dir}" ]]; then
    fail "${ext_dir} not found"
    exit 1
  fi

  # The editor's parsing AND source edits run in the `weft` CLI binary
  # (the extension only shuttles requests to it), so "an editor bug"
  # very often needs a CLI rebuild, not an extension one. When this run
  # skips the CLI step, refuse to quietly ship an extension on top of a
  # stale binary: if any Rust source is newer than the installed
  # binary, say so and stop (rerun with --cli, or the full default).
  # A precondition of the whole section, however the .vsix is produced:
  # a downloaded extension on top of a stale local binary keeps the
  # stale behavior exactly the same way a built one would. Checked
  # BEFORE the --bump below, so a refused run has touched nothing.
  if [[ $build_cli -eq 0 && -x "${weft_bin}" ]]; then
    # The reference is the BINARY's own mtime, carried onto a temp
    # file with `touch -r` (which dereferences the symlink; GNU
    # `find -newer <symlink>` does NOT, it reads the link's own mtime,
    # which every install refreshes whether or not the binary moved).
    bin_ref="$(mktemp -t weft-binref.XXXXXX)"
    if ! touch -r "${weft_bin}" "${bin_ref}"; then
      rm -f "${bin_ref}"
      warn "cannot read ${weft_bin}'s timestamp; skipping the stale-binary check"
    else
      # Streams kept apart: a scan can BOTH find a stale source and
      # exit non-zero (an unreadable directory on the way), and the
      # stale-binary refusal with its recovery must win over the scan
      # noise in that case.
      find_err="$(mktemp -t weft-findlog.XXXXXX)"
      find_rc=0
      stale_src="$(find "${here}/crates" "${here}/catalog" \
        \( -name '*.rs' -o -name '*.toml' -o -name '*.json' \) \
        -newer "${bin_ref}" -print -quit 2>"${find_err}")" || find_rc=$?
      rm -f "${bin_ref}"
      if [[ -n "${stale_src}" ]]; then
        rm -f "${find_err}"
        fail "the installed weft binary is OLDER than ${stale_src#"${here}"/}; the editor's parse/edit logic lives in that binary, so an extension-only install would keep the stale behavior. Run ${C_BOLD}./setup.sh --cli --vscode${C_RESET} (or the full default)."
        exit 1
      fi
      if [[ ${find_rc} -ne 0 ]]; then
        # A failed scan must not read as "binary is current": that is
        # exactly the silent pass this guard exists to prevent.
        fail "cannot scan for sources newer than ${weft_bin}: $(cat "${find_err}")"
        rm -f "${find_err}"
        exit 1
      fi
      rm -f "${find_err}"
    fi
  fi

  # The release gesture, mirroring the browser extension's: the
  # version in package.json is what makes CI publish a pushed build to
  # the VS Code Marketplace + Open VSX, so it moves ONLY on --bump,
  # never as a side effect of a rebuild (an automatic bump once
  # dirtied the tree on every build, killing the prebuilt fast path
  # for the CLI too, and turned ordinary pushes into store releases).
  # Last thing before the build: every precondition above already
  # passed, so a run that bumps also delivers.
  if [[ $do_bump -eq 1 ]]; then
    if ! command -v pnpm >/dev/null 2>&1 || ! command -v node >/dev/null 2>&1; then
      fail "--bump edits extension-vscode/package.json with pnpm, which needs node + pnpm on PATH"
      exit 1
    fi
    prev_ext_ver="$(node -p "require('${ext_dir}/package.json').version")"
    (cd "${ext_dir}" && pnpm version patch --no-git-tag-version >/dev/null)
    bumped_ext_ver="$(node -p "require('${ext_dir}/package.json').version")"
    ok "version bumped: ${C_DIM}${prev_ext_ver} ${SYM_ARROW} ${bumped_ext_ver}${C_RESET} ${C_DIM}(pushing this publishes to the VS Code Marketplace + Open VSX)${C_RESET}"
    bump_pending_notes+=("extension-vscode/package.json is already bumped to v${bumped_ext_ver}: re-run WITHOUT --bump, or revert it")
  fi

  # Prebuilt path: CI packaged the .vsix from this exact commit, so
  # download it instead of compiling (same ladder as the CLI: a failed
  # download drops to the local build when the toolchain is there).
  vsix_path=""
  if [[ $use_prebuilt_vsix -eq 1 ]]; then
    if acquire_prebuilt "weft-vscode.vsix" "${vsix_sha256}" "${prebuilt_dir}/weft-vscode.vsix"; then
      vsix_path="${prebuilt_dir}/weft-vscode.vsix"
      current_ver="${prebuilt_vscode_version}"
    else
      if command -v pnpm >/dev/null 2>&1 && command -v node >/dev/null 2>&1; then
        warn "prebuilt .vsix download failed; building locally instead"
      else
        fail "the prebuilt .vsix could not be downloaded and no Node toolchain is on PATH"
        hint "install Node 20+ and pnpm, or re-run once the network is back"
        exit 1
      fi
    fi
  fi

  # The whole local build as one named operation (like migration_cleanup
  # above): compile, fingerprint, bump, package. Sets `vsix_path` +
  # `current_ver`; called just below when no prebuilt .vsix landed.
  build_vsix_locally() {
  node_major="$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
  if [[ "${node_major}" -lt 20 ]]; then
    fail "needs Node 20+ (got $(node -v 2>/dev/null || echo 'none'))"
    hint "try: ${C_BOLD}nvm use 24 && ./setup.sh --vscode${C_RESET}"
    exit 1
  fi
  if ! command -v pnpm >/dev/null 2>&1; then
    fail "pnpm not on PATH"
    hint "install: ${C_BOLD}npm i -g pnpm${C_RESET}"
    exit 1
  fi

  pushd "${ext_dir}" >/dev/null

  # The shared graph webview lives in the sibling `../packages/weft-graph` package,
  # consumed FROM SOURCE (one renderer for the extension + the website). It has no
  # install of its own; the extension bundles it through vite's `resolve.dedupe`
  # (its bare deps resolve to THIS app's node_modules). svelte-check
  # (check:webview) needs the same redirect, so point the package's node_modules at
  # the extension's install. Created up-front (not inside the rebuild branch) so a
  # bare `pnpm run check:webview` resolves too. One symlink, no dep-list dup.
  ln -sfn ../../extension-vscode/node_modules ../packages/weft-graph/node_modules
  # Same borrow for the grammars package: its only dependency is the
  # highlight.js the extension already installs, for its own tests.
  ln -sfn ../../extension-vscode/node_modules ../packages/weft-syntax/node_modules

  # Skip rebuild if nothing under src/ + config + package.json has
  # changed since the last successful build. We hash inputs and
  # compare to a stamp file. The .vsix from the last run is reused
  # for the install step.
  hash_dir="${HOME}/.local/share/weft/vscode-hashes"
  hash_file="${hash_dir}/extension.hash"
  # The fingerprint is EVERYTHING the package can ship: every tracked
  # file of the extension and its two source-consumed sibling packages
  # (weft-graph's webview, weft-syntax's grammars), plus untracked
  # not-ignored ones (a just-created source file must not be invisible
  # to the stamp), hashed by content. A hand-kept file list here
  # repeatedly went stale (.vscodeignore, media/, the README all ship
  # and were once missing); enumerating from git closes the class.
  # Ignored files (out/, node_modules/) stay out, and so does
  # media/webview (tracked but DERIVED from weft-graph's sources,
  # which are hashed; hashing the output too would make every build
  # dirty its own stamp). A deleted-but-still-tracked file hashes as
  # its absence, so a mid-refactor worktree changes the fingerprint
  # instead of killing the run.
  if git -C "${here}" rev-parse HEAD >/dev/null 2>&1; then
    current_hash="$(
      {
        git -C "${here}" ls-files -z extension-vscode packages/weft-graph packages/weft-syntax \
          ':(exclude)extension-vscode/media/webview'
        git -C "${here}" ls-files -z --others --exclude-standard \
          extension-vscode packages/weft-graph packages/weft-syntax \
          ':(exclude)extension-vscode/media/webview'
      } | {
        cd "${here}" && sort -z | while IFS= read -r -d '' f; do
          if [[ -e "$f" ]]; then ${sha256_bin} "$f"; else printf 'absent  %s\n' "$f"; fi
        done
      } | sha256 | awk '{print $1}'
    )"
  else
    # No git, no reliable input list: never reuse a cached build.
    current_hash="no-git-$$-$(date +%s)"
  fi

  # One build at a time: the stamp and the output filename are shared,
  # and a second run packaging in place while this one installs could
  # hand VS Code a half-written file.
  vsix_lock="${hash_dir}.lock"
  mkdir -p "$(dirname "${vsix_lock}")"
  vsix_lock_waits=0
  until mkdir "${vsix_lock}" 2>/dev/null; do
    if [[ -L "${vsix_lock}" || (-e "${vsix_lock}" && ! -d "${vsix_lock}") ]]; then
      # A plain file or a symlink on the lock path is never another
      # run's lock (the lock is always a real directory): waiting on
      # it would spin forever. -L separately, because a DANGLING
      # symlink fails -e yet still makes mkdir refuse.
      fail "the extension build lock path ${vsix_lock} exists and is not a directory; remove it and re-run"
      exit 1
    fi
    # mkdir failed: another run's lock, or the parent refuses creates
    # (a root-owned entry docker left under ~/.local/share/weft, an
    # immutable/full filesystem). Distinguish by ATTEMPTING a create,
    # never by predicate (`-w` lies for root and ACLs, and probing the
    # lock's absence would false-fail on a concurrent release).
    if probe="$(mktemp -d "$(dirname "${vsix_lock}")/.lockprobe.XXXXXX" 2>/dev/null)"; then
      rmdir "${probe}"
    else
      fail "cannot create the extension build lock at ${vsix_lock}; is $(dirname "${vsix_lock}") writable?"
      exit 1
    fi
    # A breadcrumb up front and then one a minute: a silent wait on a
    # stale lock would be indistinguishable from a hang.
    if [[ $((vsix_lock_waits % 30)) -eq 0 ]]; then
      hint "another setup.sh is building the extension; waiting ($((vsix_lock_waits * 2))s so far; if none is running, remove the stale lock: rmdir ${vsix_lock})"
    fi
    vsix_lock_waits=$((vsix_lock_waits + 1))
    sleep 2
  done
  trap 'rc=$?; rmdir "${vsix_lock}" 2>/dev/null || true; log_run_exit "${rc}"' EXIT
  # Read the stamp only after the lock is held: a run that waited here
  # must compare against the build the earlier run just finished, not
  # against the stamp as it stood before that build.
  prior_hash="$(cat "${hash_file}" 2>/dev/null || echo)"

  # Reuse the cached .vsix when the CONTENT hash matches AND the
  # current version's package is on disk. The hash is the whole
  # freshness test on purpose: the stamp is written only after
  # packaging succeeds, so a matching stamp proves the .vsix was built
  # from inputs hashing exactly like today's (an interrupted build
  # never stamps). An mtime guard here once cost a full extension
  # rebuild on every branch switch (git rewrites mtimes of
  # content-identical files); content identity does not. A missing
  # .vsix is NOT a repackage-without-recompile (out/ is gitignored and
  # unguaranteed at that point); it is simply not fresh.
  current_ver="$(node -p "require('./package.json').version")"
  vsix="weft-vscode-${current_ver}.vsix"
  if [[ -f "${vsix}" && "${current_hash}" == "${prior_hash}" ]]; then
    ok "no source changes (compiled ${C_DIM}v${current_ver}${C_RESET})"
  else
    spin "pnpm install" pnpm install --prefer-offline
    spin "tsc compile" pnpm run compile
    spin "vite bundle webview" pnpm run bundle:webview
    spin "vite bundle markdown preview" pnpm run bundle:markdown-preview
    rm -f weft-vscode-*.vsix
    spin "package .vsix" pnpm dlx @vscode/vsce package \
      --no-dependencies --allow-missing-repository --skip-license
    mkdir -p "${hash_dir}"
    printf '%s' "${current_hash}" >"${hash_file}"
  fi

  popd >/dev/null

  # The build is done; hand the lock back before the install step (the
  # install keys on its own content stamp and tolerates concurrency).
  rmdir "${vsix_lock}" 2>/dev/null || true
  trap 'log_run_exit $?' EXIT

  if [[ ! -f "${ext_dir}/${vsix}" ]]; then
    fail "no .vsix for v${current_ver} was produced"
    exit 1
  fi
  vsix_path="${ext_dir}/${vsix}"

  ok "packaged ${C_DIM}${vsix_path}${C_RESET}"
  }
  if [[ -z "${vsix_path}" ]]; then
    build_vsix_locally
  fi

  # Auto-install via `code` IPC, using the shared live-socket probe (closed
  # terminals leave dead sockets in VSCODE_IPC_HOOK_CLI under WSL/remote-SSH).

  # The installed version of the extension, queried through the SAME live
  # socket we install through (so it reflects this VS Code window, not some
  # global registry). Empty if not installed / no socket.
  # Whether VS Code already runs these exact bytes: the version alone
  # cannot say (it moves only on --bump, so most rebuilds keep it), so
  # a content stamp remembers the sha of the last .vsix handed over,
  # and `--force` makes VS Code take a same-version package.
  vsix_sha="$(sha256 "${vsix_path}" | cut -d' ' -f1)"
  installed_vsix_stamp="${HOME}/.local/share/weft/vscode-hashes/installed.sha"
  if ! command -v code >/dev/null 2>&1; then
    warn "'code' not on PATH"
    hint "install manually: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
  elif ! live_sock="$(pick_live_vscode_socket)"; then
    warn "no live VS Code window found"
    hint "run inside a VS Code terminal: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
  else
    installed_ver="$(installed_ext_version "${live_sock}")"
    if [[ "${installed_ver}" == "${current_ver}" \
       && "$(cat "${installed_vsix_stamp}" 2>/dev/null)" == "${vsix_sha}" ]]; then
      ok "VS Code already running ${C_DIM}v${current_ver}${C_RESET} with these exact bytes; nothing to install"
    elif VSCODE_IPC_HOOK_CLI="${live_sock}" \
        code --install-extension "${vsix_path}" --force >/dev/null 2>&1; then
      mkdir -p "$(dirname "${installed_vsix_stamp}")"
      printf '%s' "${vsix_sha}" > "${installed_vsix_stamp}"
      ok "installed ${C_DIM}v${current_ver}${C_RESET} into VS Code (was ${C_DIM}v${installed_ver:-none}${C_RESET})"
      hint "reload to apply: ${C_BOLD}Developer: Reload Window${C_RESET} (host code), or reopen the graph panel (webview-only changes)"
    else
      warn "code --install-extension failed via live socket"
      hint "run inside a VS Code terminal: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
    fi
  fi
fi

# ---- browser extension -----------------------------------------------

if [[ $build_browser -eq 1 ]]; then
  section "Browser extension"
  hint "${C_DIM}default install does NOT rebuild this; pass --browser to refresh${C_RESET}"
  bx_dir="${here}/extension-browser"
  # WXT writes both unpacked builds (build/<browser>-mv*) and zipped
  # artifacts (build/weft-extension-<browser>.zip) into this dir.
  # Filenames are unversioned, so each rebuild overwrites the
  # previous output and git tracks the latest zip directly.
  out_dir="${bx_dir}/build"
  if [[ ! -d "${bx_dir}" ]]; then
    fail "${bx_dir} not found"
    exit 1
  fi

  # Sign requires Mozilla AMO API keys. Check up front so we don't
  # waste five minutes building before failing.
  if [[ $do_sign -eq 1 ]]; then
    if [[ -f "${here}/.env.extension" ]]; then
      set -a
      # shellcheck disable=SC1091
      source "${here}/.env.extension"
      set +a
    fi
    if [[ -z "${WEB_EXT_API_KEY:-}" || -z "${WEB_EXT_API_SECRET:-}" ]]; then
      fail "signing needs WEB_EXT_API_KEY + WEB_EXT_API_SECRET in ${here}/.env.extension (or pass --no-sign)"
      hint "get keys: ${C_DIM}https://addons.mozilla.org/en-US/developers/addon/api/key/${C_RESET}"
      exit 1
    fi
    if ! command -v web-ext >/dev/null 2>&1; then
      fail "signing needs web-ext on PATH (or pass --no-sign)"
      hint "install: ${C_BOLD}pnpm i -g web-ext${C_RESET}"
      exit 1
    fi
  fi

  pushd "${bx_dir}" >/dev/null

  # Bump only on --bump: the version in package.json is what makes CI
  # submit a pushed build to the stores, so a plain rebuild must never
  # move it. Zip filenames stay unversioned so callers always reference
  # the same path; git history + package.json track which version is in
  # each commit.
  current_version="$(node -p "require('./package.json').version")"
  if [[ $do_bump -eq 1 ]]; then
    pnpm version patch --no-git-tag-version >/dev/null
  fi
  version="$(node -p "require('./package.json').version")"
  if [[ "${version}" != "${current_version}" ]]; then
    ok "version bumped: ${C_DIM}${current_version} ${SYM_ARROW} ${version}${C_RESET} ${C_DIM}(pushing this submits to the stores)${C_RESET}"
    bump_pending_notes+=("extension-browser/package.json is already bumped to v${version}: re-run WITHOUT --bump, or revert it")
    # A tracked signed .xpi from an older version must not outlive the
    # bump when this run will not re-sign it (signing needs both the
    # sign switch AND firefox in the target set); a stale signed build
    # beside newer zips would get committed as if it matched. Said out
    # loud: it is a TRACKED file, and a silent removal would leave the
    # working tree showing a deletion the user never made.
    if [[ $do_sign -eq 0 || $target_firefox -eq 0 ]]; then
      if [[ -f build/weft-extension-firefox.xpi ]]; then
        rm -f build/weft-extension-firefox.xpi
        warn "removed the tracked signed .xpi: it was signed for v${current_version} and this run does not re-sign. Run ${C_BOLD}./setup.sh --browser --firefox${C_RESET} (signing on) before committing."
      fi
    fi
  else
    hint "version: ${C_DIM}${version}${C_RESET}"
  fi

  # Install deps if needed.
  if [[ ! -d "node_modules" || "package.json" -nt "node_modules" ]]; then
    pnpm install
  fi

  # Per-target unpacked build.
  [[ $target_chrome  -eq 1 ]] && pnpm -s build
  [[ $target_firefox -eq 1 ]] && pnpm -s build:firefox
  [[ $target_edge    -eq 1 ]] && pnpm -s build:edge
  [[ $target_opera   -eq 1 ]] && pnpm -s build:opera
  [[ $target_safari  -eq 1 ]] && pnpm -s build:safari

  # Per-target zip. WXT writes build/weft-extension-<browser>.zip
  # SYNC: the zip basenames <-> extension-browser/wxt.config.ts (the
  #       zip artifactTemplate/sourcesTemplate + package.json "name"),
  #       .github/workflows/release.yml (publish-browser: the per-store
  #       zip paths in the submit step)
  # for each. The unversioned name means each rebuild overwrites
  # cleanly; no purge step needed.
  [[ $target_chrome  -eq 1 ]] && pnpm -s zip
  [[ $target_firefox -eq 1 ]] && pnpm -s zip:firefox
  [[ $target_edge    -eq 1 ]] && pnpm -s zip:edge
  [[ $target_opera   -eq 1 ]] && pnpm -s zip:opera
  [[ $target_safari  -eq 1 ]] && pnpm -s zip:safari

  # Optional Firefox signing. web-ext sign drops a .xpi into
  # build/ alongside the zips; we name it unversioned for the
  # same reason as the zips.
  if [[ $do_sign -eq 1 && $target_firefox -eq 1 ]]; then
    rm -f build/weft-extension-firefox.xpi
    hint "signing Firefox with web-ext ${C_DIM}(1-2 min)${C_RESET}"
    spin "web-ext sign (Firefox)" web-ext sign \
      --source-dir build/firefox-mv2 \
      --artifacts-dir build \
      --api-key="${WEB_EXT_API_KEY}" \
      --api-secret="${WEB_EXT_API_SECRET}" \
      --channel unlisted
    # web-ext picks its own filename; rename to unversioned.
    signed_xpi="$(ls -t build/*.xpi 2>/dev/null | head -1 || true)"
    if [[ -z "${signed_xpi}" ]]; then
      fail "signed .xpi not found in build/"
      exit 1
    fi
    mv -f "${signed_xpi}" "build/weft-extension-firefox.xpi"
  fi

  popd >/dev/null

  # Per-target completion summary.
  for t in chrome firefox edge opera safari; do
    var="target_${t}"
    [[ "${!var}" -ne 1 ]] && continue
    case "$t" in
      chrome)  label="Chrome / Brave / Vivaldi / Arc" ;;
      firefox) label="Firefox unsigned, load via about:debugging" ;;
      edge)    label="Edge" ;;
      opera)   label="Opera" ;;
      safari)  label="Safari (needs xcrun on macOS)" ;;
    esac
    ok "weft-extension-${t}.zip ${C_DIM}(${label})${C_RESET}"
  done
  if [[ $do_sign -eq 1 && $target_firefox -eq 1 ]]; then
    ok "weft-extension-firefox.xpi ${C_DIM}(Firefox signed)${C_RESET}"
  fi

  ok "browser extension v${version} ready in ${C_DIM}${out_dir}/${C_RESET}"
fi

# ---- post-install summary --------------------------------------------
#
# Print this once per default install (CLI + daemon + VS Code). It's
# the first thing a fresh user sees that tells them what to actually
# DO with the install they just ran.

if [[ $build_cli -eq 1 ]]; then
  case ":${PATH}:" in
    *":${bin_dir}:"*)
      path_ok=1
      ;;
    *)
      path_ok=0
      ;;
  esac

  if [[ $is_default_install -eq 1 ]]; then
    rule="$(printf '%.0s─' $(seq 1 64))"
    printf '\n%s%s%s\n' "${C_DIM}" "${rule}" "${C_RESET}"
    printf '%s%s%s %s%sSetup complete.%s\n\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_BOLD}" "${C_BLUE}" "${C_RESET}"

    printf '%s%sRunning:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    # SYNC: weft-system <-> crates/weft-core/src/infra/mod.rs (SYSTEM_NAMESPACE)
    printf '  %s%s%s dispatcher  %s%s%s  %s(kind cluster, weft-system ns)%s\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" \
      "${C_DIM}" "http://127.0.0.1:9999" "${C_RESET}" \
      "${C_DIM}" "${C_RESET}"
    printf '  %s%s%s postgres    %sin-cluster, durable across daemon restarts%s\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
    printf '  %s%s%s VS Code     %sinstalled (reload your window if it is open)%s\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
    if [[ -n "${public_url}" ]]; then
      printf '  %s%s%s public URL  %s%s%s\n' \
        "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_BOLD}" "${public_url}" "${C_RESET}"
      printf '              %sevent pushes + signal fire links ONLY; everything else 404s%s\n' \
        "${C_DIM}" "${C_RESET}"
      printf '              %sgive providers %s%s/events/<service>/<topic>%s%s; it changes if the tunnel restarts%s\n' \
        "${C_DIM}" "${C_RESET}${C_BOLD}" "${public_url}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '              %sclose it with %s./setup.sh --no-public-url%s\n' \
        "${C_DIM}" "${C_RESET}${C_BOLD}" "${C_RESET}"
    fi

    printf '\n%s%sTry it out:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %s%s%s sanity-check       %sweft daemon status%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s scaffold + run     %sweft new my-first-weave && cd my-first-weave && weft run%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s open dashboard     %shttp://127.0.0.1:9999%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"

    printf '\n%s%sBrowser extension%s %s(HumanQuery / in-page weaves)%s\n' \
      "${C_BOLD}" "${C_BLUE}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
    printf '  %sbuild it once:%s     %s./setup.sh --browser --no-sign%s\n' \
      "${C_DIM}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %sthen Load unpacked %sextension-browser/build/chrome-mv3%s %svia chrome://extensions%s\n' \
      "${C_DIM}" "${C_RESET}${C_BOLD}" "${C_RESET}" "${C_DIM}" "${C_RESET}"

    printf '\n%s%sDay-to-day:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %s%s%s tail logs          %sweft daemon logs --tail 200 -f%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s restart            %sweft daemon restart%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s wipe everything    %s./setup.sh --uninstall --purge%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '%s%s%s\n' "${C_DIM}" "${rule}" "${C_RESET}"
  fi

  if [[ $path_ok -eq 1 ]]; then
    printf '\n%s%s%s %s is on PATH. try: %sweft --help%s\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${bin_dir}" "${C_BOLD}" "${C_RESET}"
  else
    printf '\n%s%s%s %s is NOT on your PATH; add to your shell rc:\n' \
      "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "${bin_dir}"
    printf '    %sexport PATH="%s:$PATH"%s\n' "${C_BOLD}" "${bin_dir}" "${C_RESET}"
  fi
fi
