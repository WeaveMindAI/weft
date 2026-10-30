#!/usr/bin/env bash
# setup.sh: build and install everything weft-related on the local
# machine.
#
# By default, runs the install pipeline for the everyday loop:
#   - CLI                  (cargo build, copied into the install's bin
#                          folder; on main, also linked into ~/.local/bin)
#   - Daemon               (bring the local install up, or refresh it:
#                          Postgres, the object store, and weft's
#                          runtime restarted on the new binary)
#   - VS Code extension    (compile, package .vsix, install into VS Code)
#
# Which install: the branch this checkout is on decides. On `main` it is
# the default install, the one `weft` on your PATH drives. On any other
# branch it is that branch's own install, beside the default one with its
# own database, ports and binaries (in ~/.local/share/weft/installs/<name>),
# so you can build and test a branch without touching main's. A branch
# always builds from source (the published build is main's), and skips
# the VS Code and browser extensions: to try the editor on a branch, run
# scripts/dev-editor.sh. Inside the checkout, `weft` hands over to the
# branch's own binary by itself. The install this run set up is recorded
# in .weft-install, which the other scripts read.
#
# The browser extension is OPT-IN via `--browser` since rebuilding
# it signs Firefox and builds every target, which is heavier than
# most rebuild loops need.
#
# Prebuilt artifacts: on a CLEAN checkout of main at a commit CI has
# already built (the rolling `latest` release), the CLI and runtime binaries
# and the .vsix are downloaded instead of compiled and the shared
# images are pulled from the registry, so a fresh install needs no Rust or Node
# toolchain and takes minutes, not tens of minutes. Any local change
# at all, tracked or not, takes the build path for everything (the
# published artifacts are keyed to one exact commit).
#
# Disk hygiene (every install that touches docker, not just --purge):
#   - orphaned anonymous docker volumes are reaped (postgres:18
#     declares a VOLUME, so a throwaway test database whose container
#     was removed without -v strands one forever; named volumes are
#     never touched),
#   - a --cli install (the default includes it) bounds the BuildKit
#     cache to 20GB (LRU), and one that builds weft from source removes
#     from the workspace target/ every compiled piece a newer build of
#     the same piece replaced (`weft trim-cargo-target`),
#   - the daemon start removes the worker containers and images a weft
#     from before workers ran as processes left for this install (a
#     local install's workers are programs compiled on this machine),
#   - after the daemon refresh, `weft clean --images --build-plane`
#     drops the worker programs no install on this machine still uses
#     (the standard worker of the weft this update replaced among them),
#     the shared build crates no build staged for 30 days, and the
#     builder bases and node-test images an older weft left that no
#     install records using; and `weft clean --images --all` reclaims
#     everything of weft's nothing runs any more (unreferenced workers,
#     stale weft-infra-* tags, old runtimes, and what an older weft left:
#     standard worker images and BuildKit worker compile caches). KEEP
#     set = the dispatcher's referenced set (running projects' worker
#     hashes, live tasks', every project's infra image tags, every live
#     infra unit's recorded refs), so a running project's current worker
#     and a live project's infra images survive the update untouched;
#     the user resyncs a project when they want it on the new engine,
#     and that project's old worker goes on a LATER install's reclaim
#     once nothing points at it.
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
# Questions:
#   --yes         answer yes to what an install asks before it wipes
#                 something (an older weft on this machine, or this
#                 install's database from before runs were renamed), for
#                 scripts and machines with no terminal.
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
#   --verbose     show each step's output as it runs. Without it, a long
#                 step (a build, an image pull) shows a spinner with the
#                 last line it printed, and a failed step shows the end
#                 of its output and the file holding all of it.
#   An install writes a START line and a DONE/FAIL line (with the
#   section it died in) to ~/.local/share/weft/setup-runs.log. A run
#   refused on its arguments records only the FAIL; a --purge deletes
#   the journal along with the rest of the directory, so the next
#   install starts a fresh one.
#
# Public trigger surface (event triggers delivered BY providers):
#   --public-url    open a public address for the doors outside callers
#                   use (/events/..., /signal/..., the file links, live
#                   routes, the OAuth callback) through an outbound
#                   tunnel, so provider event pushes reach this local
#                   install. The management API is never on it.
#                   Persisted across runs, per install: a branch's
#                   install opens its own (its own named tunnel when the
#                   checkout's .env.local names one, else the named one
#                   it last opened, else a quick one).
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
#   --rebuild     rebuild the runtime image and recompile the standard
#                 worker program even when nothing they are built from
#                 has changed. For when either is corrupt or
#                 hand-modified; a plain run already rebuilds whatever
#                 actually moved.
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
#   --release         With --migration: write the one released migration,
#                     the one that goes in the PR, and bring your database
#                     (this checkout's install's) along: if it ran
#                     drafts, it is told the release is
#                     already in it; if it never saw one, the release runs
#                     on it now. One command either way, nothing is lost.
#   --rename SPEC     With --migration: a rename, which no diff can tell
#                     from a drop and an add (the drop throws the rows
#                     away). SPEC is table.old_column=new_column or
#                     old_table=new_table; repeat it, in the order the
#                     renames happen. A migration where something goes
#                     while something arrives beside it is refused until
#                     every such pair is named, or --unrelated says they
#                     are separate things.
#
# Removal:
#   --uninstall   Remove the default install's user-facing pieces but
#                 preserve work (a branch's install goes with
#                 scripts/remove-worktree.sh instead). Stops every
#                 install's runtime and takes it off the service manager,
#                 uninstalls the VS Code extension, drops the CLI and
#                 the runtime binary. PRESERVES: the Postgres container
#                 and its files, the object-store container + its data
#                 volume, infra units, docker images, BuildKit cache,
#                 cargo target/, browser extensions. Reinstall via
#                 ./setup.sh and your projects + history come back
#                 instantly.
#   --purge       TRUE clean slate. Deletes every container weft
#                 started (infra units, Postgres, the tunnel, and an
#                 older weft's workers) and their volumes, every
#                 weft-built docker image (weft-runtime, every weft
#                 infra image, and an older weft's weft-worker images), the object-store container + data volume +
#                 seaweedfs image, the BuildKit cache, the workspace
#                 target/ cargo cache, ~/.local/share/weft (THE
#                 DATABASE'S FILES under postgres-data/, the install's
#                 config and keys, prebuilt binaries, and every branch's
#                 install under installs/), this checkout's
#                 .weft-install, and the
#                 untracked extension build artifacts. Also deletes the
#                 Kubernetes cluster an older weft ran, if this machine
#                 still has it. The next install pays a full
#                 cold-rebuild cost. Can combine with --uninstall.
#
#                 SHARED base images (commonly reused by other
#                 docker projects on the host) are kept by default.
#                 Add the matching flag to remove them too:
#                   --postgres   remove postgres:18-alpine (and the
#                                postgres:18 image --migration pulls)
#                   --kind       remove kindest/node images (an older weft's)
#                   --debian     remove debian:trixie-slim (and the
#                                debian:bookworm-slim an older weft used)
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

# spin_status <logfile> <width>: the last non-empty line the command
# wrote, colours stripped and cut to <width>, for the spinner line.
# Progress bars rewrite one line with \r, so those count as line ends.
spin_status() {
  tail -c 4096 "$1" 2>/dev/null \
    | tr '\r' '\n' \
    | sed -e $'s/\033\\[[0-9;?]*[A-Za-z]//g' -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' \
    | awk 'NF { last = $0 } END { print last }' \
    | cut -c "1-$2"
}

# spin "label" cmd args... -- run cmd with its output written to a log
# file, showing a spinner, the time so far and the last line the command
# wrote, so a long build shows it is moving without flooding the
# terminal. On success the line becomes `✓ label (time)` and the log is
# deleted. On failure it becomes `✗ label`, the log's last 60 lines are
# shown, and the log is kept and named so the whole output can be read.
# --verbose streams the command's output instead. With no terminal, one
# line before and one after.
spin() {
  local label="$1"; shift
  if [[ $verbose -eq 1 ]]; then
    step "${label}"
    if "$@"; then
      ok "${label}"
      return 0
    else
      local rc=$?
      fail "${label}"
      return $rc
    fi
  fi
  # One mktemp log per call: a fixed /tmp name would let two concurrent
  # runs interleave and delete each other's output (and hand any local
  # user a symlink-overwrite seat).
  local logfile started rc=0
  logfile="$(mktemp -t weft-setup.XXXXXX.log)"
  started=$SECONDS
  if [[ $HAS_TTY -eq 0 ]]; then
    printf '  %s %s\n' "${SYM_STEP}" "${label}"
    "$@" >"${logfile}" 2>&1 || rc=$?
  else
    "$@" >"${logfile}" 2>&1 &
    local pid=$! i=0 status="" cols width
    cols="$(tput cols 2>/dev/null || echo 80)"
    while kill -0 "${pid}" 2>/dev/null; do
      local frame="${SP_FRAMES[$((i % ${#SP_FRAMES[@]}))]}"
      # The status line is re-read every half second, not every frame:
      # tail and sed per frame would cost more than the spinner shows.
      if (( i % 6 == 0 )); then
        width=$(( cols - ${#label} - 16 ))
        status=""
        (( width > 10 )) && status="$(spin_status "${logfile}" "${width}")"
      fi
      printf '\r\033[K  %s%s%s %s %s(%ss)%s %s%s%s' \
        "${C_CYAN}" "${frame}" "${C_RESET}" "${label}" \
        "${C_DIM}" "$((SECONDS - started))" "${C_RESET}" \
        "${C_DIM}" "${status}" "${C_RESET}"
      sleep 0.08
      i=$((i + 1))
    done
    wait "${pid}" || rc=$?
    printf '\r\033[K'
  fi
  if [[ $rc -eq 0 ]]; then
    ok "${label} ${C_DIM}($((SECONDS - started))s)${C_RESET}"
    rm -f "${logfile}"
    return 0
  fi
  fail "${label}"
  tail -n 60 "${logfile}" >&2 || true
  hint "the whole output is in ${logfile}"
  return $rc
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

# weft_image_ids, remove_docker_object, remove_docker_images_by_id and
# what --uninstall / --purge remove, shared with scripts/scrub-old-install.sh.
# shellcheck source=scripts/lib/weft-cleanup.sh
. "${here}/scripts/lib/weft-cleanup.sh"
# with_build_slot (one heavy cargo build at a time on this machine), and
# machine_lock_release_mine for the exit.
# shellcheck source=scripts/lib/machine-lock.sh
. "${here}/scripts/lib/machine-lock.sh"
# weft_which (which install a weft binary acts on from this checkout)
# and weft_record_checkout_install (.weft-install).
# shellcheck source=scripts/lib/checkout-install.sh
. "${here}/scripts/lib/checkout-install.sh"
# weft_vscode_link_borrowers and weft_vscode_compile, shared with
# scripts/dev-editor.sh.
# shellcheck source=scripts/lib/vscode-build.sh
. "${here}/scripts/lib/vscode-build.sh"

# One docker-daemon reachability probe, the same shape everywhere it
# gates a best-effort action. (The --purge refusal keeps its own
# PATH-then-daemon form: it must TELL docker-missing and daemon-down
# apart in its error, so it does not share this shape.)
docker_reachable() {
  docker version --format '{{.Server.Version}}' >/dev/null 2>&1
}

# The engine's major version, empty when the daemon is unreachable.
# Gates the one step whose SAFETY depends on the engine generation
# (the anonymous-only scope of `docker volume prune`, Docker >= 23).
docker_server_major() {
  # `|| true`: under pipefail an unreachable daemon would otherwise
  # fail the pipeline and, through the caller's assignment, the whole
  # script under set -e; empty output IS the unreachable answer.
  { docker version --format '{{.Server.Version}}' 2>/dev/null || true; } | cut -d. -f1
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

# One `code` call through a live IPC socket, with a deadline.
#
# Every `code` call here is answered by the extension host of the window
# that owns the socket, and that host is somebody else's process: busy
# with its own extensions, or wedged behind a socket a closed terminal
# left connectable. A plain `--list-extensions` on a loaded remote
# window has been measured at over a minute, printing nothing the whole
# time, which reads as a hung install. So no call here is unbounded, and
# the caller decides what a timeout means.
#
# Exit status is `code`'s own, or 124 when the deadline passed.
code_ipc() {
  local sock="$1" deadline="$2"
  shift 2
  VSCODE_IPC_HOOK_CLI="${sock}" with_deadline "${deadline}" code "$@"
}

# Run a command with a deadline in seconds (fractions allowed), in
# plain bash: macOS ships no `timeout`, and asking every Mac to install
# coreutils for four call sites is the wrong trade. Exit status is the
# command's own, or 124 when the deadline passed, the same contract as
# `timeout`, so the callers read it the same way.
with_deadline() {
  local secs="$1"
  shift
  "$@" &
  local pid=$!
  ( sleep "${secs}"; kill "${pid}" 2>/dev/null ) &
  local killer=$!
  local rc=0
  wait "${pid}" || rc=$?
  # The killer is still sleeping when the command finished on its own;
  # a killer that already fired is gone and the kill is a harmless miss.
  kill "${killer}" 2>/dev/null
  wait "${killer}" 2>/dev/null || true
  # 143 is "killed by SIGTERM", which is what the killer sends.
  if [[ $rc -eq 143 ]]; then
    return 124
  fi
  return "${rc}"
}

# Nothing asks the window what it has installed any more: the answer
# costs a round trip to its extension host (over a minute, measured, on
# a loaded remote window) and both callers can get it cheaper. The
# install side reads the stamp it wrote last time; the uninstall side
# just tries the uninstall, which answers the same question by working
# or not.

# Print the path of a LIVE VS Code IPC socket (closed terminals leave dead
# sockets behind, so probe for one that answers). VS Code writes them
# under /run/user/$UID on Linux and under $TMPDIR on macOS, so both are
# searched, newest first. Used by both the install side and the
# uninstall/reinstall side; defined once here so the two can't drift.
# Returns non-zero if no live socket is found.
pick_live_vscode_socket() {
  local sock
  for sock in $(ls -1t "/run/user/${UID}"/vscode-ipc-*.sock "${TMPDIR:-/tmp}"/vscode-ipc-*.sock 2>/dev/null); do
    if command -v socat >/dev/null 2>&1; then
      if with_deadline 0.3 socat -u /dev/null UNIX-CONNECT:"${sock}" >/dev/null 2>&1; then
        printf '%s' "${sock}"; return 0
      fi
    elif command -v nc >/dev/null 2>&1; then
      if with_deadline 0.3 nc -U -z "${sock}" >/dev/null 2>&1; then
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
migration_renames=()
do_release=0
rebuild_flag=""
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
# --yes: answer yes to every question asked before a wipe.
assume_yes=0
# --verbose: stream every step's output instead of the spinner.
verbose=0

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
run_log_dir="${weft_state_dir}"
run_log="${run_log_dir}/setup-runs.log"
# Best-effort at both ends: the journal is a diagnostic aid, and a
# root-owned ~/.local/share/weft (docker can auto-create such entries)
# must not hard-kill the very install that would explain it.
if ! mkdir -p "${run_log_dir}" 2>/dev/null || ! { : >> "${run_log}"; } 2>/dev/null; then
  run_log=/dev/null
  warn "cannot write the run journal under ${run_log_dir}; continuing without it"
fi
# The journal keeps the last 400 lines (a couple of hundred runs), so it
# never grows without end.
if [[ "${run_log}" != /dev/null ]] && [[ "$(wc -l < "${run_log}")" -gt 400 ]]; then
  tail -n 400 "${run_log}" > "${run_log}.tmp" 2>/dev/null && mv "${run_log}.tmp" "${run_log}" 2>/dev/null || rm -f "${run_log}.tmp"
fi
current_section="(argument parsing)"
# Whether this run has started changing the install (its binaries, its
# daemon, its extensions): until then a failure leaves the install as it
# was, and the exit line says so.
install_touched=0
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
  # A build slot this run still holds (it died mid-build) goes back
  # first, so a waiting run elsewhere does not sit on it.
  machine_lock_release_mine
  if [[ ${code} -eq 0 ]]; then
    printf '%s DONE  ok\n' "$(date '+%Y-%m-%d %H:%M:%S')" >> "${run_log}"
  else
    printf '%s FAIL  exit=%s in section %s\n' \
      "$(date '+%Y-%m-%d %H:%M:%S')" "${code}" "${current_section}" >> "${run_log}"
    local journal_note=""
    [[ "${run_log}" != /dev/null ]] && journal_note=" (journal: ${run_log})"
    local state_note="the install is incomplete"
    [[ ${install_touched} -eq 0 ]] && state_note="nothing was installed"
    printf '  %s%s%s this run FAILED in %s%s%s; %s%s\n' \
      "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "${C_BOLD}" "${current_section}" "${C_RESET}" \
      "${state_note}" "${journal_note}" >&2
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
    --rename)
      shift
      [[ $# -gt 0 ]] || { fail "--rename needs what: --rename table.old_column=new_column, or --rename old_table=new_table"; exit 1; }
      migration_renames+=(--rename "$1") ;;
    --unrelated) migration_renames+=(--unrelated) ;;

    --from-source) from_source=1 ;;
    --yes)         assume_yes=1 ;;
    --verbose)     verbose=1 ;;

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
    || -n "${public_url_flag}" || -n "${rebuild_flag}" \
    || $from_source -eq 1 || "${profile}" != "release" || $assume_yes -eq 1 \
    || $component_flag_seen -eq 1 || $no_daemon_seen -eq 1 ]]; then
    fail "an --uninstall / --purge run builds and refreshes nothing, so --bump, --no-sign, --migration, --public-url/--no-public-url, --rebuild, --debug, --from-source, --yes, the component flags (--cli/--daemon/--vscode/--browser), --no-daemon and the browser-target flags do not apply here"
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

# The full install (CLI + daemon + VS Code, no browser), the one shape
# that gets the post-install summary. Computed ONCE, here,
# where every component flag is final; an uninstall/purge run is never
# an install, whatever the component defaults say.
is_full_install=0
if [[ $do_uninstall -eq 0 && $do_purge -eq 0 \
  && $build_cli -eq 1 && $refresh_daemon -eq 1 && $build_vscode -eq 1 && $build_browser -eq 0 ]]; then
  is_full_install=1
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
# and source tree they were built from plus a sha256 per asset. When
# this checkout holds that source with no local change, downloading is
# equivalent to building,
# so the install skips the compilers entirely (no Rust or Node
# toolchain needed). Any local change, a different source tree, or no
# network takes the build path instead, and every branch says so.
#
# SYNC: release tag + asset names + manifest.json keys (commit, tree,
#       vscode_version, sha256_<asset>) <->
#       .github/workflows/release.yml (the linux and macos matrix `asset`
#       values, the vsix `mv` target, the release job's manifest
#       generation, release-macos), scripts/release-cli.sh
release_assets_url="https://github.com/WeaveMindAI/weft/releases/download/latest"
# Where downloads land before they are installed: shared by every
# checkout, and only ever main's release, which only the default install
# runs.
prebuilt_dir="${weft_state_dir}/prebuilt"
prebuilt_commit=""
prebuilt_vscode_version=""
use_prebuilt_cli=0
use_prebuilt_vsix=0
cli_sha256=""
runtime_sha256=""
vsix_sha256=""
cli_asset=""
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)   cli_asset="weft-x86_64-linux" ;;
  Linux-aarch64)  cli_asset="weft-aarch64-linux" ;;
  Darwin-arm64)   cli_asset="weft-aarch64-macos" ;;
  Darwin-x86_64)  cli_asset="weft-x86_64-macos" ;;
esac
# The runtime published beside each CLI.
# SYNC: asset names <-> .github/workflows/release.yml (the runtime binaries)
runtime_asset="${cli_asset:+${cli_asset/weft-/weft-runtime-}}"
# The standard worker published beside each CLI: the program a local
# install's projects run when their nodes are all the standard library's.
# SYNC: the worker asset name and layout <-> .github/actions/build-binaries, crates/weft-cli/src/host_programs.rs (pack_standard_program)
worker_asset="${cli_asset:+${cli_asset/weft-/weft-worker-}}"
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
  if [[ -n "${write_migration}" ]]; then
    # The migration lands in the source this run, and the runtime
    # carries every migration in its binary: the published one cannot.
    hint "--migration adds a migration to the source; building locally"
    return 0
  fi
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
  # The source tree, not the commit: a merge that brings in exactly the
  # published source is another commit with the same tree.
  prebuilt_tree="$(manifest_value "tree")"
  if [[ -z "${prebuilt_tree}" ]]; then
    hint "the published manifest names no source tree; building locally"
    return 0
  fi
  if [[ "${prebuilt_tree}" != "$(git -C "${here}" rev-parse 'HEAD^{tree}')" ]]; then
    hint "no prebuilt artifacts for this source yet (latest published: commit ${prebuilt_commit:0:12}); building locally. If you just pulled, CI is likely still building; re-run in a while to download instead."
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
      runtime_sha256="$(manifest_value "sha256_${runtime_asset}")"
      if [[ -n "${cli_sha256}" && -n "${runtime_sha256}" ]]; then
        use_prebuilt_cli=1
      else
        hint "this commit's ${cli_asset} or ${runtime_asset} is not published (still building in CI, or its build failed); building the CLI and the runtime locally. Re-run in a while to download instead."
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

# The published standard worker, unpacked where the install's workers
# start from: <data dir>/workers/<its hash>/ (the hash travels inside the
# asset, the binary hash the CLI computes for this same source). Without
# it the install's first start compiles the standard worker here, once.
install_prebuilt_standard_worker() {
  local want_sha
  want_sha="$(manifest_value "sha256_${worker_asset}")"
  if [[ -z "${worker_asset}" || -z "${want_sha}" ]]; then
    hint "no published standard worker for this commit and machine; the install's first start compiles it here, once"
    return 0
  fi
  local packed="${prebuilt_dir}/${worker_asset}"
  if ! acquire_prebuilt "${worker_asset}" "${want_sha}" "${packed}"; then
    warn "the published standard worker could not be downloaded; the install's first start compiles it here, once"
    return 0
  fi
  local hash
  if ! hash="$(tar -xzOf "${packed}" hash)" || ! [[ "${hash}" =~ ^[0-9a-f]+$ ]]; then
    warn "the published standard worker names no binary hash; the install's first start compiles it here, once"
    return 0
  fi
  local dest="${weft_state_dir}/workers/${hash}"
  if [[ -f "${dest}/worker" ]]; then
    return 0
  fi
  local unpacking="${dest}.unpacking.$$"
  rm -rf "${unpacking}" && mkdir -p "${unpacking}"
  tar -xzf "${packed}" -C "${unpacking}" worker left-out.json
  if [[ -e "${dest}" ]]; then
    # A folder with no program yet (a compile stopped halfway, or one
    # running now, which then puts the same program over this one): the
    # record and then the program go into it by rename, the order a
    # compile places them in. Its holds stay.
    if ! mv -f "${unpacking}/left-out.json" "${dest}/left-out.json" || ! mv -f "${unpacking}/worker" "${dest}/worker"; then
      rm -rf "${unpacking}"
      warn "the published standard worker could not be put in ${dest}; the install's first start compiles it here, once"
      return 0
    fi
    rm -rf "${unpacking}"
  else
    mv "${unpacking}" "${dest}"
  fi
  ok "standard worker ready ${C_DIM}(${hash:0:12}, from the published build)${C_RESET}"
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
# Writing it needs a Postgres to build two schemas in and compare them, which
# must not be the one weft runs, so this runs a throwaway container and takes
# it away again. Releasing also needs to reach the real one (this checkout's
# install's), to swap its drafts for the released file; the generator finds
# that one itself, the same way `weft` picks the install from where it runs,
# which is why it runs from this checkout.
if [[ ${#migration_renames[@]} -gt 0 && -z "${write_migration}" ]]; then
  fail "--rename and --unrelated only mean something with --migration <name>"
  exit 1
fi
if [[ $do_release -eq 1 && -z "${write_migration}" ]]; then
  fail "--release only means something with --migration <name>: it collapses that run's drafts"
  exit 1
fi

if [[ -n "${write_migration}" ]]; then
  section "migration"
  install_touched=1
  # Everything this run's shape needs, named up front instead of a
  # bare command-not-found mid-run: cargo (the schema generator
  # compiles from source) and docker (the throwaway
  # scratch postgres).
  migration_missing=""
  command -v cargo >/dev/null 2>&1 || migration_missing="cargo (install Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh)"
  command -v docker >/dev/null 2>&1 \
    || migration_missing="${migration_missing:+${migration_missing}; }docker (runs the scratch postgres)"
  if [[ -n "${migration_missing}" ]]; then
    fail "--migration needs: ${migration_missing}"
    exit 1
  fi
  # shellcheck source=scripts/lib/throwaway-postgres.sh
  . "${here}/scripts/lib/throwaway-postgres.sh"
  # Chain onto the script's existing exit logger so a Ctrl-C mid-run
  # leaves no container behind.
  migration_cleanup() {
    # -v: the scratch postgres leaves an anonymous volume behind on a
    # plain rm (see scripts/lib/throwaway-postgres.sh).
    [[ -n "${THROWAWAY_PG_CONTAINER:-}" ]] \
      && docker rm -f -v "${THROWAWAY_PG_CONTAINER}" >/dev/null 2>&1 || true
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
  fi
  # One generator for every table the install's database holds. It
  # reads the migration history off the disk, so a new group's origin.sql
  # it freezes is planned from in the same run, and it applies and
  # records a release itself: the install below only rebuilds weft with
  # the new files in it. A failure leaves this command re-runnable: the
  # generator only deletes
  # the drafts after the released file is recorded on the live
  # database, and a released change answers "nothing changed" on the
  # re-run.
  # SYNC: the example name <-> crates/weft-runtime/Cargo.toml ([[example]] schema_migration)
  run_schema_migration() {
    (cd "${here}" && cargo run -q -p weft-runtime --example schema_migration \
      -- "${write_migration}" ${release_flag} ${migration_renames[@]+"${migration_renames[@]}"})
  }
  if ! with_build_slot run_schema_migration; then
    fail "writing the migration failed; fix the cause and re-run the same command"
    exit 1
  fi
  migration_cleanup
  trap 'log_run_exit $?' EXIT
  hint "carrying on with the install, which applies it"
fi

bin_dir="${prefix}/bin"
weft_bin="${bin_dir}/weft"
# Tab completion. fish loads a script from its completions folder by
# itself; bash and zsh are told in their startup file, inside a block
# between these two marker lines, which setup rewrites on every install
# and removes on uninstall. Everything between the markers is setup's.
fish_completion_file="${XDG_CONFIG_HOME:-${HOME}/.config}/fish/completions/weft.fish"
completion_rc_files=("${ZDOTDIR:-${HOME}}/.zshrc" "${HOME}/.bashrc")
completion_block_begin='# >>> weft tab completion (written by weft setup.sh) >>>'
completion_block_end='# <<< weft tab completion <<<'
# Take the block out of one startup file. Rewritten through `cat >` so
# a startup file that is a link (a dotfiles repo) stays a link.
remove_completion_block() { # <rc file>
  [[ -f "$1" ]] && grep -qF "${completion_block_begin}" "$1" || return 0
  local kept
  kept="$(awk -v b="${completion_block_begin}" -v e="${completion_block_end}" \
    '$0 == b { skip = 1; next } $0 == e { skip = 0; next } !skip' "$1")" || return 1
  printf '%s\n' "${kept}" > "$1"
}

# The install this run acts on, as `weft daemon which` answers it from
# this checkout (WEFT_INSTALL if set, else the checkout's branch: `main`
# is the default install, any other branch its own), so nothing here
# works it out a second time. Sets install_env / install_dir /
# install_picked_by (what chose it, for messages), the two binaries'
# homes in the install's bin folder, and on_default_install.
# `too_old_fix` is what to tell the user when `weft` predates installs
# per branch and cannot answer.
# SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
resolve_install() { # weft too_old_fix
  local rc=0
  weft_which "${here}" "$1" || rc=$?
  if [[ $rc -eq 2 ]]; then
    fail "$2"
    exit 1
  elif [[ $rc -ne 0 ]]; then
    fail "could not ask $1 which install this checkout uses (its answer is above)"
    exit 1
  fi
  install_env="${WHICH_ENV}"
  install_dir="${WHICH_DIR}"
  if [[ -n "${WHICH_BRANCH}" ]]; then
    install_picked_by="this checkout's branch, ${WHICH_BRANCH}"
  else
    install_picked_by="WEFT_INSTALL=${WEFT_INSTALL:-}"
  fi
  # SYNC: the runtime's home <-> crates/weft-cli/src/commands/daemon.rs (runtime_binary)
  installed_bin="${WHICH_BIN}/weft"
  installed_runtime="${WHICH_BIN}/weft-runtime"
  on_default_install=0
  if [[ "${install_env}" == "default" ]]; then
    on_default_install=1
  fi
  hint "install: ${C_BOLD}${install_env}${C_RESET}${C_DIM} (${install_dir})${C_RESET}"
}

# Put a binary at `dest`: copied to a temp beside it and renamed over
# it, the same way download_verified lands a download. A `weft` that is
# still running (the extension in another VS Code window, a `weft run`
# in a terminal) holds the old file open, and overwriting it in place
# fails with "Text file busy"; a rename swaps the path while every
# running process keeps its old inode until it exits.
install_binary() { # src dest
  local src="$1" dest="$2" tmp err
  mkdir -p "$(dirname "${dest}")"
  tmp="${dest}.$$.build"
  err="$(cp "${src}" "${tmp}" 2>&1 >/dev/null && chmod 0755 "${tmp}" && mv -f "${tmp}" "${dest}" 2>&1 >/dev/null)" || {
    rm -f "${tmp}"
    fail "could not install ${src} to ${dest}"
    printf '%s\n' "${err}" >&2
    exit 1
  }
}

# cargo, run from the checkout: cargo reads its config (the checkout's
# .cargo/config.toml, which weft-runtime refuses to build without) from
# the folder it runs in, never from where --manifest-path points.
cargo_in_checkout() {
  (cd "${here}" && cargo "$@")
}

# Build the CLI and the runtime from this checkout into target/<profile>
# (sets target_dir). Sources can change WHILE this script runs (an AI
# session editing in parallel with a reinstall is the everyday case
# here). cargo builds what existed when it started, so a source edit
# landing mid-build ships a binary that is ALREADY stale, silently.
# Build, then check whether any workspace source is newer than the
# produced binary; if so, build again (once more is always enough: the
# recheck is instant), and if it STILL moves, say so loudly instead of
# pretending the install is current. Returns non-zero on a failed build.
build_cli_from_source() {
  local build_pass recheck_out
  # A bare word, not an array: expanding an EMPTY array under `set -u`
  # is fatal on bash 3.2 (macOS's /bin/bash), same idiom as
  # ${rebuild_flag} elsewhere.
  local release_flag=""
  if [[ "${profile}" == "release" ]]; then
    target_dir="${here}/target/release"
    release_flag="--release"
  else
    target_dir="${here}/target/debug"
  fi
  for build_pass in 1 2 3; do
    # Explicit, never left to `set -e`: the build slot runs this
    # function as a condition, where errexit is off, and a failed build
    # would otherwise go on to install whatever an earlier one left.
    spin "cargo build ${release_flag:+${release_flag} }-p weft-cli -p weft-runtime" \
      cargo_in_checkout build ${release_flag} -p weft-cli -p weft-runtime \
      || return 1
    if [[ ! -x "${target_dir}/weft" || ! -x "${target_dir}/weft-runtime" ]]; then
      fail "build output missing: ${target_dir}/weft or ${target_dir}/weft-runtime"
      exit 1
    fi
    # cargo's own freshness check is the authority on whether the
    # binary matches the sources (mtime scans over the tree false-alarm
    # on files that are not build inputs). A repeated build doing zero
    # work == current; it doing work == a source edit landed mid-build
    # and the first binary shipped stale, so loop. The `||` keeps a
    # recheck FAILURE from dying silently under `set -e` (the
    # assignment's status is cargo's), with the captured output shown
    # instead of discarded.
    recheck_out="$(cargo_in_checkout build ${release_flag} -p weft-cli -p weft-runtime 2>&1)" || {
      fail "the freshness recheck build failed (a source edit landed mid-build?)"
      printf '%s\n' "${recheck_out}" >&2
      exit 1
    }
    # [[:space:]], never \s: BSD grep (macOS) has no \s and would
    # read the pattern as a literal, silently disabling this check.
    if ! grep -q '^[[:space:]]*Compiling' <<<"${recheck_out}"; then
      return 0
    fi
    if [[ "${build_pass}" -eq 3 ]]; then
      fail "sources are still changing under the build; the installed binary does NOT include them. Re-run setup.sh once the edits settle."
      exit 1
    fi
    hint "source changed during the build; building again"
  done
}

# Fail listing every tool in `missing` (with its line in
# `install_hints`), deduplicated, so one round trip installs them all.
# Returns when nothing is missing.
fail_on_missing_tools() { # section
  [[ ${#missing[@]} -gt 0 ]] || return 0
  local item s already
  local seen=() unique_missing=() seen_hints=() unique_hints=()
  # Dedupe (node + pnpm may be requested by multiple components).
  for item in "${missing[@]}"; do
    already=0
    for s in "${seen[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
    if [[ $already -eq 0 ]]; then
      unique_missing+=("$item")
      seen+=("$item")
    fi
  done
  for item in "${install_hints[@]}"; do
    already=0
    for s in "${seen_hints[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
    if [[ $already -eq 0 ]]; then
      unique_hints+=("$item")
      seen_hints+=("$item")
    fi
  done

  section "$1"
  fail "missing required tools:"
  for item in "${unique_missing[@]}"; do
    printf '    %s%s%s %s\n' "${C_RED}" "${SYM_ARROW}" "${C_RESET}" "${item}" >&2
  done
  printf '\n'
  hint "install:"
  for item in "${unique_hints[@]}"; do
    printf '    %s%s%s\n' "${C_DIM}" "${item#  }" "${C_RESET}" >&2
  done
  printf '\n'
  hint "re-run ${C_BOLD}./setup.sh${C_RESET}${C_DIM} once everything above is on PATH${C_RESET}"
  exit 1
}

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
  fi

  # node and pnpm, for the extensions, are checked once the install is
  # known: only the default install builds them.
  fail_on_missing_tools "Pre-flight"
fi

# ---- uninstall / purge ----------------------------------------------

if [[ $do_uninstall -eq 1 || $do_purge -eq 1 ]]; then
  # --uninstall takes the DEFAULT install's pieces, and --purge every
  # install on the machine, so both are refused from a checkout whose
  # own install is another one (a branch's) rather than reaching past
  # it. The default weft answers which install that is, or else the one
  # this checkout recorded; WEFT_INSTALL=default says it is the default
  # install you mean, with nothing to ask. With neither weft there (an
  # uninstall finishing what an earlier one started), this checkout set
  # up no install of its own.
  uninstall_asker=""
  if [[ -x "${weft_bin}" ]]; then
    uninstall_asker="${weft_bin}"
  elif [[ -f "${here}/${weft_install_record}" ]]; then
    uninstall_asker="$(head -n1 "${here}/${weft_install_record}" | tr -d '\r')/bin/weft"
    [[ -x "${uninstall_asker}" ]] || uninstall_asker=""
  fi
  if [[ "${WEFT_INSTALL:-}" == "default" ]]; then
    uninstall_asker=""
  elif [[ -z "${uninstall_asker}" && -n "${WEFT_INSTALL:-}" ]]; then
    fail "WEFT_INSTALL=${WEFT_INSTALL} names another install than the default one, which --uninstall and --purge take; unset it, or remove that install with ${C_BOLD}weft daemon remove${C_RESET}"
    exit 1
  fi
  if [[ -n "${uninstall_asker}" ]]; then
    resolve_install "${uninstall_asker}" "${uninstall_asker} is older than installs per branch, so it cannot say whether this checkout uses the default install. If it is the default install you mean to take off, say so: ${C_BOLD}WEFT_INSTALL=default ./setup.sh ${orig_args}${C_RESET}"
    if [[ $on_default_install -eq 0 ]]; then
      if [[ $do_purge -eq 1 ]]; then
        fail "this run's install is ${install_env} (picked by ${install_picked_by}), and --purge wipes every install on this machine, the default one and every branch's, with all their data. To take only this install off, run ${C_BOLD}scripts/remove-worktree.sh ${here}${C_RESET} (or ${C_BOLD}weft daemon remove${C_RESET} in this checkout to keep the worktree); to purge everything, run it from your main checkout"
      else
        fail "this run's install is ${install_env} (picked by ${install_picked_by}); --uninstall removes the default install. To take this install off, run ${C_BOLD}scripts/remove-worktree.sh ${here}${C_RESET} (or ${C_BOLD}weft daemon remove${C_RESET} in this checkout to keep the worktree)"
      fi
      exit 1
    fi
  else
    # SYNC: the default install's bin folder <-> crates/weft-core/src/infra/install.rs (Install::bin_dir)
    installed_bin="${weft_state_dir}/bin/weft"
    installed_runtime="${weft_state_dir}/bin/weft-runtime"
  fi

  if [[ $do_uninstall -eq 1 ]]; then
    section "Uninstall"
    install_touched=1
    hint "${C_DIM}preserving data; pass --purge to wipe everything${C_RESET}"

    # 1. Stop weft's runtime, then take it off the service manager so
    #    it does not come back at the next login. Best-effort: a
    #    missing CLI or an already-stopped runtime doesn't fail. Named
    #    as the default install and run from this checkout, so the front
    #    door never hands over to another install.
    if command -v "${weft_bin}" >/dev/null 2>&1 || [[ -L "${weft_bin}" ]]; then
      if (cd "${here}" && WEFT_INSTALL=default "${weft_bin}" daemon stop) >/dev/null 2>&1; then
        ok "weft's runtime stopped (data kept)"
      else
        hint "weft's runtime: already stopped; nothing to stop"
      fi
    else
      hint "weft's runtime: no weft binary on disk; nothing to stop"
    fi
    # Every install's runtime (named ones too) off the service manager,
    # and any a detached start left running.
    weft_stop_runtimes

    # 2. Remove the VS Code extension if `code` is on PATH. Uses
    #    the same live-IPC-socket discovery as the install side
    #    so an open VS Code window picks up the change without a
    #    manual reload.
    if command -v code >/dev/null 2>&1; then
      if live_sock="$(pick_live_vscode_socket)"; then
        # Asking first whether it is installed costs a round trip to
        # the window's extension host; the uninstall answers the same
        # question by working or not, and fails harmlessly when there
        # is nothing there.
        if code_ipc "${live_sock}" 180 --uninstall-extension "${ext_id}" >/dev/null 2>&1; then
          ok "VS Code extension uninstalled"
          rm -f "${weft_state_dir}/vscode-hashes/installed.sha"
        else
          hint "VS Code extension: not installed, or the window was busy; if it is still there: code --uninstall-extension ${ext_id}"
        fi
      elif with_deadline 180 code --uninstall-extension "${ext_id}" >/dev/null 2>&1; then
        ok "VS Code extension uninstalled"
        rm -f "${weft_state_dir}/vscode-hashes/installed.sha"
      else
        # With no live window this path also fires for "not
        # installed"; the hedge is honest here.
        hint "VS Code extension: not installed, or no window to reach; if it is still there: code --uninstall-extension ${ext_id}"
      fi
    else
      hint "VS Code: 'code' not on PATH; skipping extension removal"
    fi

    # 3. Drop the CLI symlink AND the binary it points at. The binary
    #    is an installed piece, not work, so an uninstall that left it
    #    behind would leave ~25MB the user has no reason to look for.
    if [[ -L "${weft_bin}" || -f "${weft_bin}" ]]; then
      rm -f "${weft_bin}"
      ok "removed ${C_DIM}${weft_bin}${C_RESET}"
    else
      hint "CLI symlink at ${weft_bin} already absent"
    fi
    for installed in "${installed_bin}" "${installed_runtime}"; do
      if [[ -f "${installed}" ]]; then
        rm -f "${installed}"
        ok "removed ${C_DIM}${installed}${C_RESET}"
      fi
    done
    if [[ -f "${fish_completion_file}" ]]; then
      rm -f "${fish_completion_file}"
      ok "removed ${C_DIM}${fish_completion_file}${C_RESET}"
    fi
    for rc_file in "${completion_rc_files[@]}"; do
      if [[ -f "${rc_file}" ]] && grep -qF "${completion_block_begin}" "${rc_file}"; then
        if remove_completion_block "${rc_file}"; then
          ok "removed weft tab completion from ${C_DIM}${rc_file}${C_RESET}"
        else
          warn "could not edit ${rc_file}; delete the lines between the two '# weft tab completion' markers by hand"
        fi
      fi
    done

    # 4. Hints for the manual cleanup we deliberately don't do.
    #    Only relevant when uninstall runs without --purge; if the
    #    user chained --purge, those things are already gone.
    if [[ $do_purge -eq 0 ]]; then
      printf '\n%s%sWhat is preserved:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
      printf '  %sPostgres%s %s(the weft-postgres container, still running)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %s~/.local/share/weft%s %s(the database: postgres, history, projects; manifest stamps, prebuilt binaries)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sobject store%s %s(weft-object-store container + data volume)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_DIM}" "${C_RESET}"
      printf '  %sdocker images%s %s(weft-runtime, weft-infra-*)%s\n' \
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
    install_touched=1
    hint "${C_DIM}true clean slate; next install pays a full rebuild cost${C_RESET}"

    # A purge with the docker CLI present but the daemon down cannot do
    # most of its job, and every "is it there?" probe below would read
    # absence into the silence: refuse up front instead of printing a
    # wall of green over untouched state.
    if command -v docker >/dev/null 2>&1 \
      && ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
      fail "the docker daemon is not reachable, so nothing docker holds can be purged: weft's containers, images, the object-store container + volume, and the BuildKit cache all remain"
      hint "start docker and re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
      exit 1
    fi

    # 1. Every install's runtime, so nothing writes while its data goes
    #    (a purge without --uninstall has not stopped it yet), then the
    #    Kubernetes cluster an older weft ran, when this machine still
    #    has it.
    weft_stop_runtimes
    weft_delete_old_cluster

    # 2. Every container and volume weft started, then every image it
    #    built or ran. (The daemon answered the gate above, so "not
    #    present" below really means absent.)
    if ! command -v docker >/dev/null 2>&1; then
      warn "docker not on PATH; everything docker holds (images, the object store, the BuildKit cache) is untouched"
    else
      weft_remove_containers_and_volumes
      weft_remove_images || {
        fail "could not list weft's images (docker stopped answering?); re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
        exit 1
      }
      if docker image prune --force \
        --filter "label=weft.dev/project" >/dev/null 2>&1; then
        ok "pruned dangling project-labelled images"
      else
        warn "could not prune the dangling project-labelled images"
      fi

      # Shared base images, gated. `--postgres` covers BOTH postgres
      # images weft pulls: the install's one and the --migration
      # scratch one (leaving the scratch image behind would break the
      # clean-slate floor with a ~450MB orphan nobody is told about).
      # SYNC: postgres image tags <-> crates/weft-cli/src/commands/daemon.rs
      #       (POSTGRES_IMAGE, 18-alpine),
      #       scripts/lib/throwaway-postgres.sh (the scratch image, 18)
      if [[ $purge_postgres -eq 1 ]]; then
        remove_docker_object image postgres:18-alpine " ${C_DIM}(--postgres)${C_RESET}"
        remove_docker_object image postgres:18 " ${C_DIM}(--postgres, the --migration scratch image)${C_RESET}"
      fi
      if [[ $purge_kind -eq 1 ]]; then
        weft_remove_kind_images " ${C_DIM}(--kind)${C_RESET}" || {
          fail "could not list the kindest/node images (docker stopped answering?); re-run ${C_BOLD}./setup.sh --uninstall --purge${C_RESET}"
          exit 1
        }
      fi
      if [[ $purge_debian -eq 1 ]]; then
        remove_docker_object image debian:trixie-slim " ${C_DIM}(--debian)${C_RESET}"
        remove_docker_object image debian:bookworm-slim " ${C_DIM}(--debian, an older weft's)${C_RESET}"
      fi

      # The object store's image: niche enough that nothing else on the
      # host wants it, so a purge removes it unconditionally.
      # SYNC: seaweedfs image <-> crates/weft-cli/src/commands/daemon.rs (OBJECT_STORE_IMAGE)
      remove_docker_object image chrislusf/seaweedfs:4.51
      remove_docker_object image chrislusf/seaweedfs:3.80 " ${C_DIM}(an older weft's)${C_RESET}"

      if docker buildx prune --force >/dev/null 2>&1; then
        ok "pruned BuildKit cache"
      else
        warn "could not prune the BuildKit cache; prune it by hand: docker buildx prune --force"
      fi
    fi

    # 3. Workspace cargo target/ + the staged builder-base docker context
    # a `weft build-images` run leaves (both derived artifacts,
    # regenerated on the next build). A failed removal warns and the
    # purge keeps going: dying here would abandon every later step over
    # one stubborn directory.
    remove_dir_reporting "${here}/target" "${C_DIM}target/${C_RESET}" \
      "" "; remove it by hand: rm -rf ${here}/target"
    # The staging lock lives BESIDE the dir (see
    # weft-cli images.rs ensure_builder_base_at), so it needs its
    # own removal or it survives the purge.
    remove_dir_reporting "${here}/.weft-base-context" "${C_DIM}.weft-base-context/${C_RESET}" \
      " ${C_DIM}(builder-base staging context)${C_RESET}" "; remove it by hand: rm -rf ${here}/.weft-base-context"
    remove_dir_reporting "${here}/.weft-base-context.lock" "${C_DIM}.weft-base-context.lock${C_RESET}"
    # The record of which install this checkout set up: every install
    # goes below.
    remove_dir_reporting "${here}/${weft_install_record}" "${C_DIM}${weft_install_record}${C_RESET}"

    # 4. Daemon-local state, every install's database among it.
    if [[ -d "${weft_state_dir}" ]]; then
      if weft_remove_state_dir; then
        ok "removed ${C_DIM}~/.local/share/weft/${C_RESET}"
        # The run journal lived in the directory we just purged.
        # Recreating it to append the exit line would undo the clean
        # slate, so the journal ends here for this run.
        run_log=/dev/null
      else
        warn "could not remove ${C_DIM}~/.local/share/weft/${C_RESET}; remove it manually: sudo rm -rf ~/.local/share/weft"
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
      for debian_image in debian:trixie-slim debian:bookworm-slim; do
        if docker image inspect "$debian_image" >/dev/null 2>&1; then
          skipped_lines+=("$debian_image|--debian")
        fi
      done
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

# ---- an older weft (every install that touches docker) -----------------
#
# The weft that ran on a local Kubernetes cluster cannot be upgraded in
# place: its database starts a new schema history and the runtime
# refuses to boot beside it. So an install that finds one offers to wipe
# it first (scripts/scrub-old-install.sh), then carries on as a fresh
# install. A machine with nothing to wipe sees none of this.
if [[ $build_cli -eq 1 || $refresh_daemon -eq 1 ]] && weft_old_install_present; then
  section "An older weft"
  warn "an older weft (the one that ran on a local Kubernetes cluster) is on this machine,"
  warn "and this version cannot carry it forward. It is going to be wiped: the projects on"
  warn "your disk are not touched, but their run history, versions and stored connections are."
  named_installs="$(weft_named_installs)"
  if [[ -n "${named_installs}" ]]; then
    warn "These named installs go too, with their databases: ${named_installs}."
  fi
  if [[ $assume_yes -eq 0 ]]; then
    if [[ ! -t 0 ]]; then
      fail "no terminal to ask on; run ./setup.sh --yes to wipe it and install"
      exit 1
    fi
    # End of input (Ctrl-D) is a no: under set -e a failed read would
    # otherwise end the run without a word.
    read -r -p "  Do you want to proceed? [Y/n] " answer || answer=n
    if [[ -n "${answer}" && "${answer}" != "y" && "${answer}" != "Y" ]]; then
      hint "nothing wiped and nothing installed; run ./setup.sh again when you are ready"
      exit 1
    fi
  fi
  install_touched=1
  "${here}/scripts/scrub-old-install.sh" --yes || { fail "the wipe did not finish; see above, then run ./setup.sh again"; exit 1; }
  ok "the older weft is gone; installing this one"
fi

# ---- disk hygiene (every install) --------------------------------------
#
# One docker-level reap runs on every docker-touching install, not just
# --cli: the
# orphaned ANONYMOUS volume leak is not CLI-specific (postgres:18
# declares a VOLUME, so any throwaway test database whose container was
# removed without -v strands one forever). `docker volume prune` (no
# -a) touches ONLY anonymous volumes no container references: the
# object store's and any running test's named/attached volumes are not
# anonymous, so they are safe by construction. That anonymous-only
# scope is Docker >= 23 behavior: an older engine prunes unused NAMED
# volumes too, and the object store's named volume is unattached
# whenever its container is between a remove and a recreate, so on
# such an engine this step would delete every stored file. The floor
# is PROBED here, before the prune, because this section runs on every
# flag combination, ahead of anything else that would fail on an old
# engine (the `--max-used-space` bound lives in the CLI section).
# A silent skip would let the user believe the reap happened, so an
# unreachable daemon or an old engine says so; and unlike the CLI
# section's BuildKit bound (loud by design: it is the user-visible
# disk-pressure relief), this reap is junk collection, so a failed
# prune WARNS and the install continues: hygiene must not fail the
# install.
# Only on an install that touches docker at all (the CLI build or the
# daemon refresh): a --vscode-only install never asked for docker, so
# a "docker unreachable" warning there would read as a failed install.
if [[ $build_cli -eq 1 || $refresh_daemon -eq 1 ]]; then
section "Disk hygiene"
docker_major="$(docker_server_major)"
if [[ -z "${docker_major}" ]]; then
  warn "docker unreachable; the orphaned anonymous-volume reap is skipped this install"
elif [[ ! "${docker_major}" =~ ^[0-9]+$ || "${docker_major}" -lt 23 ]]; then
  warn "docker engine ${docker_major:-?} is older than 23, whose volume prune would also take unused NAMED volumes; the anonymous-volume reap is skipped this install"
elif spin "reap orphaned anonymous docker volumes" \
  docker volume prune --force; then
  :
else
  warn "the anonymous-volume reap failed; re-run ${C_BOLD}docker volume prune --force${C_RESET} by hand"
fi
fi

# ---- CLI -------------------------------------------------------------

if [[ $build_cli -eq 1 ]]; then
  section "CLI"

  # ---- disk hygiene, BEFORE the build -------------------------------
  #
  # docker's BuildKit layer cache is bounded on every --cli install (the
  # default includes it): every runtime and infra image build adds
  # entries, and nothing prunes them. (The anonymous-volume reap is NOT
  # here: it runs on every install regardless of flags, in its own
  # section above. The cargo target/ is trimmed after the build, below.)
  # Gate on daemon REACHABILITY, not binary presence: a --cli install
  # on a machine whose docker isn't running must still produce a
  # binary. With a live daemon, a failed prune fails loud; with none,
  # SAY the bound was skipped instead of letting silence read as done.
  # --all and no age filter: without --all BuildKit leaves most records
  # out of the prune, and an `until` filter makes everything used inside
  # its window untouchable whatever the cap says; both let the cache grow
  # far past the cap (once to 293GB). Least recently used goes first, so
  # what the last builds used is the last to go.
  # SYNC: 20GB and the flags <-> crates/weft-platform-local/src/images.rs
  #       (BUILD_CACHE_CAP, build_cache_bound_args)
  if docker_reachable; then
    spin "bound BuildKit cache to 20GB (least recently used first)" \
      docker builder prune --force --all --max-used-space 20GB
  else
    warn "docker unreachable; the BuildKit cache bound is skipped this install"
  fi

  # Prebuilt path: this checkout matches the commit CI built, so the
  # published binary IS this source compiled. It lands in the shared
  # prebuilt folder first: the published build is main's, and only the
  # binary's own answer below says whether this checkout is on main. A
  # failed download drops to the local build when a toolchain is there
  # (the same present -> pull -> build ladder the daemon images use).
  if [[ $use_prebuilt_cli -eq 1 ]]; then
    if ! acquire_prebuilt "${cli_asset}" "${cli_sha256}" "${prebuilt_dir}/${cli_asset}" 0755 \
      || ! acquire_prebuilt "${runtime_asset}" "${runtime_sha256}" "${prebuilt_dir}/${runtime_asset}" 0755; then
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
  # The binary this run installs is the one that says where it goes:
  # the download, unless it answers with an install other than the
  # default one, which always builds from source (the published build
  # is the default install's); then the build, asked again.
  if [[ $use_prebuilt_cli -eq 1 ]]; then
    resolve_install "${prebuilt_dir}/${cli_asset}" "the published weft (${prebuilt_dir}/${cli_asset}) has no ${C_BOLD}weft daemon which${C_RESET}, though it was built from this very source; re-run with ${C_BOLD}--from-source${C_RESET} and report it"
    if [[ $on_default_install -eq 1 ]]; then
      new_cli="${prebuilt_dir}/${cli_asset}"
      new_runtime="${prebuilt_dir}/${runtime_asset}"
      install_prebuilt_standard_worker
    else
      use_prebuilt_cli=0
      if ! command -v cargo >/dev/null 2>&1; then
        fail "this run's install is ${install_env} (picked by ${install_picked_by}), which always builds from source (the published build is the default install's), and no Rust toolchain is on PATH"
        exit 1
      fi
      hint "this run's install is ${install_env} (picked by ${install_picked_by}), which always builds from source (the published build is the default install's)"
    fi
  fi

  if [[ $use_prebuilt_cli -eq 0 ]]; then
    hint "${C_DIM}cargo's incremental cache makes re-runs near-instant${C_RESET}"
    # One heavy build at a time on this machine (parallel worktrees
    # once ran it out of memory): a run that finds another building
    # says so and waits its turn.
    with_build_slot build_cli_from_source
    new_cli="${target_dir}/weft"
    new_runtime="${target_dir}/weft-runtime"
    # cargo never deletes what a newer build replaced, so target/ grows
    # by a copy of every piece each dependency move or update touches.
    # Each piece kept is the newest of its kind (one per way it is built:
    # tests, clippy, this release build), so no build that follows
    # compiles more than it would have. A failed trim costs only disk.
    if trimmed="$("${new_cli}" trim-cargo-target "${here}/target" 2>&1)"; then
      ok "${trimmed} from target/"
    else
      warn "could not trim target/: ${trimmed}"
    fi
    # An unplaced binary (one in target/) answers for the checkout it
    # runs in, which is how this run learns where to install before
    # anything is installed.
    resolve_install "${new_cli}" "${new_cli}, just built from this checkout, has no ${C_BOLD}weft daemon which${C_RESET}; is this checkout older than installs per branch? Then use the setup.sh that came with it"
  fi
else
  # This run builds no CLI, so the install's own weft, through the
  # default install's front door, says which install this checkout uses.
  if [[ ! -x "${weft_bin}" ]]; then
    fail "no weft at ${weft_bin} to ask which install this checkout uses; run ./setup.sh with --cli (or no component flags) first"
    exit 1
  fi
  resolve_install "${weft_bin}" "${weft_bin} is older than installs per branch, so it cannot say which install this checkout uses. Run a full ${C_BOLD}./setup.sh${C_RESET} in your main checkout to install a weft that can, or run this one with ${C_BOLD}--cli${C_RESET} so it builds its own"
fi

# ---- what this install takes ------------------------------------------
#
# Decided now that the install is known and before anything is copied
# into it, so a refused run has touched nothing installed. Only the
# default install gets the extensions: a branch's editor is
# scripts/dev-editor.sh, and every editor keeps the default install's.
# A branch's run that would have built the editor prepares its sources
# instead, for the checks (see "the editor's sources" below).
prepare_editor_sources=0
if [[ $on_default_install -eq 0 ]]; then
  if [[ $do_bump -eq 1 ]]; then
    fail "--bump builds the extensions to release them, and install ${install_env} (picked by ${install_picked_by}) builds no extension; bump in the package folder instead: ${C_BOLD}pnpm version patch --no-git-tag-version${C_RESET}"
    exit 1
  fi
  prepare_editor_sources=$build_vscode
  if [[ $build_vscode -eq 1 || $build_browser -eq 1 ]]; then
    hint "install ${install_env} (picked by ${install_picked_by}) gets no VS Code or browser extension (your editor keeps the default install's); to try this checkout's editor, run ${C_BOLD}scripts/dev-editor.sh${C_RESET}"
    build_vscode=0
    build_browser=0
  fi
fi
# The oldest Node the extension builds run on: vsce, ovsx and wxt need 22,
# and the Vite both extensions build with needs 22.12 on that line.
node_floor_major=22
node_floor_minor=12
node_floor="${node_floor_major}.${node_floor_minor}"
node_meets_floor() {
  local version
  version="$(node -v 2>/dev/null)" || return 1
  [[ "$version" =~ ^v([0-9]+)\.([0-9]+) ]] || return 1
  (( BASH_REMATCH[1] > node_floor_major || (BASH_REMATCH[1] == node_floor_major && BASH_REMATCH[2] >= node_floor_minor) ))
}
# What the extension builds need, now that it is known they run.
missing=()
install_hints=()
if [[ $build_vscode -eq 1 && $use_prebuilt_vsix -eq 0 ]]; then
  if ! node_meets_floor; then
    missing+=("node ${node_floor}+")
    install_hints+=("  Node:    https://nodejs.org/en/download (or 'nvm install 24')")
  fi
  if ! command -v pnpm >/dev/null 2>&1; then
    missing+=("pnpm")
    install_hints+=("  pnpm:    npm i -g pnpm")
  fi
fi
if [[ $build_browser -eq 1 ]]; then
  if ! node_meets_floor; then
    missing+=("node ${node_floor}+ (browser extension)")
    install_hints+=("  Node:    https://nodejs.org/en/download (or 'nvm install 24')")
  fi
  if ! command -v pnpm >/dev/null 2>&1; then
    missing+=("pnpm (browser extension)")
    install_hints+=("  pnpm:    npm i -g pnpm")
  fi
fi
fail_on_missing_tools "Extensions"

# ---- a database from before runs were renamed --------------------------
#
# A database from before runs were renamed cannot be carried forward: the
# runtime refuses to boot on one, and the CLI this run installs cannot
# talk to the runtime still serving it. So the question is asked here,
# before anything is installed, and a no leaves the install exactly as it
# was; the wipe itself waits for the daemon step, which stops the old
# runtime first. A run that installs the CLI without refreshing the
# daemon is refused instead: it would put that CLI in front of the old
# runtime.
# SYNC: resource names <-> crates/weft-core/src/infra/install.rs (resource_prefix)
wipe_stale_db=0
if [[ $build_cli -eq 1 || $refresh_daemon -eq 1 ]] && command -v docker >/dev/null 2>&1; then
  if [[ "${install_env}" == "default" ]]; then install_prefix="weft"; else install_prefix="weft-${install_env}"; fi
  pg_container="${install_prefix}-postgres"
  if ! stale_db="$(weft_database_before_runs "${pg_container}")"; then
    fail "could not read the database in ${pg_container} to check its age; see docker, then run ./setup.sh again"
    exit 1
  fi
  if [[ "${stale_db}" == "yes" ]]; then
    section "A database from before runs were renamed"
    if [[ $refresh_daemon -eq 0 ]]; then
      fail "this install's database (${install_env}) was built by a weft from before runs were renamed, and this run installs a CLI that cannot talk to it without refreshing the daemon; nothing installed. Run ./setup.sh without --no-daemon (or with --daemon) to wipe and rebuild it"
      exit 1
    fi
    warn "this install's database (${install_env}) was built by a weft from before runs were renamed,"
    warn "and this version cannot carry it forward. It is going to be wiped: the projects on your"
    warn "disk are not touched, but their run history, versions, stored connections, stored files"
    warn "and running infra are."
    if [[ $assume_yes -eq 0 ]]; then
      if [[ ! -t 0 ]]; then
        fail "no terminal to ask on; nothing wiped and nothing installed; run ./setup.sh --yes to wipe it and install"
        exit 1
      fi
      # End of input (Ctrl-D) is a no.
      read -r -p "  Do you want to proceed? [Y/n] " answer || answer=n
      if [[ -n "${answer}" && "${answer}" != "y" && "${answer}" != "Y" ]]; then
        hint "nothing wiped and nothing installed; run ./setup.sh again when you are ready"
        exit 1
      fi
    fi
    wipe_stale_db=1
  fi
fi

# ---- nothing on while weft is replaced ---------------------------------
#
# An update replaces the running weft, and an older and a newer weft never
# run side by side: the install has nothing on (a trigger, an infra copy, a
# run going), which `weft maintenance on` gets it to, and `weft maintenance
# off` brings back after. Asked of the running install, before anything is
# replaced, by the CLI this run installs (`weft maintenance check`, which
# lets through an install that is not set up, not running, or whose weft
# predates the maintenance). Only when this run replaces the runtime: one
# whose runtime binary is already the installed one restarts nothing.
if [[ $refresh_daemon -eq 1 ]]; then
  checker="${new_cli:-${installed_bin}}"
  replaces_runtime=1
  if [[ -n "${new_runtime:-}" && -x "${installed_runtime}" ]] && cmp -s "${new_runtime}" "${installed_runtime}"; then
    replaces_runtime=0
  fi
  if [[ $replaces_runtime -eq 1 && -x "${checker}" ]]; then
    if ! "${checker}" help maintenance >/dev/null 2>&1; then
      hint "${checker} has no ${C_BOLD}weft maintenance${C_RESET}, so whether something is on cannot be checked; going ahead"
    elif ! maintenance_check="$(env WEFT_REPO_ROOT="${here}" WEFT_INSTALL="${install_env}" "${checker}" maintenance check --update ./setup.sh 2>&1)"; then
      fail "${maintenance_check#error: }"
      exit 1
    else
      hint "${maintenance_check}"
    fi
  fi
fi

# Every check that can refuse this run without touching the install is
# above; from here on it changes the install.
install_touched=1

# What the other scripts and the development editor read to find this
# checkout's install, once it is in place: after the daemon start when
# this run refreshes it, else as soon as the binaries are.
record_checkout_install() {
  if ! weft_record_checkout_install "${here}" "${install_dir}"; then
    fail "could not write ${here}/${weft_install_record}, which the scripts and the development editor read to find this checkout's install"
    exit 1
  fi
}

if [[ $build_cli -eq 1 ]]; then

  # A prebuilt binary bakes its BUILDER's checkout path as its
  # compile-time repo location, which does not exist here; record where
  # the repo actually lives (THIS tree, every run, so a moved or
  # re-cloned checkout re-points it) so the binary can find its own
  # catalog / manifests / build context. Written BEFORE the binary lands
  # on PATH: the other order could publish a weft that cannot find its
  # checkout. Loud on failure WITH the recovery. Only the prebuilt path
  # writes it, and that path is main's, so the default install's.
  # SYNC: repo-root file <-> crates/weft-catalog/src/lib.rs (weft_repo_root, rung 4)
  if [[ $use_prebuilt_cli -eq 1 ]]; then
    repo_root_file="${weft_state_dir}/repo-root"
    if ! { mkdir -p "$(dirname "${repo_root_file}")" \
        && printf '%s' "${here}" > "${repo_root_file}"; } 2>/dev/null; then
      fail "the prebuilt CLI needs ${repo_root_file} to find this checkout, and that path is not writable"
      hint "fix the ownership (docker can leave root-owned entries there): ${C_BOLD}sudo chown -R \"\$(id -u):\$(id -g)\" ~/.local/share/weft${C_RESET}${C_DIM}, then re-run${C_RESET}"
      exit 1
    fi
  fi

  # Copy the binaries OUT of target/ (or the download folder) into the
  # install's own bin folder, which is where `weft daemon start` runs the
  # runtime from and where the default install's PATH link points.
  # Linking straight into target/ made the build cache load-bearing:
  # `rm -rf target` (or the size cap above firing) left `weft` as a
  # dangling symlink with nothing saying why. The copy costs ~25MB and
  # makes target/ pure cache, safe to delete at any moment.
  install_binary "${new_cli}" "${installed_bin}"
  install_binary "${new_runtime}" "${installed_runtime}"
  ok "installed ${C_DIM}${installed_bin}${C_RESET} and its runtime ${C_DIM}(install ${install_env})${C_RESET}"

  # Only the default install owns the `weft` on PATH and the shell's tab
  # completion; a branch's weft is reached through it (inside the
  # checkout, `weft` hands over to the branch's own binary).
  if [[ $on_default_install -eq 1 ]]; then
    mkdir -p "${bin_dir}"
    ln -sfn "${installed_bin}" "${weft_bin}"
    ok "linked ${C_DIM}${weft_bin}${C_RESET} ${SYM_ARROW} ${C_DIM}${installed_bin}${C_RESET}"

    # Tab completion. The shell's side only knows how to ask `weft` for
    # the candidates (`COMPLETE=<shell> weft` prints it), so the commands
    # and flags always come from the installed binary. The startup-file
    # block regenerates that script in every new shell, so it always
    # matches the binary; the fish file is rewritten on every install for
    # the same reason. A shell that cannot be set up only loses tab
    # completion, so a failure warns and the install carries on.
    #
    # Every zsh and bash startup file that exists gets its block, plus the
    # login shell's even when it does not exist yet (a fresh Mac has no
    # ~/.zshrc). `$SHELL` alone is not enough: it names the LOGIN shell,
    # and a bashrc that ends in `exec zsh` makes every terminal a zsh. A
    # block only ever runs in its own shell, so an extra one costs nothing.
    for completion_shell in zsh bash; do
      if [[ "${completion_shell}" == zsh ]]; then
        completion_rc="${completion_rc_files[0]}"
      else
        completion_rc="${completion_rc_files[1]}"
      fi
      [[ -f "${completion_rc}" || "$(basename "${SHELL:-}")" == "${completion_shell}" ]] || continue
      if err="$( {
          remove_completion_block "${completion_rc}" \
          && { [[ ! -s "${completion_rc}" || -z "$(tail -c 1 "${completion_rc}")" ]] || printf '\n' >> "${completion_rc}"; } \
          && {
            printf '%s\n' "${completion_block_begin}"
            # zsh registers completions through `compdef`, which only
            # exists once the completion system is loaded; a bare zshrc
            # (the macOS default) never loads it.
            if [[ "${completion_shell}" == zsh ]]; then
              printf '%s\n' '(( $+functions[compdef] )) || { autoload -Uz compinit && compinit; }'
            fi
            printf 'if [ -x "%s" ]; then eval "$(COMPLETE=%s "%s")"; fi\n' "${weft_bin}" "${completion_shell}" "${weft_bin}"
            printf '%s\n' "${completion_block_end}"
          } >> "${completion_rc}"
        } 2>&1 )"; then
        ok "${completion_shell} tab completion ${SYM_ARROW} ${C_DIM}${completion_rc}${C_RESET} (new terminals)"
      else
        warn "${completion_shell} tab completion not set up in ${completion_rc}: ${err}"
      fi
    done
    if command -v fish >/dev/null 2>&1; then
      if err="$( { mkdir -p "$(dirname "${fish_completion_file}")" && COMPLETE=fish "${weft_bin}" >"${fish_completion_file}"; } 2>&1 )"; then
        ok "fish tab completion ${SYM_ARROW} ${C_DIM}${fish_completion_file}${C_RESET}"
      else
        rm -f "${fish_completion_file}"
        warn "fish tab completion not installed: ${err}"
      fi
    fi
  else
    hint "this branch's weft is ${C_BOLD}${installed_bin}${C_RESET}${C_DIM}; inside this checkout, the ${C_RESET}${C_BOLD}weft${C_RESET}${C_DIM} on your PATH hands over to it${C_RESET}"
  fi
fi
if [[ $refresh_daemon -eq 0 && -x "${installed_bin}" ]]; then
  record_checkout_install
fi

# ---- daemon refresh / start ------------------------------------------
#
# One verb whether weft is running or not: `weft daemon start` brings the
# local install up (or refreshes it) and restarts weft's runtime on the
# binary this run just installed.

if [[ $refresh_daemon -eq 1 ]]; then
  section "Daemon"
  if [[ ! -x "${installed_bin}" ]]; then
    fail "install ${install_env} has no weft at ${installed_bin} to start it with; run ./setup.sh with --cli (or no component flags)"
    exit 1
  elif ! command -v docker >/dev/null 2>&1; then
    warn "docker not on PATH; skipping"
    record_checkout_install
  else
    # The install's own weft, told which install it is: it would pick
    # the same one from this checkout, and saying so keeps every child
    # it starts on it too.
    install_env_cmd=(env WEFT_REPO_ROOT="${here}" WEFT_INSTALL="${install_env}" "${installed_bin}")

    # The database from before runs were renamed, agreed to above before
    # anything was installed: wiped now, with everything only it gave a
    # meaning to (its stored files, its projects' workers and infra), and
    # the start below builds a fresh one. Only this install's: its config,
    # its keys and every other install stay as they are.
    if [[ $wipe_stale_db -eq 1 ]]; then
      "${install_env_cmd[@]}" daemon stop >/dev/null 2>&1 || true
      weft_wipe_database "${pg_container}" "${install_dir}" "${install_env}" "${install_prefix}" || { fail "the database of ${install_env} could not be removed; see above, then run ./setup.sh again"; exit 1; }
      ok "the old database of ${install_env} is gone; starting on a fresh one"
    fi

    # In the build slot: a start whose runtime image moved builds it
    # around the weft-runtime this run installed (a copy, seconds), or,
    # when this machine's glibc is newer than the image's Debian and the
    # start says so, compiles it with cargo inside docker; and one whose
    # standard worker moved compiles that on this machine: as heavy as
    # the CLI build.
    spin "starting weft (pulls or builds its images the first time)" \
      with_build_slot "${install_env_cmd[@]}" daemon start ${rebuild_flag} ${public_url_flag}
    record_checkout_install

    # Drop what an engine change strands (the standard worker program of
    # the weft this update replaced, and the builder bases and node-test
    # images an older weft left), now that the start above has recorded
    # what this install uses: the CLI keeps everything any install on
    # this machine records, and removes nothing of a kind while an install
    # has not recorded yet (it names that install). Warn-only: hygiene
    # must not fail the install.
    if spin "removing builder files and images nothing uses any more" \
      "${install_env_cmd[@]}" clean --images --build-plane; then
      :
    else
      warn "the build-plane image sweep failed; re-run ${C_BOLD}weft clean --images --build-plane${C_RESET} by hand"
    fi

    # Reclaim what the update stranded, now that the refreshed daemon
    # can answer for what is still in use: `weft clean --images --all`
    # keeps exactly the dispatcher's referenced set (running projects'
    # current workers, draining workers', live tasks', every project's
    # infra image tags, every live infra unit's recorded image refs)
    # and removes everything else of weft's: unreferenced workers,
    # stale `weft-infra-*` tags, old runtimes, and what an older weft
    # left.
    # This is the "old version gone, running things untouched" pass:
    # a project the user has not resynced yet KEEPS its old-engine
    # worker (it is still its running pointer) and the reclaim catches
    # it on a later install once the resync moves the pointer.
    # No health re-probe before it: `weft daemon start` returning 0
    # already implies the runtime answered (the CLI waits for it as
    # the start's last step), and a failed start exits this
    # script under set -e. The clean itself fails loudly if the daemon
    # became unreachable in between, and the warn below is the recovery
    # hint. Warn-only either way: hygiene must not fail the install.
    # Not safe against a `weft build`/`run` in flight in another window
    # (a fresh image is referenced by nothing until its register lands;
    # that run fails loudly and rebuild heals), so it runs AFTER the
    # reconcile, when this window's own builds are done.
    # Under the spinner like every step: what it removed is in its log,
    # and --verbose shows it as it happens.
    # No address passed: the CLI reads the port the start above just
    # saved in ports.json (a WEFT_DISPATCHER_URL the user exported still
    # reaches it through the environment).
    if spin "removing weft images nothing runs any more" \
      "${install_env_cmd[@]}" clean --images --all; then
      :
    else
      warn "the image reclaim failed; re-run ${C_BOLD}weft clean --images --all${C_RESET} by hand once the daemon is settled"
    fi
  fi
elif [[ $build_cli -eq 1 ]]; then
  # Worker and infra images are only ever reclaimed through the
  # daemon refresh above (the reclaim needs the daemon's referenced
  # set, and the engine sweep deliberately leaves workers alone: a
  # running project keeps its old-engine image until it resyncs). A
  # silent skip would let unreferenced worker images pile up on a
  # --cli / --no-daemon workflow with nothing ever saying so; a
  # --vscode-only install builds no images and gets no such line.
  section "Daemon"
  hint "daemon refresh skipped, so no image reclaim this install; run ${C_BOLD}weft clean --images --all${C_RESET} with the daemon up to reclaim stale worker/infra images"
fi

# The address the daemon recorded for the install's public trigger
# surface, when it is open. Read from the install's own state file so
# setup.sh never re-derives it (one authority, and `weft daemon status`
# reads the same).
# SYNC: public-url-enabled, public-url <-> crates/weft-cli/src/commands/daemon.rs (public_url_marker, public_url_file)
public_url=""
if [[ -r "${install_dir}/public-url-enabled" \
   && -r "${install_dir}/public-url" ]]; then
  public_url="$(tr -d '[:space:]' < "${install_dir}/public-url")"
fi
if [[ -n "${public_url}" && ( $is_full_install -eq 0 || $on_default_install -eq 0 ) ]]; then
  # Only a full install on main prints the summary block, so say it here.
  hint "public surface: ${C_BOLD}${public_url}${C_RESET}${C_DIM} (event pushes, signal fire links, file share links, OAuth callback)"
fi

# ---- the editor's sources, on a branch --------------------------------
#
# A branch's install gets no editor (scripts/dev-editor.sh runs one from
# source when you want to try it), but the checks every change to the
# editor or the graph runs (`pnpm run compile`, `svelte-check`, the
# packages' tests) need the extension's dependencies installed and the
# shared packages borrowing them: the same wiring CI does
# (`borrow-packages`) and dev-editor.sh does. Without it every check in a
# fresh worktree fails on missing modules in files nobody touched.
if [[ $prepare_editor_sources -eq 1 ]]; then
  section "Editor sources"
  ext_dir="${here}/extension-vscode"
  if ! command -v pnpm >/dev/null 2>&1; then
    warn "pnpm is not on PATH, so the editor's checks (pnpm run compile, svelte-check) cannot run in this checkout; install it with ${C_BOLD}npm i -g pnpm${C_RESET} and run ./setup.sh again"
  else
    weft_vscode_link_borrowers "${ext_dir}"
    if [[ ! -d "${ext_dir}/node_modules" || "${ext_dir}/package.json" -nt "${ext_dir}/node_modules" || "${ext_dir}/pnpm-lock.yaml" -nt "${ext_dir}/node_modules" ]]; then
      spin "pnpm install (extension-vscode)" pnpm -C "${ext_dir}" install --prefer-offline \
        || { fail "the editor's dependencies did not install; see above, then run ./setup.sh again"; exit 1; }
    fi
    ok "the editor's sources are ready for their checks"
  fi
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
  if [[ $build_cli -eq 0 && -x "${installed_bin}" ]]; then
    # The reference is the BINARY's own mtime, carried onto a temp
    # file with `touch -r` (which dereferences the symlink; GNU
    # `find -newer <symlink>` does NOT, it reads the link's own mtime,
    # which every install refreshes whether or not the binary moved).
    bin_ref="$(mktemp -t weft-binref.XXXXXX)"
    if ! touch -r "${installed_bin}" "${bin_ref}"; then
      rm -f "${bin_ref}"
      warn "cannot read ${installed_bin}'s timestamp; skipping the stale-binary check"
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
        fail "cannot scan for sources newer than ${installed_bin}: $(cat "${find_err}")"
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
        hint "install Node ${node_floor}+ and pnpm, or re-run once the network is back"
        exit 1
      fi
    fi
  fi

  # The whole local build as one named operation (like migration_cleanup
  # above): compile, fingerprint, bump, package. Sets `vsix_path` +
  # `current_ver`; called just below when no prebuilt .vsix landed.
  build_vsix_locally() {
  if ! node_meets_floor; then
    fail "needs Node ${node_floor}+ (got $(node -v 2>/dev/null || echo 'none'))"
    hint "try: ${C_BOLD}nvm use 24 && ./setup.sh --vscode${C_RESET}"
    exit 1
  fi
  if ! command -v pnpm >/dev/null 2>&1; then
    fail "pnpm not on PATH"
    hint "install: ${C_BOLD}npm i -g pnpm${C_RESET}"
    exit 1
  fi

  pushd "${ext_dir}" >/dev/null

  # Up front, not inside the rebuild branch, so a bare
  # `pnpm run check:webview` resolves too (scripts/lib/vscode-build.sh).
  weft_vscode_link_borrowers "${ext_dir}"

  # Skip rebuild if nothing under src/ + config + package.json has
  # changed since the last successful build. We hash inputs and
  # compare to a stamp file. The .vsix from the last run is reused
  # for the install step.
  hash_dir="${weft_state_dir}/vscode-hashes"
  hash_file="${hash_dir}/extension.hash"
  # The fingerprint is EVERYTHING the package can ship: every tracked
  # file of the extension and its source-consumed sibling packages
  # (weft-graph's webview, weft-syntax's grammars, weft-connect's
  # picker), plus untracked
  # not-ignored ones (a just-created source file must not be invisible
  # to the stamp), hashed by content. A hand-kept file list here
  # repeatedly went stale (.vscodeignore, media/, the README all ship
  # and were once missing); enumerating from git closes the class.
  # Ignored files (out/, node_modules/, the media/webview bundle built
  # from weft-graph's sources) stay out, which also keeps a build from
  # dirtying its own stamp. A deleted-but-still-tracked file hashes as
  # its absence, so a mid-refactor worktree changes the fingerprint
  # instead of killing the run.
  if git -C "${here}" rev-parse HEAD >/dev/null 2>&1; then
    current_hash="$(
      {
        git -C "${here}" ls-files -z extension-vscode packages/weft-graph packages/weft-syntax packages/weft-connect
        git -C "${here}" ls-files -z --others --exclude-standard \
          extension-vscode packages/weft-graph packages/weft-syntax packages/weft-connect
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
    weft_vscode_compile "${ext_dir}" spin
    rm -f weft-vscode-*.vsix
    # The repo LICENSE rides inside the .vsix (the stores refuse a
    # licenseless extension; the local package matches CI's).
    cp "${here}/LICENSE" LICENSE
    spin "package .vsix" pnpm dlx @vscode/vsce package --no-dependencies
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

  # Whether VS Code already runs these exact bytes. The version alone
  # cannot say (it moves only on --bump, so most rebuilds keep it), so a
  # stamp remembers the sha of the last .vsix handed over. The sha is
  # the whole key: the version lives inside the package, so two versions
  # can never share one.
  #
  # Matching the stamp costs nothing, and that is the point. Both ways of
  # asking VS Code itself are a round trip to somebody else's extension
  # host, measured on a loaded remote window at over a minute to list the
  # extensions and minutes to hand over a package it already has. Neither
  # is worth paying to learn something a local file already knows.
  #
  # The stamp can be wrong in exactly one way: the extension was removed
  # in VS Code without setup.sh. Then this says "nothing to install"
  # while nothing is installed, and the fix is the line printed below.
  # That is worth minutes of everybody's time on every other run.
  vsix_sha="$(sha256 "${vsix_path}" | cut -d' ' -f1)"
  installed_vsix_stamp="${weft_state_dir}/vscode-hashes/installed.sha"
  if ! command -v code >/dev/null 2>&1; then
    warn "'code' not on PATH"
    hint "install manually: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
  elif [[ "$(cat "${installed_vsix_stamp}" 2>/dev/null)" == "${vsix_sha}" ]]; then
    ok "VS Code already has ${C_DIM}v${current_ver}${C_RESET} with these exact bytes; nothing to install"
    hint "if it is missing from VS Code, install it by hand: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
  elif ! live_sock="$(pick_live_vscode_socket)"; then
    warn "no live VS Code window found"
    hint "run inside a VS Code terminal: ${C_BOLD}code --install-extension '${vsix_path}' --force${C_RESET}"
  else
    # Only now, on the path that installs anyway, and bounded: the
    # window answers when its extension host gets to it.
    step "handing v${current_ver} to VS Code (its extension host answers when it can)"
    if code_ipc "${live_sock}" 180 --install-extension "${vsix_path}" --force >/dev/null 2>&1; then
      mkdir -p "$(dirname "${installed_vsix_stamp}")"
      printf '%s' "${vsix_sha}" > "${installed_vsix_stamp}"
      ok "installed ${C_DIM}v${current_ver}${C_RESET} into VS Code"
      hint "reload to apply: ${C_BOLD}Developer: Reload Window${C_RESET} (host code), or reopen the graph panel (webview-only changes)"
    else
      warn "VS Code did not take the extension (it may have been busy)"
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
# Print this once per full install (CLI + daemon + VS Code) of the
# default install. It's the first thing a fresh user sees that tells
# them what to actually DO with the install they just ran. The PATH
# check runs on a branch too: the branch's weft is reached through the
# default install's link.

if [[ $build_cli -eq 1 ]]; then
  case ":${PATH}:" in
    *":${bin_dir}:"*)
      path_ok=1
      ;;
    *)
      path_ok=0
      ;;
  esac

  # The PATH line comes first: until it is fixed, none of the commands
  # below work.
  if [[ $path_ok -eq 0 ]]; then
    printf '\n%s%s%s %sweft is not on your PATH yet.%s Add this line to your shell rc, then open a new terminal:\n' \
      "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '    %sexport PATH="%s:$PATH"%s\n' "${C_BOLD}" "${bin_dir}" "${C_RESET}"
  fi

  if [[ $is_full_install -eq 1 && $on_default_install -eq 1 ]]; then
    printf '\n%s%s%s %sweft is installed and running.%s\n\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  Start a project (it asks which AI assistant you use, then says how to open it):\n\n'
    printf '    %sweft new my-project%s\n\n' "${C_BOLD}" "${C_RESET}"
    if [[ -n "${public_url}" ]]; then
      printf '  %sPublic address for outside callers: %s%s%s\n' \
        "${C_DIM}" "${C_RESET}${C_BOLD}" "${public_url}" "${C_RESET}"
    fi
    printf '  %sCheck it: weft daemon status   Logs: weft daemon logs -f   Help: weft --help%s\n' \
      "${C_DIM}" "${C_RESET}"
  fi
fi
