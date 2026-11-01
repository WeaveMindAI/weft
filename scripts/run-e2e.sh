#!/usr/bin/env bash
# Run the Layer-4 e2e suite: every test side by side, a few at a time.
#
# Usage:
#   scripts/run-e2e.sh                        # every test
#   scripts/run-e2e.sh plain live_chat        # every test in these files
#   scripts/run-e2e.sh accesses::some_test    # just this test
#   scripts/run-e2e.sh --failed               # the tests that did not pass last run
#   scripts/run-e2e.sh -j 8 ...               # how many tests run at once (4)
#   scripts/run-e2e.sh --keep-going           # keep starting tests after a failure
#   scripts/run-e2e.sh --clean                # only remove what failed runs kept
#
# What a run does, once: brings this checkout's install to current code
# (setup.sh, which on a branch is the branch's own install, never main's),
# builds the test binary, removes whatever earlier failed runs kept, then
# runs the tests one by one, as many at once as -j says, longest first (by
# how long each took last time) so a slow one never starts last and holds
# the run open. Each test's output goes to
# <install dir>/e2e/run/<file>::<test>.log, which on main is
# ~/.local/share/weft/e2e/run/ (`weft daemon which` names the install dir).
#
# One run at a time on the machine, whichever checkout it comes from: a
# run that finds another one going says which, and waits for its turn.
# Everything the run starts that may outlive it (setup.sh's runtime, the
# tests and the cells they start) runs with the lock's descriptor closed,
# so the next run never waits on what this one left behind.
# The tests crash under load, so several at once would only fail each
# other. The heavy cargo builds also take turns with every other build on
# the machine (setup.sh's, the node tests').
#
# When a test passes it removes everything it made. When one fails, it keeps
# it (its projects, its cell if it had one) so you can look, and the runner
# stops starting new tests, lets the running ones finish, and writes what
# the install looked like next to that test's log. What it kept stays until
# the next run starts (or `--clean`), so look before you run again.
#
# No test runs longer than TEST_LIMIT (5 minutes): past it the runner stops
# the test and everything it started, and counts it failed, saying so in
# its log. A hang (a wait on something that never comes) otherwise holds
# the whole run open with nothing on screen. A test that needs longer is
# too big: split it.
#
# Ctrl-C (or a TERM) stops every running test, and everything each one
# started, before the runner exits; the tests it cut short and the ones it
# never started are what --failed runs next.
set -u
# `wait -n -p` is bash 5.1; associative arrays and mapfile are bash 4.
if [ "${BASH_VERSINFO[0]:-0}" -lt 5 ] || { [ "${BASH_VERSINFO[0]}" -eq 5 ] && [ "${BASH_VERSINFO[1]}" -lt 1 ]; }; then
  echo "scripts/run-e2e.sh needs bash 5.1 or newer; this is bash ${BASH_VERSION:-unknown}." >&2
  echo "On macOS: brew install bash, then run it with that bash." >&2
  exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || exit 1
# shellcheck source=scripts/lib/machine-lock.sh
. "$SCRIPT_DIR/lib/machine-lock.sh"
# shellcheck source=scripts/lib/checkout-install.sh
. "$SCRIPT_DIR/lib/checkout-install.sh"

CLEANUP=("machine_lock_release_mine")
cleanup() {
  for c in "${CLEANUP[@]}"; do eval "$c"; done
}
trap cleanup EXIT

usage() {
  echo "usage: scripts/run-e2e.sh [-j <n>] [--keep-going] [--failed | <file|file::test> ...] | --clean" >&2
  echo "  no args        every test" >&2
  echo "  <file>         every test in crates/weft-e2e/tests/e2e/<file>.rs" >&2
  echo "  <file>::<test> one test" >&2
  echo "  --failed       the tests that did not pass in the last run (failed or never started)" >&2
  echo "  -j <n>         how many tests run at once (default 4)" >&2
  echo "  --keep-going   keep starting tests after one fails, to see every failure" >&2
  echo "  --clean        only remove everything failed runs kept, run nothing" >&2
}

JOBS=4
SELECTED=()
FROM_FAILED=0
CLEAN=0
KEEP_GOING=0
while [ $# -gt 0 ]; do
  case "$1" in
    -j) JOBS="${2:-}"; shift 2 || { usage; exit 1; } ;;
    --failed) FROM_FAILED=1; shift ;;
    --clean) CLEAN=1; shift ;;
    --keep-going) KEEP_GOING=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option '$1'." >&2; usage; exit 1 ;;
    *) SELECTED+=("$1"); shift ;;
  esac
done
if [ "$CLEAN" -eq 1 ] && { [ "$FROM_FAILED" -eq 1 ] || [ ${#SELECTED[@]} -gt 0 ]; }; then
  echo "--clean runs no tests, so it takes no --failed and no test names" >&2
  exit 1
fi
if [ "$FROM_FAILED" -eq 1 ] && [ ${#SELECTED[@]} -gt 0 ]; then
  echo "--failed picks the tests itself; name tests or pass --failed, not both" >&2
  exit 1
fi
if ! [[ "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "-j takes a positive number, not '$JOBS'" >&2
  exit 1
fi

# ---------- One run (or clean) at a time on the machine ----------
# A clean during a run would remove what a running test is using, and
# two runs at once crash each other's tests under the load.
machine_lock_take "$weft_e2e_run_lock" "the e2e run lock (one e2e run at a time on this machine)" || exit 1

# ---------- The install, on current code ----------
# setup.sh is idempotent: on unchanged code it rebuilds nothing and
# restarts nothing. Once per run, never per test. --clean skips it: on
# changed code it would restart the runtime a failure kept for inspection,
# and removing what was kept only needs the install as it is (the clean
# fails loudly if no runtime answers). Its log goes beside this install's
# other e2e files once the install is known; until then, in a temp file.
# setup.sh refuses to replace a runtime that has something on (`weft
# maintenance check`), and what failed runs kept (their projects, triggers
# on, infra up) is exactly that. None of it is worth waiting for or bringing
# back, since the sweep below removes it: so an install with something on
# goes into maintenance first, its runs cancelled, through the weft it runs
# now, and the maintenance is lifted once the sweep is done (below), with
# whatever is left of it coming back.
if [ "$CLEAN" -eq 0 ]; then
  MAINTENANCE_LOG="$(mktemp -t weft-e2e-maintenance.XXXXXX)"
  if weft_checkout_install "$REPO_ROOT" 2>/dev/null && "$WHICH_BIN/weft" help maintenance >/dev/null 2>&1 \
    && ! WEFT_INSTALL="$WHICH_ENV" "$WHICH_BIN/weft" maintenance check >"$MAINTENANCE_LOG" 2>&1; then
    echo "the install has something on (what failed runs kept); putting it in maintenance, its runs cancelled, for the update"
    if ! WEFT_INSTALL="$WHICH_ENV" "$WHICH_BIN/weft" maintenance on --running cancel >>"$MAINTENANCE_LOG" 2>&1; then
      echo "\`weft maintenance on --running cancel\` failed, so setup.sh would refuse the update; its output is in $MAINTENANCE_LOG" >&2
      echo "scripts/run-e2e.sh --clean removes what failed runs kept" >&2
      exit 1
    fi
  fi
  echo "bringing this checkout's install to current code (setup.sh --cli --daemon)"
  SETUP_LOG="$(mktemp -t weft-e2e-setup.XXXXXX)"
  if ! without_machine_locks ./setup.sh --cli --daemon >"$SETUP_LOG" 2>&1; then
    echo "setup.sh failed; its output is in $SETUP_LOG" >&2
    exit 1
  fi
fi

# ---------- Which install the tests run against ----------
# The one setup.sh just set up for this checkout, with its own weft first
# on PATH and named in WEFT_INSTALL for everything this runner starts: the
# harness spawns `weft` from temp folders, where "where you stand" would
# otherwise mean the default install.
# SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
# SYNC: WEFT_E2E_KEPT_DIR <-> crates/weft-e2e/src/kept.rs (KEPT_DIR_ENV)
weft_checkout_install "$REPO_ROOT" || exit 1
INSTALL_DIR="$WHICH_DIR"
INSTALL_WEFT="$WHICH_BIN/weft"
export WEFT_INSTALL="$WHICH_ENV"
export PATH="$WHICH_BIN:$PATH"
echo "testing install $WHICH_ENV ($INSTALL_DIR)"

# Outside target/, which setup.sh empties when it grows past its cap.
E2E_DIR="$INSTALL_DIR/e2e"
RUN_DIR="$E2E_DIR/run"
DURATIONS="$E2E_DIR/durations"
FAILED_LIST="$E2E_DIR/failed"
# The ledger of what tests made that has no name to find it by later (a
# connection, a cell); what a failed test leaves in it is what --clean
# removes.
export WEFT_E2E_KEPT_DIR="$E2E_DIR/kept"
mkdir -p "$E2E_DIR"
[ "$CLEAN" -eq 0 ] && mv -f "$SETUP_LOG" "$E2E_DIR/setup.log" && mv -f "$MAINTENANCE_LOG" "$E2E_DIR/maintenance.log"

# Run one command with its output in a log, for a step whose waiting
# line (the build slot's) must still reach the terminal.
logged() { # log cmd...
  local log="$1"
  shift
  "$@" >"$log" 2>&1
}

# ---------- Build the test binary once ----------
# Every e2e test is in one binary (tests/e2e/main.rs, one module per file).
# Then each test runs the binary directly: dozens of `cargo test`s at once
# would only queue on cargo's build lock. The build is the one
# `cargo nextest run --workspace` makes (nextest builds through `cargo test
# --no-run`), so after the unit tests it compiles weft-e2e alone, the one
# crate the `e2e` feature changes. Anything narrower compiles most of the
# workspace a second time: `cargo build` uses the dev profile where `cargo
# test` uses the test one, which changes every build script's dependencies,
# and selecting only `--test e2e` leaves the other crates' dev-dependencies
# out, which turns on different features of the crates under them. The
# same build makes the clean binary below (a package's binaries are built
# for its integration tests), beside the test binary's `deps/` folder.
# `--clean` runs no test, so it builds the clean binary alone: the cleanup
# needs no test of any crate to compile.
BUILD_OUT="$E2E_DIR/build.log"
if [ "$CLEAN" -eq 1 ]; then
  echo "building the clean binary"
  if ! with_build_slot logged "$BUILD_OUT" cargo build --workspace --features weft-e2e/e2e --bin weft-e2e-clean --message-format json-render-diagnostics; then
    echo "building the clean binary failed:" >&2
    tail -60 "$BUILD_OUT" >&2
    exit 1
  fi
  CLEAN_BINARY="$(sed -n 's|.*"executable":"\([^"]*/weft-e2e-clean\)".*|\1|p' "$BUILD_OUT" | tail -1)"
else
  echo "building the e2e test binary"
  if ! with_build_slot logged "$BUILD_OUT" cargo test --workspace --features weft-e2e/e2e --no-run; then
    echo "building the e2e tests failed:" >&2
    tail -60 "$BUILD_OUT" >&2
    exit 1
  fi
  # cargo names the binary relative to the workspace root.
  BINARY="$(sed -n 's|^ *Executable tests/e2e/main\.rs (\(.*\))$|\1|p' "$BUILD_OUT")"
  case "$BINARY" in
    "") echo "the e2e build named no test binary; its output is in $BUILD_OUT" >&2; exit 1 ;;
    /*) ;;
    *) BINARY="$REPO_ROOT/$BINARY" ;;
  esac
  CLEAN_BINARY="$(dirname "$(dirname "$BINARY")")/weft-e2e-clean"
fi
if [ ! -x "$CLEAN_BINARY" ]; then
  echo "the build made no clean binary (looked for '$CLEAN_BINARY'); its output is in $BUILD_OUT" >&2
  exit 1
fi

# ---------- What earlier failed runs kept ----------
# Kept so a failure could be looked at; starting a new run means that is
# done. Removed before anything new is made, so it never mixes with this
# run's state.
echo "removing what earlier failed runs kept"
if ! without_machine_locks "$CLEAN_BINARY" >"$E2E_DIR/clean.log" 2>&1; then
  echo "removing what earlier runs kept failed; its output is in $E2E_DIR/clean.log" >&2
  exit 1
fi
# The maintenance the update ran in, lifted now that what failed runs
# kept is gone: the tests switch things on. An install that is not in
# maintenance answers that there is nothing to lift.
if [ "$CLEAN" -eq 0 ] && ! "$INSTALL_WEFT" maintenance off >>"$E2E_DIR/maintenance.log" 2>&1; then
  echo "\`weft maintenance off\` failed, and the tests cannot switch anything on in maintenance; its output is in $E2E_DIR/maintenance.log" >&2
  exit 1
fi
rm -rf "$RUN_DIR"
if [ "$CLEAN" -eq 1 ]; then
  rm -f "$FAILED_LIST"
  echo "removed everything failed e2e runs kept"
  exit 0
fi

# ---------- Auto-provisioned dependencies ----------
# Everything a test needs that a local machine can serve comes from the
# install under test: its Postgres, and its own e2e bucket in the
# machine's object store (made the first time, then reused by every run,
# so nothing here needs tearing down). Only genuinely external services
# (Slack, GitHub, Google, Telegram, a real mailbox, an OpenRouter key) come
# from the operator's env / repo-root .env. Any WEFT_E2E_* var already set
# wins.

# The install's Postgres, for the tests that seed rows directly: the
# address its runtime uses, from the secrets its start wrote, read the way
# the runtime reads that file (split at the first `=`, both sides trimmed,
# the last line for a name wins).
# SYNC: secrets.env <-> crates/weft-core/src/infra/install.rs (SECRETS_FILE, Install::secret,
#       env_file_values: the parser of record), crates/weft-cli/src/commands/daemon.rs (secrets:
#       the writer)
SECRETS="$INSTALL_DIR/secrets.env"
# install_secret <name>: the value <name> has in the install's secrets,
# failing (non-zero, with the reason on stderr) when the file is missing or
# names no value for it. Called inside `$(...)`, so every caller adds
# `|| exit 1`: an exit in there would end only the subshell.
install_secret() {
  if [ ! -f "$SECRETS" ]; then
    echo "$SECRETS does not exist; is the install up? (./setup.sh --daemon)" >&2
    return 1
  fi
  local value
  value="$(awk -v want="$1" '
    { eq = index($0, "=") }
    eq > 0 {
      key = substr($0, 1, eq - 1); gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      if (key == want) { value = substr($0, eq + 1); gsub(/^[[:space:]]+|[[:space:]]+$/, "", value) }
    }
    END { print value }' "$SECRETS")"
  if [ -z "$value" ]; then
    echo "$SECRETS names no $1; start the install again with ./setup.sh --daemon, which writes it" >&2
    return 1
  fi
  printf '%s\n' "$value"
}
if [ -z "${WEFT_E2E_DATABASE_URL:-}" ]; then
  WEFT_E2E_DATABASE_URL="$(install_secret WEFT_DATABASE_URL)" || exit 1
  export WEFT_E2E_DATABASE_URL
fi

# An S3 store: the machine already runs one ('weft-object-store', its keys
# made once per machine, shared by every install); a bucket of the install's
# own in it serves the S3 tests, so two installs' runs never share one:
# `weft-e2e` for the default install, `weft-e2e-<name>` for a branch's. The
# endpoint is dialled by the WORKER running the node under test, a process on
# this machine, so it is the store's loopback address, which a stored
# connection base may reach over plain http.
# SYNC: the store's published port <-> crates/weft-cli/src/commands/daemon.rs (ensure_object_store)
if [ -z "${WEFT_E2E_S3_ENDPOINT:-}" ]; then
  if [ -z "$(docker ps -q -f name='^weft-object-store$')" ]; then
    echo "no running weft-object-store container; is the install up?" >&2
    exit 1
  fi
  # weed shell's exit code and chatter are both unreliable (it prints
  # "created" even for an existing bucket and exits 0 on errors), so the
  # create is fire-and-forget and the LIST afterwards is the one source of
  # truth.
  BUCKET="$(weft_e2e_bucket "$WHICH_NAME")"
  docker exec weft-object-store sh -c \
    "echo \"s3.bucket.create -name $BUCKET\" | weed shell" >/dev/null 2>&1
  BUCKET_OUT="$(docker exec weft-object-store sh -c 'echo "s3.bucket.list" | weed shell' 2>&1)"
  if ! printf '%s\n' "$BUCKET_OUT" | grep -Eq "(^|[[:space:]])$BUCKET([[:space:]]|$)"; then
    echo "could not create the $BUCKET bucket in the machine's object store. weed shell said:" >&2
    printf '%s\n' "$BUCKET_OUT" >&2
    exit 1
  fi
  STORE_AT="$(docker port weft-object-store 8333/tcp | grep -m1 '^127\.0\.0\.1:' || true)"
  if [ -z "$STORE_AT" ]; then
    echo "the weft-object-store container publishes no loopback port; start the install again with ./setup.sh --daemon, which recreates it" >&2
    exit 1
  fi
  export WEFT_E2E_S3_ENDPOINT="http://$STORE_AT"
  export WEFT_E2E_S3_REGION="us-east-1"
  # The store's keys, made once per machine, handed to every install among
  # its secrets.
  # SYNC: the store's key names <-> crates/weft-platform-traits/src/object_store.rs (OBJECT_STORE_ACCESS_KEY_ENV, OBJECT_STORE_SECRET_KEY_ENV)
  WEFT_E2E_S3_ACCESS_KEY_ID="$(install_secret WEFT_OBJECT_STORE_ACCESS_KEY)" || exit 1
  WEFT_E2E_S3_SECRET_ACCESS_KEY="$(install_secret WEFT_OBJECT_STORE_SECRET_KEY)" || exit 1
  export WEFT_E2E_S3_ACCESS_KEY_ID WEFT_E2E_S3_SECRET_ACCESS_KEY
  export WEFT_E2E_S3_BUCKET="$BUCKET"
fi

# ---------- Which tests ----------
# Every test, asked of the binary itself, so a new test (or a new
# `tests/e2e/<name>.rs` module) runs without anyone updating anything. A
# test is named after its module path, `<file>::<test>`.
mapfile -t ALL_TESTS < <(
  "$BINARY" --list --format terse 2>/dev/null | sed -n 's/^\(.*\): test$/\1/p' | sort
)
if [ "$FROM_FAILED" -eq 1 ]; then
  if [ ! -s "$FAILED_LIST" ]; then
    echo "--failed: the last run left nothing that did not pass" >&2
    exit 1
  fi
  mapfile -t SELECTED < "$FAILED_LIST"
fi
if [ ${#SELECTED[@]} -eq 0 ]; then
  CHOSEN=("${ALL_TESTS[@]}")
else
  CHOSEN=()
  for want in "${SELECTED[@]}"; do
    matched=0
    for t in "${ALL_TESTS[@]}"; do
      if [ "$t" = "$want" ] || [ "${t%%::*}" = "$want" ]; then CHOSEN+=("$t"); matched=1; fi
    done
    if [ "$matched" -eq 0 ]; then
      echo "no test file or test named '$want'. Test files:" >&2
      printf '%s\n' "${ALL_TESTS[@]}" | sed 's/::.*//' | sort -u | sed 's/^/  /' >&2
      exit 1
    fi
  done
fi

# Longest first, by how long each took last time; a test never timed yet
# counts as long, since nothing says it is not.
declare -A LAST_SECS=()
if [ -f "$DURATIONS" ]; then
  while read -r name secs; do LAST_SECS["$name"]="$secs"; done < "$DURATIONS"
fi
mapfile -t TESTS < <(
  for t in "${CHOSEN[@]}"; do printf '%s %s\n' "${LAST_SECS[$t]:-999999}" "$t"; done \
    | sort -rn | awk '{print $2}' | awk '!seen[$0]++'
)

# ---------- Run ----------
mkdir -p "$RUN_DIR"
: > "$FAILED_LIST"

# What the install looked like when a test failed, next to its log: every
# container, the runtime's log of the install under test and of any cell
# the failed test kept, its worker registry, and the logs of the kept
# projects' worker processes and infra containers.
# SYNC: labels weft-install, weft.project <-> crates/weft-platform-local/src/docker.rs
# SYNC: workers/running.json, workers/logs/<name>.log, weft-w-<project>- <->
#       crates/weft-platform-local/src/registry.rs (Registry::log_of),
#       crates/weft-platform-local/src/runner.rs (worker_name_prefix)
post_mortem() {
  local t="$1" dir="$RUN_DIR/$1.post-mortem"
  mkdir -p "$dir"
  docker ps -a --filter label=weft-install \
    --format '{{.Names}}\t{{.Status}}\t{{.Label "weft.role"}}\t{{.Label "weft.project"}}' >"$dir/containers.txt" 2>&1
  tail -n 5000 "$INSTALL_DIR/runtime.log" >"$dir/runtime.log" 2>&1
  local installs=("$INSTALL_DIR") cell cell_dir
  # Every cell and project the test made, as it says the moment it makes
  # one: a test stopped for running too long never reaches the lines its
  # guards print when they drop.
  # SYNC: the "made" lines <-> crates/weft-e2e/src/cell.rs, crates/weft-e2e/src/teardown.rs
  for cell in $(sed -n "s/^weft-e2e: made cell '\(e2e[a-z0-9]*\)'.*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
    # The cell's folder, as the CLI places it (a subshell, so the
    # install under test's WHICH_* stay as they are).
    if cell_dir="$(weft_which "$REPO_ROOT" "$INSTALL_WEFT" "$cell" && printf '%s' "$WHICH_DIR")"; then
      tail -n 5000 "$cell_dir/runtime.log" >"$dir/runtime-$cell.log" 2>&1
      installs+=("$cell_dir")
    else
      echo "could not ask weft where cell $cell lives" >"$dir/runtime-$cell.log"
    fi
  done
  # Which worker processes each install runs, and the logs of every
  # project the test kept, while they are there: a worker that stopped
  # once idle is forgotten, its log removed with it, and an infra
  # container's log goes with the container.
  local install project name log
  for install in "${installs[@]}"; do
    [ -f "$install/workers/running.json" ] && cp "$install/workers/running.json" "$dir/running-$(basename "$install").json"
  done
  for project in $(sed -n "s/^weft-e2e: made project '[^']*' (\([0-9a-f-]*\)).*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
    for install in "${installs[@]}"; do
      for log in "$install/workers/logs/weft-w-${project//-/}-"*.log; do
        [ -f "$log" ] && tail -n 5000 "$log" >"$dir/$(basename "$log")" 2>&1
      done
    done
    for name in $(docker ps -a --filter "label=weft.project=$project" --format '{{.Names}}' 2>/dev/null); do
      docker logs --tail 5000 "$name" >"$dir/$name.log" 2>&1
    done
  done
}

declare -A RUNNING=()   # pid -> test
declare -A STARTED=()   # test -> start time (s)
PASSED=()
FAILED=()
STOPPING=0
# The subshell execs setsid, which (not being a group leader) calls
# setsid() and execs the test in place: the test keeps the pid the runner
# holds, and that pid is its process group.
SETSID=()
command -v setsid >/dev/null 2>&1 && SETSID=(setsid)

# The most one test may take, in seconds (see the header).
# SYNC: TEST_LIMIT <-> crates/weft-e2e/src/client.rs TEST_LIMIT
TEST_LIMIT=300
# test pid -> the watchdog that stops it at TEST_LIMIT.
declare -A WATCHDOG=()

# Call off the watchdog of the test at `pid` (it and its sleep).
stop_watchdog() {
  local dog="${WATCHDOG[$1]:-}"
  unset "WATCHDOG[$1]"
  [ -n "$dog" ] || return 0
  # The watchdog first, its sleep after: a subshell outliving its sleep
  # reports the kill ("Terminated") in the run's output.
  local sleeper
  sleeper=$(pgrep -P "$dog")
  kill "$dog" 2>/dev/null
  wait "$dog" 2>/dev/null
  [ -z "$sleeper" ] || kill $sleeper 2>/dev/null
}

# Stop the test at `pid` and everything it started.
stop_test() {
  local pid="$1"
  if [ ${#SETSID[@]} -gt 0 ]; then
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
  else
    pkill -TERM -P "$pid" 2>/dev/null
    kill -TERM "$pid" 2>/dev/null
  fi
}

# Every test this run has not seen pass (failed, cut short, never started)
# goes on the --failed list.
record_unfinished() {
  local t
  declare -A finished=()
  for t in "${PASSED[@]}" "${FAILED[@]}"; do finished["$t"]=1; done
  for t in "${TESTS[@]}"; do [ -n "${finished[$t]:-}" ] || echo "$t" >> "$FAILED_LIST"; done
}

# Ctrl-C or TERM: stop every running test and what it started, wait for
# them, so nothing outlives the lock; then
# leave the list --failed needs.
on_interrupt() {
  local code="$1" pid
  trap '' INT TERM
  echo ""
  echo "interrupted: stopping ${#RUNNING[@]} running test(s)"
  for pid in "${!RUNNING[@]}"; do
    stop_watchdog "$pid"
    stop_test "$pid"
  done
  for pid in "${!RUNNING[@]}"; do wait "$pid" 2>/dev/null; done
  record_unfinished
  echo "the tests cut short and the ones never started are on the --failed list; what the cut ones made stays until the next run (or --clean)"
  exit "$code"
}
trap 'on_interrupt 130' INT
trap 'on_interrupt 143' TERM
# Each test runs in its own session (setsid), so a closed terminal's
# hangup reaches only this runner: it passes the stop on, or the tests
# would keep deploying against the install after the runner is gone.
trap 'on_interrupt 129' HUP

finish_one() {
  local pid="$1" status="$2" t="${RUNNING[$1]}"
  unset "RUNNING[$pid]"
  stop_watchdog "$pid"
  local secs=$(( $(date +%s) - STARTED[$t] ))
  if [ "$status" -eq 0 ]; then
    PASSED+=("$t")
    LAST_SECS["$t"]="$secs"
    printf '  ok    %-70s %4ss\n' "$t" "$secs"
  else
    FAILED+=("$t")
    echo "$t" >> "$FAILED_LIST"
    local why="FAIL"
    [ -e "$RUN_DIR/$t.timed-out" ] && why="SLOW"
    printf '  %-4s  %-70s %4ss   log: %s\n' "$why" "$t" "$secs" "$RUN_DIR/$t.log"
    post_mortem "$t"
    if [ "$STOPPING" -eq 0 ] && [ "$KEEP_GOING" -eq 0 ]; then
      STOPPING=1
      echo "  (a test failed: starting no new ones, letting the running ones finish)"
    fi
  fi
}

# Reap one finished test. Bash moves a finished job out of its job table
# whenever a `wait` for one pid runs (the watchdog's), and `wait -n` then
# no longer knows it ("no such job"), though `wait <pid>` still has its
# status. So a test already gone is reaped by pid first, and `wait -n`
# only ever waits on tests still running.
wait_one() {
  local done_pid status pid
  while true; do
    for pid in "${!RUNNING[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid"
        status=$?
        finish_one "$pid" "$status"
        return
      fi
    done
    done_pid=""
    wait -n -p done_pid "${!RUNNING[@]}" 2>/dev/null
    status=$?
    if [ -n "$done_pid" ]; then
      finish_one "$done_pid" "$status"
      return
    fi
  done
}

echo "running ${#TESTS[@]} test(s), $JOBS at a time; logs in $RUN_DIR"
RUN_STARTED=$(date +%s)
for t in "${TESTS[@]}"; do
  [ "$STOPPING" -eq 1 ] && break
  while [ ${#RUNNING[@]} -ge "$JOBS" ]; do wait_one; done
  [ "$STOPPING" -eq 1 ] && break
  STARTED["$t"]=$(date +%s)
  # In its own process group (setsid), so an interrupt can stop the test
  # and everything it started (weft, docker) in one kill.
  ( close_machine_locks_here; cd "$REPO_ROOT/crates/weft-e2e" && exec "${SETSID[@]}" "$BINARY" --exact "$t" --test-threads=1 --nocapture ) \
    >"$RUN_DIR/$t.log" 2>&1 &
  RUNNING[$!]="$t"
  pid=$!
  (
    close_machine_locks_here
    sleep "$TEST_LIMIT"
    touch "$RUN_DIR/$t.timed-out"
    echo "[run-e2e] stopped: over the ${TEST_LIMIT}s every test must finish in" >>"$RUN_DIR/$t.log"
    stop_test "$pid"
  ) &
  WATCHDOG[$pid]=$!
done
while [ ${#RUNNING[@]} -gt 0 ]; do wait_one; done

# Remember how long each test took, for the next run's order.
for name in "${!LAST_SECS[@]}"; do printf '%s %s\n' "$name" "${LAST_SECS[$name]}"; done \
  | sort > "$DURATIONS"

NOT_RUN=$(( ${#TESTS[@]} - ${#PASSED[@]} - ${#FAILED[@]} ))
# A test that never started has not passed either: --failed runs it too.
trap - INT TERM
record_unfinished
ELAPSED=$(( $(date +%s) - RUN_STARTED ))
echo ""
printf 'the tests ran in %dm%02ds\n' $(( ELAPSED / 60 )) $(( ELAPSED % 60 ))

# Reclaim what the run left, pass or fail: a passing test removed what it
# made, so nothing references its fixtures' worker programs any more; a
# failed test kept its projects, and the reclaim keeps every program a
# project still points at. The same pass drops the worker compile caches
# no install on the machine uses, which a long session of rebuilds
# otherwise piles up by the tens of gigabytes.
RECLAIMED=1
without_machine_locks "$INSTALL_WEFT" clean --images --all >"$E2E_DIR/reclaim.log" 2>&1 || RECLAIMED=0
if [ $RECLAIMED -eq 0 ]; then
  echo "The final reclaim failed; its output is in $E2E_DIR/reclaim.log. Re-run '$INSTALL_WEFT clean --images --all' by hand (WEFT_INSTALL=$WEFT_INSTALL)." >&2
fi
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "${#FAILED[@]} failed, ${#PASSED[@]} passed, $NOT_RUN not started."
  echo "Each failure kept what it made; its log and a post-mortem are in $RUN_DIR."
  echo "It stays until the next run starts. Re-run what has not passed yet (failed or not started) with --failed."
  exit 1
fi
[ $RECLAIMED -eq 1 ] || exit 1
echo "All ${#PASSED[@]} e2e tests passed."
