#!/usr/bin/env bash
# Run the Layer-4 e2e suite: every test side by side, a few at a time.
#
# Usage:
#   scripts/run-e2e.sh                        # every test
#   scripts/run-e2e.sh plain live_chat        # every test in these files
#   scripts/run-e2e.sh accesses::some_test    # just this test
#   scripts/run-e2e.sh --failed               # the tests that did not pass last run
#   scripts/run-e2e.sh -j 8 ...               # how many tests run at once (16)
#   scripts/run-e2e.sh --keep-going           # keep starting tests after a failure
#   scripts/run-e2e.sh --clean                # only remove what failed runs kept
#
# What a run does, once: brings the install to current code (setup.sh),
# removes whatever earlier failed runs kept, builds every test binary, then
# runs the tests one by one, as many at once as -j says, longest first (by
# how long each took last time) so a slow one never starts last and holds
# the run open. Each test's output goes to
# ~/.local/share/weft/e2e/run/<file>::<test>.log.
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

# Outside target/, which setup.sh empties when it grows past its cap, and
# per machine rather than per checkout: every worktree drives the same
# install, so one run at a time is a rule for the machine.
# SYNC: the weft data dir <-> crates/weft-cli/src/commands/daemon.rs (data_dir)
E2E_DIR="$HOME/.local/share/weft/e2e"
RUN_DIR="$E2E_DIR/run"
DURATIONS="$E2E_DIR/durations"
FAILED_LIST="$E2E_DIR/failed"
LOCK="$E2E_DIR/run.lock"
# The ledger of what tests made that has no name to find it by later (a
# connection); what a failed test leaves in it is what --clean removes.
# SYNC: WEFT_E2E_KEPT_DIR <-> crates/weft-e2e/src/kept.rs (KEPT_DIR_ENV)
export WEFT_E2E_KEPT_DIR="$E2E_DIR/kept"
mkdir -p "$E2E_DIR"

# ---------- One run (or clean) at a time ----------
# A clean during a run would remove what a running test is using, and two
# runs (from any checkout: they share the install) would redeploy it under
# each other. The lock is a symlink whose target is the holder's pid:
# creating it both takes the lock and names the holder in one atomic step
# (portable, unlike flock, which macOS lacks), so no run ever sees a lock
# with no holder in it. A lock whose holder is dead is taken over by
# renaming it aside, which only one run can do; the run that did checks it
# moved the stale lock it read, and puts it back otherwise.
take_lock() {
  local holder aside="$LOCK.stale.$$"
  while ! ln -s "$$" "$LOCK" 2>/dev/null; do
    holder="$(readlink "$LOCK" 2>/dev/null)" || continue   # released meanwhile: try again
    if ! [[ "$holder" =~ ^[0-9]+$ ]] || kill -0 "$holder" 2>/dev/null; then
      echo "another e2e run (pid ${holder:-unknown}) is in progress; wait for it, or stop it first" >&2
      echo "(if nothing is running, remove $LOCK)" >&2
      exit 1
    fi
    if mv "$LOCK" "$aside" 2>/dev/null; then
      if [ "$(readlink "$aside" 2>/dev/null)" = "$holder" ]; then
        rm -f "$aside"
      else
        # Another run took over between our read and our rename: that
        # lock is live, give it back and refuse.
        mv "$aside" "$LOCK" 2>/dev/null || rm -f "$aside"
        echo "another e2e run took the lock at $LOCK first" >&2
        exit 1
      fi
    fi
  done
}
release_lock() {
  [ "$(readlink "$LOCK" 2>/dev/null)" = "$$" ] && rm -f "$LOCK"
}
CLEANUP=("release_lock")
cleanup() {
  for c in "${CLEANUP[@]}"; do eval "$c"; done
}
trap cleanup EXIT
take_lock

usage() {
  echo "usage: scripts/run-e2e.sh [-j <n>] [--keep-going] [--failed | <file|file::test> ...] | --clean" >&2
  echo "  no args        every test" >&2
  echo "  <file>         every test in crates/weft-e2e/tests/<file>.rs" >&2
  echo "  <file>::<test> one test" >&2
  echo "  --failed       the tests that did not pass in the last run (failed or never started)" >&2
  echo "  -j <n>         how many tests run at once (default 16)" >&2
  echo "  --keep-going   keep starting tests after one fails, to see every failure" >&2
  echo "  --clean        only remove everything failed runs kept, run nothing" >&2
}

JOBS=16
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

# ---------- The install, on current code ----------
# setup.sh is idempotent: on unchanged code it rebuilds nothing and
# restarts nothing. Once per run, never per test. --clean skips it: on
# changed code it would restart the runtime a failure kept for inspection,
# and removing what was kept only needs the install as it is (the clean
# fails loudly if no runtime answers).
if [ "$CLEAN" -eq 0 ]; then
  echo "bringing the install to current code (setup.sh --cli --daemon)"
  if ! ./setup.sh --cli --daemon >"$E2E_DIR/setup.log" 2>&1; then
    echo "setup.sh failed; its output is in $E2E_DIR/setup.log" >&2
    exit 1
  fi
fi

# ---------- What earlier failed runs kept ----------
# Kept so a failure could be looked at; starting a new run means that is
# done. Removed before anything new is made, so it never mixes with this
# run's state.
echo "removing what earlier failed runs kept"
if ! cargo build -q -p weft-e2e --features e2e --bin weft-e2e-clean >"$E2E_DIR/clean.log" 2>&1 \
  || ! "$REPO_ROOT/target/debug/weft-e2e-clean" >>"$E2E_DIR/clean.log" 2>&1; then
  echo "removing what earlier runs kept failed; its output is in $E2E_DIR/clean.log" >&2
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
# install already running here: its Postgres, and a `weft-e2e` bucket in
# its object store (made the first time, then reused by every run, so
# nothing here needs tearing down). Only genuinely external services
# (Slack, GitHub, Google, Telegram, a real mailbox, an OpenRouter key) come
# from the operator's env / repo-root .env. Any WEFT_E2E_* var already set
# wins.

# The default install's Postgres, for the tests that seed rows directly:
# the address its runtime uses, from the secrets its start wrote.
# SYNC: secrets.env <-> crates/weft-cli/src/commands/daemon.rs (secrets),
#       crates/weft-e2e/src/platform.rs (secret)
if [ -z "${WEFT_E2E_DATABASE_URL:-}" ]; then
  SECRETS="$HOME/.local/share/weft/secrets.env"
  WEFT_E2E_DATABASE_URL="$(sed -n 's/^WEFT_DATABASE_URL=//p' "$SECRETS" 2>/dev/null | head -1)"
  if [ -z "$WEFT_E2E_DATABASE_URL" ]; then
    echo "$SECRETS names no WEFT_DATABASE_URL; is the install up?" >&2
    exit 1
  fi
  export WEFT_E2E_DATABASE_URL
fi

# An S3 store: the install already runs one ('weft-object-store', on the
# install's Docker network, local-dev identities); a dedicated e2e bucket
# in it serves the S3 tests. The endpoint is dialled by the WORKER running
# the node under test, a container on that same network, so it names the
# store by its address there: a private IP, which a stored connection base
# may reach over plain http (a container name is not one).
# SYNC: the store's address on the network <-> crates/weft-cli/src/commands/daemon.rs
#       (OBJECT_STORE_CONTAINER, worker_endpoint)
if [ -z "${WEFT_E2E_S3_ENDPOINT:-}" ]; then
  if [ -z "$(docker ps -q -f name='^weft-object-store$')" ]; then
    echo "no running weft-object-store container; is the install up?" >&2
    exit 1
  fi
  # weed shell's exit code and chatter are both unreliable (it prints
  # "created" even for an existing bucket and exits 0 on errors), so the
  # create is fire-and-forget and the LIST afterwards is the one source of
  # truth.
  docker exec weft-object-store sh -c \
    'echo "s3.bucket.create -name weft-e2e" | weed shell' >/dev/null 2>&1
  BUCKET_OUT="$(docker exec weft-object-store sh -c 'echo "s3.bucket.list" | weed shell' 2>&1)"
  if ! printf '%s\n' "$BUCKET_OUT" | grep -q 'weft-e2e'; then
    echo "could not create the weft-e2e bucket in the install's object store. weed shell said:" >&2
    printf '%s\n' "$BUCKET_OUT" >&2
    exit 1
  fi
  STORE_IP="$(docker inspect weft-object-store --format '{{(index .NetworkSettings.Networks "weft").IPAddress}}')"
  export WEFT_E2E_S3_ENDPOINT="http://$STORE_IP:8333"
  export WEFT_E2E_S3_REGION="us-east-1"
  export WEFT_E2E_S3_ACCESS_KEY_ID="weft-local"
  export WEFT_E2E_S3_SECRET_ACCESS_KEY="weft-local-dev-secret"
  export WEFT_E2E_S3_BUCKET="weft-e2e"
fi

# ---------- Build every test binary once ----------
# Then each test runs its binary directly: dozens of `cargo test`s at once
# would only queue on cargo's build lock.
echo "building the e2e test binaries"
BUILD_OUT="$E2E_DIR/build.log"
if ! cargo test -p weft-e2e --features e2e --no-run >"$BUILD_OUT" 2>&1; then
  echo "building the e2e tests failed:" >&2
  tail -60 "$BUILD_OUT" >&2
  exit 1
fi
# cargo names each binary relative to the workspace root.
declare -A BINARY=()
while read -r name path; do
  case "$path" in /*) ;; *) path="$REPO_ROOT/$path" ;; esac
  BINARY["$name"]="$path"
done < <(
  sed -n 's|^ *Executable tests/\([A-Za-z0-9_]*\)\.rs (\(.*\))$|\1 \2|p' "$BUILD_OUT"
)

# ---------- Which tests ----------
# Every test of every file, asked of the binaries themselves, so a new test
# (or a new `tests/<name>.rs`) runs without anyone updating anything. A test
# is named `<file>::<test>`.
mapfile -t ALL_TESTS < <(
  for file in $(printf '%s\n' "${!BINARY[@]}" | sort); do
    "${BINARY[$file]}" --list --format terse 2>/dev/null \
      | sed -n "s/^\(.*\): test\$/$file::\1/p"
  done
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
      printf '  %s\n' "${!BINARY[@]}" | sort >&2
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
export WEFT_E2E_RUN_DIR="$RUN_DIR"

# What the install looked like when a test failed, next to its log: every
# container, the runtime's log of the default install and of any cell the
# failed test kept, and the logs of the kept projects' containers.
# SYNC: labels weft-install, weft.project <-> crates/weft-platform-local/src/docker.rs
post_mortem() {
  local t="$1" dir="$RUN_DIR/$1.post-mortem" root="$HOME/.local/share/weft"
  mkdir -p "$dir"
  docker ps -a --filter label=weft-install \
    --format '{{.Names}}\t{{.Status}}\t{{.Label "weft.role"}}\t{{.Label "weft.project"}}' >"$dir/containers.txt" 2>&1
  tail -n 5000 "$root/runtime.log" >"$dir/runtime.log" 2>&1
  local cell
  # Every cell and project the test made, as it says the moment it makes
  # one: a test stopped for running too long never reaches the lines its
  # guards print when they drop.
  # SYNC: the "made" lines <-> crates/weft-e2e/src/cell.rs, crates/weft-e2e/src/teardown.rs
  for cell in $(sed -n "s/^weft-e2e: made cell '\(e2e[a-z0-9]*\)'.*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
    tail -n 5000 "$root/installs/$cell/runtime.log" >"$dir/runtime-$cell.log" 2>&1
  done
  # The containers of every project the test kept, while they still run
  # (an idle worker is removed, taking its log with it).
  local project name
  for project in $(sed -n "s/^weft-e2e: made project '[^']*' (\([0-9a-f-]*\)).*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
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
  bin="${BINARY[${t%%::*}]}"
  STARTED["$t"]=$(date +%s)
  # In its own process group (setsid), so an interrupt can stop the test
  # and everything it started (weft, docker) in one kill.
  ( cd "$REPO_ROOT/crates/weft-e2e" && exec "${SETSID[@]}" "$bin" --exact "${t#*::}" --test-threads=1 --nocapture ) \
    >"$RUN_DIR/$t.log" 2>&1 &
  RUNNING[$!]="$t"
  pid=$!
  (
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
# made, so its fixtures' worker images now reference nothing; a failed
# test kept its projects, and the reclaim keeps every image a project
# still points at. The same pass drops the worker compile cache this
# checkout moved off, which a long session of rebuilds otherwise piles up
# by the tens of gigabytes.
RECLAIMED=1
weft clean --images --all >"$E2E_DIR/reclaim.log" 2>&1 || RECLAIMED=0
if [ $RECLAIMED -eq 0 ]; then
  echo "The final reclaim failed; its output is in $E2E_DIR/reclaim.log. Re-run 'weft clean --images --all' by hand." >&2
fi
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "${#FAILED[@]} failed, ${#PASSED[@]} passed, $NOT_RUN not started."
  echo "Each failure kept what it made; its log and a post-mortem are in $RUN_DIR."
  echo "It stays until the next run starts. Re-run what has not passed yet (failed or not started) with --failed."
  exit 1
fi
[ $RECLAIMED -eq 1 ] || exit 1
echo "All ${#PASSED[@]} e2e tests passed."
