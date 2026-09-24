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
# What a run does, once: brings the cluster to current code (setup.sh),
# builds every test binary, removes whatever earlier failed runs kept, then
# runs the tests one by one, as many at once as -j says, longest first (by
# how long each took last time) so a slow one never starts last and holds
# the run open. Each test's output goes to
# ~/.local/share/weft/e2e/run/<file>::<test>.log.
#
# When a test passes it removes everything it made. When one fails, it keeps
# it (its projects, its cell if it had one) so you can look, and the runner
# stops starting new tests, lets the running ones finish, and writes what
# the cluster looked like next to that test's log. What it kept stays until
# the next run starts (or `--clean`), so look before you run again.
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
# cluster, so one run at a time is a rule for the machine.
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
# runs (from any checkout: they share the cluster) would redeploy it under
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

# ---------- The cluster, on current code ----------
# setup.sh is idempotent: on unchanged code it rebuilds nothing and rolls
# nothing. Once per run, never per test. --clean skips it: on changed code
# it would roll the dispatcher a failure kept for inspection, and removing
# what was kept only needs the install as it is (the clean fails loudly if
# no dispatcher answers).
if [ "$CLEAN" -eq 0 ]; then
  echo "bringing the cluster to current code (setup.sh --cli --daemon)"
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
# Everything a test needs that a local machine can serve is provisioned
# HERE, and torn down when the run ends. Only genuinely external services
# (Slack, GitHub, Google, Telegram, a real mailbox, an OpenRouter key) come
# from the operator's env / repo-root .env. Any WEFT_E2E_* var already set
# wins.

# The default install's Postgres, for the tests that seed rows directly: a
# port-forward on a port the system picks.
if [ -z "${WEFT_E2E_DATABASE_URL:-}" ]; then
  PF_OUT="$E2E_DIR/postgres-forward.out"
  # SYNC: weft-db <-> crates/weft-core/src/infra/instance.rs (the default install's db_namespace)
  kubectl -n weft-db port-forward svc/weft-postgres :5432 >"$PF_OUT" 2>&1 &
  PF_PID=$!
  CLEANUP+=("kill $PF_PID 2>/dev/null || true")
  PG_PORT=""
  for _ in $(seq 1 60); do
    PG_PORT="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$PF_OUT" | head -1)"
    [ -n "$PG_PORT" ] && break
    kill -0 "$PF_PID" 2>/dev/null || break
    sleep 0.5
  done
  if [ -z "$PG_PORT" ]; then
    echo "the Postgres port-forward never listened:" >&2
    cat "$PF_OUT" >&2
    exit 1
  fi
  # SYNC: local-dev PG credentials <-> deploy/k8s/postgres.yaml (WEFT_DATABASE_URL secret),
  #       crates/weft-e2e/src/platform.rs (PG_USER/PG_PASSWORD/PG_DBNAME),
  #       setup.sh (WEFT_LIVE_DATABASE_URL)
  export WEFT_E2E_DATABASE_URL="postgres://weft:weft-local-dev@127.0.0.1:$PG_PORT/weft"
  echo "provisioned: WEFT_E2E_DATABASE_URL via port-forward (port $PG_PORT)"
fi

# An S3 store: the daemon ALREADY runs SeaweedFS as a host docker
# container ('weft-object-store', S3 gateway on WEFT_SEAWEED_PORT,
# local-dev identities); reuse it with a dedicated e2e bucket rather
# than spawning a second store. The endpoint below is dialled by the
# WORKER POD running the node under test, so it names this machine as
# pods see it: on Docker Desktop the address `host.docker.internal`
# resolves to inside the node, on Linux the kind docker network's
# gateway. The broker's egress denies private ranges, and the daemon
# derives that address's /32 opening from its own object-store
# endpoint, so nothing is exported here for it.
# SYNC: the machine address pods dial <->
#       crates/weft-cli/src/commands/daemon.rs (machine_address_for_pods)
if [ -z "${WEFT_E2E_S3_ENDPOINT:-}" ] && command -v docker >/dev/null 2>&1; then
  MACHINE_IP="$(docker exec weft-local-control-plane getent hosts host.docker.internal 2>/dev/null \
    | awk '{print $1}' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)"
  if [ -z "$MACHINE_IP" ]; then
    MACHINE_IP="$(docker network inspect kind \
      --format '{{range .IPAM.Config}}{{.Gateway}}{{"\n"}}{{end}}' 2>/dev/null \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)"
  fi
  if [ -n "$MACHINE_IP" ] && [ -n "$(docker ps -q -f name='^weft-object-store$')" ]; then
    # The bucket stays across runs (it is the store the daemon owns,
    # not ours to tear down), so "already exists" is as good as
    # created. Any OTHER failure must not export the S3 vars: the test
    # would then fail deep inside the run instead of skipping loudly.
    # weed shell's exit code and chatter are both unreliable (it prints
    # "created" even for an existing bucket and exits 0 on errors), so
    # the create is fire-and-forget and the LIST afterwards is the one
    # source of truth: the bucket exists or the S3 e2e skips.
    docker exec weft-object-store sh -c \
      'echo "s3.bucket.create -name weft-e2e" | weed shell' >/dev/null 2>&1
    BUCKET_OUT="$(docker exec weft-object-store sh -c \
      'echo "s3.bucket.list" | weed shell' 2>&1)"
    BUCKET_STATUS=$?
    if [ "$BUCKET_STATUS" -eq 0 ] && printf '%s\n' "$BUCKET_OUT" | grep -q 'weft-e2e'; then
      export WEFT_E2E_S3_ENDPOINT="http://$MACHINE_IP:${WEFT_SEAWEED_PORT:-9096}"
      export WEFT_E2E_S3_REGION="us-east-1"
      export WEFT_E2E_S3_ACCESS_KEY_ID="weft-local"
      export WEFT_E2E_S3_SECRET_ACCESS_KEY="weft-local-dev-secret"
      export WEFT_E2E_S3_BUCKET="weft-e2e"
      echo "provisioned: S3 vars pointed at the daemon's SeaweedFS ($WEFT_E2E_S3_ENDPOINT, bucket weft-e2e)"
    else
      echo "SKIP: the weft-e2e bucket does not exist in the daemon's SeaweedFS (list exit $BUCKET_STATUS);" >&2
      echo "      not exporting WEFT_E2E_S3_* so the S3 e2e skips instead of failing deep. weed shell said:" >&2
      printf '%s\n' "$BUCKET_OUT" >&2
    fi
  else
    echo "warning: no running weft-object-store container (or no kind network); the S3 e2e will skip" >&2
  fi
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

# What the cluster looked like when a test failed, next to its log: every
# pod, the recent events, and the dispatcher logs of the default install and
# of any cell the failed test kept.
post_mortem() {
  local t="$1" dir="$RUN_DIR/$1.post-mortem"
  mkdir -p "$dir"
  kubectl get pods -A -o wide >"$dir/pods.txt" 2>&1
  kubectl get events -A --sort-by=.lastTimestamp >"$dir/events.txt" 2>&1
  # SYNC: weft-system <-> crates/weft-core/src/infra/instance.rs (the default install's system_namespace)
  kubectl -n weft-system logs statefulset/weft-dispatcher --tail=2000 >"$dir/dispatcher.log" 2>&1
  # The pooled tiers the test's signals and infra went through.
  local pod
  for pod in $(kubectl -n weft-system get pods -o name 2>/dev/null | grep -E 'pod/(listener|weft-infra-supervisor)-'); do
    kubectl -n weft-system logs "$pod" --tail=2000 >"$dir/${pod#pod/}.log" 2>&1
  done
  local cell
  for cell in $(sed -n "s/.*cell '\(e2e[a-z0-9]*\)' NOT finished.*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
    kubectl -n "weft-$cell-system" logs statefulset/weft-dispatcher --tail=2000 \
      >"$dir/dispatcher-$cell.log" 2>&1
    kubectl -n "weft-$cell-system" get pods -o wide >"$dir/pods-$cell.txt" 2>&1
  done
  # The worker pods of every project the test kept, while they still run
  # (an idle worker exits, taking its log with it).
  local project
  for project in $(sed -n "s/.*project '[^']*' (\([0-9a-f-]*\)) NOT finished.*/\1/p" "$RUN_DIR/$t.log" | sort -u); do
    kubectl get pods -A -l "weft.dev/project=$project" \
      -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' 2>/dev/null \
      | while IFS=/ read -r ns pod; do
          kubectl -n "$ns" logs "$pod" --all-containers --tail=5000 >"$dir/worker-$pod.log" 2>&1
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

# Every test this run has not seen pass (failed, cut short, never started)
# goes on the --failed list.
record_unfinished() {
  local t
  declare -A finished=()
  for t in "${PASSED[@]}" "${FAILED[@]}"; do finished["$t"]=1; done
  for t in "${TESTS[@]}"; do [ -n "${finished[$t]:-}" ] || echo "$t" >> "$FAILED_LIST"; done
}

# Ctrl-C or TERM: stop every running test and what it started, wait for
# them, so nothing outlives the lock and the Postgres port-forward; then
# leave the list --failed needs.
on_interrupt() {
  local code="$1" pid
  trap '' INT TERM
  echo ""
  echo "interrupted: stopping ${#RUNNING[@]} running test(s)"
  for pid in "${!RUNNING[@]}"; do
    if [ ${#SETSID[@]} -gt 0 ]; then
      kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
    else
      pkill -TERM -P "$pid" 2>/dev/null
      kill -TERM "$pid" 2>/dev/null
    fi
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
# would keep deploying against the cluster after the runner is gone.
trap 'on_interrupt 129' HUP

finish_one() {
  local pid="$1" status="$2" t="${RUNNING[$1]}"
  unset "RUNNING[$pid]"
  local secs=$(( $(date +%s) - STARTED[$t] ))
  if [ "$status" -eq 0 ]; then
    PASSED+=("$t")
    LAST_SECS["$t"]="$secs"
    printf '  ok    %-70s %4ss\n' "$t" "$secs"
  else
    FAILED+=("$t")
    echo "$t" >> "$FAILED_LIST"
    printf '  FAIL  %-70s %4ss   log: %s\n' "$t" "$secs" "$RUN_DIR/$t.log"
    post_mortem "$t"
    if [ "$STOPPING" -eq 0 ] && [ "$KEEP_GOING" -eq 0 ]; then
      STOPPING=1
      echo "  (a test failed: starting no new ones, letting the running ones finish)"
    fi
  fi
}

wait_one() {
  local done_pid status
  wait -n -p done_pid "${!RUNNING[@]}"
  status=$?
  finish_one "$done_pid" "$status"
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
  # and everything it started (weft, kubectl, docker) in one kill.
  ( cd "$REPO_ROOT/crates/weft-e2e" && exec "${SETSID[@]}" "$bin" --exact "${t#*::}" --test-threads=1 --nocapture ) \
    >"$RUN_DIR/$t.log" 2>&1 &
  RUNNING[$!]="$t"
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
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "${#FAILED[@]} failed, ${#PASSED[@]} passed, $NOT_RUN not started."
  echo "Each failure kept what it made; its log and a post-mortem are in $RUN_DIR."
  echo "It stays until the next run starts. Re-run what has not passed yet (failed or not started) with --failed."
  exit 1
fi

# Every test passed, so every test removed what it made, and the worker
# images of the fixtures this run built now reference nothing.
if ! weft clean --images --all >"$E2E_DIR/clean.log" 2>&1; then
  echo "All ${#PASSED[@]} e2e tests passed, BUT the final worker-image reclaim failed ($E2E_DIR/clean.log)." >&2
  echo "Re-run 'weft clean --images --all' by hand." >&2
  exit 1
fi
echo "All ${#PASSED[@]} e2e tests passed."
