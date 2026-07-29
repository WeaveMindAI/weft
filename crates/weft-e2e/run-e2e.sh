#!/usr/bin/env bash
# Run the Layer-4 e2e suite test-by-test, STOPPING at the first failure.
#
# Why not a single `cargo test`: the tests share one real cluster. If a test
# fails we want to STOP immediately and inspect the cluster in the exact state
# that test left it, NOT keep running the rest and pile more state on top (which
# buries the evidence and can cascade). A passing test cleans up after itself
# (its project is `weft rm`'d and any pooled-pod clone is swept on its success
# path); a FAILING test deliberately leaves its project + clones behind, and
# this script halts right there so you can look.
#
# Usage:
#   crates/weft-e2e/run-e2e.sh                   # every test, in order, stop on first fail
#   crates/weft-e2e/run-e2e.sh listener_move     # just these test binaries, in order
#   crates/weft-e2e/run-e2e.sh --from storage    # the full ordered suite, but START at
#                                                # the first test whose name contains
#                                                # `storage`, skipping everything before it
#
# `--from <name>` is the "resume where it broke" switch: when a mid-suite run
# stops on a failure, re-run with `--from <that_test>` (a substring is enough) to
# pick up from that test through the end, in the same order, without listing the
# rest by hand. It matches against the FULL ordered suite, so the heavy tests
# still run last.
#
# On failure the script prints which test failed and exits non-zero; the cluster
# is left as-is for investigation (see the crate README for the read-only kubectl
# probes that are allowed).
set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/../.." || exit 1

# ---------- Auto-provisioned dependencies ----------
# Everything a test needs that a local machine can serve is provisioned
# HERE, automatically, and torn down when the suite passes (a failing
# run keeps it up, like the cluster, for inspection). Only genuinely
# external services (Slack, GitHub, Google, Telegram, a real mailbox,
# an OpenRouter key) come from the operator's env / repo-root .env.
# Nothing is overridden: any WEFT_E2E_* var already set wins.

CLEANUP=()
cleanup_provisioned() {
  for c in "${CLEANUP[@]}"; do eval "$c"; done
}

# The cluster itself must exist before anything can be port-forwarded;
# the first test's ensure step re-runs setup.sh anyway (idempotent), so
# this only pays the bring-up when the cluster is absent outright.
if ! kubectl get namespace weft-db >/dev/null 2>&1; then
  echo "cluster not up; running setup.sh first"
  ./setup.sh || exit 1
fi

# The store's Postgres, for the tests that seed rows directly: a
# port-forward of the cluster's own weft-postgres (the local-dev creds
# from deploy/k8s/postgres.yaml).
if [ -z "${WEFT_E2E_DATABASE_URL:-}" ]; then
  kubectl -n weft-db port-forward svc/weft-postgres 15433:5432 >/dev/null 2>&1 &
  PF_PID=$!
  CLEANUP+=("kill $PF_PID 2>/dev/null || true")
  export WEFT_E2E_DATABASE_URL="postgres://weft:weft-local-dev@127.0.0.1:15433/weft"
  for _ in $(seq 1 30); do
    (exec 3<>/dev/tcp/127.0.0.1/15433) 2>/dev/null && break
    sleep 1
  done
  echo "provisioned: WEFT_E2E_DATABASE_URL via port-forward (pid $PF_PID)"
fi

# An S3 store: the daemon ALREADY runs SeaweedFS as a host docker
# container ('weft-object-store', S3 gateway on WEFT_SEAWEED_PORT,
# local-dev identities); reuse it with a dedicated e2e bucket rather
# than spawning a second store. Pods reach it at the kind docker
# network's gateway address; the broker's egress denies private
# ranges, so that address is opened via WEFT_STORE_ALLOW_CIDR (the
# knob that exists exactly for a private-range object store), applied
# by the first test's setup.sh run.
if [ -z "${WEFT_E2E_S3_ENDPOINT:-}" ] && command -v docker >/dev/null 2>&1; then
  KIND_GATEWAY="$(docker network inspect kind \
    --format '{{range .IPAM.Config}}{{.Gateway}}{{"\n"}}{{end}}' 2>/dev/null \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)"
  if [ -n "$KIND_GATEWAY" ] && [ -n "$(docker ps -q -f name='^weft-object-store$')" ]; then
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
      export WEFT_E2E_S3_ENDPOINT="http://$KIND_GATEWAY:${WEFT_SEAWEED_PORT:-9096}"
      export WEFT_E2E_S3_REGION="us-east-1"
      export WEFT_E2E_S3_ACCESS_KEY_ID="weft-local"
      export WEFT_E2E_S3_SECRET_ACCESS_KEY="weft-local-dev-secret"
      export WEFT_E2E_S3_BUCKET="weft-e2e"
      export WEFT_STORE_ALLOW_CIDR="${WEFT_STORE_ALLOW_CIDR:-$KIND_GATEWAY/32}"
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

# Test binaries are DISCOVERED from crates/weft-e2e/tests/*.rs (never a
# hardcoded list, which silently drops a newly-added test if someone forgets
# to update it). A new `tests/<name>.rs` is picked up automatically.
mapfile -t DISCOVERED < <(
  find "$SCRIPT_DIR/tests" -maxdepth 1 -name '*.rs' -printf '%f\n' \
    | sed 's/\.rs$//' \
    | sort
)
if [ ${#DISCOVERED[@]} -eq 0 ]; then
  echo "no e2e test files found under $SCRIPT_DIR/tests" >&2
  exit 1
fi

# Run order is alphabetical EXCEPT the heavy pooled-pod overlap scenarios go
# LAST (they clone real pods + drive scale-down, the slowest and most
# cluster-stateful), so a cheap breakage surfaces before we pay for them. This
# is a substring match on names, so it keeps working as tests are added/renamed
# without listing each test by hand.
LAST_PATTERN='supervisor_pool|listener_scaledown|listener_move'
HEAVY=()
LIGHT=()
for t in "${DISCOVERED[@]}"; do
  if [[ "$t" =~ $LAST_PATTERN ]]; then
    HEAVY+=("$t")
  else
    LIGHT+=("$t")
  fi
done
ALL_TESTS=("${LIGHT[@]}" "${HEAVY[@]}")

# `--from <name>`: run the full ordered suite starting at the first test whose
# name contains <name> (a substring), through the end. The "resume where it
# broke" switch, so a mid-suite failure needs no re-listing of what's left.
if [ "${1:-}" = "--from" ]; then
  FROM="${2:-}"
  if [ -z "$FROM" ]; then
    echo "--from needs a test name (substring), e.g. --from storage_multipart" >&2
    exit 1
  fi
  START=-1
  for i in "${!ALL_TESTS[@]}"; do
    if [[ "${ALL_TESTS[$i]}" == *"$FROM"* ]]; then
      START=$i
      break
    fi
  done
  if [ "$START" -lt 0 ]; then
    echo "--from: no test name contains '$FROM'. Known tests, in run order:" >&2
    printf '  %s\n' "${ALL_TESTS[@]}" >&2
    exit 1
  fi
  TESTS=("${ALL_TESTS[@]:$START}")
  echo "Resuming from '${ALL_TESTS[$START]}' (${#TESTS[@]} test(s) remaining)."
else
  # Bare test names (no flags): `run-e2e.sh openrouter storage`. An unknown
  # flag would otherwise be taken for a test name and produce a baffling
  # cargo error, so reject it here with the usage.
  for arg in "$@"; do
    if [[ "$arg" == -* ]]; then
      echo "unknown option '$arg'." >&2
      echo "usage: run-e2e.sh [--from <name>] [<test> ...]" >&2
      echo "  no args      run every test, in order" >&2
      echo "  <test> ...   run only these (bare names, no --test)" >&2
      echo "  --from <n>   run the ordered suite starting at the first name containing <n>" >&2
      echo "" >&2
      echo "known tests, in run order:" >&2
      printf '  %s\n' "${ALL_TESTS[@]}" >&2
      exit 1
    fi
  done
  TESTS=("$@")
  if [ ${#TESTS[@]} -eq 0 ]; then
    TESTS=("${ALL_TESTS[@]}")
  fi
fi

for t in "${TESTS[@]}"; do
  echo ""
  echo "========================================================================"
  echo "  e2e: $t"
  echo "========================================================================"
  if ! cargo test -p weft-e2e --features e2e --test "$t" -- --test-threads=1; then
    echo ""
    echo "########################################################################"
    echo "  FAILED: $t  -- stopping. Cluster left as-is for investigation."
    echo "  (passing tests already cleaned up; this one's project + any clones"
    echo "   are kept. See crates/weft-e2e/README.md for allowed read-only probes.)"
    echo "########################################################################"
    echo "  (the auto-provisioned postgres port-forward is kept up too.)"
    exit 1
  fi
done

cleanup_provisioned
echo ""
echo "All e2e tests passed."
