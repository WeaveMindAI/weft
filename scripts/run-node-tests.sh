#!/usr/bin/env bash
# Run the catalog's node self-tests package-by-package, STOPPING at the
# first failing package (the same discipline as scripts/run-e2e.sh:
# a failure halts right there so its output is the last thing on screen,
# instead of burying it under the rest of the suite).
#
# Usage:
#   scripts/run-node-tests.sh                       # every package, basic + fake
#   scripts/run-node-tests.sh --tier live           # every package, live tier
#   scripts/run-node-tests.sh --tier basic --tier fake --tier live
#   scripts/run-node-tests.sh web slack             # just these packages (or node types)
#   scripts/run-node-tests.sh web --test one_real_single_result_search --tier live
#   scripts/run-node-tests.sh --from slack          # the full ordered suite, but START
#                                                   # at the first package whose name
#                                                   # contains `slack`
#
# `--from <name>` is the "resume where it broke" switch: when a mid-suite
# run stops on a failure, re-run with `--from <that_package>` to pick up
# from there through the end without listing the rest by hand.
#
# Live runs remember what already passed: after a package's whole live
# tier runs green, its `weft node-test-hash` is recorded in the scratch
# project's `.live-pass-cache`, and later live runs skip the package
# until its test OUTCOME inputs move: the package's own code (nodes,
# tests, shared package files) or the catalog's type registry (a
# sibling's type edit changes how this package's ports resolve).
# Engine edits recompile the test server but do not invalidate a
# recorded pass (live tests cost real money).
# `--retest` forces the recorded packages to run anyway.
#
# The suite runs inside a scratch project (target/node-tests, created on
# first use) whose base catalog is re-synced from THIS checkout on every
# run, so the tests always exercise the checkout's nodes, with the weft of
# the install ./setup.sh set up for this checkout (on a branch, the
# branch's own). Each `weft test-node` compiles its packages first, so it
# takes its turn in the machine's build slot (one heavy cargo build at a
# time, shared with setup.sh and the e2e runner): sequentially one
# package at a time, with --parallel the one call that tests them all.
#
# Live-tier credentials come from the environment / the repo-root .env
# (uncommitted; the weft CLI loads it itself). For a pasted-key
# service the CLI reads WEFT_NODE_TEST_<SERVICE>_<FIELD> (for example
# WEFT_NODE_TEST_EXA_KEY) and mints an ephemeral grant, deleted after
# the run. OAuth services (slack, google, ...) cannot be pasted: connect
# them once in the editor and the CLI picks the grant up automatically.
# Live tests that need a target the account cannot self-provision read
# WEFT_NODE_TEST_<NAME> fixtures from the same environment (for example
# WEFT_NODE_TEST_SLACK_CHANNEL_ID, WEFT_NODE_TEST_S3_BUCKET,
# WEFT_NODE_TEST_GMAIL_TO, WEFT_NODE_TEST_GOOGLE_SHEET_ID,
# WEFT_NODE_TEST_GOOGLE_DOC_ID); a missing
# one fails that test naming exactly what to set.
set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || exit 1

# shellcheck source=scripts/lib/machine-lock.sh
. "$SCRIPT_DIR/lib/machine-lock.sh"
# shellcheck source=scripts/lib/checkout-install.sh
. "$SCRIPT_DIR/lib/checkout-install.sh"
trap machine_lock_release_mine EXIT

# This checkout's install, its own weft first on PATH and named for every
# weft this script starts.
# SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
# WEFT_INSTALL=<name> (`default` is the main install) runs the tests with
# that install's weft instead, from any checkout and with no setup.sh
# here: the weft is only the tool, the nodes under test are this
# checkout's (WEFT_REPO_ROOT below). Right for a branch that changes
# catalog nodes; one that changes the compiler or the CLI should be
# tested with its own install.
if [ -n "${WEFT_INSTALL:-}" ]; then
  weft_which "$REPO_ROOT" "$(command -v weft || echo weft)" "$WEFT_INSTALL" || exit 1
  if [ "$WHICH_INSTALLED" != 1 ]; then
    echo "WEFT_INSTALL=$WEFT_INSTALL names the install in $WHICH_DIR, which has no weft; run ./setup.sh in the checkout that owns it" >&2
    exit 1
  fi
else
  weft_checkout_install "$REPO_ROOT" || exit 1
fi
export WEFT_INSTALL="$WHICH_ENV"
export PATH="$WHICH_BIN:$PATH"
# A stale binary without the test-node verb would fail confusingly
# mid-suite; refuse up front naming the fix.
if ! weft test-node --help >/dev/null 2>&1; then
  echo "the weft of this checkout's install ($WHICH_BIN/weft) is too old (no test-node verb); rebuild it (./setup.sh)" >&2
  exit 1
fi

# The installed weft resolves its stdlib through WEFT_REPO_ROOT first:
# pin it to this checkout so the scratch project's base catalog (and the
# test servers) are built from the code being tested, not from wherever
# the binary was installed.
export WEFT_REPO_ROOT="$REPO_ROOT"

# The weft CLI loads the repo-root .env itself (dotenvy walks up from
# the working directory), so nothing is exported here.

# ---------- Arguments ----------
TIERS=()
TARGETS=()
FROM=""
TEST_NAME=""
PARALLEL=""   # ""=sequential, 0=all at once, N=up to N side by side
RETEST=0      # 1 = ignore the live-pass cache (still records fresh passes)
usage() {
  echo "usage: run-node-tests.sh [--tier basic|fake|live]... [--from <name>] [--test <name>] [--parallel [N]] [<package|node> ...]" >&2
  echo "  no --tier      basic + fake" >&2
  echo "  no targets     every package with tests, in order" >&2
  echo "  --from <n>     the ordered suite starting at the first package containing <n>" >&2
  echo "  --parallel     one weft test-node for every package: one build, then every" >&2
  echo "                 package side by side; --parallel N runs up to N at once." >&2
  echo "                 Every package runs, failing or not; each one's output is" >&2
  echo "                 printed as one block when it finishes." >&2
  echo "  --retest       live: re-run packages whose exact code already passed" >&2
}
while [ $# -gt 0 ]; do
  case "$1" in
    --tier)
      [ $# -ge 2 ] || { echo "--tier needs a value (basic|fake|live)" >&2; exit 1; }
      TIERS+=("$2"); shift 2 ;;
    --from)
      [ $# -ge 2 ] || { echo "--from needs a package name (substring)" >&2; exit 1; }
      FROM="$2"; shift 2 ;;
    --test)
      [ $# -ge 2 ] || { echo "--test needs a test name" >&2; exit 1; }
      TEST_NAME="$2"; shift 2 ;;
    --parallel)
      # Bare --parallel = everything at once (0 = "all");
      # --parallel N = up to N packages side by side.
      if [ $# -ge 2 ] && [[ "$2" =~ ^[0-9]+$ ]]; then
        PARALLEL="$2"; shift 2
      else
        PARALLEL=0; shift
      fi ;;
    --retest) RETEST=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option '$1'." >&2; usage; exit 1 ;;
    *) TARGETS+=("$1"); shift ;;
  esac
done
if [ -n "$FROM" ] && [ ${#TARGETS[@]} -gt 0 ]; then
  echo "--from and explicit targets are alternatives; pick one" >&2
  exit 1
fi

# Ctrl+C / TERM aborts the WHOLE run, including the weft invocation
# in flight. The terminal's SIGINT already reaches every child (they
# share the process group); a TERM only reaches the script, so it is
# forwarded. Either way the script then WAITS: each weft invocation
# needs its moment to delete the ephemeral grants it created.
aborted() {
  trap - INT TERM
  [ "$1" = TERM ] && kill -TERM 0 2>/dev/null
  wait
  echo ""
  echo "aborted; in-flight invocations were waited out so their cleanup ran" >&2
  exit 130
}
trap 'aborted INT' INT
trap 'aborted TERM' TERM

TIER_FLAGS=()
LIVE=0
for t in ${TIERS[@]+"${TIERS[@]}"}; do
  TIER_FLAGS+=("--tier" "$t")
  [ "$t" = "live" ] && LIVE=1
done
# The tier/test scope as one string, echoed into the re-run suggestions
# so a live failure's suggested command re-runs live, not the
# basic+fake default.
SCOPE=""
for t in ${TIERS[@]+"${TIERS[@]}"}; do SCOPE+=" --tier $t"; done
[ -n "$TEST_NAME" ] && SCOPE+=" --test $TEST_NAME"

# ---------- The scratch project ----------
PROJECT_DIR="$REPO_ROOT/target/node-tests"
if [ ! -f "$PROJECT_DIR/weft.toml" ]; then
  echo "creating the scratch project at target/node-tests"
  mkdir -p "$REPO_ROOT/target"
  (cd "$REPO_ROOT/target" && weft new node-tests --assistant none) || exit 1
else
  # Re-sync the base catalog so edits in this checkout's catalog/ are
  # what actually runs.
  (cd "$PROJECT_DIR" && weft catalog update) || exit 1
fi

# ---------- Package discovery ----------
# Packages whose nodes declare a tests.rs, from THIS checkout's catalog
# (never a hardcoded list: a new tests.rs is picked up automatically).
# A tests.rs's package is the nearest enclosing package.toml; a node
# with no enclosing package.toml is a bare node, whose package name is
# its own directory name (the catalog's discovery rule).
mapfile -t ALL_PACKAGES < <(
  find "$REPO_ROOT/catalog" -name tests.rs | while IFS= read -r f; do
    node_dir="$(dirname "$f")"
    dir="$node_dir"
    found=""
    while [ "$dir" != "$REPO_ROOT" ] && [ "$dir" != "$(dirname "$dir")" ]; do
      if [ -f "$dir/package.toml" ]; then
        sed -n 's/^name *= *"\(.*\)".*/\1/p' "$dir/package.toml" | head -1
        found=1
        break
      fi
      dir="$(dirname "$dir")"
    done
    [ -z "$found" ] && basename "$node_dir"
  done | sort -u
)
if [ ${#ALL_PACKAGES[@]} -eq 0 ]; then
  echo "no catalog package declares node tests (no tests.rs under catalog/)" >&2
  exit 1
fi

if [ -n "$FROM" ]; then
  START=-1
  for i in "${!ALL_PACKAGES[@]}"; do
    if [[ "${ALL_PACKAGES[$i]}" == *"$FROM"* ]]; then
      START=$i
      break
    fi
  done
  if [ "$START" -lt 0 ]; then
    echo "--from: no package name contains '$FROM'. Packages with tests, in run order:" >&2
    printf '  %s\n' "${ALL_PACKAGES[@]}" >&2
    exit 1
  fi
  TARGETS=("${ALL_PACKAGES[@]:$START}")
  echo "Resuming from '${ALL_PACKAGES[$START]}' (${#TARGETS[@]} package(s) remaining)."
elif [ ${#TARGETS[@]} -eq 0 ]; then
  TARGETS=("${ALL_PACKAGES[@]}")
fi

# ---------- Live prerequisites ----------
if [ "$LIVE" = 1 ]; then
  # Live tests run on the install's workers; bring it up if it does not
  # answer.
  # A start may compile the runtime's image, so it takes the build slot.
  if ! weft daemon status 2>/dev/null | grep -q '^weft: running'; then
    echo "weft is not running; running weft daemon start first"
    with_build_slot weft daemon start || exit 1
  fi
  # The dispatcher only runs node tests for a registered project; a
  # first `weft run` registers it (and proves the build pipeline).
  # Content-addressed workers make this cheap after the first time; the
  # first time compiles the worker, so it takes the build slot too.
  register_project() { (cd "$PROJECT_DIR" && weft run); }
  with_build_slot register_project || exit 1
fi

# ---------- The live-pass cache ----------
# Live tests cost real provider money, so a package whose code has not
# moved since its last fully green live run is skipped. Identity is
# `weft node-test-hash`: the package's own files (nodes, tests, shared
# package files) plus the catalog's type registry; engine edits
# recompile the test server but never re-spend a recorded pass.
# Recorded ONLY after a run
# that covered the package's whole live tier (never under --test);
# `--retest` ignores recorded passes for this run.
CACHE_FILE="$PROJECT_DIR/.live-pass-cache"
declare -A PKG_HASH
CACHE_ACTIVE=0
if [ "$LIVE" = 1 ] && [ -z "$TEST_NAME" ]; then
  CACHE_ACTIVE=1
  if ! HASHES="$(cd "$PROJECT_DIR" && weft node-test-hash)"; then
    echo "weft node-test-hash failed; the weft on PATH may predate it (rebuild via ./setup.sh)" >&2
    exit 1
  fi
  while read -r pkg hash; do
    [ -n "$pkg" ] && PKG_HASH["$pkg"]="$hash"
  done <<< "$HASHES"
fi

# True when this package's recorded live pass matches its current hash.
cache_hit() {
  [ "$CACHE_ACTIVE" = 1 ] || return 1
  [ "$RETEST" = 0 ] || return 1
  [ -f "$CACHE_FILE" ] || return 1
  local hash="${PKG_HASH[$1]-}"
  [ -n "$hash" ] && grep -qxF "$1 $hash" "$CACHE_FILE"
}

# Record a package's fully green live run (replacing its old line).
cache_record() {
  [ "$CACHE_ACTIVE" = 1 ] || return 0
  local hash="${PKG_HASH[$1]-}"
  [ -n "$hash" ] || return 0
  { [ -f "$CACHE_FILE" ] && awk -v p="$1" '$1 != p' "$CACHE_FILE"; echo "$1 $hash"; } \
    > "$CACHE_FILE.tmp" && mv "$CACHE_FILE.tmp" "$CACHE_FILE"
}

# ---------- Run ----------
# One `weft test-node` over the given packages, with the run's scope.
# Under --parallel the CLI runs the packages side by side, and inside
# each package the tests too, under the same cap (0 = all at once).
run_pkgs() {
  local CMD=(weft test-node "$@")
  CMD+=(${TIER_FLAGS[@]+"${TIER_FLAGS[@]}"})
  [ -n "$TEST_NAME" ] && CMD+=(--test "$TEST_NAME")
  [ "$LIVE" = 1 ] && CMD+=(--yes)
  [ -n "$PARALLEL" ] && CMD+=(--parallel "$PARALLEL")
  (cd "$PROJECT_DIR" && "${CMD[@]}")
}

# One package in the build slot, for the sequential run.
run_pkg_in_slot() {
  with_build_slot run_pkgs "$1"
}

fail_banner() {
  echo ""
  echo "########################################################################"
  echo "  FAILED: $1  -- stopping."
  echo "  Re-run just this scope:   scripts/run-node-tests.sh $1$SCOPE"
  echo "  Resume the suite here:    scripts/run-node-tests.sh --from $1$SCOPE"
  echo "########################################################################"
}

# Drop the packages whose recorded live pass still matches, so both
# modes below only see real work (and the X/Y counts mean "to run").
RUN_LIST=()
for pkg in "${TARGETS[@]}"; do
  if cache_hit "$pkg"; then
    echo "· $pkg: unchanged since its last green live run, skipping (--retest to force)"
  else
    RUN_LIST+=("$pkg")
  fi
done
TOTAL=${#RUN_LIST[@]}
if [ "$TOTAL" = 0 ]; then
  echo ""
  echo "Every selected package is unchanged since its last green live run."
  exit 0
fi

if [ -z "$PARALLEL" ]; then
  n=0
  for pkg in "${RUN_LIST[@]}"; do
    n=$((n + 1))
    echo ""
    echo "========================================================================"
    echo "  node tests: $pkg  ($n/$TOTAL)"
    echo "========================================================================"
    if ! run_pkg_in_slot "$pkg"; then
      fail_banner "$pkg"
      exit 1
    fi
    cache_record "$pkg"
  done
else
  # One weft test-node for every package: one workspace, one build, then
  # the packages side by side. Its stdout (the per-test lines and, with
  # several packages, one line per package) is kept to learn which
  # packages passed; their output blocks go to stderr, straight through.
  OUT_FILE="$(mktemp)"
  trap 'rm -f "$OUT_FILE"; machine_lock_release_mine' EXIT
  run_all() {
    run_pkgs "${RUN_LIST[@]}" | tee "$OUT_FILE"
    return "${PIPESTATUS[0]}"
  }
  st=0
  with_build_slot run_all || st=$?
  if [ "$st" = 0 ]; then
    for pkg in "${RUN_LIST[@]}"; do cache_record "$pkg"; done
  else
    # With several packages the CLI ends on one line per package:
    # `✓ package <name>: ...` or `✗ package <name>: ...`.
    # SYNC: the `✓ package` lines <-> crates/weft-cli/src/commands/test_node/mod.rs (package_line)
    mapfile -t PASSED < <(sed -n 's/^✓ package \([^:]*\):.*/\1/p' "$OUT_FILE")
    mapfile -t FAILED < <(sed -n 's/^✗ package \([^:]*\):.*/\1/p' "$OUT_FILE")
    for pkg in ${PASSED[@]+"${PASSED[@]}"}; do cache_record "$pkg"; done
    if [ ${#FAILED[@]} -gt 0 ]; then
      for pkg in "${FAILED[@]}"; do
        fail_banner "$pkg"
      done
    elif [ "$TOTAL" = 1 ]; then
      fail_banner "${RUN_LIST[0]}"
    else
      # No package line: the targets were one package in the end (node
      # types of it), or it stopped before any package ran (the build, a
      # target that did not resolve). Either way the error is above.
      echo ""
      echo "########################################################################"
      echo "  FAILED (see above)  -- stopping."
      echo "########################################################################"
    fi
    exit 1
  fi
fi

echo ""
echo "All selected node tests passed."
