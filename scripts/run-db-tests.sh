#!/usr/bin/env bash
# Run the suites that need a real Postgres, against a throwaway one.
#
# These tests are behind the `db-tests` feature, off by default, so a plain
# `cargo test` never builds them. They read $DATABASE_URL and create their own
# randomly named database per test, dropping it afterwards.
#
# Why a container rather than the Postgres weft runs: these tests create and
# drop databases on whatever server they are pointed at, and the one your local
# weft uses is holding your projects and your execution history.
#
# Usage:
#   scripts/run-db-tests.sh                 # every crate that has them
#   scripts/run-db-tests.sh weft-broker     # just this crate
#   scripts/run-db-tests.sh weft-dispatcher an_upgraded_database
#                                           # and just tests matching a name
#                                           # (a filter matches TEST NAMES,
#                                           # not file names; matching none
#                                           # fails loudly)
#   scripts/run-db-tests.sh weft-dispatcher test_name db_lifecycle
#                                           # compile and run only this test file
#
# Set DATABASE_URL yourself and the script uses that server instead of
# starting a container, which is what CI does.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

# Every crate whose tests need a database. A new one goes here.
ALL_CRATES=(weft-dispatcher weft-broker weft-access-store weft-task-store)
CRATES=("${ALL_CRATES[@]}")

only_crate="${1:-}"
filter="${2:-}"
test_target="${3:-}"
if [ "$#" -gt 3 ] || { [ -n "$test_target" ] && { [ -z "$only_crate" ] || [ -z "$filter" ]; }; }; then
  echo "usage: scripts/run-db-tests.sh [crate [test-name-filter [test-file]]]" >&2
  exit 1
fi
if [ -n "$only_crate" ]; then
  found=0
  for c in "${ALL_CRATES[@]}"; do [ "$c" = "$only_crate" ] && found=1; done
  if [ "$found" -eq 0 ]; then
    echo "no db tests in '$only_crate'; it is one of: ${ALL_CRATES[*]}" >&2
    exit 1
  fi
  CRATES=("$only_crate")
fi

command -v cargo-nextest >/dev/null 2>&1 || {
  echo "cargo-nextest is missing; install it with: cargo install cargo-nextest --locked" >&2
  exit 1
}

if [ -z "${DATABASE_URL:-}" ]; then
  # shellcheck source=lib/throwaway-postgres.sh
  . "$SCRIPT_DIR/lib/throwaway-postgres.sh"
  echo "waiting for postgres..."
  # Trap installed BEFORE the start: the helper exports the container
  # name before running it, so a Ctrl-C during the pull or the
  # readiness wait still removes it.
  trap 'docker rm -f -v "${THROWAWAY_PG_CONTAINER:-}" >/dev/null 2>&1 || true' EXIT
  start_throwaway_postgres weft-db-tests || exit 1
  export DATABASE_URL="$THROWAWAY_DATABASE_URL"
fi

echo "running against $DATABASE_URL"
# Every crate in ONE nextest run: cargo builds them all at once, and
# nextest runs every test of every test binary side by side (cargo test
# runs the binaries one after another). Each test makes its own database,
# so nothing they share needs them in order.
# Every crate is BUILT every time, with the same features, and the crate,
# file and name narrow only what RUNS: building one crate alone unifies
# its dependencies' features differently, and cargo would rebuild them
# each time a run switched between one crate and all of them.
nextest_args=()
for crate in "${ALL_CRATES[@]}"; do
  nextest_args+=(-p "$crate" --features "$crate/db-tests")
done
selection=""
for crate in "${CRATES[@]}"; do
  selection="${selection:+$selection | }package($crate)"
done
selection="($selection)"
[ -n "$test_target" ] && selection="$selection & binary(=$test_target)"
# A filter is a plain substring of TEST NAMES (`~`, nextest's contains
# matcher), not a regex and not a file name. nextest refuses a run in
# which no test matched, so a filter that selects nothing fails loudly
# instead of reporting green on a suite where nothing executed.
[ -n "$filter" ] && selection="$selection & test(~$filter)"
nextest_args+=(-E "$selection")
cargo nextest run --no-fail-fast "${nextest_args[@]}"
