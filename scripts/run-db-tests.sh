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
#
# Set DATABASE_URL yourself and the script uses that server instead of
# starting a container, which is what CI does.
# pipefail matters: the filtered branch pipes cargo through tee, and
# without it the pipeline's exit code is tee's, so a failing filtered
# run would report green.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

# Every crate whose tests need a database. A new one goes here.
CRATES=(weft-dispatcher weft-broker weft-access-store weft-task-store)

only_crate="${1:-}"
filter="${2:-}"
if [ -n "$only_crate" ]; then
  found=0
  for c in "${CRATES[@]}"; do [ "$c" = "$only_crate" ] && found=1; done
  if [ "$found" -eq 0 ]; then
    echo "no db tests in '$only_crate'; it is one of: ${CRATES[*]}" >&2
    exit 1
  fi
  CRATES=("$only_crate")
fi

if [ -z "${DATABASE_URL:-}" ]; then
  # shellcheck source=lib/throwaway-postgres.sh
  . "$SCRIPT_DIR/lib/throwaway-postgres.sh"
  echo "waiting for postgres..."
  # Trap installed BEFORE the start: the helper exports the container
  # name before running it, so a Ctrl-C during the pull or the
  # readiness wait still removes it.
  trap 'docker rm -f "${THROWAWAY_PG_CONTAINER:-}" >/dev/null 2>&1 || true' EXIT
  start_throwaway_postgres weft-db-tests || exit 1
  export DATABASE_URL="$THROWAWAY_DATABASE_URL"
fi

echo "running against $DATABASE_URL"
# A plain string, not an array: expanding an empty array under `set -u`
# is an error on the bash 3.2 macOS ships.
failed=""
for crate in "${CRATES[@]}"; do
  echo
  echo "=== $crate"
  if [ -n "$filter" ]; then
    crate_failed=0
    out="$(cargo test -p "$crate" --features db-tests -- "$filter" 2>&1 | tee /dev/stderr)" \
      || crate_failed=1
    # A filter that matches nothing "passes" with zero tests run, which
    # would report green on a suite where nothing executed. Filters
    # match TEST NAMES, not file names. Matched with a bash regex, not
    # a grep pipe (`grep -q` closes the pipe on the first hit, and
    # under pipefail the producer's SIGPIPE would read as a failure),
    # held in a variable so no escaping is needed (portable to the
    # bash 3.2 macOS ships). Checked only when the crate BUILT and
    # PASSED: a compile error or a test failure must not also blame
    # the filter.
    ran_some='test result:.* [1-9][0-9]* passed'
    if [ "$crate_failed" -eq 0 ] && [[ ! "$out" =~ $ran_some ]]; then
      echo "the filter '$filter' matched no test in $crate (filters match test names, not files)" >&2
      crate_failed=1
    fi
    [ "$crate_failed" -eq 1 ] && failed="$failed $crate"
  else
    cargo test -p "$crate" --features db-tests || failed="$failed $crate"
  fi
done

echo
if [ -n "$failed" ]; then
  echo "FAILED:$failed" >&2
  exit 1
fi
echo "all green"
