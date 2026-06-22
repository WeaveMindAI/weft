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
#   crates/weft-e2e/run-e2e.sh                # every test, in order, stop on first fail
#   crates/weft-e2e/run-e2e.sh listener_move  # just these test binaries, in order
#
# On failure the script prints which test failed and exits non-zero; the cluster
# is left as-is for investigation (see the crate README for the read-only kubectl
# probes that are allowed).
set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/../.." || exit 1

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

TESTS=("$@")
if [ ${#TESTS[@]} -eq 0 ]; then
  TESTS=("${ALL_TESTS[@]}")
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
    exit 1
  fi
done

echo ""
echo "All e2e tests passed."
