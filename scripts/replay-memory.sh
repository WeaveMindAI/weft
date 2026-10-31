#!/usr/bin/env bash
# How much memory painting a long run's record takes: builds and runs
# `crates/weft-dispatcher/examples/replay_memory.rs` in release, which
# makes the record of a loop of N iterations (nine events each, a value of
# a kilobyte each by default), paints it a page at a time the way a replay
# does, and prints the peak resident memory. The default, 30000
# iterations, is a run of 270,000 events. The loop gathers each value into
# its output list, which grows with the run; with `no-gather` it gathers
# nothing.
#
# Usage:
#   scripts/replay-memory.sh                      # 30000 iterations
#   scripts/replay-memory.sh 15000                # another length
#   scripts/replay-memory.sh 30000 8              # values of 8 bytes
#   scripts/replay-memory.sh 30000 1024 no-gather # a loop that gathers nothing
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1
cargo run --release --example replay_memory -- "${1:-30000}" "${2:-1024}" ${3:+"$3"}
