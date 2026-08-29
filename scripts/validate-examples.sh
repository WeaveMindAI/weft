#!/usr/bin/env bash
# Validate every example project through the real compile pipeline.
#
# The examples are the first thing a new user copies, and nothing else
# compiles them, so a broken graph or a dead @file(...) reference would
# ship silently. A fresh checkout has no credentials connected, so the
# connect-an-account diagnostics are the expected baseline; anything
# else fails.
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is not on PATH; it is needed to read the validate output" >&2
  exit 1
fi

status=0
for main in examples/*/main.weft; do
  dir="$(dirname "$main")"
  if ! out="$(cd "$dir" && cargo run --quiet --locked -p weft-cli -- validate < main.weft)"; then
    echo "FAIL $dir: weft validate itself errored" >&2
    status=1
    continue
  fi
  # A jq failure (the output shape changed) must fail the example, not
  # print "ok" over diagnostics nobody read.
  if ! unexpected="$(printf '%s' "$out" | jq -r '
    .diagnostics[]
    | select((.code == "rule-runtime"
              and ((.message | contains("has no connected"))
                   or (.message | contains("no connection picked")))) | not)
    | "  line \(.line): [\(.code)] \(.message)"')"; then
    echo "FAIL $dir: could not read the validate output as JSON:" >&2
    printf '%s\n' "$out" >&2
    status=1
    continue
  fi
  if [ -n "$unexpected" ]; then
    echo "FAIL $dir: diagnostics beyond the expected connect-a-credential ones:" >&2
    echo "$unexpected" >&2
    status=1
  else
    echo "ok $dir"
  fi
done
exit "$status"
