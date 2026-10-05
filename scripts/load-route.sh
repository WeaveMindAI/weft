#!/usr/bin/env bash
# Call one route many times at once and say how it held up: how many calls
# a second it answered, how long they took, and what they answered.
#
#   scripts/load-route.sh <url> [calls] [at-once] [method] [body]
#
#   scripts/load-route.sh http://127.0.0.1:14111/connect/local/load/ping 1000 50
#   scripts/load-route.sh http://127.0.0.1:14111/connect/local/load/burn 200 20 POST '{"rounds":200000}'
#
# Every call has a 30 second cap, and the whole run is capped at ten
# minutes. A call that hit its cap shows as status 000. The calls are made
# with curl, one process each, so past a few hundred calls a second it is
# this script, not the route, that is the limit.
set -euo pipefail

url=${1:?"usage: scripts/load-route.sh <url> [calls] [at-once] [method] [body]"}
calls=${2:-1000}
at_once=${3:-50}
method=${4:-GET}
body=${5:-}

out=$(mktemp)
trap 'rm -f "$out"' EXIT

args=(-s -o /dev/null -m 30 -X "$method" -w '%{http_code} %{time_total}\n')
if [[ -n "$body" ]]; then
  args+=(-H 'content-type: application/json' --data "$body")
fi

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
started=$(now)
seq "$calls" | timeout 600 xargs -P "$at_once" -I{} curl "${args[@]}" "$url" >"$out" || true
ended=$(now)

elapsed=$(echo "$ended - $started" | bc)
n=$(wc -l <"$out" | tr -d ' ')
if [[ "$n" -eq 0 ]]; then
  echo "no call answered"
  exit 1
fi
# The times in order, for the median and the 95th of them.
times=$(awk '{ print $2 }' "$out" | sort -n)
nth() { echo "$times" | sed -n "$1p"; }
echo "$n calls in $(printf '%.1f' "$elapsed")s: $(echo "scale=1; $n / $elapsed" | bc) a second"
echo "took: average $(awk '{ s += $2 } END { printf "%.3f", s / NR }' "$out")s, median $(nth $((n / 2 + 1)))s, 95th $(nth $((n * 95 / 100 + 1)))s, slowest $(nth "$n")s"
echo "answered: $(awk '{ print $1 }' "$out" | sort | uniq -c | awk '{ printf "%s x%s ", $2, $1 }')"
