#!/bin/sh
# Bounds a cargo compile cache one build owns (the runtime image's, the
# builder base's, a worker image's) to what that build used. Run after a
# `cargo build` whose JSON messages (`--message-format=json...`) went to
# a file: cargo reports there every unit the build used, the ones it
# reused untouched included, and everything else goes (superseded
# dependency versions, engine artifacts of an older lock, package slots
# the stdlib moved past).
#
# usage: weft-cache-gc.sh this-build <target/release> <cargo messages>
#
# A unit is known by the hash cargo names all its files with:
# `deps/lib<crate>-<hash>.rlib`, `deps/<crate>-<hash>.d`,
# `.fingerprint/<package>-<hash>/`, `build/<package>-<hash>/`. Removing a
# unit's files and its fingerprint together leaves cargo nothing stale: it
# compiles the unit again if a later build wants it.
#
# The source is crates/weft-compiler/src/weft-cache-gc.sh: codegen writes a
# copy into every crate it generates (`worker_image::CACHE_GC_SCRIPT`), and
# the runtime image runs it from the source tree it compiles.
# It never fails a build: every caller reports a failure and carries on.
set -eu
mode="$1"; release="$2"; messages="$3"
deps="$release/deps"; fingerprints="$release/.fingerprint"; scripts="$release/build"
[ -d "$deps" ] || exit 0
if ! [ -s "$messages" ]; then
  echo "weft: no cargo messages at $messages; the compile cache is left as it is" >&2
  exit 0
fi

used="$(mktemp)"
trap 'rm -f "$used"' EXIT
# Used: every unit cargo reported, by its hash.
grep -oE "$release/(deps|build)/[^\"/]+-[0-9a-f]{16}" "$messages" | sed 's/.*-//' > "$used" || true
# The binary the build asked for is reported where cargo linked it, at the
# top of `release/`, which is a hard link of its unit's file in `deps/`.
untraced=""
for top in $(grep -oE "\"$release/[^\"/]+\"" "$messages" | tr -d '"' | sort -u); do
  [ -f "$top" ] || continue
  traced=""
  for file in "$deps"/*; do
    if [ "$file" -ef "$top" ]; then
      unit="${file##*/}"
      echo "${unit##*-}" >> "$used"
      traced=1
      break
    fi
  done
  [ -n "$traced" ] || untraced="$untraced ${top##*/}"
done

# The names in `$1` (a directory) whose unit hash this build did not use.
unused_in() {
  ls -A "$1" | awk -v used="$used" '
    BEGIN { while ((getline h < used) > 0) keep[h] = 1 }
    {
      unit = $0; sub(/\..*/, "", unit)
      n = split(unit, part, "-"); hash = part[n]
      if (n > 1 && length(hash) == 16 && hash ~ /^[0-9a-f]+$/ && !(hash in keep)) print
    }'
}

[ "$mode" = this-build ] || { echo "weft: unknown sweep '$mode' (this-build)" >&2; exit 1; }
if [ -n "$untraced" ]; then
  echo "weft: cannot tell which compiled unit is$untraced (cargo copied it instead of linking it); the compile cache is left as it is" >&2
  exit 0
fi
for dir in "$deps" "$fingerprints" "$scripts"; do
  [ -d "$dir" ] || continue
  for name in $(unused_in "$dir"); do
    rm -rf "${dir:?}/$name"
  done
done
# Incremental state is kept per crate in a directory cargo's hash does
# not name (`incremental/<crate>-<rustc's own id>`), so it is bounded by
# count: a crate keeps as many directories as this build used units of
# it, the most recently written first (rustc writes into a crate's
# directory each time it compiles it). A unit's dep-info names its
# crate: in `deps/`, or for a build script in its own `build/` dir.
incremental="$release/incremental"
if [ -d "$incremental" ]; then
  crates="$(mktemp)"
  { ls -A "$deps"; for dir in "$scripts"/*/; do [ -d "$dir" ] && ls -A "$dir"; done; } | awk -v used="$used" '
    BEGIN { while ((getline h < used) > 0) keep[h] = 1 }
    /\.d$/ {
      unit = $0; sub(/\.d$/, "", unit)
      n = split(unit, part, "-")
      if (part[n] in keep) { sub(/-[^-]*$/, "", unit); print unit }
    }' > "$crates"
  for name in $(ls -t "$incremental" | awk -v crates="$crates" '
    BEGIN { while ((getline c < crates) > 0) wanted[c]++ }
    { crate = $0; sub(/-[^-]*$/, "", crate); if (++seen[crate] > wanted[crate]) print }'); do
    rm -rf "${incremental:?}/$name"
  done
  rm -f "$crates"
fi
exit 0
