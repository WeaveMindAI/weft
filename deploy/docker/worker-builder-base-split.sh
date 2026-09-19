#!/bin/sh
# Split a finished cargo target directory into the half that changes
# when the engine changes and the half that does not, so the builder
# base can carry them as two image layers.
#
# usage: worker-builder-base-split.sh <target dir> <out dir>
#
# The big half is the crates.io dependency tree: about 1.1 GB of rlibs
# and metadata that is byte-identical on every build with the same
# `Cargo.lock` and the same toolchain. The small half is what weft
# itself compiles: the engine crates and the `pkg_<package>` crate per
# stdlib package, about 180 MB, which an engine edit does move.
#
# `COPY --from` keys its layer on the content it copies, so writing the
# big half to its own directory is what lets the base rebuild after an
# engine edit re-export only the small one. Everything here uses
# `cp -a`: cargo decides a compiled unit is fresh by comparing mtimes,
# so the artifacts must reach the image with the ones cargo gave them.
#
# Ownership is read off the file name, which is how cargo names things:
# a unit of crate `c` is `libc-<hash>.rlib` in `deps/` and `c-<hash>` in
# `.fingerprint/` and `build/`. Weft's own are the ones called `weft*`
# or `pkg_*`; nothing on crates.io is.
#
# BOTH spellings are matched because cargo uses both: `deps/` names a
# unit by its CRATE name, where the hyphens are underscores
# (`libweft_core-<hash>.rlib`), while `.fingerprint/` and `build/` name
# it by its PACKAGE name, hyphens intact (`weft-core-<hash>/`). Matching
# only the underscore form left every weft fingerprint in the big half,
# where its contents moved on every engine edit and cost that layer its
# cache, which is the whole point of the split.
#
# An unknown name goes to the big half, which is the safe side: being
# wrong there costs one slow build the first time that file appears,
# while being wrong the other way puts churn in the layer that must
# stay still.
set -eu
target="$1"; out="$2"
release="$target/release"
[ -d "$release" ] || { echo "weft: $release does not exist; nothing to split" >&2; exit 1; }

mkdir -p "$out/vendor/release" "$out/weft/release"

# Whatever sits beside `release/` goes with the small half. It is a
# couple of kilobytes (`CACHEDIR.TAG`, `.rustc_info.json`), and
# `.rustc_info.json` is cargo's own scratch: its bytes move on nearly
# every build, so one byte of it in the big half would cost the whole
# 1.2 GB layer its cache. Measured: it was the ONLY thing that differed
# between two builds after an engine edit, and it alone defeated the
# split.
for entry in "$target"/* "$target"/.[!.]*; do
  [ -e "$entry" ] || continue
  [ "${entry##*/}" = "release" ] && continue
  cp -a "$entry" "$out/weft/"
done

# The three directories cargo fills per compiled unit, split by owner.
for dir in deps build .fingerprint; do
  [ -d "$release/$dir" ] || continue
  mkdir -p "$out/vendor/release/$dir" "$out/weft/release/$dir"
  for entry in "$release/$dir"/* "$release/$dir"/.[!.]*; do
    [ -e "$entry" ] || continue
    name="${entry##*/}"
    case "${name#lib}" in
      weft_*|weft-*|pkg_*|pkg-*) cp -a "$entry" "$out/weft/release/$dir/" ;;
      *)            cp -a "$entry" "$out/vendor/release/$dir/" ;;
    esac
  done
done

# What is left directly under `release/`: the linked stock binary and
# its dep-info, plus cargo's own lock and its empty `incremental` /
# `examples` directories. All of it is this build's, so it goes with
# the small half.
for entry in "$release"/* "$release"/.[!.]*; do
  [ -e "$entry" ] || continue
  case "${entry##*/}" in
    deps|build|.fingerprint) continue ;;
  esac
  cp -a "$entry" "$out/weft/release/"
done

vendor_size="$(du -sh "$out/vendor" 2>/dev/null | cut -f1)"
weft_size="$(du -sh "$out/weft" 2>/dev/null | cut -f1)"
echo "weft: builder base split into $vendor_size of dependencies (a cached layer while the lock holds) and $weft_size of weft's own crates"
