#!/usr/bin/env bash
# Put the weft CLI the release built from a checkout's exact source into
# a directory, for a CI job that would otherwise compile it (an install
# on GCP from a fork, a project's deploy workflow).
#
# The release names the source tree it was built from (manifest.json's
# `tree`), so a fork that merged weft into an unchanged copy matches too,
# under a commit of its own. When the release for that tree is still
# being built upstream (the usual case right after a merge into main),
# this waits for it, printing where it is, rather than compiling the
# same thing again.
#
# Usage: scripts/release-cli.sh <checkout> <dest-dir>
#   Exit 0: <dest-dir>/weft is the release's linux x86_64 CLI, checked
#           against its sha256.
#   Exit 2: no release was built from this source; build it instead.
#   Anything else is an error.
#
# GH_TOKEN, when set, authenticates the GitHub API reads (a job's own
# token reads a public repository's runs); unauthenticated reads share a
# small hourly limit per address.
set -euo pipefail

checkout="${1:-}"
dest="${2:-}"
if [ -z "$checkout" ] || [ -z "$dest" ]; then
  echo "usage: $0 <checkout> <dest-dir>" >&2
  exit 1
fi

# SYNC: the release repository, tag, asset name and manifest.json keys <-> .github/workflows/release.yml (the release job), setup.sh (prebuilt artifacts block)
repo=WeaveMindAI/weft
base="https://github.com/$repo/releases/download/latest"
asset=weft-x86_64-linux
tree="$(git -C "$checkout" rev-parse 'HEAD^{tree}')"

api() {
  local auth=()
  [ -n "${GH_TOKEN:-}" ] && auth=(-H "Authorization: Bearer $GH_TOKEN")
  curl -fsSL --max-time 20 "${auth[@]}" -H "Accept: application/vnd.github+json" "https://api.github.com/$1"
}

# Prints the manifest when it was built from this tree.
published_manifest() {
  local manifest
  manifest="$(curl -fsSL --max-time 10 "$base/manifest.json" 2>/dev/null)" || return 1
  [ "$(jq -r '.tree // empty' <<<"$manifest")" = "$tree" ] || return 1
  printf '%s' "$manifest"
}

# Prints the url of a release run building this tree that has not
# finished yet, if any. GitHub not answering counts as none: the caller
# then builds, which is slower but just as right.
release_in_flight() {
  local status runs
  for status in in_progress queued; do
    if ! runs="$(api "repos/$repo/actions/workflows/release.yml/runs?status=$status&per_page=20")"; then
      echo "could not ask GitHub whether weft's release for this source is still running; not waiting for it" >&2
      return 0
    fi
    jq -r --arg tree "$tree" \
      '[.workflow_runs[] | select(.head_commit.tree_id == $tree) | .html_url][0] // empty' <<<"$runs" \
      | grep . && return 0
  done
  return 0
}

manifest="$(published_manifest)" || manifest=""
if [ -z "$manifest" ]; then
  run="$(release_in_flight)"
  if [ -z "$run" ]; then
    echo "the release has no CLI built from this source ($tree)"
    exit 2
  fi
  echo "weft's release for this source is still being built ($run); waiting for it instead of compiling"
  while [ -z "$manifest" ]; do
    sleep 30
    manifest="$(published_manifest)" || manifest=""
    [ -n "$manifest" ] && break
    if [ -z "$(release_in_flight)" ]; then
      # It may have published between the two reads.
      manifest="$(published_manifest)" || manifest=""
      if [ -z "$manifest" ]; then
        echo "that release finished without publishing a CLI for this source"
        exit 2
      fi
    else
      echo "still building ($run)"
    fi
  done
fi

sha="$(jq -r --arg k "sha256_$asset" '.[$k] // empty' <<<"$manifest")"
if [ -z "$sha" ]; then
  echo "the release for this source carries no $asset (its build failed)"
  exit 2
fi
mkdir -p "$dest"
curl -fsSL --retry 3 "$base/$asset" -o "$dest/weft"
# A release being replaced serves a binary its manifest no longer vouches
# for: that is no CLI for this source.
if ! echo "$sha  $dest/weft" | sha256sum -c - >/dev/null; then
  rm -f "$dest/weft"
  echo "the published $asset no longer matches the manifest (a newer release is replacing it)"
  exit 2
fi
chmod +x "$dest/weft"
echo "using the CLI weft's release built from commit $(jq -r .commit <<<"$manifest")"
