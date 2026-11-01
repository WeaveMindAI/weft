#!/usr/bin/env bash
# Put a weft CLI built from a checkout's exact source into a directory,
# for a CI job that would otherwise compile it (an install on GCP from a
# fork, a project's deploy workflow). It looks in two places, in order:
#
# 1. The install, when one is named: the CLI its own install workflow
#    built (or downloaded) for this source and stored in the install
#    (`GET /install/cli/<tree>`, answered with a link and the CLI's
#    sha256). A fork's install is the only place its own CLI exists.
# 2. weft's release: it names the source tree it was built from
#    (manifest.json's `tree`), so a fork that merged weft into an
#    unchanged copy matches too, under a commit of its own. When the
#    release for that tree is still being built upstream (the usual case
#    right after a merge into main), this waits for it, printing where it
#    is, rather than compiling the same thing again.
#
# Neither has it: the caller compiles it.
#
# Usage: scripts/release-cli.sh <checkout> <dest-dir> [<install-url>]
#   <install-url> is asked with the operator key in WEFT_OPERATOR_KEY.
#   Exit 0: <dest-dir>/weft is a linux x86_64 CLI built from this source,
#           checked against its sha256.
#   Exit 2: neither has a CLI built from this source; build it instead.
#   Anything else is an error.
#
# GH_TOKEN, when set, authenticates the GitHub API reads (a job's own
# token reads a public repository's runs); unauthenticated reads share a
# small hourly limit per address.
set -euo pipefail

checkout="${1:-}"
dest="${2:-}"
install="${3:-}"
if [ -z "$checkout" ] || [ -z "$dest" ]; then
  echo "usage: $0 <checkout> <dest-dir> [<install-url>]" >&2
  exit 1
fi

# SYNC: the release repository, tag, asset name and manifest.json keys <-> .github/workflows/release.yml (the release job), setup.sh (prebuilt artifacts block)
repo=WeaveMindAI/weft
base="https://github.com/$repo/releases/download/latest"
asset=weft-x86_64-linux
tree="$(git -C "$checkout" rev-parse 'HEAD^{tree}')"

# Download <url> to <dest-dir>/weft and check it against <sha256>.
# Returns 1 when it does not match (the file is removed), 2 when the
# download failed.
download_checked() {
  mkdir -p "$dest"
  curl -fsSL --retry 3 "$1" -o "$dest/weft" || return 2
  if ! echo "$2  $dest/weft" | sha256sum -c - >/dev/null 2>&1; then
    rm -f "$dest/weft"
    return 1
  fi
  chmod +x "$dest/weft"
}

# The CLI the install stored for this source. Returns 1 when it stored
# none (an install whose weft predates the route answers the same 404);
# any other answer is an error, since the install was named and asked.
# SYNC: the route and its answer <-> crates/weft-core/src/install.rs (InstallCli), crates/weft-dispatcher/src/api/install_cli.rs (the route)
from_install() {
  local answer status url sha
  answer="$(mktemp)"
  if [ -z "${WEFT_OPERATOR_KEY:-}" ]; then
    echo "WEFT_OPERATOR_KEY is not set, so $install cannot be asked for its CLI" >&2
    exit 1
  fi
  status="$(curl -sS --max-time 60 -o "$answer" -w '%{http_code}' \
    -H "Authorization: Bearer $WEFT_OPERATOR_KEY" "${install%/}/install/cli/$tree")" || {
    echo "could not reach $install to ask for its CLI" >&2
    rm -f "$answer"
    exit 1
  }
  case "$status" in
    200) ;;
    404)
      rm -f "$answer"
      echo "$install stores no CLI built from this source ($tree)"
      return 1
      ;;
    *)
      echo "$install answered $status when asked for its CLI: $(cat "$answer")" >&2
      rm -f "$answer"
      exit 1
      ;;
  esac
  url="$(jq -r '.url // empty' "$answer")"
  sha="$(jq -r '.sha256 // empty' "$answer")"
  rm -f "$answer"
  if [ -z "$url" ] || [ -z "$sha" ]; then
    echo "$install answered its CLI with no link or no sha256" >&2
    exit 1
  fi
  local got=0
  download_checked "$url" "$sha" || got=$?
  case "$got" in
    0) ;;
    1)
      echo "the CLI $install stores for this source does not match its sha256; run the install workflow again to store it anew" >&2
      exit 1
      ;;
    *)
      echo "could not download the CLI $install stores for this source" >&2
      exit 1
      ;;
  esac
  echo "using the CLI $install stores for this source"
}

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

if [ -n "$install" ] && from_install; then
  exit 0
fi

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
# A release being replaced serves a binary its manifest no longer vouches
# for: that is no CLI for this source.
got=0
download_checked "$base/$asset" "$sha" || got=$?
case "$got" in
  0) ;;
  1)
    echo "the published $asset no longer matches the manifest (a newer release is replacing it)"
    exit 2
    ;;
  *)
    echo "could not download the release's $asset" >&2
    exit 1
    ;;
esac
echo "using the CLI weft's release built from commit $(jq -r .commit <<<"$manifest")"
