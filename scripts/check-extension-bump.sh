#!/usr/bin/env bash
# Refuse a change to an extension that forgot to bump its version.
#
# The stores publish exactly when a package's version has no record
# tag yet (see .claude/skills/releasing/SKILL.md), so an editor change
# merged under the version the stores already carry publishes nothing,
# and nobody notices until a user asks why the fix is not out. This
# compares the branch against its base: for each extension, if any
# file it is built from changed and its package.json version did not,
# the check fails and prints the bump command.
#
# Usage: scripts/check-extension-bump.sh <base-ref>
#   base-ref is the commit the branch merges into (CI passes the PR's
#   base; locally, `origin/main`).
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

base="${1:-}"
if [ -z "$base" ]; then
  echo "usage: $0 <base-ref>" >&2
  exit 2
fi
if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
  echo "base ref '$base' is not a commit here (fetch it first)" >&2
  exit 2
fi
merge_base="$(git merge-base "$base" HEAD)"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is not on PATH; it is needed to read package.json" >&2
  exit 1
fi

# One line per extension: the package directory, then every path its
# build reads. The VS Code extension bundles the shared graph package
# (the graph webview) and ships files from the syntax package through
# symlinks (the grammar and the markdown highlighter), so an edit in
# either ships in it. The browser extension reads neither: it is the
# task popup and never draws a graph.
packages=(
  "extension-vscode extension-vscode packages/weft-graph packages/weft-syntax"
  "extension-browser extension-browser"
)

version_at() {
  # $1 = ref ("" for the working tree), $2 = package dir
  if [ -z "$1" ]; then
    jq -r .version "$2/package.json"
  else
    git show "$1:$2/package.json" | jq -r .version
  fi
}

failed=0
for entry in "${packages[@]}"; do
  read -r -a parts <<<"$entry"
  pkg="${parts[0]}"
  sources=("${parts[@]:1}")
  # Against the working tree, not HEAD: CI's tree is the commit, and a
  # local run then also sees a bump that is not committed yet.
  if git diff --quiet "$merge_base" -- "${sources[@]}"; then
    echo "ok   $pkg: nothing it is built from changed"
    continue
  fi
  before="$(version_at "$merge_base" "$pkg")"
  after="$(version_at "" "$pkg")"
  if [ "$before" = "$after" ]; then
    echo "FAIL $pkg changed but its version is still $after."
    echo "     The stores publish a version once, so this change would never reach them."
    echo "     Bump it in this branch:  (cd $pkg && pnpm version patch --no-git-tag-version)"
    failed=1
  else
    echo "ok   $pkg: $before -> $after"
  fi
done
exit "$failed"
