#!/usr/bin/env bash
# Create a new git worktree for parallel feature work.
#
# Branches off whatever branch you're currently on, into a new branch with
# the name you give. The worktree lives as a sibling of the repo at
# ../weft-trees/<branch>. The gitignored .env files are copied in (setup.sh
# reads them but never creates them); everything else (node deps, builds,
# daemon) is handled by running ./setup.sh inside the worktree afterwards.
#
# Usage:
#   scripts/add-worktree.sh <new-branch-name>
#
# Example:
#   scripts/add-worktree.sh feat/storage-plane
#     -> branch feat/storage-plane off the current branch
#     -> worktree at ../weft-trees/feat-storage-plane
#     -> then: cd ../weft-trees/feat-storage-plane && ./setup.sh <flags>
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: scripts/add-worktree.sh <new-branch-name>" >&2
  exit 1
fi

new_branch="$1"

# Resolve repo root regardless of where the script is invoked from.
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

base_branch="$(git rev-parse --abbrev-ref HEAD)"

# Slashes are legal in branch names but make nested dirs; flatten for the
# folder name while keeping the real branch name intact in git.
dir_name="${new_branch//\//-}"
worktree_path="$repo_root/../weft-trees/$dir_name"

if git show-ref --verify --quiet "refs/heads/$new_branch"; then
  echo "error: branch '$new_branch' already exists" >&2
  exit 1
fi
if [ -e "$worktree_path" ]; then
  echo "error: worktree path already exists: $worktree_path" >&2
  exit 1
fi

echo "==> branching '$new_branch' off '$base_branch'"
git worktree add -b "$new_branch" "$worktree_path" "$base_branch"

# Resolve the real path now that the dir exists (collapses the ../).
worktree_path="$(cd "$worktree_path" && pwd)"

# Copy gitignored env files the worktree needs to run.
for env_file in .env .env.extension; do
  if [ -f "$repo_root/$env_file" ]; then
    cp "$repo_root/$env_file" "$worktree_path/$env_file"
    echo "==> copied $env_file"
  fi
done

echo ""
echo "worktree ready:"
echo "  $worktree_path"
echo "  branch: $new_branch (off $base_branch)"
echo ""
echo "next:   cd $worktree_path && ./setup.sh <flags>   # builds + installs deps"
echo "remove: git worktree remove $worktree_path"
