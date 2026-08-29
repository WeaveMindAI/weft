#!/usr/bin/env bash
# Create a new git worktree for parallel feature work.
#
# Branches off whatever branch you're currently on, into a new branch with
# the name you give. The worktree lives as a sibling of the repo at
# ../weft-trees/<branch>.
#
# The gitignored local-config files (.env, .env.extension, access-apps.json)
# are SYMLINKED to the main worktree's copies rather than duplicated: setup.sh
# reads them but never creates them, and one set of credentials editable from
# any worktree beats N copies that drift. Everything else (node deps, builds,
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

# The MAIN worktree, which is where the shared local-config files live and
# where the symlinks below must point. `--show-toplevel` above is the CURRENT
# worktree, so running this from inside one would otherwise chain the new
# worktree's links to that worktree instead of to the canonical copies.
# `--git-common-dir` is the main `.git` in both cases; its parent is the main
# worktree.
main_root="$(cd "$(git rev-parse --git-common-dir)" && cd .. && pwd)"

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

# Link the gitignored local-config the worktree needs to run. Symlinks, not
# copies: editing any one of these from any worktree edits the single real
# file, so credentials and tunnel settings never drift between trees.
for cfg_file in .env .env.extension access-apps.json; do
  if [ -f "$main_root/$cfg_file" ]; then
    ln -sfn "$main_root/$cfg_file" "$worktree_path/$cfg_file"
    echo "==> linked $cfg_file -> $main_root/$cfg_file"
  else
    echo "==> skipped $cfg_file (not present in $main_root)"
  fi
done

echo ""
echo "worktree ready:"
echo "  $worktree_path"
echo "  branch: $new_branch (off $base_branch)"
echo ""
echo "next:   cd $worktree_path && ./setup.sh <flags>   # builds + installs deps"
echo "remove: git worktree remove $worktree_path"
