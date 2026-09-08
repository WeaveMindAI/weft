---
name: weft-updating
description: "Updating the weft installation itself. Read when the user asks to update or upgrade weft, or when something broke right after one: locating the checkout, the git pull plus setup.sh walk, what setup.sh rebuilds and what it preserves, and fixing a failed update."
---

# Updating weft

Weft is a git checkout somewhere on the user's machine plus what `./setup.sh` installed from it. Updating is two steps in that checkout: `git pull`, then `./setup.sh`. An update never edits a file inside the user's projects; the per-project follow-up is one command, covered below.

Projects created with `weft new <name> --assistant cline` (shorthand `cl`; the choice is remembered, so plain `weft new` installs it too once picked) have their Tangle persona (`.clinerules/` and `.cline/`) symlinked from the checkout, so the prompt update half is automatic: the `git pull` itself refreshes Tangle in every such project at once. Projects where Tangle was copied in by hand do not follow; re-copy the template into each of them after an update.

## Finding the checkout

The install records the checkout: `cat ~/.local/share/weft/repo-root`. In a project that has Tangle, `readlink -f .clinerules` also names it (the checkout root is 3 levels up from what that prints, `<checkout>/tangle/cline/.clinerules`). `weft --version` prints the running version, and `git -C <checkout> log --oneline -1` shows which commit is installed.

## The walk

1. **Look before pulling.** `git -C <checkout> status --short` and `git -C <checkout> log --oneline -3`: local changes in the checkout force the full compile path instead of the prebuilt one (slower, not broken, so never "clean them up" yourself), and a pull over them can conflict. State both facts to the user plainly before moving.
2. **Pull.** `git -C <checkout> pull`. Fast-forward is the normal outcome. On conflicts: show the user the conflicted files, resolve carefully with their input, never discard their changes (`git stash`, `git checkout .`, `git reset --hard` are not yours to run), then re-run the pull.
3. **Run setup.** `cd <checkout> && ./setup.sh`. On a clean checkout of a commit CI has already built, this downloads the prebuilt CLI and extension and pulls the daemon images: minutes, no Rust or Node toolchain needed. Any local change at all switches everything to the compile path, which is longer but fine. The default refreshes the CLI, the daemon (images rebuilt, the kind pod restarted or started), and the VS Code extension; the browser extension is opt-in with `--browser` (it signs for Firefox, which is slow; `--no-sign` skips signing).
4. **Verify.** `weft --version` (the new number), `weft daemon status` (running, port-forward up). If the daemon is not coming up, read `weft daemon logs --tail 100` before touching anything; most refusals name their own fix in there.
5. **Per project, after the update.** `weft catalog update` syncs this project's stdlib copy to the new weft (a diagnostic naming the catalog or a stale copy is the standing signal to do this even outside update day). Running projects keep executing on the worker image they were started with, by design; when the user wants a project moved onto the new engine, `weft resync` does it, and its old image is reclaimed by a later install once nothing points at it.

## What an update never does

- It never edits the user's projects: not the source, not the connections, not the journal.
- It leaves a project's infra stopped: after the update, `weft resync` (or `weft run`) says `infra not running for: ...`, and `weft infra start` brings it back.
- It never destroys data by default. setup.sh's disk hygiene is bounded and conservative: build caches are capped, unreferenced images are reclaimed, and the keep-set preserves every running project's current worker image and every live project's infra images.
- `--uninstall` and `--purge` are removal operations, not update operations. They are never part of this walk.

## When the update breaks

- **The pull conflicted.** Resolve with the user, never around them; their checkout, their changes.
- **The build failed.** Read the error before guessing. On the compile path a Rust toolchain (and for the extensions, Node plus pnpm) is required; a missing toolchain is the most common cause and the error says so.
- **The daemon refuses to start.** `weft daemon logs --tail 100`; the log usually names the failing component and the fix. A second `./setup.sh` retry is safe (it is an idempotent reconcile). Only if the cluster itself is wedged does `./setup.sh --rebuild-cluster` apply, and it deletes and recreates the local kind cluster: say what that means and get the user's yes first.
- **The version did not change.** Either the pull brought nothing new (`git -C <checkout> log --oneline -1` to confirm) or setup.sh served prebuilt artifacts for the same commit. Both are fine; nothing is stuck.
- **A project misbehaves after the update.** First `weft catalog update` in that project (the stdlib copy lags by default), then a run; if the wrongness survives, it is a real finding and the normal debugging loop applies.

The flags that matter here, all opt-in: `--cli`, `--daemon`, `--vscode`, `--browser` (refresh one component instead of the default set), `--no-daemon` (skip the daemon refresh), `--from-source` (force local compilation), `--debug` (debug-profile CLI), `--rebuild` (rebuild daemon images even when unchanged), `--rebuild-cluster` (recreate the kind cluster, see above).
