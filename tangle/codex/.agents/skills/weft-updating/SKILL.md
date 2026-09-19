---
name: weft-updating
description: "Read when the user asks to update or upgrade weft, or when something broke right after one: locating the checkout, the git pull plus setup.sh walk, what setup.sh rebuilds and what it preserves, and fixing a failed update."
---

# Updating weft

[the checkout] is the git clone of weft on the user's machine; `./setup.sh` in it is what installed weft. An update is `git pull` in [the checkout], then `./setup.sh`, then one command per project. An update never edits a file inside the user's projects.

Projects created with `weft new <name> --assistant codex` (shorthand `cx`; the choice is remembered, so plain `weft new` installs it too once picked) have their Tangle persona (`AGENTS.md`, `.agents/` and `.codex/`) COPIED into the project, so a project keeps the version that created it. After the `./setup.sh`, `weft tangle update` inside a project re-copies its Tangle from [the checkout]; run it in each project the user wants on the new version. It replaces every file Tangle owns and leaves anything the assistant wrote beside them alone, so the diff is worth reading.

## Finding the checkout

`cat ~/.local/share/weft/repo-root` prints [the checkout]. `weft --version` prints the running version, and `git -C <checkout> log --oneline -1` shows which commit is installed.

## The walk

setup.sh has two paths. [the prebuilt path] downloads the CLI and extension CI already built and pulls the daemon images: minutes, no Rust or Node toolchain needed. [the compile path] builds everything locally: slower, not broken, and it needs a Rust toolchain (plus Node and pnpm for the extensions). [the checkout] with no local changes, at a commit CI has built, takes [the prebuilt path]; any local change at all, or `--from-source`, takes [the compile path].

1. **Look before pulling.** `git -C <checkout> status --short` and `git -C <checkout> log --oneline -3`. Local changes force [the compile path], and a pull over them can conflict. You tell the user both before moving, and you never clean the changes up yourself.
2. **Pull.** `git -C <checkout> pull`. On conflicts you show the user the conflicted files, resolve with their input, then re-run the pull. If you catch yourself typing `git stash`, `git checkout .` or `git reset --hard` in [the checkout], stop and write: "Wait. Their changes." Then show the user the files and ask.
3. **Run setup.** `cd <checkout> && ./setup.sh`. The default refreshes the CLI, the daemon (images rebuilt, the kind pod restarted or started), and the VS Code extension; the browser extension is opt-in with `--browser` (it signs for Firefox, which is slow; `--no-sign` skips signing).
4. **Verify.** `weft --version` (the new number), `weft daemon status` (running). If the daemon is not coming up, you read `weft daemon logs --tail 100` before touching anything: the refusal names its fix there.
5. **Per project, after the update.** `weft catalog update` syncs this project's stdlib copy to the new weft (a diagnostic naming the catalog or a stale copy is the signal to run it on any day). `weft tangle update` re-copies this project's Tangle personas, skills and commands from the new weft; they are ordinary project files, so nothing else brings them forward. Running projects keep executing on the worker image they were started with; when the user wants a project moved onto the new engine, `weft resync` does it, and its old image is reclaimed by a later install once nothing points at it.

## What an update never does

- It never edits the user's projects: not the source, not the connections, not the journal.
- It leaves a project's infra stopped: after the update, `weft resync` (or `weft run`) says `infra not running for: ...`, and `weft infra start` brings it back.
- It never destroys data by default. setup.sh's disk hygiene is bounded: build caches are capped, unreferenced images are reclaimed, and the keep-set preserves every running project's current worker image and every live project's infra images.
- `--uninstall` and `--purge` are removal operations and never part of this walk.

## When the update breaks

- **The pull conflicted.** Step 2: with the user, never around them.
- **The build failed.** You read the error before guessing. A missing toolchain on [the compile path] is the most common cause, and the error says so.
- **The daemon refuses to start.** `weft daemon logs --tail 100` names the failing component and the fix. A second `./setup.sh` is safe (it reconciles, so running it twice changes nothing). A changed cluster shape (a port, a kind version) rebuilds the cluster on its own and says so; `./setup.sh --rebuild-cluster` forces that rebuild when the cluster itself is wedged. Either way every project's own database is destroyed with the node, so you say what that means and get the user's yes first.
- **The version did not change.** Either the pull brought nothing new (`git -C <checkout> log --oneline -1` to confirm) or setup.sh served prebuilt artifacts for the same commit. Neither is stuck.
- **A project misbehaves after the update.** First `weft catalog update` in that project (the stdlib copy lags by default), then a run; if the wrongness survives, it is a real finding and the normal debugging loop applies.

The flags that matter here, all opt-in: `--cli`, `--daemon`, `--vscode`, `--browser` (refresh one component instead of the default set), `--no-daemon` (skip the daemon refresh), `--from-source` (force [the compile path]), `--debug` (debug-profile CLI), `--rebuild` (rebuild daemon images even when unchanged), `--rebuild-cluster` (recreate the kind cluster even when unchanged, see above).
