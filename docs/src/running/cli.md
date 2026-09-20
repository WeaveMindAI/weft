# The CLI

Every command takes `--dispatcher <url>` (or the `WEFT_DISPATCHER_URL`
environment variable) and `--json`, which prints one JSON object per line on
stdout while logs go to stderr. That is how the VS Code extension drives the
CLI.

## Everyday

| Command | What it does |
|---|---|
| `weft new <name>` | scaffold a project: `weft.toml`, `src/main.weft`, `nodes/` with the standard library seeded in |
| `weft new <name> --assistant <who>` | the same, plus Tangle, the AI builder persona, copied in for that assistant (`cc` Claude Code, `kc` Kilo Code, `cu` Cursor, and the rest; repeat the flag for several). The choice is remembered for later `weft new` runs; `--assistant none` stops it. See [Your first program](../start/first-program.md). |
| `weft tangle update [--assistant <who>]` | re-copy this project's Tangle from the installed weft, the way `weft catalog update` re-copies the standard library. With no flag it refreshes the assistants the project already has; with one it also installs an assistant that is not here yet. It **replaces every file Tangle owns**, so your own edits to those personas, skills and commands go; anything your assistant wrote beside them stays. |
| `weft build` | build the project's worker image and register the project, starting nothing. Registering is also how you put back code the dispatcher no longer holds: it records the compiled program under its own hash, which is what a past run's values are worked out from, so the run reads again. It does not add a version to the tree; `weft checkpoint` does that |
| `weft run [--detach] [--referenced]` | compile, register, fire one execution, stream its events until completion, including across waits. `--detach` returns after starting it. The whole catalog is compiled by default; `--referenced` opts into compiling only the graph's node types. Every run records the code it ran as a version: [Versions](versions.md). |
| `weft run --target <node>` | the same, but it runs only these nodes and whatever they need. Repeatable, and several targets run the union of what each needs; any node can be a target. See [what actually runs](../language/mental-model.md#what-happens-when-you-hit-run). |
| `weft run --seed [--seed-before <node>] [--seed-until <node>]` | reuse compatible completed work from head's run. Before excludes the named node from reuse; until permits it too. Changed code, inputs and dependencies still invalidate reuse. See [Seeding](versions.md#seeding-run-only-what-changed). |
| `weft run --before <node>` | run what that node needs and NOT the node. The mirror of `--target`, and how you run a program up to the act you do not want performed: `weft run --before post` does everything the posting step needs and stops there. Repeatable. |
| `weft run [<example>] [--referenced] [--seed] [--root] [--from <node>=<ports-json>]... [--emit <node>=<ports-json>]... [--target <id>]... [--before <id>]... [--group <id>=<ports-json>] [--fire <trigger>=<wake-json>] [--save <name>]` | build and start one execution; `--detach` returns its color. `--from` supplies backup inputs at a start, `--emit` supplies outputs without executing that node. Real producers take precedence over backups. `--target` includes the endpoint; `--before` excludes it. `--group` selects a whole group or included file, with its input payload. `--fire` runs exactly one trigger using a matching bake. Ordinary groups can be cut precisely; loops stay whole. `--seed-before` / `--seed-until` bound compatible reuse. Named examples supply saved starting parameters; current code runs. Clear and replacement rules are in [Versions](versions.md) |
| `weft follow <project>` | live event stream for a project |
| `weft stop <color>` | cancel a running execution. Every command that takes a color also takes the first characters of one (`weft stop 3f2a`), as long as they name a single run |
| `weft ps` | list registered projects |
| `weft status` | the runtime's overall state. If the project is not registered, explains how to run it or activate its triggers. |

## Executions

| Command | What it does |
|---|---|
| `weft executions [--limit N] [--project <id>] [--phase fire\|trigger_setup\|infra_setup]` | recent executions, newest first, with each run's status, phase, local start time, entry node and the tags it put on itself ([stopping other runs](../nodes/steering-executions.md)). "Has my trigger fired since the change" is `--phase fire`: it hides the setup runs an activate or resync makes. |
| `weft events <color> [--node <id>] [--kind <kind>] [--full]` | one execution's events in order, one line each: local time, kind, node, and a short summary of the value or error. Values are cut short so a long run stays readable; `--node` keeps one node's events, spelled the way the source reads (`triage.classify` is the node `classify` of the file the site `triage` includes, and only that use of it), `--kind` one kind (`node_failed`; a substring works, so `failed` catches both failure kinds), `--full` prints the values whole, and `--json` prints the replay rows the graph view reads. |
| `weft logs [color]` | the run's log: the lines its nodes wrote, and every failure the journal recorded about it (a node failing, a port refusing a value, the run failing or being cancelled) as `error` and `warn` lines naming the node they are about. The last 1000 lines; `--limit` raises that (up to 20000), and a full page says the run may have written more. |
| `weft clean [color]` | purge journal data. Asks before deleting runs; pass `--yes` when scripting. Naming a subject takes all of it: a color deletes that run, `--project <id>` deletes that project's whole history (runs outlive the project, so this is how a removed project's history is erased). With no subject it deletes runs older than `--keep-days` (30), or everything with `--all`. Also `--images` (reclaim worker images nothing runs any more, scoped to the current project's images; a global sweep of dangling untagged build leftovers rides along. With `--all`: every project's, the kind node's copies, stale `weft-infra-*` tags, and old builder-base images; whatever the dispatcher's referenced set covers survives) and prints the size of both compile caches on the machine: the node-test cache `weft test-node` builds into (it wipes itself past `WEFT_TEST_CACHE_CAP_GB`, 6 by default) and the cache every worker image build shares. That second cache bounds itself: each build drops the compiled node packages and worker crates no build has linked for 30 days, and `--all` also drops a whole cache no build has touched for 30 days (one is left behind whenever the toolchain or the builder changes). `--build-cache` throws away the whole BuildKit cache, that compile cache included, so the next build seeds a fresh one from the builder base, which already holds every stock node compiled; it drops the node-test cache too, so the next `weft test-node` builds cold. `setup.sh` runs `--images --all` after every daemon refresh. |

## Versions and examples

| Command | What it does |
|---|---|
| `weft checkpoint [<label>] [--root]` | record the files as a version under head, no run, no build. Works as the first thing you do in a project: it tells the dispatcher the project exists, without building anything. `already at <id>` when nothing changed. `--root` (on `run` too) records a version with no parent and refuses if that same version already has a parent. On `run` it also means nothing is seeded. |
| `weft tree` | the version tree: every version, what changed in it against its parent, and its runs beneath it. It marks HEAD's version, the activated version, and HEAD's run, which is the one your next `--seed` inherits from. `--json` adds `disk_version`: the version your files on disk match right now, or null when they match none. |
| `weft branch <version\|label\|color> [--discard]` | restore that version's files and move head there. Restoring a version clears head's run, so the next `--seed` falls back to the newest finished or parked run on that version, or on the nearest ancestor version that has one. If you want a particular run as your seed, name its color instead of a version. Refuses on unkept edits, naming the files. |
| `weft diff <ref> <ref> [--full]` | compare observed outputs for human or AI review, including frozen focus nodes. A ref is a color, its unambiguous prefix or `example:<name>`. Differences are evidence and do not fail the command |
| `weft freeze <name> [<run>] [--expect <node>]...` | save that run's starting parameters and observed outputs in `examples/<name>.json`; default is head's run. `--expect` marks nodes to focus on during review. Run and diff leave the accepted file intact; freeze again after accepting its replacement |
| `weft examples` | list saved parameters and frozen examples; inspect them, rerun with `weft run <name>`, then compare with `weft diff` |
| `weft bake [--referenced]` | prepare trigger inputs without arming listeners. A manual `--fire` requires a matching bake; use `--referenced` here when the run uses `--referenced`. Activation also prepares and records a bake before arming |
| `weft wake <color> <node>` | resolve a pure time wait now. `<node>` is the waiting node's place, spelled the way the source reads (`triage.hold` for the `hold` inside the file the site `triage` includes; a file included twice holds one wait per site). A wait that expects a value is refused, naming its kind. |
| `weft prune <version> [--yes]` | delete a version, everything under it, and their runs. Asks first. Refuses on head's version, the activated version, any version a frozen example was frozen from or any ancestor of one, and while a run in the subtree is running. It names each reason. |

For what each verb does and what every refusal means, go and read
[Versions, seeded runs and frozen examples](versions.md).

## Triggers

| Command | What it does |
|---|---|
| `weft activate [project]` | register every trigger and mint its address |
| `weft deactivate [project]` | drop the registrations |
| `weft cancel-activate` | abort an activation in progress |
| `weft cancel-build` | abort a build in progress |
| `weft cancel-running` | cancel executions currently in flight |
| `weft resync` | re-register an active project's triggers against the current source (a parked or hibernated project refuses; `weft activate` brings it back) |

"Turn this project off" means different things depending on what is in flight,
so `deactivate` takes a `--mode` to say which you meant:

| If you want work in flight to be | Pass |
|---|---|
| thrown away | `--mode wipe`, the default |
| kept, and resumable when you turn the project back on | `--mode hibernate` |
| kept, and queued to run the moment you turn it back on | `--mode park` |

Every verb that turns triggers off takes this same flag: `deactivate`,
`resync`, and `infra stop` / `terminate` / `upgrade` on an active project. Ask
from a terminal and it offers you the three; from a script or an agent, with no
flag, it wipes. That default is deliberate. The two preserving modes carry
signals and suspended runs across a change to the program, and on a project you
are still building that is how a run ends up waiting for something nobody will
ever answer. Keeping the work in flight is the thing you say out loud.

Executions that are running right now are a separate question, answered by
`--running-policy`. The default, `wait`, lets them finish while new fires are
held; `--drain-timeout <seconds>` caps that wait at 600 seconds by default,
after which the stragglers are cancelled. `--running-policy cancel` kills them
immediately. With `--mode hibernate`, `--grace <minutes>` is how long the
hibernation window lasts, 15 by default.

Waiting is a hibernate thing. Under `--mode park` the running executions are
left exactly as they are and the command lands at once, because park's whole
promise is that they stay alive with no time limit; a park that waited would
end by cancelling the very runs it was keeping. If you want them stopped under
park, say so with `--running-policy cancel`.

Turning a project back on asks the mirror question, because reactivating has to
decide what happens to whatever the deactivation kept. On a terminal `weft
activate` prompts you. To answer up front, pass one of:

| `--reactivate-choice` | What happens |
|---|---|
| `execute_parked_keep_suspended` | queued work runs, paused work stays paused waiting for its answer |
| `keep_suspended_only` | paused work stays paused, queued work is dropped |
| `wipe_all` | both are dropped and the project starts clean |

## Infrastructure

| Command | What it does |
|---|---|
| `weft infra start` | bring every unit up to spec, then wait until they are ready |
| `weft infra stop` | take units down per their stop behavior |
| `weft infra upgrade` | stop then start. On an active project this deactivates the triggers and **leaves them off**, so activate again when you are ready. |
| `weft infra terminate` | delete every infra resource, disks included unless a node's spec preserves them |
| `weft infra status` | health and endpoint URL per infra instance, each named the way the source reads it (`db`, or `one.db` for the `db` inside the file the site `one` includes). A file included twice runs two instances, and both are listed. |
| `weft infra logs [node] [--tail N] [-f]` | what the infra containers wrote: every infra node of the project, or one node's, each line prefixed with its pod and container. The place a failure inside an image is read from. |
| `weft infra cancel` | abort an operation in progress |
| `weft infra node-stop <node> [--force]` | stop one instance, named as `weft infra status` lists it (`one.db`). `--force` overrides a unit's stop behavior. |
| `weft infra node-terminate <node>` | terminate one instance, named the same way |

See [Infrastructure nodes](../nodes/infrastructure.md) for what the verbs
actually do to a unit.

## The daemon

| Command | What it does |
|---|---|
| `weft daemon start [--rebuild] [--rebuild-cluster] [--public-url\|--no-public-url]` | start the runtime. `--rebuild` re-makes the shared images under their existing tags and rolls everything onto the new bytes (kind only; on a k8s cluster, publish with `weft build-images --push` instead). `--rebuild-cluster` deletes and recreates the kind cluster even when its shape did not change (a shape change, a port or a kind version, rebuilds on its own, saying so first); every project's own database lives inside the node and is destroyed with it. |
| `weft daemon stop` | stop it |
| `weft daemon status` | is it up, and its public address if it has one |
| `weft daemon restart` | the same reconcile as `weft daemon start` (an alias): apply what changed, roll what needs it |
| `weft daemon logs [--tail N] [-f]` | tail the runtime log |
| `weft build-images [--push \| --push-suffix <s> \| --print]` | make every shared image (the four system images, worker builder base and full-library worker) exist locally under its content-addressed ref. `--push` publishes them to the registry (the release workflow's verb); `--print` only prints the refs this tree resolves to, without building images |
| `weft build-base` | make just the worker builder-base image exist locally |

Aliased to `weft d`.

## Nodes and the catalog

| Command | What it does |
|---|---|
| `weft test-node [target]` | run node self-tests: the basic and fake tiers, locally, no cluster. `--tier live` adds the real-credential tier, which **can spend money**, so it asks first (`--yes` to skip the prompt). See [Testing a node](../nodes/testing.md). |
| `weft node-test-hash [target]` | the content hash of a package's test inputs. Run it when you want to know whether anything a package's tests depend on has changed since they last passed. |
| `weft infra list-doors` | the pieces of this project's infrastructure a client on this machine can reach, and the address each answers on. A door is part of what a node IS (its endpoint declares it, usually behind an input like `PostgresDatabase`'s `reachable`), so this only reports: there is nothing here to open or close. The address is not in the source because the cluster allocates the port, which is why you ask. |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's standard library. It **wipes and recopies** that folder, so copy anything you edited in there out first. |
| `weft describe-nodes [--stdlib] [--list \| --node <Type> [--compact]]` | print the catalog. `--list` is one line per node type (type, tags, one-line description): the cheap first look, and how you find a node. `--node <Type> --compact` is one node's wiring view (no labels, icons, connect recipes, or null knobs), the token-cheap thing a model reads before wiring it; without `--compact` it is the full resolved metadata. The bare form prints the whole catalog as JSON for the editor's palette, and it is large. |

## Compiler surfaces

| Command | What it does |
|---|---|
| `weft parse [--file]` | parse leniently, print the project as JSON |
| `weft validate [--file]` | validate strictly, print diagnostics as JSON |
| `weft parse-server` | a long-lived line-delimited JSON server, keeping the catalog warm |

These three are how the editor gets live feedback. `parse` is lenient, so it
answers on incomplete source; `validate` is strict.

## Files and access

| Command | What it does |
|---|---|
| `weft connect` | the editor's Connect panel, in the terminal: pick a stored connection for an access node, connect a new account (paste a key, browser sign-in, shared app), upgrade one, forget one, or disconnect the node. Sees access nodes inside `@include`d files too, however deeply nested; a subgraph included in two places is one file, so one pick connects every inclusion. Interactive by default; every choice has a flag (`--help` lists them) so scripts never hang on a prompt. `--json` works with the flag-driven actions (`--list`, `--grant`, `--disconnect`, `--forget`); the walkthroughs print for a person. |
| `weft files ls` | stored runtime files |
| `weft files inspect <key>` | one file's metadata |
| `weft files download <key>` | fetch it |
| `weft files rm <key>` | delete one file, or a whole space if the target ends in `/`. Non-interactively `--yes` is required, since a prefix wipe can take kept files with it. |
| `weft files usage` | how much is stored |
| `weft token mint --name "..."` | mint a browser-extension token. Prints the connect URL **once**. |
| `weft token ls` | list minted tokens |
| `weft token revoke <id>` | kill one |
| `weft listener inspect` | what the listener tier is currently holding. Reach for it when a trigger stopped firing: if what it holds disagrees with what the project registered, a cleanup went wrong. |
| `weft rm [project]` | remove a project: triggers wiped, runs cancelled, infra terminated, stored data reclaimed. Asks first; `--yes` answers it (required when scripting). `--journal`, `--local`, `--all`, `--force` |

`weft token mint` also takes repeatable `--projects` and `--tags` flags that
narrow what a token can ever see, and `--display` / `--displays`,
which GRANT it what a node is showing (a token says nothing about displays and
reads none). `--display` takes a node the way you write it (`test.whatsapp`), resolved
against the project you are in:
[Scoping a token](browser-extension.md#scoping-a-token).

## The environment

| Variable | Default | Set it when |
|---|---|---|
| `WEFT_DISPATCHER_URL` | `http://localhost:9999` | the runtime is not on this machine, or you moved its port |
| `WEFT_DISPATCHER_PORT` | `9999` | something else already has 9999. The port is baked into the local cluster, so changing it makes the next `weft daemon start` rebuild the cluster; go and read [Port 9999 is taken](../start/troubleshooting.md#the-cli-cannot-reach-the-runtime-or-port-9999-is-taken) before you do |
| `CREDENTIAL_ENCRYPTION_KEY` | a development key, with a warning on every boot | before you store a credential you care about. It seals them at rest, and changing it later strands every connection you had. |
| `WEFT_ACCESS_APPS_FILE` | | you are pointing weft at a [shared-credentials file](../connections/the-apps-file.md) |
| `WEFT_PUBLIC_TUNNEL_TOKEN` | | you want a [permanent public address](../connections/public-address.md) instead of the random one |
| `WEFT_PUBLIC_TUNNEL_HOSTNAME` | | the same, and both have to be set together |

A `.env` in the project is loaded automatically. A malformed one is fatal
rather than partially applied.
