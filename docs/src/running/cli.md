# The CLI

Every command takes `--dispatcher <url>` (or the `WEFT_DISPATCHER_URL`
environment variable) and `--json`, which prints one JSON object per line on
stdout while logs go to stderr. That is how the VS Code extension drives the
CLI.

## Everyday

| Command | What it does |
|---|---|
| `weft new <name>` | scaffold a project: `weft.toml`, `main.weft`, `nodes/` with the standard library seeded in |
| `weft build` | compile the project to a native binary |
| `weft run [--detach]` | compile, register, fire one execution, stream its events |
| `weft run --target <node>` | the same, but run only what these output nodes need. Repeatable, and a non-output target is refused. See [what actually runs](../language/mental-model.md#what-actually-runs). |
| `weft follow <project>` | live event stream for a project |
| `weft stop <color>` | cancel a running execution |
| `weft ps` | list registered projects |
| `weft status` | the runtime's overall state |

## Executions

| Command | What it does |
|---|---|
| `weft executions [--limit N]` | recent executions |
| `weft events <color>` | every journal event for one execution, in order |
| `weft logs [color]` | log lines |
| `weft clean [color]` | purge journal data. Naming a subject takes all of it: a color deletes that run, `--project <id>` deletes that project's whole history (runs outlive the project, so this is how a removed project's history is erased). With no subject it deletes runs older than `--keep-days` (30), or everything with `--all`. Also `--images`, `--build-cache`. |

## Triggers

| Command | What it does |
|---|---|
| `weft activate [project]` | register every trigger and mint its address |
| `weft deactivate [project]` | drop the registrations |
| `weft cancel-activate` | abort an activation in progress |
| `weft cancel-build` | abort a build in progress |
| `weft cancel-running` | cancel executions currently in flight |
| `weft resync` | reconcile registrations against the current source |

"Turn this project off" means different things depending on what is in flight,
so `deactivate` takes a `--mode` to say which you meant:

| If you want work in flight to be | Pass |
|---|---|
| thrown away | `--mode wipe` |
| kept, and resumable when you turn the project back on | `--mode hibernate` |
| kept, and queued to run the moment you turn it back on | `--mode park` |

Executions that are running right now are a separate question, answered by
`--running-policy`. The default, `wait`, lets them finish while new fires are
held; `--drain-timeout <seconds>` caps that wait at 600 seconds by default,
after which the stragglers are cancelled. `--running-policy cancel` kills them
immediately. With `--mode hibernate`, `--grace <minutes>` is how long the
hibernation window lasts, 15 by default.

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
| `weft infra status` | per-node health and endpoint URLs |
| `weft infra cancel` | abort an operation in progress |
| `weft infra node-stop <id> [--force]` | stop one node. `--force` overrides a unit's stop behavior. |
| `weft infra node-terminate <id>` | terminate one node |

See [Infrastructure nodes](../nodes/infrastructure.md) for what the verbs
actually do to a unit.

## The daemon

| Command | What it does |
|---|---|
| `weft daemon start [--rebuild] [--rebuild-cluster] [--public-url\|--no-public-url]` | start the runtime. `--rebuild` re-makes the shared images under their existing tags and rolls everything onto the new bytes (kind only; on a k8s cluster, publish with `weft build-images --push` instead). `--rebuild-cluster` allows deleting and recreating the kind cluster when its shape changed; every project's own database lives inside the node and is destroyed with it, so this never happens without the flag. |
| `weft daemon stop` | stop it |
| `weft daemon status` | is it up, and its public address if it has one |
| `weft daemon restart` | the same reconcile as `weft daemon start` (an alias): apply what changed, roll what needs it |
| `weft daemon logs [--tail N] [-f]` | tail the runtime log |
| `weft build-images [--push \| --push-suffix <s> \| --print]` | make every shared image (the four system images plus the worker builder base) exist locally under its content-addressed ref. `--push` publishes them to the registry (the release workflow's verb); `--print` only prints the refs this tree resolves to, touching nothing |
| `weft build-base` | make just the worker builder-base image exist locally |

Aliased to `weft d`.

## Nodes and the catalog

| Command | What it does |
|---|---|
| `weft test-node [target]` | run node self-tests: the basic and fake tiers, locally, no cluster. `--tier live` adds the real-credential tier, which **can spend money**, so it asks first (`--yes` to skip the prompt). See [Testing a node](../nodes/testing.md). |
| `weft node-test-hash [target]` | the content hash of a package's test inputs. Run it when you want to know whether anything a package's tests depend on has changed since they last passed. |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's standard library. It **wipes and recopies** that folder, so copy anything you edited in there out first. |
| `weft describe-nodes [--stdlib]` | print the catalog as JSON. This is how you hand a model the full node vocabulary. |

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
| `weft files ls` | stored runtime files |
| `weft files inspect <key>` | one file's metadata |
| `weft files download <key>` | fetch it |
| `weft files rm <key>` | delete one file, or a whole space if the target ends in `/`. Non-interactively `--yes` is required, since a prefix wipe can take kept files with it. |
| `weft files usage` | how much is stored |
| `weft token mint --name "..."` | mint a browser-extension token. Prints the connect URL **once**. |
| `weft token ls` | list minted tokens |
| `weft token revoke <id>` | kill one |
| `weft listener inspect` | what the listener tier is currently holding. Reach for it when a trigger stopped firing: if what it holds disagrees with what the project registered, a cleanup went wrong. |
| `weft rm [project]` | remove a project. `--journal`, `--local`, `--all`, `--force` |

`weft token mint` also takes repeatable `--projects` and `--tags` flags that
narrow what a token can ever see:
[Scoping a token](browser-extension.md#scoping-a-token).

## The environment

| Variable | Default | Set it when |
|---|---|---|
| `WEFT_DISPATCHER_URL` | `http://localhost:9999` | the runtime is not on this machine, or you moved its port |
| `WEFT_HTTP_PORT` | `9999` | something else already has 9999 |
| `CREDENTIAL_ENCRYPTION_KEY` | a development key, with a warning on every boot | before you store a credential you care about. It seals them at rest, and changing it later strands every connection you had. |
| `WEFT_ACCESS_APPS_FILE` | | you are pointing weft at a [shared-credentials file](../connections/the-apps-file.md) |
| `WEFT_PUBLIC_TUNNEL_TOKEN` | | you want a [permanent public address](../connections/public-address.md) instead of the random one |
| `WEFT_PUBLIC_TUNNEL_HOSTNAME` | | the same, and both have to be set together |

A `.env` in the project is loaded automatically. A malformed one is fatal
rather than partially applied.
