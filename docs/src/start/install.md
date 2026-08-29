# Install

One script builds everything and leaves a working runtime on your machine.

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
./setup.sh
```

The first run takes a while: it compiles the Rust workspace, builds container
images, creates a local Kubernetes cluster, and starts the runtime. Later runs
are incremental.

## What you need first

If you run the script with no flags it builds every component, so it checks for
all of these before it starts and names every missing one at once.

| Tool | Why | Get it |
|---|---|---|
| `cargo` | compiles the CLI and the runtime | [rustup.rs](https://rustup.rs/) |
| `docker` | runs Postgres and the cluster | [docs.docker.com](https://docs.docker.com/get-docker/) |
| `kubectl` | talks to the local cluster | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |
| `kind` | the local cluster itself | [kind.sigs.k8s.io](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) |
| `node` 20+ and `pnpm` | builds the VS Code extension | [nodejs.org](https://nodejs.org/en/download), then `npm i -g pnpm` |

If you are on macOS you also need a newer Bash than the one Apple ships:
`brew install bash`.

And if you only want part of it, the flags below skip the rest and skip their
checks with them. `./setup.sh --cli` needs nothing but `cargo`.

## What you get

One binary, `weft`, symlinked into `~/.local/bin`. If that is not on your
`PATH`, the script prints the exact line to add to your shell config.

The runtime itself is not a binary on your machine: the dispatcher and the
workers run as containers in the local cluster, which is why the script builds
images as well as compiling. Plus the VS Code extension, where the graph view
and the live execution view live.

The script leaves the runtime up, so by the time it finishes the daemon is
listening on port 9999 and you can go straight to your first program.

## Picking a subset

Flags combine, so `--cli --daemon` does both and skips the editor.

| If you want | Pass |
|---|---|
| just the `weft` command | `--cli` |
| just the runtime rebuilt and restarted | `--daemon` |
| just the VS Code extension | `--vscode` |
| the browser extension too, which is opt-in because it bumps versions and signs | `--browser` |
| the CLI compiled much faster, while you are iterating | `--debug` |
| the binary somewhere other than `~/.local` | `--prefix PATH` |
| the CLI rebuilt without touching the running daemon | `--no-daemon` |

## Your settings file

Weft reads a `.env` next to your project, and there is nothing in it you need
to get started. Two settings are worth knowing about before you store anything
you care about: `CREDENTIAL_ENCRYPTION_KEY`, which seals stored credentials at
rest and boots with a development key until you set it, and
`WEFT_PUBLIC_TUNNEL_TOKEN`, if you want a permanent public address rather than
a fresh random one each time. The full list is
[the environment](../running/cli.md#the-environment).

A malformed `.env` fails the boot rather than being half applied.

## Removing it

There are two levels of it, depending on whether you want your work back
afterwards.

```bash
./setup.sh --uninstall            # take the tools away, keep the work
./setup.sh --uninstall --purge    # take everything
```

`--uninstall` stops the daemon, removes the VS Code extension, and drops the
`weft` symlink. It deliberately keeps the cluster, the database, the object
store and its volume, the built images, the BuildKit cache, and `target/`. Run
`./setup.sh` again and your projects and their whole execution history are back
in seconds.

Neither is ever needed to apply an update. `./setup.sh` brings an existing
install to whatever the code now says: the schema, the images, the manifests,
the ingress and gateway controllers, and the object store's container. The one
thing it never does on its own is rebuild the cluster: when the cluster's
shape or your `kind` version has moved, it stops and asks for
`--rebuild-cluster`, because every project's own database lives inside the
cluster's node and dies with it. Your system database survives a rebuild
either way; its files live in `~/.local/share/weft/postgres-data` rather than
inside the cluster.

`--purge` is the real clean slate: the cluster goes, the images go, the
database volume goes. Reach for it when you want to prove a fresh machine would
work.

## A note on the cluster

Weft runs your programs as pods on Kubernetes, including on your laptop, where
the cluster is a single `kind` node inside Docker. That is what lets a project
ask for a Postgres, a headless browser, or a model server as a node you drop on
the graph, and get a real container with health checks and a lifecycle.

You never write YAML. You will not think about the cluster again until you read
[Infrastructure nodes](../nodes/infrastructure.md).

Next: [your first program](first-program.md).
