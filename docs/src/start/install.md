# Install

You need [Docker](https://docs.docker.com/get-docker/),
[kubectl](https://kubernetes.io/docs/tasks/tools/),
[kind](https://kind.sigs.k8s.io/) and [Rust](https://rustup.rs/).
Use VS Code for the graph editor.

From a terminal:

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
./setup.sh
```

The script checks prerequisites and prints instructions for anything missing.
It does not install those tools for you.

The installer starts a local Kubernetes cluster and the weft runtime, adds
`weft` under `~/.local/bin`, and installs the VS Code extension. If it cannot
reach VS Code, it prints the path of the extension package and instructions
for installing it manually.

If your shell cannot find `weft` afterwards, add the PATH line printed by the
installer to your shell configuration. Then go to
[Your first program](first-program.md).

## Which build you get

The installer can download the CLI and VS Code extension when your checkout
is unchanged and its commit matches the published build. Otherwise it builds
them locally and tells you why.

If the installer asks for extension build tools, follow the source setup in
[Contributing](https://github.com/WeaveMindAI/weft/blob/mvp/CONTRIBUTING.md#set-up).

## Add human questions

Install **Weft tasks** separately from
[Firefox Add-ons](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/) or the
[Chrome Web Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm).
You do not need it for the first program. When you reach
[the human-step walkthrough](a-person-in-the-loop.md), you will connect it to
your runtime and answer a question from your program.

## Choose what to install

The default installs the CLI, runtime and VS Code extension. To select parts,
combine these flags:

| What you want | Command |
|---|---|
| CLI only | `./setup.sh --cli` |
| Runtime only | `./setup.sh --daemon` |
| VS Code extension only | `./setup.sh --vscode` |
| CLI and runtime, without the editor | `./setup.sh --cli --daemon` |
| A different install prefix | `./setup.sh --prefix /your/path` |

Source-build and release flags belong in
[Contributing](https://github.com/WeaveMindAI/weft/blob/mvp/CONTRIBUTING.md).

## Keep your data through an update

After you update the checkout, run `./setup.sh` again. It compares the code
and installed state, applies database migrations, and refreshes the parts
that changed. You do not need to uninstall first.

The cluster contains your project containers, including databases created by
infrastructure nodes. A cluster rebuild can destroy their disks. If the
installer requires `--rebuild-cluster`, read its data-loss notice before
proceeding. The runtime's own Postgres data is stored separately under
`~/.local/share/weft/postgres-data` and survives that rebuild.

For what runs locally and where the data lives, read
[How the runtime is built](../running/architecture.md).

## Remove weft

To remove the CLI and VS Code extension and stop the daemon:

```bash
./setup.sh --uninstall
```

This keeps the cluster and its remaining pods, stored data, images and build
caches. The browser extension stays installed too. You can reinstall with
`./setup.sh` and reuse the preserved data. If the script cannot remove the
VS Code extension, it prints the manual command.

To also discard the local runtime data and cluster:

```bash
./setup.sh --uninstall --purge
```

A purge deletes the cluster, runtime database, object-store data and weft
images and caches. Use it only when you intend to lose that local state.
Your project source files remain in their directories.
