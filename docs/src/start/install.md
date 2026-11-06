# Install

This gets you a weft runtime on your own machine, the `weft` command, and the
graph editor in VS Code. It is mostly waiting, so keep reading while it runs.

[Docker](https://docs.docker.com/get-docker/) has to be there first.
weft's runtime is a program on your machine. Its database, its file store and
the infrastructure your programs ask for (a database, a model server) run in
Docker containers next to it, and your programs themselves run as plain
processes, each in its project's folder. If Docker is missing, the installer
stops and prints the link to install it.

You also need [Rust](https://rustup.rs/) whenever weft has to compile
something on your machine: a project with nodes of its own or with a
standard node you edited (under `nodes/base_catalog/`), a project you build
with `--referenced`, a live node test, and `./setup.sh --rebuild`. The
install itself needs it on a branch's install, which always builds the CLI
and the standard worker from source. The default install needs it whenever
it cannot use what CI published for your exact checkout: you have local
changes, CI has not finished building the commit you just pulled, you passed
`--debug` or `--from-source`, or the release has nothing for your kind of
machine. The installer says which one it hit, then compiles what it could
not download.
For which projects compile and which share the standard worker, go and read
[the worker](../running/architecture.md#the-worker).

The installer is a bash script, so on Windows run it inside WSL.

## Install

```bash
curl -fsSL https://weft.weavemind.ai/install.sh | bash
```

It downloads weft into `~/weft` and runs the installer from there. To put it
somewhere else, start the line with `WEFT_DIR=<folder>`. Leave that folder
where it is afterwards: weft reads its standard library and its assistant
templates from it.

On a fresh machine the installer asks no questions: the first run starts the
database and pulls images, then sets the runtime up as a service of your user (systemd on Linux, launchd on macOS), so it comes
back on its own after a reboot. Each long step shows a spinner with the last
line it printed; if you want to see everything as it happens, add
`bash -s -- --verbose` in place of the final `bash`.

## Put your keys in

Fill in the services you expect to use often, and leave the rest alone. We
recommend at least [OpenRouter](https://openrouter.ai/keys), because a single
key there reaches most models.

A service you set up here becomes one click forever after: a node that calls it
picks the key up on its own, and your AI assistant can wire one in without
stopping to ask you for anything. A service you skip asks you for a credential
the first time you need it, which is fine for the ones you touch once.

```bash
cd ~/weft
cp access-apps.example.json access-apps.json
```

Open it and edit in place, leaving the services you are not using on their
placeholder values. weft ignores an entry it cannot use, and reads the file
each time it needs a key, so a change takes effect at once.

```json
  "openrouter": [{
    "kind": "api_key",
    "label": "Runtime key",
    "key": "sk-or-REPLACE_WITH_YOUR_KEY"
  }],
```

The file also ships entries for Slack, Google, Notion, Airtable, fal,
ElevenLabs, Exa, Firecrawl and Mistral. Most of them just need a key pasted in.
Slack and Google want you to register an application with that provider first,
which is a longer job, so do those only if they are services you will live in.
For the whole format, go and read
[the apps file](../connections/the-apps-file.md).

## Check it came up

```bash
weft daemon status
```

A healthy answer counts your projects:

```text
weft: running at http://127.0.0.1:14111 (install 'default'); 0 project(s)
public address: closed
```

`closed` there is the normal state and not a fault. It means nothing of yours
is reachable from the internet, which is what you want until you need
[a public address](../build/public-address.md).

If it says `weft: unreachable at ...`, run `weft daemon logs` to see how the
runtime's log ends (the line also names the log file), then run the install
line again and read the last block it prints. If that block has scrolled away, every run writes the
same thing to `~/.local/share/weft/setup-runs.log`.

Two things that can go differently.

**If `weft` is not found**, the installer warned that its directory is not on
your `PATH` and printed an `export PATH=...` line. Put that line in your
shell's startup file (`~/.bashrc` for bash, `~/.zshrc` for zsh) and open a new
terminal.

**If VS Code was closed, or `code` is not on your `PATH`**, the installer built
the editor extension but could not install it, and printed the exact
`code --install-extension ...` command to finish the job. Without the extension
everything the `weft` command does still works; you just will not see the
graph.

The installer also needs Rust when it cannot download a prebuilt `weft` for
your machine, and Node 22.12 or newer with pnpm when it cannot download a prebuilt editor
extension. It tells you which, up front, and stops rather than half-installing.

## Add the browser extension

When a program stops to ask a person something, the question shows up in a
browser extension called **Weft tasks**. Install it now, from
[Firefox Add-ons](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/) or the
[Chrome Web Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm),
and close it again. It has nothing to show you yet. You connect it to your
runtime in [putting a person in the loop](../build/a-person-in-the-loop.md).

## Keep your projects elsewhere

Your projects do not belong inside `~/weft`. Go wherever you keep code:

```bash
cd ~/code
```

Then [build your first program](first-program.md).

## When you come back to this page

**To change your keys**, edit `~/weft/access-apps.json`; the change takes effect at once
([the apps file](../connections/the-apps-file.md)).

**To update**, run `weft maintenance on` first, which takes down every
trigger that is on and stops your programs' infrastructure, then run the
install line again, then `weft maintenance off`, which builds those projects again on the new
weft and brings back exactly what `on` took down. The install line updates `~/weft` and runs the
installer, the same as `git pull` and `./setup.sh` in that folder, and the
installer refuses to replace the running weft while something is on. Your
projects and their history stay where they are. Before you do, check the
[changelog](https://github.com/WeaveMindAI/weft/blob/main/CHANGELOG.md) for
anything that needs you. For what each step does, go and read
[updating weft](../running/cli.md#updating-weft).

If something in a project does not come back with `weft maintenance off`,
everything else is back anyway, and `off` says what went wrong. If the
message says the project's copy of the standard nodes is older than this
weft, run `weft catalog update` in that folder (it replaces
`nodes/base_catalog/`, so any edits you made in there are lost); otherwise
fix what it says. Then run `weft resync` there, which builds the project and
brings back what the update took down.

If you installed weft before it stopped running on Kubernetes, that update is
the exception: its database cannot come along. `./setup.sh` finds the old
install and asks before wiping it. The wipe deletes the old cluster, your run
history, versions and stored connections, and every container and image weft
made, along with every named install on the machine (the question lists them).
Your project folders stay, and `weft run` registers them again. If
`./setup.sh` has no terminal to ask on, run `scripts/scrub-old-install.sh`
yourself first.

If your install's database was built before executions were renamed runs,
that update starts it over too. `./setup.sh` asks before wiping that one
install's database: its run history, versions and stored connections go,
and so does everything only that database gave a meaning to (the files the
install stored, and the workers and infra of its projects). Your project
folders, the install's settings and every other install stay. Without a terminal to ask on, run `./setup.sh --yes`.

**To remove it**, run `./setup.sh --uninstall` in `~/weft`. It stops the runtime and
removes the CLI and the editor extension. Your database, stored connections
and your programs' infrastructure stay, so a later `./setup.sh` brings
everything back. Adding `--purge` deletes all of that too, your programs' own
databases included, and stops every program an infra node runs as a process
on your machine, in every install.
