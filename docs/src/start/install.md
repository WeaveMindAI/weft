# Install

This gets you a weft runtime on your own machine, the `weft` command, and the
graph editor in VS Code. It is mostly waiting, so keep reading while it runs.

Three tools have to be there first: [Docker](https://docs.docker.com/get-docker/),
[kubectl](https://kubernetes.io/docs/tasks/tools/) and
[kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation). kind runs
a Kubernetes cluster inside Docker, and that cluster is where your programs
live. The installer checks for all three and stops with the install links if
one is missing.

The installer is a bash script, so on Windows run it inside WSL.

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
```

## Put your keys in before you install

Fill in the services you expect to use often, and leave the rest alone. We
recommend at least [OpenRouter](https://openrouter.ai/keys), because a single
key there reaches most models.

A service you set up here becomes one click forever after: a step that calls it
picks the key up on its own, and your AI assistant can wire one in without
stopping to ask you for anything. A service you skip asks you for a credential
the first time you need it, which is fine for the ones you touch once.

```bash
cp access-apps.example.json access-apps.json
```

Open it and edit in place, leaving the services you are not using on their
placeholder values. weft ignores an entry it cannot use.

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

## Install

```bash
./setup.sh
```

It asks you nothing. The first run creates the cluster and pulls images, which
the installer itself estimates at two to three minutes.

## Check it came up

```bash
weft daemon status
```

A healthy answer names the cluster and counts your projects:

```text
daemon: running (cluster 'weft', system ns 'weft-system'); 0 project(s)
public trigger surface: closed
```

`closed` there is the normal state and not a fault. It means nothing of yours
is reachable from the internet, which is what you want until you need
[a public address](../build/public-address.md).

If it says `daemon: unreachable at ...`, run `./setup.sh` again and read the
last block it prints. If that block has scrolled away, every run writes the
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
your machine, and Node 20 with pnpm when it cannot download a prebuilt editor
extension. It tells you which, up front, and stops rather than half-installing.

## Add the browser extension

When a program stops to ask a person something, the question shows up in a
browser extension called **Weft tasks**. Install it now, from
[Firefox Add-ons](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/) or the
[Chrome Web Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm),
and close it again. It has nothing to show you yet. You connect it to your
runtime in [putting a person in the loop](../build/a-person-in-the-loop.md).

## Now leave this folder

Your projects do not belong inside the weft checkout. Go wherever you keep
code:

```bash
cd ~/code
```

Then [build your first program](first-program.md).

## When you come back to this page

**To change your keys**, edit `access-apps.json` in the checkout and run
`weft daemon start`. It takes a few seconds and you do not need the installer.
Deleting the file does not wipe the keys weft already holds; for that, run
`weft daemon start --clear-access-apps`.

**To update**, pull and run `./setup.sh` again. Your projects and their history
stay where they are.

**To remove it**, run `./setup.sh --uninstall`. It stops the runtime and
removes the CLI and the editor extension. Your cluster, database and stored
connections stay, so a later `./setup.sh` brings everything back. Adding
`--purge` deletes all of that too, and your programs' own databases live in
that cluster.
