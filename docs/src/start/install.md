# Install

This gets you a weft runtime on your own machine, the `weft` command, and the
graph editor in VS Code. It is mostly waiting, so keep reading while it runs.

One tool has to be there first: [Docker](https://docs.docker.com/get-docker/).
weft's runtime is a program on your machine, and everything it starts (your
programs, their databases and servers) runs in Docker containers next to it.
If Docker is missing, the installer stops and prints the link to install it.

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

The installer asks no questions: the first run starts the database and pulls
images, then sets the runtime up as a service of your user (systemd on Linux, launchd on macOS), so it comes
back on its own after a reboot.

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
runtime's log ends (the line also names the log file), then run `./setup.sh`
again and read the
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

**To change your keys**, edit `access-apps.json` in the checkout; the change takes effect at once
([the apps file](../connections/the-apps-file.md)).

**To update**, pull and run `./setup.sh` again. Your projects and their history
stay where they are. Before you do, check the
[changelog](https://github.com/WeaveMindAI/weft/blob/main/CHANGELOG.md) for
anything that needs you.

If you installed weft before it stopped running on Kubernetes, that update is
the exception: its database cannot come along. `./setup.sh` finds the old
install and asks before wiping it. The wipe deletes the old cluster, your run
history, versions and stored connections, and every container and image weft
made, along with every named install on the machine (the question lists them).
Your project folders stay, and `weft run` registers them again. If
`./setup.sh` has no terminal to ask on, run `scripts/scrub-old-install.sh`
yourself first.

**To remove it**, run `./setup.sh --uninstall`. It stops the runtime and
removes the CLI and the editor extension. Your database, stored connections
and your programs' infrastructure stay, so a later `./setup.sh` brings
everything back. Adding `--purge` deletes all of that too, your programs' own
databases included.
