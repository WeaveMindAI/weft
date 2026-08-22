# The browser extension

The browser extension is where humans meet running programs. When an execution
reaches a `HumanQuery` node it suspends, and the task (a form, an approval)
appears in the extension of everyone whose token is allowed to see it. Someone
answers, the execution resumes exactly where it stopped. The program can wait
a minute or a week; nothing is lost in between.

<!-- CAPTURE: the extension popup showing a pending approval task, with the
     form fields visible. -->

## Build it

The default `./setup.sh` run skips the extension; ask for it explicitly:

```bash
./setup.sh --browser
```

That writes an unpacked build per browser under `extension-browser/build/`
(for Chrome, `build/chrome-mv3`) plus a zip per browser
(`build/weft-extension-<browser>.zip`). You need Node 20+ and pnpm; the
script checks and tells you if either is missing.

## Load it

- **Chrome / Edge / Opera**: open `chrome://extensions`, turn on Developer
  mode, click "Load unpacked", and pick the browser's folder under
  `extension-browser/build/`.
- **Firefox**: open `about:debugging#/runtime/this-firefox`, click "Load
  Temporary Add-on", and pick the zip. For a permanent install, Firefox
  requires a signed build: `./setup.sh --browser --sign` (needs Mozilla AMO
  API keys in `.env.extension`; the script prints where to get them).

## Connect it

The extension talks to your dispatcher with a token:

```bash
weft token mint --name "my laptop"
```

This prints a connect URL once (the server stores only a hash and can never
show it again). Paste it into the extension's popup and you're connected:
pending tasks from your programs start appearing.

Tokens can be scoped. `--projects <id>` restricts a token to a project and
`--tags <tag>` to a task tag (repeat either flag for several), so you can
hand a reviewer a token that only ever shows them the tasks meant for them.
`weft token ls` lists what's minted, `weft token revoke <id>` kills one.

## Try it end to end

Add a `HumanQuery` node to any program and run it. The execution parks on
the node, the task pops up in the extension, and your answer wakes the
program up. The graph view shows the node in its waiting state the whole
time, so you can watch the hand-off happen from both sides.

## Hacking on it

The extension is a WXT + Svelte app in `extension-browser/`. For a live
development loop:

```bash
cd extension-browser
pnpm install
pnpm dev
```

`pnpm dev` launches a browser with the extension loaded and hot-reloads as
you edit.
