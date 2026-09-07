# The browser extension

The extension is how a running program asks a person a question and waits for
the answer.

When an execution reaches a `HumanQuery` node it suspends, and the task appears
in the extension of everyone whose token is allowed to see it. Someone answers,
and the execution resumes exactly where it stopped, whether that took a minute
or a week.

![A pending approval task in the extension](../img/extension-popup.png)

<!-- IMAGE ------------------------------------------------------------------
file:  docs/src/img/extension-popup.png
kind:  screenshot
brief: The extension popup open in a browser toolbar, showing one pending task:
       a title, the context the program sent (a short LLM classification, say),
       the form fields, and Approve / Reject buttons. Show a second, collapsed
       task below it so the list nature is visible. Clean, no dev tools open.
--------------------------------------------------------------------------- -->

## Build it

If you want the extension, ask for it, because the default install skips it:
rebuilding it bumps versions and signs for Firefox, which is slower than a
normal build and rarely what you are after.

```bash
./setup.sh --browser --no-sign
```

That writes an unpacked build per browser under `extension-browser/build/`
(for Chrome, `build/chrome-mv3`) plus a zip per browser. You need Node 20 or
newer and pnpm; the script checks and tells you if either is missing.

Drop the `--no-sign` only if you want a Firefox install that survives closing
the browser. Signing needs `web-ext` on your `PATH` and Mozilla AMO keys in
`.env.extension`, and the script stops before building anything if either is
missing, printing where to get them.

## Load it

**If you are on Chrome, Edge, Opera, or another Chromium browser**: open
`chrome://extensions`, turn on Developer mode, click "Load unpacked", and pick
that browser's folder under `extension-browser/build/`.

**If you are on Firefox**: a temporary install loads the zip from
`about:debugging#/runtime/this-firefox` under "Load Temporary Add-on", and it
goes away when you close the browser. For one that sticks, install the signed
`.xpi` that `./setup.sh --browser` drops into `extension-browser/build/` when
you leave signing on.

## Connect it

```bash
weft token mint --name "my laptop"
```

This prints a connect URL **once**. The server stores only a hash and can never
show it to you again. Paste it into the extension's popup and pending tasks
start appearing. Lose it and there is no recovery: mint a second token and
`weft token revoke` the one you lost.

### Scoping a token

```bash
weft token mint --name "reviewer" --projects <id> --tags approvals
```

A token with no scope flags sees **every** task in your tenant, in every
project, until you revoke it. So if you are handing one to somebody else,
narrow it: `--projects` restricts it to specific projects and `--tags` to
specific task tags, and both flags repeat.

```bash
weft token ls
weft token revoke <id>
```

### The doors a token opens

If you are writing your own consumer instead of using the extension, the
token opens three doors on the dispatcher, the token as bearer on the first
and the last:

| Door | Set it when |
|---|---|
| `GET /signal-token/signals` | you want the tasks this token may see: one entry per parked question or registered trigger, form fields included |
| `POST /signal/{signal token}` | you are answering one; the per-task token in the listing is the credential, no bearer |
| `GET /signal-token/signals/{signal token}/files/{field}` | a field carries a stored file and you want to show it. A file arrives in the listing as its facts only (`mimeType`, `sizeBytes`, `filename`, no link); this door answers a fresh link that lives an hour, so ask each time you render. A file that expired answers a 404 saying so; show that in the image's place rather than a broken picture |

The files door is scoped like the listing: a task the token lists, a field
the form declares, a file that belongs to that task's project or run.
Anything else is a 404, and the storage key never travels.

## Try it end to end

Add a `HumanQuery` node to any program and run it with the graph open.

The execution parks on the node, the task pops up in the extension, and your
answer wakes the program. The graph shows the node in its waiting state the
whole time, so you can watch the handoff from both sides.

Then do it again, but close your laptop first and answer tomorrow. Same result,
because the execution is rows in a table rather than a process holding a
socket.

## Working on it

The extension is a WXT and Svelte app in `extension-browser/`.

```bash
cd extension-browser
pnpm install
pnpm dev
```

`pnpm dev` launches a browser with the extension loaded and hot-reloads as you
edit.
