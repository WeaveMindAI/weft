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

This prints the connect URL, then the bare token on a second line for a
script that wants only that. Both hold the same secret, shown **once**: the
server stores only a hash and can never show it to you again. Paste it into the extension's popup and pending tasks
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

If you want the token to also read what a node is **showing** (the WhatsApp
bridge's QR code, the password the Postgres node minted), say so:

```bash
weft token mint --name "my site" --display whatsapp
weft token mint --name "my site" --display test.whatsapp
weft token mint --name "my site" --displays
```

This one reads the other way round from the two above: a token that says
nothing about displays reads none of them. A QR code pairs the account to
whoever scans it, so it takes an explicit word. `--displays` opens every
display in the token's projects.

`--display` opens exactly one, and repeats for several. Name the node the way
you write it everywhere else: `whatsapp` for one in this file, `test.whatsapp`
for the `whatsapp` of the file you brought in as `test`. Run it from the
project's folder, which is where the name means something; outside one, it
tells you to go stand in the project. `--projects`, when you use it, still
bounds the grant: a scope narrows, and mint refuses a grant for a project you
scoped the token out of.

Mint refuses a name no display in that project answers to, and lists the ones
it has.

```bash
weft token ls
weft token revoke <id>
```

### The doors a token opens

If you are writing your own consumer instead of using the extension, the
token opens six doors on the dispatcher, the token as bearer on every one
except the answering door:

| Door | Set it when |
|---|---|
| `GET /signal-token/signals` | you want the tasks this token may see: one entry per parked question or registered trigger, form fields included |
| `POST /signal/{signal token}` | you are answering one; the per-task token in the listing is the credential, no bearer |
| `GET /signal-token/signals/{signal token}/files/{field}` | a field carries a stored file and you want to show it. A file arrives in the listing as its facts only (`mimeType`, `sizeBytes`, `filename`, no link); this door answers a fresh link that lives an hour, so ask each time you render. A file that is gone answers 410 saying so; show that in the image's place rather than a broken picture |
| `GET /signal-token/displays` | you want what this token can watch: one entry per display it reaches, carrying its project, its label, and `node`, the node spelled the way a person writes it, which is what the two doors below take as `{node}` |
| `GET /signal-token/displays/{project}/{node}` | you are drawing one: `{ "items": [...] }`, the same feed the editor's node panel reads. Read it on every render rather than storing it, because a QR code expires in under a minute |
| `POST /signal-token/displays/{project}/{node}/action` | the reader pressed a button one of those items carried. Send `{ "kind": "<actionKind>", "payload": ... }`; a refusal the node wrote comes back as a 400 with its text |

The files door is scoped like the listing: a task the token lists, a field
the form declares, a file that belongs to that task's project or run.
Anything else is a 404, and the storage key never travels.

The display doors are scoped by the `--display` flags above, and only by
those: tags say which SIGNALS a token sees, and a display is not a signal. A
token that reaches no display at all gets a 403 naming the flag, so you are
never left reading an empty list and wondering whether the project has
nothing to show. For what an item looks like, go and read
[what your node shows in the graph](../nodes/showing-things-in-the-graph.md).

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
