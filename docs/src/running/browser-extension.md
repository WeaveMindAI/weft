# The browser extension

This is how a running program asks somebody a question and waits for the
answer.

When a run reaches a `HumanQuery` step it parks, and the task turns up for
everyone whose token can see it. Somebody answers and the run carries on from
where it stopped, whether that was a minute ago or last week.

![A pending approval task in the extension](../img/extension-popup.png)

<!-- IMAGE ------------------------------------------------------------------
file:  docs/src/img/extension-popup.png
kind:  screenshot
brief: The extension popup open in a browser toolbar, showing one pending task:
       a title, the context the program sent (a short LLM classification, say),
       the form fields, and Approve / Reject buttons. Show a second, collapsed
       task below it so the list nature is visible. Clean, no dev tools open.
--------------------------------------------------------------------------- -->

## Install it

The extension is in the [Firefox
store](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/) and the
[Chrome Web
Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm);
the [latest
release](https://github.com/WeaveMindAI/weft/releases/tag/mvp-latest) also
carries a zip for every browser (Chrome, Firefox, Edge, Opera, Safari) if your
browser has no store listing or you prefer installing by hand.

Building it yourself is a contributor thing, and
[CONTRIBUTING](https://github.com/WeaveMindAI/weft/blob/main/CONTRIBUTING.md)
covers it.

## Connect it

```bash
weft token mint --name "my laptop"
```

That prints a connect URL **once**, and the bare token on the line after it
for a script that wants only that. Both hold the same secret, and weft keeps
only a hash of it, so it is never shown again. Paste the URL into the
extension and the tasks start turning up. Lose it and there is nothing to
recover: mint another and `weft token revoke` the old one.

### Scoping a token

```bash
weft token mint --name "reviewer" --projects <id> --tags approvals
```

A token with no scope flags sees **every** task in your tenant, in every
project, until you revoke it. So narrow it before handing it to anyone else.
`--projects` limits it to certain projects and `--tags` to certain task tags,
and both repeat.

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

The files door is scoped like the listing: a task the token lists, a field the
form declares, a file that belongs to that task's project or run. Anything
else is a 404, and the storage key never travels.

The display doors are scoped by the `--display` flags above, and only by
those: tags say which SIGNALS a token sees, and a display is not a signal. A
token that reaches no display at all gets a 403 naming the flag, so you are
never left reading an empty list and wondering whether the project has
nothing to show. For what an item looks like, go and read
[what your node shows in the graph](../nodes/showing-things-in-the-graph.md).

## Try it end to end

Add a `HumanQuery` step to any program and run it with the graph open.

The run parks on that step, the task appears in the extension, and your answer
wakes it up. The step sits in its waiting colour the whole time, so you can
watch both sides of the handover at once.

Then do it again, but shut your laptop first and answer tomorrow. Same result,
because a parked run is rows in a table rather than a process holding a socket
open.

## Working on it

The extension is a WXT and Svelte app in `extension-browser/`.

```bash
cd extension-browser
pnpm install
pnpm dev
```

`pnpm dev` launches a browser with the extension loaded and hot-reloads as you
edit.
