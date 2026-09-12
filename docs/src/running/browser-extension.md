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

That prints a connect URL **once**, because weft only keeps a hash of it.
Paste it into the extension and the tasks start turning up. Lose it and there
is nothing to recover: mint another and `weft token revoke` the old one.

### Scoping a token

```bash
weft token mint --name "reviewer" --projects <id> --tags approvals
```

A token with no scope flags sees **every** task in your tenant, in every
project, until you revoke it. So narrow it before handing it to anyone else.
`--projects` limits it to certain projects and `--tags` to certain task tags,
and both repeat.

```bash
weft token ls
weft token revoke <id>
```

### The doors a token opens

If you are writing your own consumer instead of using the extension, the token
opens three doors on the dispatcher, the token as bearer on the first and the
last:

| Door | Set it when |
|---|---|
| `GET /signal-token/signals` | you want the tasks this token may see: one entry per parked question or registered trigger, form fields included |
| `POST /signal/{signal token}` | you are answering one; the per-task token in the listing is the credential, no bearer |
| `GET /signal-token/signals/{signal token}/files/{field}` | a field carries a stored file and you want to show it. A file arrives in the listing as its facts only (`mimeType`, `sizeBytes`, `filename`, no link); this door answers a fresh link that lives an hour, so ask each time you render. A file that expired answers a **410** saying so; show that in the image's place rather than a broken picture |

The files door is scoped like the listing: a task the token lists, a field the
form declares, a file that belongs to that task's project or run. Anything
else is a 404, and the storage key never travels.

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
