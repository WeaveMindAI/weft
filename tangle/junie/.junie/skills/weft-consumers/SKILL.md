---
name: weft-consumers
description: Building your own consumer of a weft program's signals, the surface the browser extension is one client of. Read when a user wants their website, app, bot, or their own extension to list what a program is waiting on, show it, answer it, skip or cancel it, or fetch a file it carries: the api token, the doors, the payload shapes, the form kind shipped today, and how a new signal kind reaches consumers.
---

# Your own consumer of a program's signals

A running program waits on **signals**: a parked question, a trigger that
starts a run, a webhook, a timer. Every signal has a per-signal token, and
an **api token** lets an outside client list the signals it may see and
fire them. The weft browser extension is one such client, for the kind
people answer by hand (a form); anything that can send HTTP can be
another, for that kind or any kind that renders a consumer payload. This
is the whole surface, in the order a client uses it.

## The token

A consumer holds an **api token**, minted by whoever runs the project:

```bash
weft token mint --name "my website"
weft token mint --name "reviewer" --projects <id> --tags approvals
```

It prints, once, an address of the form
`http://host:port/signal-token/<token>`: the part before `/signal-token/`
is the dispatcher the client talks to, the last segment is the token. The
bare token follows on the second line, so a script takes `tail -n 1`
(or `--json` for the whole answer). The
server keeps a hash only, so a lost token is revoked (`weft token ls`,
`weft token revoke <id>`) and a new one minted. A token with no scope sees
every task of the tenant; `--projects` and `--tags` narrow it, both repeat.

Every door below except answering takes the token as
`Authorization: Bearer <token>`. It never goes in a URL: proxies and access
logs would keep it.

## The four doors

| Door | What it does |
|---|---|
| `GET /signal-token/signals` | the signals this token may see and that render a consumer payload, one JSON object each (shape below) |
| `POST /signal/{signal token}` | fire one signal with a JSON body: answer a question, start a trigger's run, deliver a webhook's payload. No bearer: the per-signal token in the listing is the credential |
| `POST /signal/{signal token}/skip` | fire it with no payload: a parked question resumes unanswered (the node's outputs close and downstream skips). No bearer, same rule as firing |
| `GET /signal-token/signals/{signal token}/files/{field}` | a fresh link for a stored file a signal's field shows, good for an hour. Ask every time you render, never store the link |

The doors know nothing of any kind: they list, fire, skip, cancel and link
the same way whatever the signal is. Two more, for a client that manages
signals rather than answers them:
`DELETE /signal/{signal token}` cancels the run behind a task (answers
204), and `DELETE /signal-token/signals` cancels every run the token sees
and drops its triggers (answers `{ colors_cancelled, entry_signals_dropped }`).
Both need a token with **no tag scope** and full project view; a narrowed
token gets 403, because a cancel reaches sibling questions of the same run
that the token may not see. `GET /signal-token/health` answers
`{ "ok": true }` for a valid token, the cheapest "is this connected" check.

## What a listed signal looks like

Each entry of the listing:

```json
{
  "token": "…",              // the per-task credential, used in the answer doors
  "nodeId": "ask",
  "kind": "form",
  "consumerKind": "human_in_the_loop",
  "title": "Approve this reply?",
  "description": "…",         // absent when the node set none
  "isResume": true,           // true: a parked question; false: a trigger to start a run
  "formSchema": { "fields": [ … ] }
}
```

`token`, `nodeId`, `kind`, `isResume` and `title` are on every entry; the
rest is the kind's own payload, and `kind` says how to read it. `isResume`
splits the list: a parked question disappears once answered (a second
answer gets 404 once it is gone, 409 while the project is parked and the
first answer waits in its queue); a trigger stays listed while the project
is active and can be fired again and again, each fire a new run.

**Which kinds appear.** A kind reaches the listing when its listener
handler renders a consumer payload. Today that is the form kind alone
(`HumanQuery` and `HumanTrigger`); timers, polls, webhooks, provider
events and live connections render none and stay out of the listing,
though their signal tokens fire through the same door. A new kind is
language work: a `Signal` type in `weft-core/src/signal/`, a handler in
`weft-listener/src/kinds/` whose `render` builds the payload (and keeps
weft's internal file references out of it), `Signal::stored_file` when
the payload shows files (the files door asks the kind, so it needs no
knowledge of any), and the node that registers it setting `consumerKind`,
the free label a client filters on. Once it renders, every client that
knows its `kind` can show and fire it; nothing else changes.

## The form kind

A form's payload is `formSchema.fields`, each field:

```json
{
  "fieldType": "approve_reject",
  "key": "ok",
  "label": "Approve?",
  "render": { "component": "buttons", "source": "static", "multiple": false, "prefilled": false },
  "value": null,             // the prefill, when the kind has one
  "config": { "approveLabel": "Yes", "rejectLabel": "No", "options": ["a", "b"] }
}
```

`render.component` is what you draw; `key` is what you answer under.

| Kind | Draw | Prefill in `value` | Answer under `key` |
|---|---|---|---|
| `display` | `readonly`: show the value, no input | the wired value (a string, or any JSON to pretty-print) | nothing |
| `display_image` | `image` | a file (below) | nothing |
| `approve_reject` | `buttons`, labels from `config.approveLabel` / `rejectLabel` | none | `true` or `false` |
| `select` / `multi_select` | `select`, options from `config.options`; `render.multiple` for several | none | one option string, or an array of them |
| `select_input` / `multi_select_input` | `select`, options from the wired `value` (a list of strings) | the option list | as above |
| `text_input` / `textarea` | `text` / `textarea`, `config.placeholder` when set | none | a string |
| `editable_text_input` / `editable_textarea` | the same, `render.prefilled` true | the wired string, shown for editing | the edited string |

The answer body is one JSON object, every answerable field present:
`{ "ok": true, "reason": "looks fine", "tone": ["formal"] }`. A field left
out lands as null on its port, which a typed port refuses: the port closes
with a warning in the journal and whatever reads it skips, so send every
field. The dispatcher answers 200 on a live project, and also 200 when the
project is parked (the answer is queued and played when it wakes); 404 for
an unknown or already-answered task token; 409 for a second answer while
the first waits in a parked project's queue; 410 when the project no
longer accepts anything; 429 when a trigger's queue is full while parked.

## A file in a form

A `display_image` field's `value` is one of two plain shapes, never weft's
internal file marker and never a storage key:

- `{ "url": "https://…", "mimeType": "image/png", "sizeBytes": 9, "filename": "cat.png" }`
  for a file that lives at a URL: use the `url` as it is.
- `{ "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" }` for a
  file weft stores: no link. Call the files door with the task's token and
  the field's `key`; it answers the first shape with a link minted at that
  moment, good for an hour.

Ask the door each time you show the form and each time the page reloads,
so a form answered a month after it parked still shows its picture. The
door is scoped like the listing: a task the token lists, a field the form
declares, a file that belongs to that task's project or run; anything else
is 404. A file that expired or was deleted answers 404 with the store's
message ("no longer available…"); show that text where the picture would
be, never a broken image.

## How the reference extension behaves

`extension-browser/` in the weft checkout is the worked example: a
background poll of the listing every 30 seconds per token, a notification
per task token it has not notified before (remembered across polls, so a
flaky poll does not re-notify), a task page that draws each field by
`render.component`, answers with `POST /signal/{token}`, offers skip, and
asks the files door for every stored image as it renders. Failures are
per token: a dispatcher that is down or answers the wrong shape puts that
token in a failed bucket with the reason, and the other tokens' tasks
still show. Its `src/lib/api.ts` is the smallest complete client of these
doors; copy its shape.

## Writing one

1. Mint a token scoped to what the client should see; keep it server side
   when the client is a website (a browser page holding the token can list
   every signal the token sees).
2. Poll the listing; read each entry by `kind`. A form carries everything
   needed to draw and answer: draw by `render.component`, answer by `key`.
3. Answer with the per-task token; treat 409 as "someone else answered",
   410 as "the project is gone", and show the message text on any other
   failure.
4. For a stored image, ask the files door on every render.
5. Check `GET /signal-token/health` on connect and after failures.
