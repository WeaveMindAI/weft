---
name: weft-consumers
description: "Read when the user wants their own website, app, bot or extension to list, show, answer, skip or cancel what a program is waiting on, or fetch a file it carries: the api token, the doors, the payload shapes, and how a new signal kind reaches a consumer."
---

# Your own consumer of a program's signals

A [signal] is one thing a running program waits on: a parked question, a
trigger that starts a run, a webhook, a timer. Every [signal] has a
[signal token], the credential that fires it (the `token` of its listing
entry). An [api token] lets a [consumer] list the [signal]s it may see. A
[consumer] is any client of the [door]s below: the weft browser extension
is one, for the form kind; anything that can send HTTP is another, for that
kind or any kind that renders a consumer payload. A frontend under `front/`
(the `weft-frontend` skill) is one more [consumer] of exactly these
[door]s, for the human steps the program parks; the program's own `Route`
nodes serve its ordinary data calls (the `weft-api` skill).

## The token

Whoever runs the project mints the [api token]:

```bash
weft token mint --name "my website"
weft token mint --name "reviewer" --projects <id> --tags approvals
```

It prints, once, an address of the form
`http://host:port/signal-token/<token>`: the part before `/signal-token/`
is the dispatcher the [consumer] talks to, the last segment is the token.
The bare token follows on the second line, so a script takes `tail -n 1`
(or `--json` for the whole answer). The server keeps a hash only, so a lost
token is revoked (`weft token ls`, `weft token revoke <id>`) and a new one
minted. A token with no scope sees every [signal] of the tenant;
`--projects` and `--tags` narrow it, both repeat.

Every [door] below except firing takes the [api token] as
`Authorization: Bearer <token>`. It never goes in a URL: proxies and access
logs would keep it. When the [consumer] is a website, it stays server side:
a browser page holding it can list every [signal] it sees.

## The four doors

A [door] is one HTTP endpoint of the dispatcher. The [door]s know nothing of
any kind: they list, fire, skip, cancel and link the same way whatever the
[signal] is.

| [door] | What it does |
|---|---|
| `GET /signal-token/signals` | the [signal]s this [api token] may see and that render a consumer payload, one [entry] each (shape below) |
| `POST /signal/{signal token}` | fire one [signal] with a JSON body: answer a question, start a trigger's run, deliver a webhook's payload. No bearer: the [signal token] is the credential |
| `POST /signal/{signal token}/skip` | fire it with no payload: a parked question resumes unanswered (the node's outputs close and downstream skips). No bearer, same rule as firing |
| `GET /signal-token/signals/{signal token}/files/{field}` | a fresh link for a stored file a [signal]'s field shows, good for an hour. You ask every time you render and never store the link |

Two more, for a [consumer] that manages [signal]s:
`DELETE /signal/{signal token}` cancels the run behind a [signal] (answers
204), and
`DELETE /signal-token/signals` cancels every run the [api token] sees and
drops its triggers (answers `{ colors_cancelled, entry_signals_dropped }`).
Both need an [api token] with **no tag scope** and full project view; a
narrowed token gets 403, because a cancel reaches sibling questions of the
same run that the token may not see. `GET /signal-token/health` answers
`{ "ok": true }` for a valid [api token].

## What a listed signal looks like

An [entry] is one object of the listing:

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

`token`, `nodeId`, `kind`, `isResume` and `title` are on every [entry]; the
rest is the kind's own payload, and `kind` says how to read it. `isResume`
splits the list: a parked question disappears once answered (a second
answer gets 404 once it is gone, 409 while the project is parked and the
first answer waits in its queue); a trigger stays listed while the project
is active and fires again and again, each fire a new run.

**Which kinds appear.** A kind reaches the listing when its listener
handler renders a consumer payload. Today the form kind is the only one that
does, so any node registering a form signal is listed (`HumanQuery` is one)
and every other kind stays out of the listing, though its [signal token]s
fire through the same [door]. A new kind is language work:
a `Signal` type in `weft-core/src/signal/`, a handler in
`weft-listener/src/kinds/` whose `render` builds the payload (and keeps
weft's internal file references out of it), `Signal::stored_file` when the
payload shows files (the files [door] asks the kind, so it needs no
knowledge of any), and the node that registers it setting `consumerKind`,
the free label a [consumer] filters on. Once it renders, every [consumer]
that knows its `kind` shows and fires it; nothing else changes.

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
with a warning in the journal and whatever reads it skips, so you send every
field. The dispatcher answers 200 on a live project, and also 200 when the
project is parked (the answer is queued and played when it wakes); 404 for
an unknown or already-answered [signal token]; 409 for a second answer
while the first waits in a parked project's queue (you show it as "someone
else answered"); 410 when the project no longer accepts anything (you show
it as "the project is gone"); 429 when a trigger's queue is full while
parked. On any other failure you show the message text.

## A file in a form

A `display_image` field's `value` is one of two plain shapes, never weft's
internal file marker and never a storage key:

- `{ "url": "https://…", "mimeType": "image/png", "sizeBytes": 9, "filename": "cat.png" }`
  for a file that lives at a URL: you use the `url` as it is.
- `{ "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" }` for a
  file weft stores: no link. You call the files [door] with the [signal
  token] and the field's `key`; it answers the first shape with a link
  minted at that moment, good for an hour.

You ask the [door] each time you show the form and each time the page
reloads, so a form answered a month after it parked still shows its
picture. The [door] is scoped like the listing: a [signal] the [api token]
lists, a field the form declares, a file that belongs to that [signal]'s
project or run; anything else is 404. A file that expired or was deleted
answers 404 with the store's message ("no longer available…"); you show
that text where the picture would be, never a broken image.

## How the reference extension behaves

`extension-browser/` in the weft checkout is the worked example: a
background poll of the listing every 30 seconds per [api token], a
notification per [signal token] it has not notified before (remembered
across polls, so a flaky poll does not re-notify), a page that draws each
field by `render.component`, fires `POST /signal/{token}`, offers skip, and
asks the files [door] for every stored image as it renders. Failures are
per [api token]: a dispatcher that is down or answers the wrong shape puts
that token in a failed bucket with the reason, and the other tokens'
[signal]s still show. Its `src/lib/api.ts` is the smallest complete client
of these [door]s; you copy its shape.

## Writing one

1. Mint an [api token] scoped to what the [consumer] sees.
2. Poll the listing; read each [entry] by `kind`; draw by
   `render.component`, answer by `key`.
3. Fire with the [signal token]; show 409, 410 and other failures as above.
4. For a stored image, ask the files [door] on every render.
5. Check `GET /signal-token/health` on connect and after failures.
