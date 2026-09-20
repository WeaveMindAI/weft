---
name: weft-consumers
description: "Read when the user wants their own website, app, bot or extension to list, show, answer, skip or cancel what a program is waiting on, fetch a file it carries, or show what one of its nodes is displaying (a WhatsApp bridge's QR code, a database's minted password): the api token, the doors, the payload shapes, and how a new signal kind reaches a consumer."
---

# Your own consumer of a weft program

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
weft token mint --name "my website" --display whatsapp
```

It prints, once, an address of the form
`http://host:port/signal-token/<token>`: the part before `/signal-token/`
is the dispatcher the [consumer] talks to, the last segment is the token.
The bare token follows on the second line, so a script takes `tail -n 1`
(or `--json` for the whole answer). The server keeps a hash only, so a lost
token is revoked (`weft token ls`, `weft token revoke <id>`) and a new one
minted. A token with no scope sees every [signal] of the tenant;
`--projects` and `--tags` narrow it, both repeat.

**The display scope reads the other way round.** `--display` and
`--displays` GRANT what a node is showing, and a token that names neither
reads none of them. A QR code pairs the account to whoever scans it, so it
takes an explicit word rather than arriving with the wildcard. Ask which
nodes the [consumer] should watch before minting, and mint the narrow one.

`--display` takes the node the way the user writes it (`whatsapp`, or
`test.whatsapp` for a node of a file included as `test`), and the CLI resolves
it against the project the user is standing in, so it is run from that folder.
An address is only a name inside one project, and the grant the dispatcher
stores carries both halves. Mint refuses a name no display in that project
answers to, and lists the ones it has.

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

## Showing what a node is showing

A [display] is what one node shows about itself while it runs. Three more
[door]s, the [api token] as bearer on all three:

| [door] | What it does |
|---|---|
| `GET /signal-token/displays` | the displays this token may watch: `{ project_id, project_name, node, node_type, kind, label? }` each, `kind` being `infra` or `trigger`. `node` is spelled the way a person writes it, and it is what `{node}` takes in the two doors below |
| `GET /signal-token/displays/{project}/{node}` | what that node is showing right now |
| `POST /signal-token/displays/{project}/{node}/action` | press a button one of its items carried: `{ "kind": "<actionKind>", "payload": ... }`. Only an `infra` node has buttons; a `trigger`'s [display] is read-only and answers 400 |

A display is `{ "items": [...] }`, each item `{ type, label, data, action? }`:

```json
{ "items": [
  { "type": "image", "label": "Scan with WhatsApp", "data": "data:image/png;base64,..." },
  { "type": "text",  "label": "Phone", "data": "not paired",
    "action": { "label": "Disconnect phone", "actionKind": "unpair",
                "confirm": "Detach the paired phone?" } }
] }
```

`type` is one of four and says how to draw it:

| `type` | `data` is | draw it as |
|---|---|---|
| `text` | a string | plain text, copyable |
| `image` | a `data:` URI or a URL | straight into an `<img src>` |
| `progress` | a number from 0 to 1 | a bar |
| `secret` | a string | masked until the reader asks to see it, and copyable without revealing |

`label` is on every item. `action` is at most one per item and draws a
button: show `confirm` first when it is there, POST `actionKind` to the
action door, and read the display again right after, because the press is
what changed it.

```js
const res = await fetch(`${base}/signal-token/displays/${project}/${node}`, {
  headers: { Authorization: `Bearer ${token}` },
});
if (!res.ok) throw new Error(await res.text());
const { items } = await res.json();

// pressing an item's button
await fetch(`${base}/signal-token/displays/${project}/${node}/action`, {
  method: 'POST',
  headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  body: JSON.stringify({ kind: item.action.actionKind, payload: null }),
});
```

`base` is the part of the minted address before `/signal-token/`, the same
one every other door uses.

**Read the display on every render, never store it.** A QR code expires in
under a minute, and a bridge that got paired in between shows a phone number
where the code was. Poll it every few seconds while the reader is looking at
it, and stop when they navigate away.

Who serves it depends on `kind`, and a client drawing items does not care: an
`infra` node's own container serves it, a `trigger`'s signal kind serves it
from the listener. What differs is what shows up. A container shows whatever
its author wrote, buttons included. A trigger with a public entry shows the
whole address a caller sends to (host, methods and all, ready to use as it is)
and how the door checks them, which is either open or a named connection and
never a secret; one that fires from inside the runtime shows what it is doing
(a timer its schedule, a poll the URL it reads and how often).

A node the token was not given, a project it cannot see and a node with
nothing to show all answer 404, so a caller walking ids learns only "nothing
here". A token that reaches no display at all gets a 403 naming the flag that
would have given it one, so ask the operator to mint a token with `--display
<node>` rather than guessing that the project shows nothing. While there
is nothing behind the display yet (the infra is not started, the project is
not activated) the door answers 404 as well: say that in the panel's place,
because starting the infra is the reader's next move.

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
5. For a node's display, read it on every render and draw by `type`.
6. Check `GET /signal-token/health` on connect and after failures.
