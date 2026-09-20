# What your node shows in the graph

A node on the canvas is a box with its ports and its config, and it can also
carry a panel on its body. Three things can go in there. Two of them you ask
for in `metadata.json`; the third, its display, an infra node writes and a
trigger gets for free.

## A file it made or received

If your node's firing produces or receives a file worth looking at, one line in
`metadata.json` puts it on the body:

```json
"display": { "kind": "media", "output": "image" }
```

For what `kind` and the port side mean, go and read
[`display`](metadata.md#display).

## Its last value

If you want a node's value visible on the canvas while people are wiring
things up, ask for the debug preview:

```json
"features": { "showDebugPreview": true }
```

The editor renders it as JSON under the body, with a copy button. Which value
depends on your node type's declared ports: one with outputs shows what it
emitted, one with none shows what flowed into it. It is the last execution's
value, and with no value to show the box says what the node is doing instead:
"Waiting for data..." before any run, "Processing..." while it runs,
"Suspended" while it waits for an answer, and "Execution complete" or
"Execution failed: ..." after.

Collapse the node and both the preview and the file display go away. An infra
or trigger feed stays visible either way.

## Its display

A **display** is what a node shows about itself while it runs. The WhatsApp
bridge shows a QR code to scan, and then the phone number that scanned it. The
Postgres node shows the password it just minted. A webhook trigger shows the
address it is listening on and how a caller gets past the door.

Two kinds of node have one, and both serve it the same way: a `GET /live`
returning `{ "items": [...] }`. So there is one panel, one set of item types,
and one thing for a reader to learn.

```json
{ "items": [
  { "type": "image", "label": "Scan with WhatsApp", "data": "data:image/png;base64,..." },
  { "type": "text",  "label": "Phone", "data": "not paired",
    "action": { "label": "Disconnect phone", "actionKind": "unpair",
                "confirm": "Detach the paired phone?" } }
] }
```

Four item types render. `text` is a copyable box, `image` takes anything an
`<img src>` accepts, `progress` takes a number from 0 to 1 and draws a bar, and
`secret` sits behind a `••••` mask until the user clicks the eye. Use `secret`
for anything that should not sit on somebody's screen, like an API key; the
copy button hands over the real value whether or not it is revealed.

An infra node's item may carry one button. Pressing it posts `{ "action":
"<actionKind>", "payload": ... }` back to the container, which is the only
party that knows what the press means, and the editor polls the display again
right after, so a fresh QR code appears at once. A trigger's display is
read-only: nothing about a registration is a reader's to change from a panel.

**If your node is an [infra node](infrastructure.md)**, you write the display:
serve `/live` from the container and name that endpoint in
`features.liveEndpoint`. Naming it is what opts in. For the full route
contract, go and read
[the routes your container serves](infrastructure.md#the-routes-your-container-serves).

**If your node is a trigger**, you write nothing: the trigger KIND serves the
display, in the listener. The kind is what mints the auth and the key at
activation, so the kind is what has something to say, and every node declaring
that kind gets the same panel for free. A kind with a public entry shows the
whole address a caller sends to, and how the door checks them: open, or against
a named connection. The key itself is never on the panel, because the listener
never sees one: the material lives on the connection and the broker answers the
check. A kind that fires from inside the runtime shows what it is doing
instead: a timer shows its schedule, a poll shows the URL it reads and how
often. A URL you configured is shown as a `secret`, masked until you click the
eye, because a poll loop's address can carry its own token.

The address is the dispatcher's to fill in, not the kind's: it carries the host
weft answers on, the tenant segment your paths are stored under, and the
`/connect/` prefix a held-connection kind is served at, and the listener knows
none of those. So it is the address somebody can copy and post to, not a
fragment to assemble.

If nothing is serving the display yet, the panel says which move makes it
appear: "Not running. Start it from the action bar." on an infra node,
"Nothing is listening for this trigger. Activate the project from the action
bar." on a trigger. If something answered but the answer was a failure, the panel carries
that text verbatim instead.

An item whose type is known but whose data is the wrong shape, a `progress`
carrying a string, is rewritten on the way through into a text line named
`<its label> (unreadable)` that says what it carried, rather than disappearing
quietly. An item weft cannot read at all becomes a line saying so, in its
place, so a container that grew a fifth item type still shows its other four.

## Somebody else's page, showing the same thing

A display is not only for the editor. If you want a website or an app you built
on top of the program to show the QR code, rather than sending its user to the
editor, mint a token that reaches it:

```bash
weft token mint --name "my site" --display whatsapp
```

Name the node the way you write it everywhere else, and run it from the
project's folder: `whatsapp` for one in this file, `test.whatsapp` for the
`whatsapp` of the file you brought in as `test`. The tool fills in which
project you meant, because that is the folder you are standing in, and an
address is only a name inside one project.

`GET /signal-token/displays` then lists what that token can watch, and
`GET /signal-token/displays/{project}/{node}` is the same feed the editor
reads, with `{node}` spelled the same way. Mint refuses a name no display in
that project answers to, and lists the ones it has.

Saying nothing about displays grants none of them, which is the opposite of how
`--projects` and `--tags` read: a QR code pairs the account to whoever scans
it, so it takes an explicit word. `--displays` opens every display of the
token's projects.

For the doors and their scoping, go and read
[the doors a token opens](../running/browser-extension.md#the-doors-a-token-opens).

## When there is no panel

Add a panel when the alternative is asking the user to go and read a log.
