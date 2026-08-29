# What your node shows in the graph

A node on the canvas is a box with its ports and its config, and it can also
carry a panel on its body. Four things can go in there. Three of them you ask
for in `metadata.json`; the fourth arrives on its own if your node is a
trigger.

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

## A live feed from an infra node

An [infra node](infrastructure.md) is a container you brought up, and it can
report what it is doing. Serve `/live`, name that endpoint in
`features.liveEndpoint`, and the editor polls it while the graph is open. For
what the endpoint must return, go and read
[the routes your container serves](infrastructure.md#the-routes-your-container-serves).

Four item types render. `text` is a copyable box, `image` takes anything an
`<img src>` accepts, `progress` takes a number from 0 to 1 and draws a bar, and
`secret` sits behind a `••••` mask until the user clicks the eye. Use `secret`
for anything that should not sit on somebody's screen, like an API key; the
copy button hands over the real value whether or not it is revealed.

An item whose type is known but whose data is the wrong shape, a `progress`
carrying a string, becomes an amber chip reading
`<its label> (unrenderable: progress)` rather than disappearing quietly.

An item is dropped before the panel sees it if it is missing its `label`, if
its `data` is neither a string nor a number, if its `type` is not one of those
four, or if it carries an `action` without a `label` and an `actionKind`.

If the feed cannot be reached, the items are replaced by the reason: "Infra not
running. Start it from the project's action bar.", or whatever the container
answered with.

## A trigger's address

There is nothing to opt into here. Any node with `features.isTrigger` gets a
panel showing what the runtime knows about its registration.

A trigger mounted on a public path shows that path, how a caller authenticates,
and the API key if one was minted, masked behind the same `••••`. After a
listener restart the plaintext is gone and the panel says so. One that fires
from inside the runtime, a timer say, has no address, and its panel carries a
single line, `Auth: public (no key)`, which means there is no key rather than
that anybody can reach it.

That panel is built by the editor from what the listener reports, not by your
node, because a trigger's address is minted at activation and lives in the
runtime. It is read-only: a panel button reaches your trigger kind's
`handle_action` in the listener, and no shipped kind implements one yet.

An unregistered trigger says so: "Trigger not registered. Activate the project
from the action bar."

## When there is no panel

Add a panel when the alternative is asking the user to go and read a log.
