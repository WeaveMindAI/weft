# What your node shows in the graph

If a node makes an image, show the image. If it is waiting for someone to
scan a pairing code, show that code. People should be able to see the
result where they wired the node, without opening a log.

You can add a file preview or a JSON preview through metadata. An
infrastructure node can also supply a live panel.

## A file it made or received

To display the file on an `image` output, add this top-level metadata field:

```json
"display": { "kind": "media", "output": "image" }
```

Use `input` instead of `output` to show a file the node receives. The value
must be a weft file value carrying a storage key or a URL; a value containing
only inline data has no handle the preview can open.

For the available display modes, see [Display](metadata.md#display).

## Its last value

If you want a JSON preview, add:

```json
"features": { "showDebugPreview": true }
```

A node with declared outputs shows its latest output. A node with no
outputs shows its input. The preview has a copy button, so you can take
the value into a test or another tool.

In the full builder, a node with no value to show displays its execution
status instead, such as “Waiting for data...” or “Execution failed” with
the error. Collapsing the node hides its JSON and file previews. Its live
panel stays visible. The simplified view shows the previews beneath the
node's heading.

## A live feed from an infra node

An [infrastructure node](infrastructure.md) can report changing information,
such as a download's progress or a service's pairing code. Serve `/live`
on one of its declared HTTP endpoints and put that endpoint's name in
`features.liveEndpoint`. The editor polls it while the graph is open.

The route returns an object with an `items` array. For example:

```json
{
  "items": [
    { "type": "text", "label": "Status", "data": "Downloading the model" },
    { "type": "progress", "label": "Download", "data": 0.65 }
  ]
}
```

| Type | Data | What appears |
|---|---|---|
| `text` | String or number | Copyable text |
| `image` | Image URL or data URL | Image |
| `progress` | Number from 0 to 1 | Progress bar |
| `secret` | String or number | Masked text with reveal and copy buttons |

A secret's copy button copies the real value even when it is hidden.
The mask keeps it off the screen; it does not remove it from the response.

An image item with numeric data or a progress item with string data shows
an amber error. A progress item with a string, for example, displays
`Download (unrenderable: progress)`. Items with a missing or non-string
label, an unknown type, or data that is neither a string nor a number are
discarded. If an item includes an action, that action needs string
`label` and `actionKind` fields too.

For action buttons and the HTTP routes, read
[The routes your container serves](infrastructure.md#the-routes-your-container-serves).

## A trigger's address

A public-entry trigger gets a panel showing its path and authentication
details. You do not need to implement this panel in the node.

If it uses an API key, the panel shows the key masked, with a
**Regenerate** button. Regenerating invalidates the previous key, so update
your callers. After a listener restart, the old key's plaintext is no
longer available to display; the same button lets you create a replacement.

Internal signals such as timers and provider events have no public-entry
panel. They do not have an address for an outside caller to copy.
