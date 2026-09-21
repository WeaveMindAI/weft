# Showing something in the graph

Every value on every wire is already in the inspector. You never hand-deliver
information to whoever is watching.

Two things go further, and they share a word, so keep them apart.

## A file, rendered on the node

Name one port in your metadata and the editor renders its value on the box
after each firing.

```json
"display": { "kind": "media", "output": "image" }
```

| `kind` | What you get |
|---|---|
| `media` | The file rendered by its own type: a picture inline, a real player for audio and video, and the file card for anything that will not play |
| `link` | Always the file card, with its details and a download button |

Exactly one of `input` and `output`, naming the port. The value has to be a
file.

Use `link` for something somebody downloads and `media` for something they
look at or listen to.

## A live panel

This is the other one, and it is a different thing: a panel that updates while
something runs, rather than a value from a firing.

Two kinds of thing produce one, and only one of them is your job.

**An infrastructure node's container.** Your container serves `/live`, and you
opt in by naming the endpoint:

```json
"features": { "liveEndpoint": "api" }
```

Declaring it is the opt-in. There is no separate flag. The dispatcher proxies
the panel to your endpoint, and a node that names none answers 404.

**A trigger's signal kind.** Written by weft, inside the listener, not by you.
A node that declares a trigger writes no display code at all, and those panels
are read-only: the listener refuses to serve one carrying a button rather than
show a button that cannot work.

### What a panel holds

A list of items, each with a label, and at most one action.

| `type` | `data` | Drawn as |
|---|---|---|
| `text` | a string | A box you can copy from |
| `image` | a string an `<img src>` takes, so a URL or a `data:` URI | The picture |
| `progress` | a number from 0 to 1 | A bar |
| `secret` | a string | Masked behind dots until somebody clicks the eye. Copy gives the real value either way |

An answer that is not a list of items comes back to the reader as an error
naming what you sent, rather than an empty panel. One item it cannot read
becomes a line saying so, in its place, and the rest of the panel still draws.

A number outside 0 to 1 on a progress bar is shown as given rather than
clamped, because clamping would hide the bug in whatever produced it.

### Who can see it

Nobody, by default. A signal token reads no display at all unless it was minted
with `--display <node>` naming that node, or `--displays` for all of them.

That is on purpose. A panel can be a credential: a bridge showing a QR code to
scan, a database showing the password it just minted. So the scope grants
nothing until you say which node.
