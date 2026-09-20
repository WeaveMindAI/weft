# Putting it on a URL

So far the program runs when you ask it to. A **trigger** node makes it run
when the outside world asks instead, and for an HTTP request that trigger is
`Route`.

Replace `src/main.weft` with:

```weft
hello = Route -> (name: String) { path: "hello", method: "POST" }
answer = Reply { status: 201 }
answer.body = hello.name
```

Every POST to `hello` fires a fresh execution. The body's `name` key comes out
on the port you declared after the arrow, and `Reply` sends it back as the
response, under a `201`. No Rust, no node folder.

Two things in that program are worth a closer look.

**The body keys are ports you declare.** A route has no `body` port. You say
which top-level keys you want (`-> (name: String, age: Number)`) and each one
arrives typed on its own port, the same way an LLM call with Parse JSON on
splits its reply. Anything you did not name is dropped, so name every key you
mean to read. The request itself is always there on the fixed ports: `method`, `path`, `params` (the `{name}` captures of the path),
`query`, `headers`, and `caller` (who the auth gate let in; null on an open
route).

A port named like a `{capture}` in the path reads that capture
(`Route -> (id: String) { path: "cards/{id}" }`). A port declared `Image`
takes a picture the body carries as a data URL or base64: the route stores it
and hands you the stored-file value, never the bytes.

**Reply is one message.** Behind a `Route` it is the response: `status`,
`headers`, the `body`, and the exchange ends. The body's shape follows the
route's `dataType`: any value on the default `json`, a `String` on `text`, a
stored file on `bytes`. A stored file inside a `json` body goes out as a link
the caller can fetch (`{ url, mimeType, filename, sizeBytes }`). Every answer
mints a fresh link and the old one dies within minutes, so what you keep
between runs is the file itself.

## Turn it on

```bash
weft activate
```

`activate` compiles the project and registers it, printing `activated <name> (<id>)`.
A project with triggers has to be activated; one without them just runs.

It prints no URL, and it does not need to: the address is fixed by the
install, so you can write it down before the program ever runs. Locally it is
`http://127.0.0.1:9999/connect/local/<your path>`. That address answers on
this machine only for now, so call it from here:

```bash
curl -X POST "http://127.0.0.1:9999/connect/local/hello" \
     -H "content-type: application/json" \
     -d '{"name":"ada"}'
```

```
"ada"
```

Each request is a full execution with its own color and its own row in the
editor's execution list. `weft follow <project>` streams them
live as they arrive.

![The executions list filling up as requests arrive](../img/executions-list.png)

<!-- IMAGE ------------------------------------------------------------------
file:  docs/src/img/executions-list.png
kind:  gif (about 8 seconds) or screenshot
brief: The VS Code sidebar's Weft executions tree, with several executions
       appearing one after another as curl requests land, each showing its
       colour id, its status (running then completed), and its timestamp.
       If a gif: fire four or five requests so rows appear in sequence and the
       running one flips to completed. Beside it, the graph view showing the
       most recent execution replayed.
--------------------------------------------------------------------------- -->

## What just happened underneath

Activating the project told the runtime: when a request arrives at this path
with this method, start an execution of this program and hand the held
connection to whichever worker picks it up. Nothing in your program is
listening.

So the endpoint exists whether or not any worker is running. When a request
arrives cold the runtime starts one, which is why the first request after an
idle period is slower. Workers shut themselves down after thirty seconds with
nothing to do.

The response head is held until your program's first outbound item: a `Reply`
sets the status, a `Stream` starts a body, a `Close` ends it bare. A program
that never does any of those holds the caller while it runs, and when the run
ends the caller gets a `500` whose body says `the run ended without
answering`. That is the program's bug, and the `500` is how you find out.

The rest of the shapes an API takes (a route that answers 404, a stream of
server-sent events, a WebSocket conversation, a route behind an API key) are
in [Building an API](../language/building-an-api.md).

That local address is your own install's. The `--public-url` tunnel, the one
provider events come in through, is a different and filtered surface: it does
not publish every `Route`. For how the two differ, read
[A public address](../connections/public-address.md).

Next: [putting a person in the loop](a-person-in-the-loop.md).
