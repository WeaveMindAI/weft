# Putting it on a URL

So far the program runs when you ask it to. A **trigger** node makes it run
when the outside world asks instead.

Replace `main.weft` with:

```weft
api = ApiEndpoint { path: "hello" }
reply = Reply

reply.started = api.started
```

Every HTTP request that hits that path fires a fresh execution.

`Reply` does not exist yet, so you are about to write it. A node is a folder
under `nodes/` with two files in it.

```bash
mkdir -p nodes/reply
```

## The declaration

`nodes/reply/metadata.json` says what the node looks like from outside.

```json
{
  "type": "Reply",
  "label": "Reply",
  "description": "Echo the caller's request body back as JSON.",
  "tags": ["live", "http"],
  "icon": "Send",
  "color": "#06b6d4",
  "inputs": [
    { "name": "started", "type": "Boolean", "required": false,
      "description": "Kick from the ApiEndpoint trigger." }
  ],
  "outputs": [
    { "name": "done", "type": "Boolean", "required": false,
      "description": "Fires true once the response has been sent." }
  ],
  "features": { "isOutputDefault": true },
  "requires_infra": false
}
```

Ports, types, and enough presentation for the editor to draw the box. This is
data, not code, which is why the compiler can read a node's shape without
compiling a single line of its Rust.

## The code

`nodes/reply/mod.rs`:

```rust
use async_trait::async_trait;
use serde_json::{json, Value};

use weft::caller::{InboundMessage, OutboundChunk};
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ReplyNode;

#[async_trait]
impl Node for ReplyNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let http = ctx.http_caller().await?;
        let req = http.request_parts()?;
        let echoed = match &req.body {
            InboundMessage::Json(v) => v.clone(),
            InboundMessage::Text(s) => Value::String(s.clone()),
            InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
        };
        http.respond(OutboundChunk::Json(json!({ "you_sent": echoed }))).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
```

The only parsing in there is reading the body you were handed.

`ctx.http_caller()` hands you a live handle to whoever is waiting on the other
end of the socket. The trigger declared the endpoint; the runtime holds the
connection open and routes it to the worker running this execution.

`#[derive(NodeManifest)]` is what connects the two files. It reads the
`metadata.json` sitting next to this source at compile time and embeds it. A
missing or malformed file is a compile error, so the declaration and the code
cannot drift apart.

## Turn it on

```bash
weft activate
```

`activate` compiles the project, registers it, and prints the live URL. A
project with triggers has to be activated; one without them just runs.

Then call it from anywhere:

```bash
curl -X POST "<the URL activate printed>" \
     -H "content-type: application/json" \
     -d '{"message":"hi"}'
```

```json
{"you_sent":{"message":"hi"}}
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

Activating the project told the runtime: when a request arrives at this path,
start an execution of this program and hand the held connection to whichever
worker picks it up. Nothing in your program is listening.

So the endpoint exists whether or not any worker is running. When a request
arrives cold the runtime starts one, which is why the first request after an
idle period is slower. Workers shut themselves down after thirty seconds with
nothing to do.

Next: [putting a person in the loop](a-person-in-the-loop.md).
