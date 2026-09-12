# An HTTP endpoint

A request can start a weft execution and receive its answer over the same
connection. Use `ApiEndpoint` to create the entry point, then let a node
read the request and reply.

This example echoes the request body as JSON. Ask Tangle to build it, or
follow the two-file node example below.

Replace `main.weft` with:

```weft
api = ApiEndpoint { path: "hello" }
reply = Reply { started: api.started }
```

`ApiEndpoint` is in the catalog. `Reply` is the node you will add now.

## Add the reply node

Create its directory inside the project:

```bash
mkdir -p nodes/reply
```

Put this declaration in `nodes/reply/metadata.json`:

```json
{
  "type": "Reply",
  "label": "Reply",
  "description": "Echo the caller's request body back as JSON.",
  "tags": ["live", "http"],
  "icon": "Send",
  "color": "#06b6d4",
  "inputs": [
    { "name": "started", "type": "Boolean", "required": true,
      "description": "Kick from the ApiEndpoint trigger." }
  ],
  "outputs": [
    { "name": "done", "type": "Boolean",
      "description": "Fires true once the response has been submitted." }
  ],
  "requires_infra": false
}
```

The declaration gives the compiler the node's inputs and outputs, and gives
the editor its label and appearance. Now put the implementation in
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

`ctx.http_caller()` gives this node the execution's HTTP connection.
`request_parts()` reads the request the runtime received; `respond()` sends
the answer. The `started` wire makes the reply node wait for the endpoint's
kick. The connection itself belongs to the execution and is reached through
the ctx.

`NodeManifest` reads the adjacent metadata at compile time. The node's Rust
code still has to implement what that declaration promises. For the full
node-authoring path, read [Your first node](../nodes/your-first-node.md).

## Activate and call it

Save the files and run:

```bash
weft activate
```

Activation builds the program and registers its trigger. Find `api` in the graph and copy the address shown on the node. From your local machine:

```bash
curl -X POST "<address from the api panel>" \
  -H "content-type: application/json" \
  -d '{"message":"hi"}'
```

The response is:

```json
{"you_sent":{"message":"hi"}}
```

Send another request and you get another execution. Open **weft → Executions**
to inspect either run.

![Two HTTP requests shown as separate executions in the sidebar](../img/executions-list.png)

<!-- IMAGE: executions-list.png. Show two completed calls to this hello
endpoint and the reply inspector containing you_sent. Brief in img/README.md. -->

## What stays up

The gateway accepts requests even when no worker is running. A request can
start a worker, so the first call after an idle period includes that startup.
An idle worker exits after thirty seconds without claimable work.

The HTTP connection is live state. If its worker dies, the runtime cannot
restore that connection. By default, a caller disconnect also cancels the
execution. For streaming responses, WebSocket conversations and work that may
outlive its caller, read [Talking to a live caller](../nodes/live-callers.md).

The local gateway address is for your local setup. The `--public-url` tunnel
used for provider events exposes a different, filtered surface; it does not
publish every `ApiEndpoint`. For how these addresses differ, read
[A public address](../connections/public-address.md).
