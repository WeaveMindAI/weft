# Your first node

The smallest node in the standard library, near enough in full.

## `metadata.json`

```json
{
  "type": "Text",
  "label": "Text",
  "description": "Emit a literal string configured at design time.",
  "tags": ["basic"],
  "icon": "Type",
  "color": "#64748b",
  "inputs": [
    { "name": "value", "type": "String", "required": true,
      "label": "Value", "description": "The string to emit." }
  ],
  "outputs": [
    { "name": "value", "type": "String",
      "description": "The configured string." }
  ]
}
```

## `mod.rs`

```rust
//! Text: emit a literal string configured at design time.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.inputs.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

That is a working node. Drop that folder under `nodes/` and `Text` is
available to every program in the project.

## The four things to notice

**Everything imports from `weft`.** One crate name, one place to look. It is
the only author-facing name.

**`#[derive(NodeManifest)]` reads the JSON.** At compile time it finds the
`metadata.json` sitting next to this source file and embeds it. The node's
type name comes from the JSON's `type` field. You never write `node_type` or
build a metadata struct by hand, and a missing or malformed JSON is a compile
error, so the two files cannot drift.

**Reads go through `ctx.inputs`.** One bag, one accessor. It does not matter
whether the value arrived on a wire, as a literal in the braces, or from the
input's declared default; the node reads it the same way.

**`pulse_downstream` is the only way out.** Returning a value does nothing.
Emitting is an explicit call, because a node may emit on several ports, may
emit repeatedly on a stream port, and may deliberately emit on none.

## Naming

Two conventions the codebase holds to everywhere:

- The `type` in metadata is PascalCase: `Text`, `SlackSendMessage`.
- The struct is that plus `Node`: `TextNode`, `SlackSendMessageNode`.
- The folder is snake_case: `text/`, `send_message/`.
- Ports are camelCase: `threadTs`, `postAt`, `scheduledId`.

## A node that actually does something

Here is the shape almost every real node has. It reads a connection, calls a
service, and emits what came back.

```rust
//! Post a message to a Slack channel and emit its timestamp.
//!
//! Emitting the permalink is best-effort: a failure to read it back is
//! logged and the node still succeeds, because failing here would invite
//! a retry that double-posts.

use async_trait::async_trait;

use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct SlackSendMessageNode;

#[async_trait]
impl Node for SlackSendMessageNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let text: String = ctx.inputs.get("text")?;

        let slack = ctx.client(&account).await?;

        let posted = slack
            .post("https://slack.com/api/chat.postMessage")
            .json(&serde_json::json!({ "channel": channel, "text": text }))
            .send()
            .await
            .node_err("posting the message")?
            .json::<serde_json::Value>()
            .await
            .node_err("decoding Slack's reply")?;

        let ts = posted["ts"].as_str()
            .ok_or_else(|| weft::node_error("Slack accepted the post but returned no timestamp"))?;

        ctx.pulse_downstream(
            NodeOutput::new()
                .set("ts", ts)
                .set("channel", channel),
        ).await
    }
}
```

Everything in it is this node's own business. The token, the refresh, the
signing, the measurement and whose account is paying all happen below
`ctx.client(&account)`, where this code cannot see them.

## The header comment

Look at the `//!` block above, and write yours the same way.

A file header states the module's **responsibility in prose** and records the
**why** behind anything non-obvious. Not a summary of what the code does: what
you cannot read from the code is why the permalink failure is swallowed, and
that is what the comment is for.

## Adding dependencies

`deps.toml` next to `mod.rs`:

```toml
[dependencies]
reqwest = { version = "0.12", features = ["json"] }
```

`weft`, `tokio`, `serde`, `serde_json`, `async-trait`, `anyhow`, `tracing` and
`uuid` are there already, so a `deps.toml` only names what those do not cover.
More about it, including how to pull in an OS package:
[Packaging](packaging.md#dependencies).

Next: [metadata.json](metadata.md) for the full declared surface, or
[the ctx](the-ctx.md) for everything a running node can reach.
