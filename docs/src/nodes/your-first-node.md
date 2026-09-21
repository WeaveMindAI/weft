# Your first node

Here is the smallest node in the catalog, whole. `Text` takes a string you
typed and sends it on.

`catalog/basic/text/metadata.json`:

```json
{
  "type": "Text",
  "label": "Text",
  "description": "Emit a literal string. Useful for prompts, labels, and config values.",
  "tags": ["literal", "string"],
  "icon": "Type",
  "color": "#64748b",
  "inputs": [
    { "name": "value", "type": "String", "widget": { "kind": "textarea" },
      "required": true, "label": "Value" }
  ],
  "outputs": [ { "name": "value", "type": "String" } ],
  "requires_infra": false
}
```

`catalog/basic/text/mod.rs`:

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

Read an input, send an output. That is the shape of every node.

## Write one

Make a folder under your project's `nodes/`, anywhere except
`nodes/base_catalog/`, which gets replaced when you update weft.

```text
nodes/word_count/
  metadata.json
  mod.rs
```

`metadata.json`:

```json
{
  "type": "WordCount",
  "label": "Count words",
  "description": "Count the words in a piece of text.",
  "tags": ["text"],
  "inputs": [
    { "name": "text", "type": "String", "required": true, "label": "Text" }
  ],
  "outputs": [
    { "name": "count", "type": "Number" },
    { "name": "longest", "type": "String" }
  ]
}
```

`mod.rs`:

```rust
//! Count the words in a piece of text, and find the longest one.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct WordCountNode;

#[async_trait]
impl Node for WordCountNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;

        let words: Vec<&str> = text.split_whitespace().collect();
        let longest = words.iter().max_by_key(|w| w.len()).copied().unwrap_or("");

        ctx.pulse_downstream(
            NodeOutput::new()
                .set("count", words.len() as f64)
                .set("longest", longest)
        ).await
    }
}
```

Now use it. The type name from your metadata is the name you write:

```weft
words = WordCount { text: draft.answer }
show = Debug { data: words.count }
```

`weft run` compiles your node into the program and runs it.

## Four things that would have bitten you

**Do not check your inputs.** No asserting that `text` is really a string, no
unwrapping something you were already promised. Every wire was checked before
anything ran. A wrong value fails before it reaches your code.

**Do not use `.get(...).unwrap_or(...)`.** That turns a real type error into
your default and you never find out. `ctx.inputs.get_or("limit", 10)` is the
same shape and fails honestly.

**Emit each port at most once.** Several calls are fine if they touch different
ports, which is how you release a value early and a `done` flag at the end.
Touching one port twice is an error naming it. Anything you never mention is
closed for you when the body returns, and that closure is what tells everything
downstream to stop waiting.

**Keep values small.** A value over 100 KB fails the emit, naming the port.
Bytes go in [storage](storage.md), and what travels the wire is the marker
saying where they are.

## Failing well

```rust
let body = response.text().await.node_err("reading the reply")?;
```

`.node_err("...")` wraps somebody else's error with what you were doing. On an
`Option`, the message is what the reader sees when it was `None`, so say what
was missing.

```rust
weft::node_bail!("pick a destination: a channel or a user");
```

`node_bail!` is for something your own code worked out. Write the message as an
instruction, because it is going in front of whoever is building the program.

Your node never names a weft error type. Those two, plus `?` on anything the
ctx hands back, cover it.

## Then a test

```rust
#[cfg(feature = "node-tests")]
mod tests;
```

and in your `impl Node`:

```rust
#[cfg(feature = "node-tests")]
fn tests(&self) -> Vec<weft::NodeTest> {
    tests::tests()
}
```

`tests.rs`:

```rust
pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("counts_words", counts_words)]
}

async fn counts_words(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&WordCountNode, json!({ "text": "one two three" })).await.ok()?;
    assert_eq!(outcome.outputs["count"], json!(3.0));
    assert_eq!(outcome.outputs["longest"], json!("three"));
    Ok(())
}
```

```bash
weft test-node WordCount
```

For the tiers, the rig and what a live test costs, go and read
[testing a node](testing.md).
