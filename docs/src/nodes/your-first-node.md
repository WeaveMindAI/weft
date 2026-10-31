# Your first node

Here is `Text`, one of the smallest nodes in the catalog, with the lines that
hook up its tests left out. `Text` takes a string you typed and sends it on.

`catalog/basic/text/metadata.json`:

```json
{
  "type": "Text",
  "label": "Text",
  "description": "Emit a literal string (a prompt, a label, a config value).",
  "tags": ["literal", "string"],
  "icon": "Type",
  "color": "#64748b",
  "inputs": [
    { "name": "value", "type": "String", "widget": { "kind": "textarea" },
      "required": true, "label": "Value" }
  ],
  "outputs": [ { "name": "value", "type": "String" } ],
  "features": { "pure": true }
}
```

`catalog/basic/text/mod.rs`:

```rust
//! Text: emit a literal string configured at design time.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let value: String = ctx.inputs.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

Read an input, send an output. That is the shape of every node.

## Write one

Make a folder anywhere under your project's `nodes/` (except
`nodes/base_catalog/`), or anywhere under `src/`, for example next to the
`.weft` file that uses it. `nodes/base_catalog/` holds the standard library, and it is a
folder of nodes like any other: weft loads and builds it the same way, and
you can delete it if you do not use it. The one thing particular to it is
that `weft catalog update` replaces it with the installed weft's copy, so
anything of yours in there would be lost.

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

use weft::{RunContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct WordCountNode;

#[async_trait]
impl Node for WordCountNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

`weft run` compiles your node into the project's worker program, on your
machine with your own Rust, and runs it. If you have not installed Rust yet,
go and read the [install page](../start/install.md).

## Five things that would have bitten you

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
saying where they are. If a big answer is not a file, go and read
[emitting](values-and-emission.md#emitting) for how to send less of it.

**If your code does heavy work, hand it to `spawn_blocking`.** Your body shares
a few threads, one per CPU, with the calls arriving at the worker and with its
other runs, and a thread only changes hands at an `.await`. Parsing a big
document, resizing an image or hashing a large file in one go keeps the thread
until it is done, and everything waiting for that thread waits with it. On a
worker with one CPU, that is everything. The worker keeps its hold on your run
from a separate thread, so a slow body never loses the run; it only slows the
rest down.

```rust
let rows = tokio::task::spawn_blocking(move || parse_csv(&text))
    .await
    .node_err("parsing the export")?;
```

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

Those two, plus `?` on anything the ctx hands back, cover almost every
failure. The exception is a mistake in the program that your code spots, like
SQL that does not parse: return `Err(WeftError::Input(..))` for that. For why,
and for which failures a program can catch on `_error` (an output weft gives
every node), go and read [which failures a program can
handle](values-and-emission.md#which-failures-a-program-can-handle).

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
