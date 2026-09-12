# Your first node

Let's give the graph a word counter: text in, the number of words out, so
`"hello from weft"` gives you `3`. Words are split on whitespace, so
punctuation stays stuck to whatever it is next to.

Do this in a practice project whose `main.weft` you can throw away. If you
need one, [Your first program](../start/first-program.md) makes it.

## Declare what the node takes and produces

Create `nodes/word_count/metadata.json`:

```json
{
  "type": "WordCount",
  "label": "Word count",
  "description": "Count whitespace-separated words in text.",
  "inputs": [
    {
      "name": "text",
      "type": "String",
      "required": true,
      "label": "Text",
      "widget": { "kind": "textarea" }
    }
  ],
  "outputs": [
    {
      "name": "count",
      "type": "Number",
      "description": "The number of whitespace-separated words."
    }
  ]
}
```

`WordCount` is what you will type in the graph, and it has to be unique across
the whole catalog, standard nodes included.

The inputs and outputs are what let the compiler check your wiring. The
description is for whoever reads it next, human or assistant, and it is worth
saying what the number actually means.

## Write the implementation

Create `nodes/word_count/mod.rs` beside the JSON:

```rust
//! Count words separated by whitespace; punctuation does not split a word.

use async_trait::async_trait;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct WordCountNode;

#[async_trait]
impl Node for WordCountNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        let count = text.split_whitespace().count();
        ctx.pulse_downstream(NodeOutput::new().set("count", count)).await
    }
}
```

`NodeManifest` pulls in the metadata sitting next to it at compile time.
`ctx.inputs.get` gets you the text whether the graph wrote it in or wired it
from somewhere else, and if it is missing or is not a string you get an error
naming that input.

The last line is what actually sends the number. Just returning `Ok(())` would
end the body having emitted nothing. And nothing here checks the counting: a
node that always emitted `42` would satisfy every declaration on the page,
which is what tests are for.

## Put it in a graph

Replace the contents of your practice project's `main.weft` with:

```weft
count = WordCount { text: "hello from weft" }
show = Debug { data: count.count }
```

Check it, then run it:

```bash
weft validate --file main.weft < main.weft
weft run
```

`validate` checks the wiring without compiling any Rust, and should print
`{"diagnostics":[]}`. `run` builds the node into the program, so it needs your
runtime up. In the graph, `count.count` and the Debug result should both be
`3`.

A duplicate node type means the name is taken, so pick another. If the Rust
build fails, the diagnostic points into `mod.rs`: a graph that validates says
nothing about whether the Rust compiles.

## Give it a test

Next, [test it](testing.md), which runs the body against a few inputs on your
own machine without going near the runtime.

For extra crates or shared helper code, read [Packaging](packaging.md). For
more input controls and declaration options, read
[metadata.json](metadata.md).
