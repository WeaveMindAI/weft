# Reading inputs, emitting outputs

Read incoming values from `ctx.inputs` and send results with
`ctx.pulse_downstream`. The body uses the same input API whether a value
came from a wire, a literal or a metadata default.

```rust
let text: String = ctx.inputs.get("text")?;
ctx.pulse_downstream(NodeOutput::new().set("value", text)).await?;
```

## One bag

`ctx.inputs` is a `ValueBag`: named JSON values with typed accessors.
A trigger's event payload lives in a separate bag, `ctx.wake`, with the
same read methods. For the event's shape, read
[Writing a trigger](writing-triggers.md).

A supplied value takes precedence over the metadata default. A wire that
closes without a value does not get replaced by that default.

## The accessors

```rust
let name: String = ctx.inputs.get("name")?;
let alias: Option<String> = ctx.inputs.opt("alias")?;
let limit: u32 = ctx.inputs.get_or("limit", 50)?;
let raw = ctx.inputs.raw("payload");
```

| Method | Missing or null | Wrong type |
|---|---|---|
| `get::<T>(name)` | Missing is an error; null must deserialize into `T` | Error naming the input |
| `opt::<T>(name)` | `None` | Error |
| `get_or(name, default)` | The supplied default | Error |
| `raw(name)` | Missing is `None`; present null stays a JSON value | No conversion |

Use `get_or` for a fallback. `get(...).unwrap_or(...)` would also hide a
type error, making a broken input look like an omitted setting.

For a required value you want to inspect as JSON, use
`ctx.inputs.get::<serde_json::Value>("payload")?`.

### One or several values

`ctx.inputs.list::<T>("attachments")?` accepts an array or a single
value. An absent or null input becomes an empty list. Every item still has
to deserialize into `T`; a bad item fails the read.

### Reading a nested object

```rust
let options = ctx.inputs.nested("options")?;
let model: String = options.get_or("model", "default-model".into())?;
```

An object becomes another bag. An absent or null value becomes an empty
bag; a present value that is not an object produces an error.

This is how a node can read an options object supplied by another node.
The framework passes the object through; the consuming node chooses which
fields it needs.

### Iterating

| Method | Values it visits |
|---|---|
| `iter()` | All named values |
| `declared()` | Inputs declared by the node type's metadata |
| `custom()` | Extra inputs on this instance, including config-derived ports |
| `in_order()?` | Present values in the node's port order |

For a script node, `custom()` can collect variables while leaving out the
metadata-declared `code` input.

`in_order()` follows port order, not arrival time. A created port's place
comes from where the author wrote it. Closed inputs are absent.
The method errors on a nested bag or wake bag because those have no port
list.

### The whole bag at once

```rust
let object = ctx.inputs.object()?;
```

`object()` borrows the JSON object; `record()` returns an owned JSON
value. The inputs bag always has an object. On `ctx.wake`, these methods
fail if no event object was delivered, so a missing event cannot pass as
an empty record.

### Files

A file input can be read as `FileHandle`:

```rust
let file = ctx.inputs.get::<weft::storage::FileHandle>("image")?;
```

For fetching its bytes or sending it to a provider, read [Storage](storage.md).

## Emitting

Build a `NodeOutput` with one entry per output you want to send:

```rust
ctx.pulse_downstream(
    NodeOutput::new()
        .set("count", count)
        .set("text", text),
).await?;
```

Each `.set` takes a value convertible to JSON. The example assumes the
node declares both output ports. A second `.set` for the same name in
this object replaces the first value.

An ordinary output can emit at most once per firing. A `Generator[T]`
output can emit repeatedly, with each emission supplying another item.

If you already have a JSON object,
`NodeOutput::new().extend_from_object(&value)` copies its keys to output
names. Use `ctx.fan_declared(&value)` to keep only keys declared as
outputs; provider response fields you do not expose are then ignored.

### Explicit closure

You can emit `count` now and `text` later. Omitting `text` from the
first emission leaves it open. When the body finishes, the runtime closes
ordinary outputs it never emitted and ends any remaining generator outputs.

To close one early:

```rust
ctx.close_port("text").await?;
```

An ordinary port cannot emit after closure. On a generator, closure ends
the sequence after the items already emitted.

An ordinary required input receiving closure causes its node to skip;
an optional input can remain absent. For the complete readiness rules,
including generators, read
[The closed pulse](../language/mental-model.md#how-a-branch-stops-the-steps-after-it).

### Waiting for the value to be taken

```rust
ctx.yield_downstream(output).await?;
```

For an ordinary output, this waits until the consumer is dispatched.
For a generator item, it waits until the consumer pulls that item.
It does not wait for the consumer to finish its work.

If delivery becomes impossible, for example because the consumer skipped
or finished without taking the value, the call fails. An unwired output
has no consumer to wait for.

For buffering and complete generator examples, read
[Streams and buses in Rust](streams-and-buses.md).

## Errors

Import `weft::NodeErrExt` to add an explanation to another library's
failure:

```rust
let body = response.json::<serde_json::Value>()
    .await
    .node_err("decoding the response")?;
```

On a `Result`, `.node_err("decoding the response")` prefixes the
underlying error with that context. On an `Option`, `None` becomes an
error containing the supplied message.

For a condition your node detects itself:

```rust
if destinations.is_empty() {
    weft::node_bail!("Choose a destination before sending the message.");
}
```

For an expression such as `ok_or_else`, use
`weft::node_error("The response has no message ID")`.

A failed body closes ordinary outputs that have not already emitted.
Their optional consumers can handle the missing result. A generator output
that is still open ends with the producer's failure. Its consumer receives
an error after any buffered items, rather than a successful end.
