# Reading inputs, emitting outputs

## One bag

A node reads its named values from `ctx.inputs`. However the value got there,
a wire, a literal in the braces, a statement literal, or the input's declared
default, it is read the same way. When several sources could supply one, a wire
or a literal wins over the declared default.

A trigger's fire payload is a **separate** bag, `ctx.wake`, with the same
accessors. See [Writing a trigger](writing-triggers.md).

## The accessors

```rust
let name: String = ctx.inputs.get("name")?;              // required, typed
let alias: Option<String> = ctx.inputs.opt("alias")?;    // absent or null -> None
let limit: u32 = ctx.inputs.get_or("limit", 50)?;        // absent -> default
let raw = ctx.inputs.raw("payload");                     // Option<&Value>
```

`get` fails loudly when the value is absent or the wrong type, and the error
names the input.

`opt` answers `None` for absent or null, but a **present, wrong-typed** value
still errors, because "you did not give me one" and "you gave me a number where
I need a string" are different situations.

If the input has a sensible fallback, reach for `get_or`. Never write
`.get(..).unwrap_or(..)`, which swallows a real type error into a
silently-wrong default.

`.raw(name)` gives the optional raw JSON for pass-through reads. A **required**
raw read is `.get::<Value>("name")?`.

### Reading a nested object

```rust
let cfg = ctx.inputs.nested("config")?;
let model: String = cfg.get_or("model", "default-model".into())?;
```

An object-valued input becomes its own bag with the same accessors. Absent
means an empty bag with every knob at its default; a present non-object value
errors loudly.

That is the **config-node pattern**, and the engine does nothing special for
it: the config node emits one plain object, the consuming node declares an
ordinary object-typed input (usually `"accepts": ["wire"]`, so a real node
must be wired), and reads that object itself. No input name triggers hidden
behavior, and an object wired to an input always arrives as that object.

### Iterating

Four projections, for nodes that loop over values without knowing their names
in advance:

| Call | Yields |
|---|---|
| `.iter()` | every named value |
| `.declared()` | only the node type's own metadata-declared inputs |
| `.custom()` | only this instance's extras: created ports, config-derived ports |
| `.in_order()` | every value that ARRIVED, in the node's port order |

`.custom()` is the one for nodes that treat "whatever the user wired in" as a
dynamic set: script variables, a query's parameters, a template's holes, form
prefill. It pairs with `canAddInputPorts` in the metadata, and it is the shape
for any open-ended set of values: a node never takes a `List` the author has
to assemble from wires, because a list literal cannot hold a wire and the
author ends up writing a Python node just to build it.

`.in_order()` is for a node that answers by ORDER. A port that delivered
nothing is absent, so the first pair is the first branch that spoke, which is
the whole of what `FirstInOrder` does. For a created port, that order is the
order the author wrote it in.

### The whole bag at once

```rust
let obj = ctx.inputs.object()?;
```

For a node that consumes or forwards the bag as a record. On `ctx.inputs` this
always answers. On `ctx.wake` it fails loudly when the fire delivered no keyed
record, so a broken delivery can never pass as an empty one.

### Files

```rust
let handle = ctx.inputs.get::<FileHandle>("image")?;
```

Reading a file value parses the handle, failing loudly when there is nothing
readable, and the storage verbs take that handle directly.

## Emitting

```rust
ctx.pulse_downstream(
    NodeOutput::new()
        .set("ts", ts)
        .set("channel", channel),
).await
```

`.set` takes anything that converts to JSON, and an already-built `Value`
passes through untouched. Chain it for more ports.

`.extend_from_object(&json)` fans a JSON object's keys onto same-named ports,
and `ctx.fan_declared(&value)` does the same restricted to ports the node
declares.

**A port not present in the output emits no pulse**, which closes it, which
skips everything downstream. That is how you express "there was no result". See
[the closure rule](../language/mental-model.md#the-closed-pulse).

A `Generator[T]` output accepts repeated emissions, each one an item of the
stream. Every other port takes at most one emission per firing, and a second
is refused.

### Explicit closure

```rust
ctx.close_port("value").await?;
```

Says "nothing will arrive here" without emitting. On a `Generator` output it
is the early end-of-stream verb, legal after any number of yields.

### Waiting for the value to be taken

```rust
ctx.yield_downstream(output).await?;
```

Same emission, but it does not return until the value was **taken**: the
consumer dispatched, or the stream item pulled.

On a stream port it is the lock-step yield. On an ordinary port it is a real
synchronization point: "do not continue until the next stage started". A
phone-call node that must not proceed until the answering node is live wants
exactly this.

It fails loudly when the delivery can never happen, because the consumer
skipped or finished without taking the value, rather than waiting forever.

## Errors

```rust
// wrap any non-weft Result or Option
let body = resp.json::<Value>().await.node_err("decoding the reply")?;

// a condition you detected yourself
weft::node_bail!("pick ONE destination: a channel or a user, not both");

// the expression form, for closures that build a message first
.ok_or_else(|| weft::node_error(format!("no id in {body}")))?
```

On a `Result`, `.node_err("doing X")` produces a node failure reading
`doing X: <the underlying error>`. On an `Option`, `None` becomes a failure
carrying the message verbatim.

Node code never names a `WeftError` variant, because the variants are the
runtime's vocabulary and a node's failure is always the same kind of thing:
this node could not do its job, here is why.

A failed node closes its outputs, so a failure propagates exactly like an
absent value, and a downstream node with an optional input is the recovery
path.

## Showing a result on the node

A node whose firing produces or receives a file worth looking at can have the
editor render it inline on the node body, by declaring a `display` block:
[metadata.json](metadata.md#display).
