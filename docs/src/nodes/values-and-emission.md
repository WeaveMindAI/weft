# Reading inputs, emitting outputs

What the methods are is on [the ctx](ctx.md). This page is what happens around
them: how a value gets into your bag, and what the runtime does with what you
send.

## Where an input's value comes from

Wired data and design-time settings live in one bag, under the port's name.
Three things can fill a port, in this order:

| Rank | Source |
|---|---|
| 1 | A value that arrived on a wire |
| 2 | A value written into the node's body, if nothing arrived |
| 3 | The `default` from the node's metadata, if the port is still empty |

Four rules sit around that.

**A closed wire kills the default.** If a wire closes without carrying a value
(weft calls that a closure: upstream finished without sending on that port, or
failed), the default does **not** fill in, because that would hide that
something did not happen.

**A delivered `null` is a value only where the type allows it.** On a port
whose type admits `Null`, `null` is data. Anywhere else, a `null` arriving on a
wire fails the run, unless the program marked that input optional (`x?: T`
where it uses your node): then it reads as nothing arrived, and the default
fills.

**A wrong type fails the run**, required or not, and the program cannot catch
it ([which failures a program can
handle](#which-failures-a-program-can-handle)). It is never dropped so the
default can take over.

**A number outside its `number` widget's `min` and `max` fails the run too**
([widget](metadata.md#widget)), and so does a fraction where the widget's
`step` is whole: a poll interval of `0` on a port with `"min": 1` fails rather
than becoming the port's default. So does a value that is not one of a
`select` widget's `options`, unless the widget sets `freeText`.

An input your metadata leaves optional can be empty when your body runs: nobody
wired it and it has no default, or the program wrote `?` on it and its wire
closed. Read such an
input with `opt` or `get_or` ([reading inputs](ctx.md#reading-inputs)). Whether
your node runs at all then is up to the program. For how a program decides, go
and read [optional and required
inputs](../language/syntax.md#optional-and-required-inputs).

Two ports get special handling before your code sees them. A connection input
arrives as a full `Access` value carrying the service, rather than the id the
editor stored. And the gate ports are stripped out entirely, so your node never
finds a `_should_flow` it did not declare.

A trigger is different again: on a real firing, its ports replay the values
that were frozen when the trigger was set up, and written literals fill
whatever is left.

## Emitting

`ctx.pulse_downstream` is the only way out.

**Once per port, per firing.** Emitting or closing the same port twice is an
error naming it:

```text
node 'reply' touched port 'text' twice in one firing. Each output port can be
emitted or closed AT MOST ONCE per firing
```

If your node emits, pauses on a wait, and its body runs again from the top
when the answer comes, the emit made again does not count as a second one: it
does nothing, and the first value stands. For why the body runs again, go and
read [surviving a restart](durable-execution.md).

**Several calls are fine if they touch different ports.** That is how you
release a bus marker early and a `done` flag at the end. If any port in one
call collides, nothing in that call is recorded, so a failed call is a clean
no-op rather than half an emission.

**A stream port is the exception.** A `Generator[T]` output takes as many
emissions as you like, one per item, until you close it. After the close,
nothing can follow.

**Anything you never mention is closed for you** when the body returns. That
closure is the signal everything downstream is waiting for. A node that decides
it has nothing to say does not need to do anything special: it just returns,
and the branch behind it skips.

**A node that fails closes whatever it had not already sent.** What it did send
stays sent. weft cannot unsend a message your node already put on Slack.

**An undeclared port is caught first**, before the once-only rule, so a typo
reads as the real problem:

```text
node 'reply' tried to emit on undeclared output port 'txet'. Declare it in
metadata.json's outputs list, or correct the port name in the node body.
```

**A value that does not fit its port's type fails the call**, naming the port,
the type and what did not fit. The type is the one in your metadata, after the program filled in any type
variable. If your metadata marks the output `narrowable`
([outputs](metadata.md#outputs)), it is the type the program declared on it,
like `-> (response: Verdict)`. If the type is an open record, extra keys are
dropped instead of failing the call ([records that take extra
keys](custom-types.md#records-that-take-extra-keys)). A refused call sends nothing and leaves the port open, so if your
code handles the `Err` instead of returning it with `?`, it can emit on that
port again with something else.

**A value over 100 KB fails the call**, naming the port. The weight is the
value written as JSON, checked on what you emit, so a node one hop later
cannot trim it for you. There are two ways out, and the refusal names both:
put the data in [storage](storage.md) and send the stored file, whose value
weighs a few hundred bytes whatever the file holds, or send less. Sending less
means sending one page of it along with where the next page starts, or cutting
it short and saying so in another output.

If you want to bound a value yourself, `weft::storage` measures it the way the
wire does, against `MAX_WIRE_VALUE_BYTES`:

| You want | Call |
|---|---|
| What a value weighs on a wire | `wire_weight(&value)` |
| The longest start of a text that weighs at most `budget`, never splitting a character | `cut_to_weight(text, budget)` |
| A text cut to `max_chars` characters, then to what one port carries, and whether anything was cut | `cap_text(text, max_chars)` |

`FetchPage` uses `cap_text` this way: a `maxChars` input, and a `truncated`
output that tells the program the page was cut.

## Waiting for the value to be taken

`pulse_downstream` sends and carries on. `yield_downstream` waits until the
value was actually taken, which is what you want when you are producing faster
than the consumer reads.

It fails rather than waiting forever when delivery becomes impossible, for
instance because the consumer skipped or the run finished. An unwired port
counts as delivered immediately.

## Building the output

```rust
NodeOutput::new()
    .set("count", words.len() as f64)
    .set("longest", longest)
```

`set` takes anything that turns into JSON. If you already have a `Value`, it
passes through rather than being wrapped again.

For a dynamic object whose keys should land on same-named ports,
`ctx.fan_declared(&value)` matches them against the ports your node actually
declares and skips the rest. That matters after a paid call: an extra key in a
provider's response should not fail your node when you already spent the money.

`NodeOutput::stored_file(stored)` fills the four ports a stored file travels
as: `file`, `filename`, `mimeType` and `sizeBytes`.

## Errors

| You want | Write |
|---|---|
| To wrap somebody else's error | `result.node_err("reading the reply")?` |
| To say what was missing when an `Option` was `None` | `option.node_err("the response had no body")?` |
| To fail on something you worked out | `weft::node_bail!("pick a channel or a user")` |
| The same, as an expression | `node_error(format!("..."))` |

Those, plus `?` on anything the ctx returns, cover every failure except a
mistake in the program itself, which you return as `Err(WeftError::Input(..))`
(see [which failures a program can handle](#which-failures-a-program-can-handle)).

Write the message as an instruction to whoever is building the program, because
that is who reads it. "pick a destination: a channel or a user" beats "invalid
configuration".

## Which failures a program can handle

Every node has an `_error` output, which weft adds when it loads the node. If
a program wires it, your node's failure comes out there and the node counts as
done; if nobody wired it, the failure fails the run. For what that looks like
from the program, go and read [when a node
fails](../language/how-a-program-runs.md#when-a-node-fails). Your body writes
no handling for it: it returns its errors with `?` as usual.

`_error` is weft's, so a body that emits on it or closes it fails. `error`,
without the underscore, is yours to use like any other name.

What your node does decide is which kind of failure it returns. A failure of
the work itself can be caught on `_error`: anything from `node_err` or
`node_bail!`, a value your node emits that does not fit its port (like a
model's answer in the wrong shape), and a wait that gave up (go and read [when
the run cannot pause](durable-execution.md#when-the-run-cannot-pause)). A
mistake in the program (a bad setting, or a value of the wrong type arriving
on an input) always fails the run, even when the program wired `_error`, because the fix is
in the program. If your node asks `ctx.output_type` about a port it does not declare,
that fails the run too, and `_error` never sees it.

So when your node spots a mistake in the program itself (SQL that does not parse or names a table that is not there, an
input value of the wrong shape, an empty field it needs), fail with
`Err(WeftError::Input(..))`. If a program could sensibly retry or route around
the failure while it runs, use `node_bail!` instead. If your node talks to a
server, decide the same way for each kind of refusal: the Postgres nodes fail
the run on a syntax error or a missing table, and put a broken constraint on
`_error`.

If your node produces a stream, its failure stops reaching `_error` once
something went out on a `Bus` or `Generator` output. So do the work that can
fail before you send the first piece. For a bus, `ctx.pending_bus` waits to
open it until you have the first piece (go and read [streams and buses in
Rust](streams-and-buses.md#hosting-a-bus)).

In a test, `rig.wire_output("_error")` wires `_error` the way a program would,
and `outcome.output("_error")` reads what came out. Without it the rig wires
nothing, so the same node fails loudly.
