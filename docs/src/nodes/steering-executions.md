# Stopping other runs

If a new message makes work on the previous message obsolete, tag the runs
with the same conversation ID. A later run can then ask weft to stop the
earlier ones.

## Tag, stop, then start the work

Use `TagRun` followed by `StopTagged`. This small program shows the order:

```weft
sender = Text { value: "user_7" }

claim = TagRun { sender: sender.value }

stop = StopTagged {
  _should_flow: claim.done
  sender: sender.value
}

work = Debug {
  _should_flow: stop.done
  data: sender.value
}
```

For a message handler, wire the conversation ID from your trigger in place
of `sender.value`, and put your answer-generating work after `stop.done`.
The `_should_flow` connections make tagging finish before the stop is
requested, then allow the new work to begin.

A stop is queued asynchronously. `stop.done` means the request was queued,
not that every older run has finished stopping. An older run may already
have sent an answer. Use this to cancel obsolete work; it does not by
itself guarantee that only one answer reaches the person.

The ordering is based on when runs register their tags. If handling two
messages happens out of arrival order, the later tag registration belongs
to the surviving run. The message's timestamp is not used.

## Calling from a node

The same operations are available inside a node body:

```rust
use weft::StopSelf;

let sender: String = ctx.inputs.get("sender")?;
ctx.tag_execution([sender.as_str()]).await?;
ctx.stop_tagged(sender.as_str(), StopSelf::Keep).await?;
```

This fragment assumes a `sender` string input containing a valid tag.
Tags belong to the current project; a stop cannot reach another project's
runs.

## `tag_execution`

```rust
ctx.tag_execution(["user_7", "batch_a"]).await?;
```

Tags accumulate on the execution. Adding the same tag again changes
neither its value nor the run's place in the tag order.

The Rust API accepts tags of 1 to 64 ASCII letters, digits, underscores,
or hyphens. Other values return an error before tagging. The catalog
nodes additionally accept nonempty strings that need conversion:
they replace unsupported characters, shorten the readable part if needed,
and append 16 hexadecimal characters from a hash of the original value.

Both catalog nodes perform the same conversion, so the same input produces
the same tag. The fingerprint reduces collisions between converted values;
it is not a uniqueness guarantee. If you call the Rust API directly,
supply valid tags yourself.

Tags appear in the execution inspector and in `weft executions`.

## `stop_tagged`

```rust
ctx.stop_tagged("user_7", StopSelf::Keep).await?;
ctx.stop_tagged("batch_a", StopSelf::Include).await?;
```

| Choice | Runs selected |
|---|---|
| `Keep`, when this run carries the tag | Live runs that registered that tag before this run |
| `Keep`, when this run does not carry the tag | Matching live runs tagged before this stop request |
| `Include` | All matching live runs when the dispatcher handles the stop, including this one |

For the catalog node, `includeSelf: true` selects `Include`.
The default is `Keep`.

The registration order prevents two concurrent tagged runs using `Keep`
from selecting each other. The later one can stop the earlier one;
the earlier one cannot stop the later one.

A selected run is cancelled through the same mechanism as
[ordinary cancellation](cancellation.md). For a suspended run, the runtime
also removes its wake registrations.

## What a stopped run looks like

The journal records `execution_cancelled` with the requesting execution
and matching tag as the cause. The inspector displays that reason, and
`weft events <color>` shows it in the execution's events.

## After a crash

Tagging again keeps the execution's original position. For a tagged run
using `Keep`, repeating the stop therefore keeps the same cutoff and
cannot reach runs tagged after it.

Other forms can select new targets when called again. An untagged run
using `Keep` gets a new cutoff at each call. `Include` has no cutoff,
so a later call can stop matching runs that appeared in the meantime.
Account for that if the node body can replay.

For saved results and replayed work, read
[Surviving a restart](durable-execution.md).
