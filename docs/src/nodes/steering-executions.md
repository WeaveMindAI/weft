# Stopping other runs

A run can put a label on itself, and any run of the same project can stop
every run carrying that label. You reach for it when a second message
should cancel the answer to the first.

## The shape you will want first

Somebody sends your assistant three messages in a row. Each message starts
a run, and each run takes a few seconds to answer, because it waits on the
language model. Without help, the person gets three answers.

If you are writing weft, two catalog nodes at the top of the program fix
it, and you never touch Rust:

```weft
telegram = TelegramAccess

ask = TelegramReceiveMessage { account: telegram.access }

claim = TagRun { sender: ask.chatId }

stop = StopTagged {
  _should_flow: claim.done
  sender: ask.chatId
}

draft = LlmInference {
  _should_flow: stop.done
  ...
}
```

Every input you wire onto `TagRun` is a tag; `StopTagged` reads its
targets the same way, and `includeSelf: true` on it takes the current run
down as well. The two `_should_flow` wires are the order: tag, then stop,
then the work.

If you are writing a node, the same two moves are two ctx calls:

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let sender: String = ctx.inputs.get("sender")?;
    ctx.tag_execution([sender.as_str()]).await?;
    ctx.stop_tagged(sender.as_str(), StopSelf::Keep).await?;
    ctx.pulse_downstream(NodeOutput::new().set("sender", sender)).await
}
```

Put that node first in the chain. Every run tags itself with the sender,
then stops every earlier run carrying the same sender, and keeps going. The
third message kills the second, which had already killed the first, and the
third run is the only one that answers. No queue and no state of your own
to write.

## `tag_execution`

```rust
ctx.tag_execution(["user_7", "batch_a"]).await?;
```

Adds labels to the run this node is part of. Any node can call it, at any
point, as often as it likes; tags add up, and tagging the same thing twice
changes nothing. A tag is one to sixty-four characters of `[A-Za-z0-9_-]`,
the same rule a node's `_tags` follows; anything else fails here, naming
the character, before anything is written.

The tags show on the run: in the inspector's footer, in the Executions
view, and in `weft executions`.

## `stop_tagged`

```rust
ctx.stop_tagged("user_7", StopSelf::Keep).await?;
ctx.stop_tagged("exp_3", StopSelf::Include).await?;
```

Stops every live run of this project carrying the tag. `StopSelf` says
whether this run is one of them:

- `Keep`: stop the others, keep running. This is the opening example: the
  newest message survives, the older ones die.
- `Include`: stop them all, this one too. One run of an experiment finds
  the experiment is broken and takes the whole batch down; its own body
  ends cancelled at its next await, exactly as `weft stop` would end it.

A stop reaches a run whatever it is doing:

- **Running.** The node in flight is told to stop the same way
  [cancellation](cancellation.md) always works: an HTTP call in flight is
  dropped, a node waiting on the flag wakes with the cancel.
- **Parked** on a person, a webhook, or a timer, with no worker alive. The
  thing that would have woken it is erased: the form is gone, the timer is
  gone. Answering the old form does nothing.
- **Waking up** at that exact moment. The wake finds the run already dead
  and does nothing with it.

The call returns as soon as the stop is durably queued; the runtime carries
it out. Do not write the next node to depend on the siblings being gone by
the time it fires.

A stop never crosses a project: the tag is only looked up among your
project's own runs.

## Two runs at once

Two messages from the same sender land a few milliseconds apart, and both
runs say "stop the others, keep me". Left alone, they would kill each other.

They do not, because of one rule: a run only stops runs that tagged
themselves **before** it did. The runtime numbers every tag in the order it
was written, and a `Keep` stop reaches only the numbers below the caller's
own. So the later of the two survives and the earlier one dies. A run that
asks for a tag it never put on itself (a supervisor clearing a sender's
whole backlog, say) has no position of its own to compare against, so it
reaches everything tagged so far.

`Include` has no ordering: every live run carrying the tag goes, whenever it
tagged itself.

## What a stopped run looks like

A stopped run ends with `execution_cancelled`, and the event says who did
it: the run that asked and the tag that matched. In `weft events <color>`
that is the `reason=` on the last line:

```
[1725370001] execution_cancelled   reason=Stopped by execution 9d3f8f4e-... (tag user_7)
```

Every node that was still running or waiting gets a `node_cancelled` with
the same reason, and the graph prints it on the node in place of the usual
"Cancelled by user".

## After a crash

A body that re-runs after a worker crash re-tags and re-asks. Both are safe:
a repeated tag lands on the same row it landed on the first time, so the
run's place in the order does not move, and a repeated stop finds its
earlier targets already ended and stops nothing new. Neither call needs the
`ctx.run` wrapper that [Surviving a restart](durable-execution.md) puts
around work that must not happen twice.
