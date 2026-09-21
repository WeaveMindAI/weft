# Cancelling and steering runs

## Nothing kills your node

A cancelled run trips a flag. Your node is what acts on it. If your body never
looks, nothing stops it, and the only sign is a run that never ends.

So anything that waits needs a way out:

```rust
let cancel = ctx.cancellation();

tokio::select! {
    result = do_the_long_thing() => result?,
    _ = cancel.cancelled() => return Ok(()),
}
```

For a loop, the cheap synchronous read:

```rust
for row in rows {
    if ctx.is_cancelled() { return Ok(()); }
    ...
}
```

That one is cheap enough to call every iteration.

The flag is sticky. Once it is set, every later check sees it, so there is no
race between the cancel arriving and your next look.

What needs an arm: every wait, every long call to somebody else, and every read
that could block for minutes.

## Stopping other runs

A run can tag itself, and a run can stop every run carrying a tag.

```rust
ctx.tag_execution(["chat_7"]).await?;
ctx.stop_tagged("chat_7", StopSelf::Keep).await?;
```

That is the debounce shape. Somebody sends three messages in a row; the third
run stops the first two and answers once.

| | What it does |
|---|---|
| `StopSelf::Keep` | Stop the others, keep running |
| `StopSelf::Include` | Stop everything carrying the tag, including this run |

It reaches running executions, ones parked on a signal or a timer, whose wake
is erased so they never come back, and ones whose wake is already on its way,
which find the run gone and do nothing. Each one is recorded as cancelled,
naming this run and the tag, so it reads as a decision rather than a failure.

It never crosses a project.

## Tag first, then stop

`StopSelf::Keep` only reaches runs that took the tag **before this one did**.

That is what stops two runs killing each other. Two messages arriving
milliseconds apart both say "stop the others, keep me", and because the order
is by when each tagged itself, the later one survives and the earlier one
stops. Without that rule you would get both dead, or neither.

So tag at the top of your node, then stop. A node that stops before it tags is
not in the order yet.

## The stop is not immediate

`stop_tagged` returns when the request is durably queued, not when the other
runs are gone. Do not write a node whose next line assumes they stopped.

Tags are `[A-Za-z0-9_-]`, one to sixty-four characters, checked before anything
is written, naming the character that was wrong. Tagging twice with the same
tag does nothing, which is what makes it safe to replay.

## In a test

The fake rig does not stop anything. It records what your node asked for, and
you assert on that:

```rust
assert_eq!(rig.execution_tags(), vec!["chat_7"]);
assert_eq!(rig.stops().len(), 1);
```
