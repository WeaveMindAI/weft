---
name: weft-resuming-reads
description: "Read before writing, fixing or reviewing a node that reads a bus or a stream (a bus reader, a `Generator` input), above all one that also waits, yields or reads at more than one line, and when a node fails with `read directly (not through its state)`, `opened a second reader` or `paused with messages of a bus it had not read yet`: the one rule a read across a pause follows, `state.read`, the shapes it allows and refuses, and each error with its fix."
---

# Reading a bus or a stream across a pause

A body that pauses runs again from its top. weft saves how far its reader got
on each bus and how many items it took off each stream, and nothing about the
lines it ran. It does not save what your code did with an item it already
read: anything you need again after a pause goes through the state.

## The calls

- `bus.cursor()?`, `cursor_from_start()?`, `cursor_at(offset)?` open your one
  reader on a bus; a body run again gets it back where it stopped.
- A **direct read** is any read not through the state: `reader.next(&mut
  state)`, `next_message`, `next_json`, `next_bytes`, `gen.next(&mut state)`,
  `gen.try_next(&state)`, `gen.drain(&mut state)`.
- `state.read(|s| &mut s.field, &mut reader).await?`: field filled, it hands
  that back and reads nothing; field empty, it reads the next item into the
  field (`Option<BusEntry>` off a bus, `Option<T>` off a `let mut rows:
  Generator<T>`). It saves nothing by itself (the field goes on record at the
  next pause, `save()`, or end of a `once` or `wait`). Set the field back to
  `None` when you are done with the item.
- `state.once(|s| &mut s.f, async |st| { .. reader.next(st).await? .. })`: the
  work reads with the state as `st`.
- `entry.as_json()`: a JSON message's kind and value, `None` for a join, a
  leave or bytes.
- `gen.drain(&mut state)` can pause too: it holds the items it took until the stream ends, and if the run pauses first weft saves them with the node's state, so the resumed body's `drain` starts from them and your state struct keeps nothing for it.
- `bus.wait_for(name, &mut state)` is not a read and the rule ignores it: it
  asks whether that name ever joined, the list of who joined comes back with
  the bus, and a body run again gets past it at once. No `once` around it.

## The rule

The first time a run of the body (first run or resumed) reaches a place where
it can pause (a bus read, a stream pull, `state.read`, `state.wait`, a yield,
a wait between a ctx helper's looks), no direct read may have happened before
it in that run, except inside a `state.once` that has finished. A filled
`state.read` or `state.wait` still counts as a place; `try_next` never waits,
so it is a direct read but no place. A place is its line and what it reads (the
bus, the stream, or the state field); one already reached in this run (the next
turn of a loop over one reader) is fine. Read directly in your node's own code, not inside a helper function, or read through the state: a helper's reads all carry the helper's line, so two phases reading through one helper look like one place. weft
checks it live and saves nothing for it; while the firing holds its run
(`ctx.hold()`) nothing it calls pauses or counts as a place, but its last
direct read counts once the hold is dropped (`let m = { let mut h =
ctx.hold(); r.next(&mut h).await? }; state.wait(..)` fails at the wait;
reading and waiting inside one hold passes). If a body reads directly and then, after a pause,
takes another path than before, it fails at the first place on the new path.
So: before a place your run can pause, branch only on your state; anything from outside (the time, a random value, a call to a service) goes in a state.once.
Without a direct read before the split nothing stops a wrong path: weft
matches each wait to the record by its number in the firing, so a resumed body
on another path that reaches a different wait of the same signal kind silently
gets the earlier wait's answer (only a different kind fails).

Allowed (`is_stop` stands for your own test; `m` is an
`Option<BusEntry>`, so in real code match on `m.as_ref().and_then(BusEntry::as_json)`):

```rust
// a loop reading directly
loop { let m = r.next(&mut state).await?; handle(m); }
// a greeting through the state, then a loop
let hello = state.read(|s| &mut s.hello, &mut r).await?;
loop { let m = r.next(&mut state).await?; }
// two buses in one loop (pause in either read)
loop {
    let a = state.read(|s| &mut s.a, &mut ra).await?;
    let b = rb.next(&mut state).await?;
    handle(a, b);
    state.a = None;
}
// two loops with a wait between (pause in each step)
let stop = state.once(|s| &mut s.first, async |st| {
    loop { let m = r.next(st).await?; if m.is_stop() { return Ok(m); } }
}).await?;
state.wait(|s| &mut s.ok, approval).await?;
loop { let m = r.next(&mut state).await?; }
```

Refused on the first run, no pause needed:

```rust
let hello = r.next(&mut state).await?;
loop { let m = r.next(&mut state).await?; }      // fails at the loop's read

let m = r.next(&mut state).await?;
state.wait(|s| &mut s.ok, approval).await?;       // fails at the wait

loop {
    let a = ra.next(&mut state).await?;
    let b = rb.next(&mut state).await?;           // fails here
}

for r in &mut readers { r.next(&mut state).await?; }  // fails at the second reader

let r1 = bus.cursor()?; let r2 = bus.cursor()?;   // fails at the second cursor
```

A loop inside a `once` that is re-entered after a pause starts over at the
reader's saved position, so anything it adds up across turns lives in the
state.

## The errors, and what to change

- `node '<id>' read directly (not through its state) at <line> before it
  waited at <line>, a place its run can pause` (`waited` may be `read a bus`,
  `pulled a stream`, `drained a stream`, `read through its state`, `yielded`,
  `waited between a helper's looks`):
  if the reads take turns in one loop (two
  buses, say), read the earlier one with `state.read`; if the earlier reads are
  a phase that ends, put that phase in a `state.once`.
- `node '<id>' opened a second reader on a bus at <line>, which it already
  opened one on at <line>`:
  open the reader once, before every loop that reads the bus,
  and use it everywhere; to keep only some messages, match on the entry or use
  `cursor.with_filter(..)`.
- `this node paused with messages of a bus it had not read yet, and the bus no
  longer holds them`: the bus came back
  without messages this reader had not reached (an ephemeral or bytes bus comes
  back empty; a journaled one keeps its window of JSON messages, but only those
  after the newest one over 100 KB). The node that creates the bus keeps it journaled, sends JSON and sets
  `window` wide enough, or this node reads with `ctx.hold()` (a held reader that
  falls behind the window skips what was dropped, with no error).
