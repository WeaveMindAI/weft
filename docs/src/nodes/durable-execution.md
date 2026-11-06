# Surviving a restart

A node can wait a week for somebody to answer and cost nothing while it waits,
because its run lets go of its worker. If your node waits, this page shows you
how to write it so it picks up where it stopped.

It does not pick up inside your `await`. When the answer comes, a worker runs
your node's `run` function (its body) again **from its first line**, and the
only thing that tells it where it got to is a small state it saved:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft::signal::{Timer, TimerSpec};
use weft::{NodeOutput, RunContext, WeftResult};

#[derive(Default, Serialize, Deserialize)]
struct Reminder {
    sent: Option<Value>,
    woke: Option<Value>,
}

async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let mut state = ctx.keep(Reminder::default()).await?;
    let sent = state.once(|s| &mut s.sent, async |_| send_reminder(&ctx).await).await?;
    let a_day = Timer { spec: TimerSpec::After { duration_ms: 86_400_000 } };
    let woke = state.wait(|s| &mut s.woke, a_day).await?;
    ctx.pulse_downstream(NodeOutput::new().set("sent", sent).set("woke", woke)).await
}
```

The first time the body runs, `once` sends the reminder and writes it into
`sent`, and `wait` waits on a one-day timer. With nothing else running, the
run pauses once it has stayed quiet for its `pauseAfterSecs`. A day later the body
starts over. `ctx.keep` hands back the state as it was saved, so `once` finds
`sent` filled and sends nothing. `wait` finds `woke` still empty, because the
pause saved the state before the answer existed, and hands over the answer that
woke the run without waiting again.

| Call | If the field is filled | If it is empty |
|---|---|---|
| `state.once(\|s\| &mut s.field, async \|st\| work).await?` | Hands the value back, runs nothing | Runs `work`, handing it the state as `st`, puts the result in the field, saves |
| `state.wait(\|s\| &mut s.field, signal).await?` | Hands the answer back, waits for nothing | Waits on the signal (or, when the body runs again for this answer, takes it at once), puts the answer in the field, saves |

`ctx.keep(initial)` hands you your state: `initial` the first time, the last
state saved every time after. It writes nothing. `state.save()` writes the
state now.

A few rules come with it:

- The state is any type that serializes and deserializes, and `state` reads
  and writes as that type (`state.sent`, `state.woke = None`). If your node
  keeps nothing, write `ctx.keep(()).await?`.
- Call `ctx.keep` once per firing (one go of your node in a run). A second
  call fails the node.
- When the run pauses, the call it paused in returns an error and your body
  stops there. Always pass it up with `?`, or, if you match the result, return
  every error you do not handle. weft runs the body again when the answer
  comes. `.node_err(..)` passes a pause, a skipped wait and a given-up wait
  through unchanged, so wrapping a call with it is safe.
- What a `once` returns has to save as something: a `()` or a `None` fails the
  node, because a body run again could not tell a `once` that ran from one
  that did not.
  Return something like `true`. A wait whose answer is JSON `null` fails the
  same way.
- The answer of a `wait` is read into the field's type, so the field's type has
  to match what the signal answers with. `Option<Value>` takes anything.
- If a person skips what a `wait` asks, the call returns
  `WeftError::WaitSkipped`. Pass it up with `?` and the firing ends skipped,
  with every output closed.
- Every call where a run can pause takes the state: a wait, a bus read, a
  bus's `wait_for`, a stream pull, a yield, and the ctx helpers that wait in a
  loop. A node that could pause without saying where it is does not compile.
  A node whose work cannot be picked up from the middle hands its bus reads,
  `wait_for`s, stream pulls and yields a `Holding` instead; a `wait` and the
  ctx helpers still take the state, though none of them pauses while the
  `Holding` exists (go and read [holding the run instead of keeping a
  state](#holding-the-run-instead-of-keeping-a-state)). Test code reads with
  `weft::context::HarnessRead`, which fails if a node body hands it to a read.
- If your node reads a bus or a stream, one more rule decides where it may
  read without going through the state. For that rule, go and read [reading a
  bus or a stream across a pause](#reading-a-bus-or-a-stream-across-a-pause).
- The state is written by `save`, at the end of each `once` and `wait`, and
  when the run pauses. weft does not write it after each bus message or stream
  item: it keeps how far those went by itself.

## Two waits with a charge between them

An expense needs a manager's click, then the card is charged, then a director
has to click too. The charge must happen once, whichever wait the run paused
in.

```rust
use weft::signal::{Predicate, ProviderEvents};
use weft::Access;

#[derive(Default, Serialize, Deserialize)]
struct Approval {
    posted: Option<Value>,
    manager: Option<Value>,
    receipt: Option<Value>,
    director: Option<Value>,
}

async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let account: Access = ctx.inputs.get("account")?;
    let mut state = ctx.keep(Approval::default()).await?;

    let posted = state.once(|s| &mut s.posted, async |_| post_buttons(&ctx, &account).await).await?;
    let manager = state.wait(|s| &mut s.manager, click(&account, &posted, "manager")).await?;
    let receipt = state.once(|s| &mut s.receipt, async |_| charge(&ctx, &manager).await).await?;
    let director = state.wait(|s| &mut s.director, click(&account, &posted, "director")).await?;

    ctx.pulse_downstream(NodeOutput::new().set("receipt", receipt).set("director", director)).await
}

fn click(account: &Access, posted: &Value, role: &str) -> ProviderEvents {
    let id = format!("{}-{role}", posted["id"].as_str().unwrap_or_default());
    ProviderEvents::new(account, "interactions", vec![Predicate::eq("callbackId", id)])
}
```

`click` builds a `ProviderEvents` signal, which waits for the service to push
an event matching its predicates: here, a click on the button with that
callback id.

If the run paused on the director's click, the state on record has `posted`,
`manager` and `receipt` filled and `director` empty. The body runs again from
the top, both `once` calls hand back what they saved (so no second set of
buttons and no second charge), the first `wait` hands back the manager's
click, and the second one takes the director's answer.

## A wait inside a loop

Five questions, asked one at a time, and the run can pause on any of them.

```rust
use serde_json::json;

#[derive(Default, Serialize, Deserialize)]
struct Interview {
    answers: Vec<Value>,
    current: Option<Value>,
}

async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let questions: Vec<String> = ctx.inputs.list("questions")?;
    let mut state = ctx.keep(Interview::default()).await?;

    while state.answers.len() < questions.len() {
        let question = &questions[state.answers.len()];
        let answer = state.wait(|s| &mut s.current, question_form(question)).await?;
        state.answers.push(answer);
        state.current = None;
    }
    ctx.pulse_downstream(NodeOutput::new().set("answers", json!(state.answers))).await
}
```

Here `question_form` builds a `Form` with one text field. One field, `current`,
serves every pass: emptying it after each answer is what makes the next
`wait` wait. When the run pauses on question 4, the state saved is
`{"answers": [<three answers>], "current": null}`. The body runs again, the
loop starts at question 4 because three answers are already there, and that
`wait` takes the answer that resumed the run.

## A stream producer that pauses

A node reads an API one page at a time, yields every row on a `Generator`
output, and rests a minute between pages to stay under the API's rate limit.

```rust
#[derive(Default, Serialize, Deserialize)]
struct Paging {
    next: Option<String>,
    page: Option<Page>,
    sent: usize,
    rested: Option<Value>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Page {
    rows: Vec<Value>,
    next: Option<String>,
}

async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let mut state = ctx.keep(Paging::default()).await?;
    loop {
        if state.next.is_some() {
            let minute = Timer { spec: TimerSpec::After { duration_ms: 60_000 } };
            state.wait(|s| &mut s.rested, minute).await?;
        }
        let cursor = state.next.clone();
        let page = state.once(|s| &mut s.page, async |_| fetch_page(&ctx, cursor).await).await?;
        for row in page.rows.into_iter().skip(state.sent) {
            ctx.yield_downstream(NodeOutput::new().set("rows", row), &mut state).await?;
            state.sent += 1;
        }
        let Some(next) = page.next else { return Ok(()) };
        state.next = Some(next);
        state.page = None;
        state.sent = 0;
        state.rested = None;
    }
}
```

The run can pause here in two places.

**On the timer between pages.** The state says which page comes next, so the
body goes straight to that `wait`, then fetches the page.

**In the middle of a page**, while a yield waits for the consumer to take a
row (the consumer is itself waiting on a person, say). weft numbers a node's
yields and saves the number of the one it paused in. Made again, that yield
sends nothing: the item yielded before the pause stands, and the call waits
for it to be taken. So the resumed body has to come back to that same yield
with the same row, which is why the page and the count of rows sent go in the
state. A page fetched again might not hold the same rows, and a body that
started the page over would send its first rows a second time.

## A stream consumer that saves as it goes

A node grades every row of a stream with a paid model call, and keeps a
count.

```rust
use weft::Generator;

#[derive(Default, Serialize, Deserialize)]
struct Grading {
    graded: u64,
    passed: u64,
}

async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let rows: Generator<Value> = ctx.inputs.get("rows")?;
    let mut state = ctx.keep(Grading::default()).await?;

    while let Some(row) = rows.next(&mut state).await? {
        if grade(&ctx, &row).await? {
            state.passed += 1;
        }
        state.graded += 1;
        state.save().await?;
    }
    ctx.pulse_downstream(NodeOutput::new().set("graded", state.graded).set("passed", state.passed)).await
}
```

weft keeps how many items the node took off each stream input, saved with its
state. If the producer above rests on its timer, this node waits in
`rows.next`, the run pauses, and the node starts again after the rows it
already took.

The `save` after each row is for a crash. If the worker dies, a durable run
runs this node again from its last save and hands it again the rows it took
since, so with a save per row, only the row it was grading is graded (and
paid for) twice. If the producer's end had already reached the node at its
last save, the body run again gets the rows it had not taken and then the
end, without waiting on the producer.

## A conversation that pauses while nobody talks

Two nodes hold a conversation on a bus. One waits for the next message in a
chat service's thread and passes it on; the other reads the bus and answers.
The bus is ephemeral, so the messages stay out of the run's record.

```rust
use weft::bus::BusOptions;
use weft::stream_journal::JournalPolicy;
use weft::NodeErrExt;

#[derive(Default, Serialize, Deserialize)]
struct Inbox {
    next: Option<Value>,
}

// The node that listens.
async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let account: Access = ctx.inputs.get("account")?;
    let thread: String = ctx.inputs.get("thread")?;
    let mut state = ctx.keep(Inbox::default()).await?;

    let opts = BusOptions { journal: JournalPolicy::ephemeral(), ..BusOptions::default() };
    let bus = ctx.open_bus("chat", opts, "inbox").await?;
    loop {
        let said = ProviderEvents::new(&account, "messages", vec![Predicate::eq("thread", thread.clone())]);
        let message = state.wait(|s| &mut s.next, said).await?;
        if message["text"] == "bye" {
            // The answering node is on the bus too, so returning would
            // leave it open for that node: end it for both.
            bus.close();
            return Ok(());
        }
        bus.send("message", message).node_err("passing the message on")?;
        state.next = None;
    }
}
```

```rust
// The node that answers.
async fn run(&self, ctx: RunContext) -> WeftResult<()> {
    let mut state = ctx.keep(()).await?;
    let bus = ctx.join_bus("chat", "assistant")?;
    let mut cursor = bus.cursor()?;
    while let Some((_from, message)) = cursor.next_json("message", &mut state).await? {
        let reply = answer(&ctx, &message).await?;
        post_reply(&ctx, &reply).await?;
    }
    Ok(())
}
```

While nobody talks, the first node waits on its `wait` and the second waits on
its cursor with nothing to read. Once that has lasted the trigger's
`pauseAfterSecs` (10 seconds unless it says otherwise), the run pauses and
lets go of its worker; a message inside those seconds goes straight through
on the same worker, with no pause.
The bus stays open through the pause, and both names stay on it, so neither
node ever sees the other leave.

When a message comes, a worker opens the bus again and runs both bodies again.
`open_bus` and `join_bus` hand each node the bus it had, under its name, and
the cursor starts where it stopped. The first node's `wait` takes the message
and sends it, and the second node reads it. An ephemeral bus comes back empty,
and nothing in this conversation is lost, because the answering node was
waiting on its cursor and had read everything.

## Reading a bus or a stream across a pause

A loop that reads a bus or a stream picks up by itself after a pause: weft
saves how far your reader got on each bus and how many items you took off
each stream, and in the body run again the reader starts from there. What
weft does not save is what your code did with an item it had already read.
So if a loop only reads, let it call the reader's own `next` (or `next_json`,
or the stream's `next`). If you need an item again
after a pause (the message you stopped to ask somebody about, the first half
of a pair), read it through the state.

`state.read(|s| &mut s.field, &mut reader)` reads through the state. If the
field is filled, it hands that back and reads nothing. If it is empty, it reads
the next item (a `BusEntry` off a bus reader, or a `T` off a `Generator<T>`,
which you then declare `mut`), puts it in the field and hands it back, or
`None` at the end. It saves nothing by itself: the field is written with the
rest of the state at the next pause, `save()`, or end of a `once` or a `wait`.
Empty the field when you are done with the item, or the next `state.read`
hands back the same one.

A body run again after a pause starts from its top, so one rule decides where
it may read without going through the state. weft checks it live, on every
run of the body, first or resumed, and saves nothing for it:

**The first time a run of your body reaches a place where it can pause, it
must not have read directly before, except inside a `state.once` that has
finished.**

A place where it can pause is a bus read, a stream pull, `state.read`,
`state.wait`, a yield, or a wait between the looks of a [ctx helper that
waits](#the-ctx-helpers-that-wait). A `state.read` or `state.wait` counts even
when its field is already filled; a `try_next` never waits, so it is a direct
read but no place. A direct read is any read that does not go through the state: `next`,
`next_message`, `next_json` or `next_bytes` on a bus reader, and `next`,
`try_next` or `drain` on a stream. A place is its line and what it reads (the
bus, the stream, or the state field), and one the run already reached is fine,
so a loop can read one reader directly turn after turn.
Read directly in your node's own code, not inside a helper function, or read
through the state: a helper's reads all carry the helper's line, so two phases
reading through one helper look like one place.

Here is what the rule prevents. Say a body reads a greeting with `r.next` and
then waits for a person. Paused in the wait and run again from the top, it
would read at the greeting's line again, from where the reader stopped, and
get the message after the greeting. So weft refuses the wait the first time
the body gets there, on the first run, whether or not it would have paused,
naming both lines:

```text
node 'intake' read directly (not through its state) at <line> before it waited
at <line>, a place its run can pause: run again after a pause, it would read at
<line> again from where its reader stopped and lose what it read there. Read
that earlier one through the state (`state.read`), or put that earlier phase
inside a `state.once`
```

In place of `waited`, the error says what that place does: `read a bus`,
`pulled a stream`, `drained a stream`, `read through its state`, `yielded`,
`waited between a helper's looks`.

If a body reads directly and then, after a pause, takes a different path than
before, it fails at the first place on the new path. So: before a place your
run can pause, branch only on your state; anything from outside (the time, a
random value, a call to a service) goes in a state.once. If a body breaks this
with no direct read before the split, nothing stops it: weft matches each wait
to the record by its number in the firing, so a resumed body that took another
path and reaches a different wait of the same signal kind silently gets the
earlier wait's answer. Only a wait of another kind fails, naming both kinds.

Two calls are left out. `bus.wait_for(name, &mut state)` is not a read: it
asks whether that name ever joined the bus, and the list of who joined comes
back with the bus after a pause, so a body run again gets past it at once.
And while your node holds its run (`ctx.hold()`), nothing it calls can pause,
so nothing it calls is a place. The last direct read it made while holding
counts once the hold is dropped: `let m = { let mut h = ctx.hold(); r.next(&mut
h).await? }; state.wait(..)` fails at the wait on its first run, while reading
and waiting inside one hold passes.

These shapes pass, written short (`r`, `ra` and `rb` are readers, and
`is_stop` stands for your own test):

```rust
// a loop reading directly
loop { let m = r.next(&mut state).await?; handle(m); }

// a greeting through the state, then a loop
let hello = state.read(|s| &mut s.hello, &mut r).await?;
loop { let m = r.next(&mut state).await?; }

// two buses in one loop (it can pause in either read)
loop {
    let a = state.read(|s| &mut s.a, &mut ra).await?;
    let b = rb.next(&mut state).await?;
    handle(a, b);
    state.a = None;
}

// two loops with a wait between (it can pause in each step)
let stop = state.once(|s| &mut s.first, async |st| {
    loop { let m = r.next(st).await?; if m.is_stop() { return Ok(m); } }
}).await?;
state.wait(|s| &mut s.ok, approval).await?;
loop { let m = r.next(&mut state).await?; }
```

These fail on the first run, with no pause needed:

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

The last one is a different rule: a run of your body opens one reader per bus,
because weft saves one position per bus for your node. For its error, go and
read [one reader per bus](streams-and-buses.md#one-reader-per-bus). A stream
needs no such rule, because every handle on a stream port shares one position.

If a run pauses inside a `once`, the `once`'s work runs again from its top
when the run comes back, but its reader carries on from where it stopped. So
anything that loop adds up across turns (the notes it gathers until a `stop`,
say) has to live in the state. Once the `once` has returned, its field is
filled, and a body run again after any later pause skips the whole loop.

### A bus reader that stops to ask somebody

A moderator reads messages off a bus and, for a risky one, asks a person
before passing it on. The version you would write first is refused:

```rust
#[derive(Default, Serialize, Deserialize)]
struct Review {
    verdict: Option<Value>,
}

let mut state = ctx.keep(Review::default()).await?;
let mut cursor = bus.cursor()?;
while let Some((_from, message)) = cursor.next_json("message", &mut state).await? {
    if is_risky(&message) {
        let verdict = state.wait(|s| &mut s.verdict, review_form(&message)).await?;
        // ... act on the verdict
        state.verdict = None;
    }
}
```

The first risky message brings the body to the `wait` after a direct read, so
the node fails there with the error above. If it were allowed, a pause in the
review would lose the message: the reader's place is saved past it, and the
body run again would read the next one. The message being handled goes
through the state:

```rust
use weft::bus::BusEntry;

#[derive(Default, Serialize, Deserialize)]
struct Review {
    handling: Option<BusEntry>,
    verdict: Option<Value>,
}

let mut state = ctx.keep(Review::default()).await?;
let mut cursor = bus.cursor()?;
while let Some(entry) = state.read(|s| &mut s.handling, &mut cursor).await? {
    if let Some(("message", message)) = entry.as_json() {
        if is_risky(message) {
            let verdict = state.wait(|s| &mut s.verdict, review_form(message)).await?;
            // ... act on the verdict
            state.verdict = None;
        }
    }
    state.handling = None;
}
```

`entry.as_json()` gives a JSON message's kind and value, and `None` for a
join, a leave or a bytes message. When the body runs again after a pause,
`handling` is filled, so `state.read` hands the message back without reading,
and the body goes straight to the `wait`.

Whether or not the run pauses, the bus keeps only its last messages, as many
as its window holds (64 unless the node that creates it sets `window` in its
`BusOptions`). While the run is held, a reader that falls further behind than
that skips the messages that were dropped. If the run pauses, only a journaled
bus carrying JSON comes back with that window, and only the messages after the
newest one over 100 KB, since the record keeps a message that large trimmed
(for the full rule, go and read [a pause keeps a bus, a crash ends
it](streams-and-buses.md#a-pause-keeps-a-bus-a-crash-ends-it)). When the body
comes back, if the bus has lost a message your reader had not read yet, the
reader's first read fails instead of skipping it:

```text
this node paused with messages of a bus it had not read yet, and the bus no
longer holds them (a bus keeps only its last window of messages, and over a
pause an ephemeral bus keeps none and a journaled one only those of that window
after the newest one over 100 KB): it cannot read on as if it had them. Make
the bus journaled with a window that holds what a reader may fall behind by, or
hold the run while reading it (`ctx.hold()`)
```

So if messages can pile up while your node waits on somebody, the node that
creates the bus has to keep it journaled (the default), send JSON, keep each
message under 100 KB, and set `window` wide enough to hold them.

## Doing something only once

`once` has one gap: if the worker dies after `work` returned and before the
save landed, the body runs `work` again. If the service on the other end must
never see it twice (a payment, say), it has to refuse the duplicate itself,
usually through an idempotency key you send with the request.

`ctx.firing_key()` is that key. It is built from the run id, the node and the
loop iterations and call sites it fires under, so the body run again after the
crash sends the same one, and the next loop iteration or the next run sends
another. It is 64 characters of hex and gives away no name from your program.
It names the firing, so if one body makes two calls that each must not
repeat, give each its own key, such as `format!("{}-refund", ctx.firing_key())`.

If the thing you must not repeat is not in a `once`, set a field that says it
is done, call `state.save()` right after, and skip the thing when that field
is set. Otherwise a body run again after a crash does it again.

`ctx.tag_run` needs neither: tagging again changes nothing. `ctx.stop_tagged`
is different when this run does not carry the tag itself: made again after a
pause or a crash, it also stops the runs that took the tag since. If that
matters, put it in a `once`, returning something from it, since a `()` fails
a `once`:

```rust
use weft::StopSelf;

// `stopped: Option<bool>` is a field of the node's state.
state
    .once(|s| &mut s.stopped, async |_| ctx.stop_tagged("chat_7", StopSelf::Keep).await.map(|()| true))
    .await?;
```

## When a run pauses

A `state.wait` never pauses its run by itself: the call stays open in your
node while the rest of the run carries on. The run pauses once every node
still running waits (in a `state.wait`, a bus read, a stream pull, a yield, or
a bus's `wait_for`, which waits for a name to join the bus), at least one of
those waits needs an answer from outside, and the run has stayed that quiet
for its trigger's `pauseAfterSecs`. Then every waiting call saves its state
and pauses together, and the run lets go of its worker. An answer that comes
within those seconds goes straight into the call waiting for it, with no
pause. If the worker is stopping, a run that can pause pauses at once and
carries on in the next worker. If none of the waits needs anything from
outside (nodes waiting only on each other's buses), nothing could ever move
the run, so it is stuck rather than paused, and weft ends it.

weft numbers each wait and each program call within a firing, which is how a
body run again is matched to what it did before. Beside your state, it saves
by itself:

- how far the node's reader on each bus read, and the name it holds on each
  bus;
- how many items it took off each stream input;
- how many items it yielded;
- the numbers its next wait and its next program call will take.

When the answer comes, a worker opens again every bus that was open, then runs
again from the top the firing that answer was for and every firing that paused
inside the run. A firing still waiting on its own signal keeps waiting. A port the node already emitted on
sends nothing a second time, and its first value stands.

A body that drains a stream (`drain`) can pause there too. `drain` takes the
items as they arrive but holds them until the stream ends. If the run pauses
first, weft saves the items it was holding with the node's state, and the
resumed body's `drain` starts from them, takes the rest, and hands over the
whole list at the end. Each item is taken once, and your state struct needs
nothing for it.

## Holding the run instead of keeping a state

Some work cannot be picked up from the middle: a session with a provider that
ends with its connection, audio being gathered into one file, an answer
streaming to a caller. No saved state brings that back. If your node does such
work, hold the run while it reads:

```rust
let mut holding = ctx.hold();
while let Some(chunk) = text.next(&mut holding).await? {
    session.send(&chunk).await.node_err("sending text to the session")?;
}
```

You hand `&mut holding` to bus reads, a bus's `wait_for`, stream pulls and
yields, where you would otherwise hand your state. While `holding` exists, no
wait of your node pauses: the calls made with it, and its `state.wait`s, its
ctx helpers' waits and its reads with the state too, wait open in their call.
The run keeps its worker the way any run that cannot pause does: for as long
as a caller is on the line, otherwise until nothing has moved in it for its
trigger's `holdSecs`, when the waiting call fails with `WeftError::WaitGaveUp`,
saying the node holds its run. Once `holding` is dropped (when the body
returns, say), the node can pause again. Meanwhile the other nodes' `state.wait`s are held in their calls
too, and an answer goes straight into the call waiting for it. Holding saves
nothing, so the node is never picked up from the middle: if its worker stops
or dies while it holds, it is treated as a node that was running with nothing
saved, and is failed (go and read [when the worker dies
mid-run](#when-the-worker-dies-mid-run)).

If you are choosing between the two: keep a state when a few fields can say
where the node is and it can carry on from there; hold when where it is lives
in something outside weft that another worker cannot rebuild.

## Waiting on a job a service runs

If your node starts something slow on a provider (a video render, say) and the
provider hands back a status address, do not loop and sleep in your body: that
holds a worker for the whole job. Submit the job in a `once`, then wait on the
status address with `PollEndpoint`:

```rust
use weft::signal::{CancelCall, CancelWith, PollEndpoint, Predicate};

#[derive(Default, Serialize, Deserialize)]
struct Render {
    submitted: Option<Value>,
    status: Option<Value>,
}

let mut state = ctx.keep(Render::default()).await?;
let submitted = state
    .once(|s| &mut s.submitted, async |_| post_json(&http, &submit_url, &payload, "submitting the render").await)
    .await?;
let id = submitted["request_id"].as_str().node_err("the submit answered no request_id")?;

let poll = PollEndpoint {
    url: format!("{API}/requests/{id}/status"),
    interval_secs: 5,
    access: Some(weft::primitive::AccessRef::from(&account)),
    filters: vec![
        Predicate::neq("status", "IN_QUEUE"),
        Predicate::neq("status", "IN_PROGRESS"),
    ],
    ..Default::default()
};
let status = state
    .wait(|s| &mut s.status, poll.cancel_with(CancelCall::put(format!("{API}/requests/{id}/cancel"))))
    .await?;
```

Without the `once`, the body running again would start a second job and pay
for it.

If the run can pause, it pauses the way [any run does](#when-a-run-pauses)
(for the other case, go and read [when the run cannot
pause](#when-the-run-cannot-pause)). weft checks the
address once
straight away (a job that is already done resumes at once), then every
`interval_secs`, signing each check with the `access` connection. An interval
under 5 seconds fails the wait at once, saying
`poll_endpoint.interval_secs must be >= 5`. The first answer that passes every
filter is what the wait returns, and the checking stops. With no filters, the
wait returns the first successful answer. An error status or a failed request
is not an answer: it is tried again at the next check and shown on the node.

If the finished answer is heavy (a crawl's status that embeds its first page
of results), list the fields your node reads next in `carry`, like
`carry: vec!["status".into(), "error".into()]`. The filters still read the
whole answer, but only those fields are handed back, so megabytes of data
never sit in the run's history; read the rest yourself once the wait is over.

Write the filters as "not one of the in-flight statuses" rather than "one of
the finished ones", the way the example does. A status you did not expect then
ends the wait, and your code can fail on it, instead of waiting for ever.

### If the run is cancelled while the job runs

Cancelling the run takes the wait down, but the provider hears nothing and
goes on working, and charging, for an answer nobody will read. If you want the
provider told, wait on `signal.cancel_with(call)`, as the example does, where
`call` is one request to send if the run is cancelled, usually to the
provider's own cancel address for the job.
Whatever cancelled the run (a person, another run's `stop_tagged`, turning
the trigger off with a take-down that cancels the runs still going), weft
sends that request once, signed with the same `access` as the checks. It is
never sent when the answer arrives, or when the run ends any other way.

`CancelCall::post`, `put`, `patch` and `delete` take the address. If the
provider's cancel wants a body, add `.with_body(json!({ .. }))`.
If the address is empty, or is not `http` or `https`, the wait fails at once,
before the run pauses, with `invalid signal spec: the cancel call's url must not be empty` or
`invalid signal spec: the cancel call's url must be http(s): got '<url>'`.

If the provider has no way to cancel a job, wait on the plain signal.

If the provider refuses the call, or does not answer within 60 seconds, weft
does not try again, and the job may still be running. The runtime's log
(`weft daemon logs` on your machine) says `a last call to an outside service
failed`, with the address and what the provider answered.

If you want to check the call in a test, read it off the wait:

```rust
assert_eq!(
    rig.awaited_signals()[0].on_cancel,
    Some(CancelCall::put("https://queue.example/requests/req-1/cancel")),
);
```

### Watching it while it waits

If you want to show how far the job has got (a progress bar for a render,
say), weft keeps the last answer a check got while the node waits, exactly as
the provider sent it. A failed check does not replace it, and an answer over
64 KiB is kept only as its size. You can read it:

- in the editor, on the waiting node's panel, while you follow the run;
- in the terminal, with `weft node-state <run> <node>`, which prints
  `render: waiting` and then `last answer: {"status":"IN_QUEUE"}`;
- in a program, with the `GetNodeState` node, which takes a run's id (a run
  gets its own id from the `GetRunId` node);
- in your own node's code, with `ctx.run_by_id(run).node_state(node)`.

## Waiting on whichever comes first

Sometimes a wait has two ways to end. Your node asks a person something with a
form, and the thing it asks about can stop needing an answer while the form is
still up: a coding agent asked a question and then went on without one. If you
wait on the form alone, the run waits on that person for ever, for an answer
nothing will read.

If you want the wait to end on either, wait on `FirstOf::either(a, b)`:

```rust
use weft::signal::{Either, FirstOf, PollEndpoint, Predicate};

#[derive(Default, Serialize, Deserialize)]
struct Asked {
    ended: Option<Either<Value, Value>>,
}

let still_asked = PollEndpoint {
    url: status_url,
    filters: vec![Predicate::eq("gone", "true")],
    ..Default::default()
};
let mut state = ctx.keep(Asked::default()).await?;
match state.wait(|s| &mut s.ended, FirstOf::either(form, still_asked)).await? {
    Either::First(submission) => { /* the person answered */ }
    Either::Second(gone) => { /* nothing waits for the answer any more */ }
}
```

The first of the two to answer ends the wait, and the other is taken down in
the same step: the form leaves every list a person answers from, and the poll
stops. An answer that still comes for the one that lost is refused, the way an
answer to any wait that already ended is. A person skipping the form skips the
whole wait. Each signal is registered as if you had waited on it alone, so
`cancel_with`, a connection and filters go on the signal they are about, never
on the `FirstOf`.

To the run it is still one wait, so it pauses, resumes and is replayed like
any other, and the field keeps which of the two answered: `{ "branch": 0 }`
for the first, `{ "branch": 1 }` for the second, beside its answer. A
`FirstOf` with fewer than two signals, or with a `FirstOf` inside it, fails
the wait at once with `invalid signal spec`. A trigger cannot use one: it
listens on its one signal.

In a node test, the answer you queue says which one answered:
`rig.signal(json!({ "branch": 1, "value": { "gone": true } }))`.

## Cleaning up what your node made at a provider

Say your node uploads a file to a provider, starts a job that reads it, and
waits for the job. If the run is cancelled during the wait, or the worker
dies, the code that deletes the upload afterwards never runs, and the file
stays in the user's account. So register the delete right after the upload
exists, before anything else can go wrong:

```rust
use weft::access::client;
use weft::signal::CancelCall;

let id = upload(&http, &file).await?;
ctx.after_node(CancelCall::delete(client::url(API, &["uploads", &id])?).always(), &account).await?;
```

weft sends that call once, through the connection you pass (`None` sends it
plain), when the node ends, however it ends: it returned, failed, was
cancelled, gave up or skipped a wait, or its worker died and the run ended
without it. Leave out `.always()` and the call goes only if the run is
cancelled before the node ends, for a job the provider would otherwise keep
running.

The call is on the run's record before `after_node` returns, which is what
lets it survive a crash:

- A worker that picks the run up after a crash still owes it, and so does
  the worker that picks up a run that paused.
- A body that runs again after a pause or a crash and registers the same
  call again (same node, call and connection) still owes it once, so you do
  not have to guard the `after_node` with a `once`.
- A run that ends with no worker driving it (cancelled while it was paused,
  or removed with its project) still sends it, from weft's own side.

weft sends the calls before it writes that the node ended, so a worker that
dies in between sends them again from the next one. Pick a call that is
safe to send twice: a `404` counts as done, so a delete is.

If the provider refuses the call, or does not answer within 60 seconds, weft
does not try again. A call sent when the node ended shows on the node's log
as `the call DELETE <url> this node owed when it ended did not go through:`
followed by the provider's own words. One sent from weft's side, for a run
nobody was driving, shows in the runtime's log, the same way as a [wait's
cancel call](#if-the-run-is-cancelled-while-the-job-runs).

`after_node` fails on a pure node, which does nothing outside the run, and
on an address that is empty or not `http` or `https`. A wait's
`cancel_with` takes only a call without `.always()`: its call goes with the
wait, which is gone once answered, and an `.always()` one is refused, naming
`ctx.after_node`.

If you want to check the call in a test, `rig.owed_calls()` lists every call
the node registered, each with the connection it goes through:

```rust
let owed = rig.owed_calls();
assert_eq!(owed[0].0, CancelCall::delete("https://api.example/uploads/123").always());
```

## When the run cannot pause

If the run cannot pause (a caller is still on the line, the trigger has
`recorded: false`, or it is a node test), `state.wait` waits inside your call
on the same worker, and returns the answer when it comes. If nothing in the
run moves for the trigger's `holdSecs`, the call fails with
`WeftError::WaitGaveUp`, saying why; while a caller is on the line, that clock
does not run. For what holds and when the clock runs, go and read [when a run
cannot pause](../language/triggers-and-routes.md#when-a-run-cannot-pause).

A wait given up is an outcome like any other: if your node can do something
sensible without the answer, match it and carry on.

```rust
use weft::WeftError;

let approved = match state.wait(|s| &mut s.approved, question_form("Send this?")).await {
    Ok(answer) => answer,
    Err(WeftError::WaitGaveUp(_)) => json!(false),
    Err(other) => return Err(other),
};
```

If you want to test that branch, `rig.signal_given_up()` makes the next wait
fail with `WaitGaveUp`, and `rig.signal_skipped()` makes it end skipped.

## The ctx helpers that wait

A few ctx calls follow something slow until it ends, checking it on a timer,
and the run can pause between two checks. Those take the state:

- `ctx.infra(node).start(&mut state)`, `.request_start(&mut state)` and
  `.rebake(&mut state)`;
- `ctx.all_infra().follow_command(id, &mut state)`;
- `ctx.project(id).code().build().start(&mut state)` and
  `ctx.project(id).code().run()...start(&mut state)`;
- `ctx.project(id).triggers().resync(take_down, &mut state)` and
  `ctx.project(id).resume_resync(&mut state)` (on another project: a resync of
  the run's own project would wait on the run itself);
- `ctx.project(id).remove(&mut state)`.

If the run paused in one of them, weft saves where the helper stood. The body
run again comes back into the same helper, which goes straight back to the
wait it paused in and makes none of its earlier calls again: no second read,
no second start. A body that comes back into a different helper first, or
reaches its own `state.wait` first, is refused. `run()...start` runs on the
working copy it built, read in the build's first check.

Every other call on your project (for the list, go and read [your project, and
the owner's other projects](ctx.md#your-project-and-the-owners-other-projects))
is made again each time the body runs again, as a new call. The one exception
is a call made since the last save, when the worker died before the next one:
the body runs again from that save and makes the call again under the number
it had. It is then the same call, so what it makes is made once (a project it
declares comes back as the same project), and what it takes down answers that it is already down. A minted
instance token behaves differently: the mint gives the same token a fresh
value, and the first one stops working. If the body must
not repeat a call after a pause, put it in a `once`.

## When the worker dies mid-run

What happens depends on the trigger's `durable` setting. For what that setting
does, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept).

**A fast run (the default) ends cancelled with its worker**, and is not run
again (go and read [what happens when something
dies](../running/architecture.md#what-happens-when-something-dies)). The
exception is a run that had paused: it survives its worker, fast or not. A
fast run with one branch waiting while another still ran ends cancelled like
any other.

**A durable run carries on in another worker**, and so does one whose worker
could not write its record for over a minute (for that case, go and read [when
a write of the record
fails](../running/the-journal.md#when-a-write-of-the-record-fails)). Each
node's start (except a pure node's, below) is on record before its body runs,
so the next worker knows exactly which nodes were running when the first one
went away.

A node that was running and had saved a state runs again from its last save,
the way a paused one does. Anything it did after that save, it does again (the
stream consumer above shows how to keep that small). Calling `ctx.keep` saves
nothing by itself: a node counts as having saved once it reached a `save`, the
end of a `once` or a `wait`, or a pause.

A node that had saved nothing is failed, because weft cannot tell how much of
its work already happened:

```text
the worker running 'billing' went away while it was running; it was not run
again, because it may have partly happened. Re-run from here once you have
checked what it did.
```

A node that was reading a stream and saved no state says `went away while it
was reading a stream` instead: it saved no state to carry on from, the rest of
its stream goes with it, and the message tells you to start the whole run
again so the stream starts over.

If you want to run that node again once you have checked, `weft run --from
<node>=<json>` starts a run there with the inputs you hand it. If you wired
the node's `_error` output, this failure goes there like any other, and that
branch carries on; otherwise the run fails.

If the node that was running is a pure one that does not read a stream, and
had not emitted yet, the next worker runs it again instead of failing it. A
pure node does nothing outside the run (for what that means, go and read the
`pure` flag in [metadata.json](metadata.md#features)), so running it twice
changes nothing. One that had emitted is failed like any other.

The last messages on a bus may never have reached the record before its
worker died. So if any node was left running (its worker died, or it was
stopping and a node did not pause within the few seconds it is given), whether it runs again from a saved state or is failed, the
next worker ends every bus the run had open as failed, with `the worker
holding this bus went away, so messages it carried may be lost`. A pause never
does this: a paused run's buses come back open.

In a durable run, if a node emitted a value before the crash and the node
reading it had not started yet, that value is still delivered and the reading
node runs.

## A run paused before this version of weft

weft used to pick a waiting node up without a saved state, so a run that
paused under that version cannot carry on, and its waiting node fails with:

```text
this run paused before this version of weft, which picks a paused node up from
the state it saved (`ctx.keep`), and 'approve' saved none: it cannot carry on.
Start the run again (`weft run --seed=<run>` reuses what it already did).
```
