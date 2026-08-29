# Cancellation

Every execution carries a cancellation flag. When the user clicks stop, or a
project is deactivated, or the dispatcher tears an execution down, the flag is
set. The engine's drive loop sees it at the next iteration and exits, dropping
the task set holding every in-flight node future, which aborts each one at its
next `.await`.

**Ordinary async Rust is cancellable instantly, with no node-side code.** A
node has to do something unusual to *escape* cancellation.

## The default

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let resp = ctx.http()
        .post("https://api.example.com/v1/messages")
        .json(&body)
        .send().await.node_err("posting to the API")?
        .json::<ApiResponse>().await.node_err("decoding the reply")?;

    ctx.pulse_downstream(NodeOutput::new().set("response", resp.text)).await
}
```

Cancelled mid-call, the future at `.send().await` is dropped, the client
closes the underlying socket, the request is cancelled in flight, and the
function exits. Nothing further is billed and nothing further runs.

The same holds for retry loops, streaming receives, and anything with regular
awaits: every iteration is a cancellation point.

Tokio cancels every primitive that respects future drop, which is most of
them: HTTP through reqwest, databases through sqlx, sleeps, file I/O,
WebSocket streams, channel receives.

## The quick reference

| Node behavior | Cancellable? | What you do |
|---|---|---|
| async HTTP, database, sleep, file I/O | yes, instantly | nothing |
| async with retries | yes, instantly | nothing |
| streaming receive | yes, instantly | nothing |
| suspended via `await_signal` | yes, engine path | nothing |
| a measured call on a connection | yes, instantly | nothing; the metering settles on its own |
| a subprocess | **the process leaks** | `.kill_on_drop(true)` |
| CPU-bound `spawn_blocking` | **the thread leaks** | pass the flag, poll it |
| a resource needing explicit cleanup | best effort | `tokio::select!` on the flag |

## Reaching the flag

```rust
let flag = ctx.cancellation();          // Arc<CancellationFlag>

flag.is_cancelled()                     // sync atomic load; cheap in tight loops
flag.cancelled().await                  // a future, for tokio::select!
flag.cancelled_err().await              // the same wait, resolving to the
                                        // error to return with `?`
```

The flag is **persistent**: once cancelled, every later `is_cancelled()`
returns true and every new `cancelled()` future resolves immediately, so there
is no window in which you can miss one.

The engine aborts your future as soon as it observes the cancel, so a
`cancelled()` branch in your body only runs if it wins that race. Treat that
cleanup as best effort.

Paid calls need nothing from you: the metering runs **below** your future and
resolves an interrupted call's real cost on its own.

## Subprocesses

Dropping a `tokio::process::Child` does **not** kill the underlying process. It
keeps running, forever, with nobody watching it.

```rust
// BAD: the future drops, `python` keeps running.
let mut child = tokio::process::Command::new("python")
    .arg(script_path)
    .spawn().node_err("starting python")?;
let status = child.wait().await.node_err("waiting on python")?;
```

One line fixes it:

```rust
// GOOD: cancel drops the future, drop kills the process.
let mut child = tokio::process::Command::new("python")
    .arg(script_path)
    .kill_on_drop(true)
    .spawn().node_err("starting python")?;
let status = child.wait().await.node_err("waiting on python")?;
```

For a graceful shutdown, letting the subprocess flush before it dies:

```rust
let mut child = tokio::process::Command::new("python").arg(script_path)
    .spawn().node_err("starting python")?;
let cancel = ctx.cancellation();

tokio::select! {
    status = child.wait() => Ok(format(status.node_err("waiting on python")?)),
    err = cancel.cancelled_err() => {
        let _ = child.start_kill();                    // SIGTERM
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            child.wait(),
        ).await;                                        // then drop kills it
        Err(err)
    }
}
```

## Blocking CPU work

Dropping a `JoinHandle` from `spawn_blocking` does **not** kill the worker
thread. The thread runs the closure to completion and only its result is
discarded.

From the user's point of view the cancel "worked": the graph stopped, the loop
exited. Meanwhile a core is still pinned.

If the work happens in chunks, pass the flag in and check it between them:

```rust
let cancel = ctx.cancellation();
let result = tokio::task::spawn_blocking(move || {
    for chunk in chunks_of(image) {
        if cancel.is_cancelled() {
            return Err(weft::node_error("cancelled"));
        }
        process_chunk(chunk);
    }
    Ok(...)
})
.await
.node_err("the image worker")??;
```

The closure is ordinary blocking code, so it cannot `.await` and builds its
error the plain way. `.node_err` on the outside handles the task itself dying;
the second `?` is your closure's own result.

`is_cancelled()` is one atomic load, so check it as often as you like.

## Cleanup on a held resource

```rust
let mut conn = open_connection().await?;
let cancel = ctx.cancellation();

loop {
    tokio::select! {
        msg = conn.recv() => {
            match msg? { Some(m) => handle(m), None => break }
        }
        err = cancel.cancelled_err() => {
            conn.send_close_message().await.ok();
            return Err(err);
        }
    }
}
```

Write this when you have something to do at the end: notify a peer, release a
lock you hold externally, flush to disk. Otherwise the default abort path
closes the connection at drop.

## Suspension is a different thing

`ctx.await_signal` is not a tokio wait. The engine journals a suspension and
the worker exits.

Cancelling a suspended execution goes through the dispatcher: it strips the
wake registration, so an external event cannot resume a dead execution, and
records the terminal event. Nodes using `await_signal` need nothing special
for cancel.
