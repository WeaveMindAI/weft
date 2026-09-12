# Cancellation

When someone stops an execution, weft cancels its running node tasks.
For ordinary async work, the engine handles this without a cancellation
loop in every node. If you launch a subprocess or blocking computation,
you need to arrange for that work to stop too.

Cancellation stops local work when it yields control. It does not undo a
request another service has already accepted. A cancelled node may have
sent a message, started a job, or incurred a charge before the stop arrived.

## Ordinary async work

A node waiting on an HTTP response, a timer, or a stream can be aborted by
dropping its future. Code after that wait does not continue in the aborted
task. This is cooperative: CPU work that does not yield can delay cancellation,
even if it is inside an `async fn`.

Dropping a request future does not establish what happened at the remote
service. If your node needs to cancel a remote job or undo a database
change, that requires the relevant service's API or transaction behavior.

| Work the node starts | What cancellation needs |
|---|---|
| Async work awaited by the node | The engine aborts the node task |
| A child process | Opt into killing it when its handle is dropped |
| An already-running `spawn_blocking` closure | Check the cancellation flag inside the closure |
| A remote operation | Use that service's cancellation mechanism if one exists |
| A durable wait | The runtime cancels the execution and its wake registration |

## Reaching the flag

Use `ctx.cancellation()` when work needs to observe the stop itself:

```rust
let cancel = ctx.cancellation();
```

| Method | Result |
|---|---|
| `cancel.is_cancelled()` | Boolean check for synchronous code |
| `cancel.cancelled().await` | Waits until cancelled, then returns `()` |
| `cancel.cancelled_err().await` | Waits until cancelled, then returns `WeftError::Cancelled` |

The last method returns the error itself. Use `return Err(error)`;
it is not a `Result` to which you can apply `?`.

Once cancellation has been set, it stays set. Later checks still see it,
and later waits return immediately.

## Subprocesses

By default, dropping a `tokio::process::Child` leaves the process running.
If the subprocess belongs to this node's work, set `kill_on_drop(true)`:

```rust
use weft::NodeErrExt;

let script_path: String = ctx.inputs.get("script")?;
let mut child = tokio::process::Command::new("python3")
    .arg(script_path)
    .kill_on_drop(true)
    .spawn()
    .node_err("starting the script")?;

let status = child.wait().await.node_err("waiting for the script")?;
if !status.success() {
    return Err(weft::node_error(format!("The script exited with {status}")));
}
```

This is a node-body fragment with a `script` string input. The script and
Python interpreter must exist in the worker image.

The option requests termination when the child handle drops. It does not
give the child time to flush or run a shutdown handler. On Unix, Tokio's
`start_kill()` also sends `SIGKILL`; it is not a graceful-shutdown call.
If the subprocess needs an orderly shutdown, implement its shutdown
protocol and keep forced termination as the fallback when that cannot finish.

## Blocking CPU work

A `spawn_blocking` closure that has started keeps running even if the
async task awaiting it is aborted. Pass the cancellation flag into the
closure and check it between chunks.

For example, this body fragment counts zero bytes from a `bytes` input:

```rust
use weft::NodeErrExt;

let bytes: Vec<u8> = ctx.inputs.get("bytes")?;
let cancel = ctx.cancellation();
let count = tokio::task::spawn_blocking(move || -> weft::WeftResult<u64> {
    let mut count = 0_u64;
    for chunk in bytes.chunks(4096) {
        if cancel.is_cancelled() {
            return Err(weft::WeftError::Cancelled);
        }
        count += chunk.iter().filter(|byte| **byte == 0).count() as u64;
    }
    Ok(count)
})
.await
.node_err("counting bytes")??;
```

The first `?` handles a failure of the task, such as a panic. The second
handles the result returned by the closure. Use `count` in your node's
output afterwards.

Cancellation is checked between chunks. If one chunk calls a blocking
library function that never returns, the flag cannot interrupt that call.

## Cleanup that needs another await

You can listen for cancellation in a `tokio::select!` branch and attempt
cleanup there. That branch competes with the engine aborting your task,
so it may never run or may itself be interrupted.

If cleanup must happen even after a worker crash, it cannot depend only
on code at the end of the node body. Arrange for the external resource to
expire or for another component to perform the cleanup.

## Suspension is a different thing

`ctx.await_signal` records a wait that can resume later. Cancellation
ends the execution and removes the registration that would resume it.
A node using that API needs no separate cancellation handler.

For how a suspended body resumes, read [Surviving a restart](durable-execution.md).
