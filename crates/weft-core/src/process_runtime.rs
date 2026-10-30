//! The tokio runtime every weft process runs on: the daemon
//! (`weft-runtime`) and every project's worker.

// Without the flag, the runtime below cannot turn its LIFO slot off. The
// workspace's `.cargo/config.toml` sets it for weft's own builds, and
// every worker build sets it (`crate::builds::WORKER_RUSTFLAGS`).
// A build that dropped it (a `RUSTFLAGS` of its own replaces the config's)
// is refused rather than shipped able to freeze.
#[cfg(not(tokio_unstable))]
compile_error!(
    "weft's runtime is built with `--cfg tokio_unstable` (the workspace's .cargo/config.toml sets it, and every \
     worker build does): build weft from its checkout, add the flag to any RUSTFLAGS of your own, and keep \
     `ENV RUSTFLAGS=\"{{worker_rustflags}}\"` before the cargo build of a custom worker Dockerfile template"
);

/// The multi-thread runtime of one weft process: one worker thread per
/// core, as `#[tokio::main]` makes but never fewer than two, with the
/// LIFO slot off.
///
/// A task a worker spawns or wakes goes into that worker's LIFO slot,
/// which no other worker can take and which wakes none of them, and the
/// sockets are only watched by a worker with nothing to run. So a worker
/// that picks up a connection (or a task an answer woke) and then runs
/// long in one poll leaves every other worker asleep and nobody watching
/// the sockets: the whole process stops answering until that poll ends
/// (measured: a 50 s replay froze the daemon for 50 s, `/health`
/// included, with 23 threads idle; a node body doing heavy work inline
/// does the same to its worker's door and lease). With the slot off,
/// such a task lands in the worker's ordinary queue, which wakes a
/// sleeping worker, and that one takes the sockets over. That takes a
/// second worker, which a one-core machine would not have.
pub fn build() -> std::io::Result<tokio::runtime::Runtime> {
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    tokio::runtime::Builder::new_multi_thread().worker_threads(cores.max(2)).enable_all().disable_lifo_slot().build()
}
