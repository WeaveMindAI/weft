//! Test-only utilities, exposed crate-wide via `#[macro_export]` so
//! any workspace crate's tests can stress-loop timing-sensitive code.
//!
//! Houses the `stress_test!` macro: a wrapper around `#[tokio::test]`
//! that runs the test body N times concurrently within one tokio
//! runtime. The same body running under N concurrent tasks on a
//! shared multi-thread runtime surfaces races (notify-arm-vs-fire
//! windows, scheduling-order assumptions, deadlock-detection
//! deadlines) that one isolated execution would let slip through.
//!
//! When to apply: stress-loop timing-sensitive tests BY CONSTRUCTION
//! instead of relying on CI re-runs to trip races by accident. Rule
//! of thumb: any test that opens a multi-thread runtime to exercise
//! tokio sync primitives (Notify, channels, firing-order or
//! stuck-detection assertions) should go through stress_test.
//!
//! Consuming crates need `futures` as a DEV-dependency: the
//! expansion calls `futures::FutureExt::catch_unwind`, resolved in
//! the consumer's namespace.
//!
//! Usage:
//!
//! ```ignore
//! stress_test!(
//!     name: my_timing_test,
//!     runs: 64,
//!     worker_threads: 4,
//!     async fn body() {
//!         // test body, panics on assertion failure as usual
//!     }
//! );
//! ```
//!
//! Expands to a single `#[tokio::test(flavor = "multi_thread",
//! worker_threads = N)]` test that joins K concurrent runs of
//! `body()`. Any panic in any run propagates as a test failure
//! identifying which iteration failed.

/// Run a test body N times concurrently inside one tokio runtime.
///
/// Each iteration runs in its own spawned task; all tasks share the
/// runtime, which is what creates contention on `tokio::sync::Notify`,
/// channels, and the scheduler. A panic in any iteration fails the
/// outer test, with the iteration index reported.
///
/// Designed for tests that exercise timing-sensitive primitives:
/// `tokio::sync::Notify::notify_one` / `notify_waiters` arm-then-check
/// races, multi-thread firing-order assertions, deadlock-detection
/// deadlines, etc. A test that uses this macro is making the rule
/// explicit: "I know this code is sensitive to scheduling; surface
/// the race deterministically here rather than wait for CI to flake."
#[macro_export]
macro_rules! stress_test {
    (
        name: $name:ident,
        runs: $runs:expr,
        worker_threads: $threads:expr,
        async fn body() $body:block
    ) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = $threads)]
        async fn $name() {
            // Panic payloads come back as Box<dyn Any>; the common
            // string forms are extracted for the failure message.
            // `catch_unwind` is fully qualified (no mid-function
            // `use`) so the macro body doesn't inject imports into
            // the consumer's scope; the consumer still needs
            // `futures` as a dev-dependency (see the module doc).
            let mut handles = Vec::with_capacity($runs);
            for iter in 0..$runs {
                handles.push(tokio::spawn(async move {
                    let body_fut = std::panic::AssertUnwindSafe(async move $body);
                    match futures::FutureExt::catch_unwind(body_fut).await {
                        Ok(()) => Ok(()),
                        Err(payload) => {
                            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                                (*s).to_string()
                            } else if let Some(s) = payload.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "<non-string panic payload>".to_string()
                            };
                            Err((iter, msg))
                        }
                    }
                }));
            }
            let mut failed: Vec<String> = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err((iter, msg))) => failed.push(format!("iter {iter}: {msg}")),
                    Err(join_err) => failed.push(format!("join error: {join_err}")),
                }
            }
            if !failed.is_empty() {
                panic!(
                    "stress_test {} failed in {}/{} iterations:\n  - {}",
                    stringify!($name),
                    failed.len(),
                    $runs,
                    failed.join("\n  - ")
                );
            }
        }
    };
}
