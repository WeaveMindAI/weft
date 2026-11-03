//! Cancellation primitive for runs and the nodes they run.
//!
//! `Arc<Notify>` alone has a fire-and-forget semantic: a `notify_waiters()`
//! call only wakes futures that are *currently* awaiting `notified()`.
//! A future created after the notify call sees nothing. That's wrong for
//! cancellation: once cancelled, every code path that observes the flag
//! later must see it.
//!
//! This wraps an `AtomicBool` (the persistent flag) plus a `Notify` (the
//! wakeup mechanism for blocked waits). `cancel_because()` sets the bool
//! AND notifies, recording why. `is_cancelled()` reads the bool
//! synchronously.
//! `cancelled()` returns a future that resolves immediately if the flag
//! is already set, or on the next notify otherwise.
//!
//! Use `is_cancelled()` at iteration boundaries (loop drivers, apply
//! pipelines) and `cancelled()` inside `tokio::select!` arms to race
//! it against work futures.
//!
//! The flag also remembers WHY, when the canceller says: a run's
//! flag is flipped by the worker acting on a `cancel_run` task, by
//! the process shutting down, or by a live caller dropping, and the terminal
//! event the driver writes afterwards has to name that cause. The first
//! cause to land is the one kept: a run cancelled twice for two reasons
//! was cancelled for the first.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::run::CancelCause;

#[derive(Debug, Default)]
pub struct CancellationFlag {
    cancelled: AtomicBool,
    notify: Notify,
    /// Why the flag was flipped. `None` only before any cancel: the one
    /// door, [`Self::cancel_because`], stores the cause before it trips
    /// the flag, so a tripped flag always carries one.
    cause: Mutex<Option<CancelCause>>,
}

impl CancellationFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Mark cancelled. Idempotent. Wakes every task currently
    /// awaiting `cancelled()`; every future call resolves
    /// immediately. Private: the only door is [`Self::cancel_because`],
    /// so a tripped flag always carries its cause and no terminal ever
    /// has to guess (a node body holds this flag through
    /// `ctx.cancellation()` and would otherwise be able to trip it
    /// causeless).
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Mark cancelled, recording why. The one door: every cancel of a
    /// run's flag goes through here, so the driver's terminal
    /// write can say what happened instead of guessing. The first
    /// cause recorded wins; a later one is ignored (the run was already
    /// cancelled for the first reason).
    pub fn cancel_because(&self, cause: CancelCause) {
        {
            let mut slot = self.cause.lock().expect("cancellation cause poisoned");
            if slot.is_none() {
                *slot = Some(cause);
            }
        }
        self.cancel();
    }

    /// The cause recorded by [`Self::cancel_because`], if any.
    pub fn cause(&self) -> Option<CancelCause> {
        self.cause.lock().expect("cancellation cause poisoned").clone()
    }

    /// Cheap synchronous check. Use at iteration boundaries.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Future that resolves the moment the flag is set. Resolves
    /// immediately if already set. Use in `tokio::select!` arms to
    /// race long-running work against cancellation.
    ///
    /// Race-safe via `tokio::pin!` + `Notified::enable()`: the
    /// `notify.notified()` future does NOT register as a waiter until
    /// it is first polled, and `notify_waiters` stores no permit, so a
    /// `cancel()` that fires between `notified()` and the await would be
    /// lost without registration. `enable()` registers synchronously
    /// before the re-check; any `cancel()` that lands after registration
    /// wakes the future, any `cancel()` that landed before is caught by
    /// the post-enable flag read.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let waiter = self.notify.notified();
        tokio::pin!(waiter);
        waiter.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        waiter.await;
    }

    /// [`Self::cancelled`] resolving to the error a node body returns
    /// to unwind as cancelled (not failed): the select-arm door, so
    /// node code never names the error type.
    /// `err = cancel.cancelled_err() => return Err(err)`.
    pub async fn cancelled_err(&self) -> crate::error::WeftError {
        self.cancelled().await;
        crate::error::WeftError::Cancelled
    }

    /// `work`'s output, or the cancelled error the moment the flag is
    /// set, whichever comes first: the door for a node's own future (a
    /// child process, its own timer, a library's call) to end with the
    /// run. `work` is dropped where it stands on a cancel.
    ///
    /// ```ignore
    /// let status = ctx.cancellation().race(child.wait()).await?.node_err("wait for the script")?;
    /// ```
    ///
    /// Everything the ctx hands out already ends this way on its own
    /// (its HTTP clients and their bodies, an endpoint call, a storage
    /// stream, a provider socket), so this is only for work of the node's
    /// own.
    pub async fn race<F: std::future::Future>(&self, work: F) -> crate::error::WeftResult<F::Output> {
        tokio::select! {
            biased;
            () = self.cancelled() => Err(crate::error::WeftError::Cancelled),
            out = work => Ok(out),
        }
    }

    /// [`Self::cancelled`] holding its own reference to the flag, for
    /// something that outlives the borrow (a stream, a response body).
    fn cancelled_owned(self: Arc<Self>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move { self.cancelled().await })
    }
}

/// Whether `error`, or anything it was caused by, is the cancelled error:
/// how a failure that crossed a library (an HTTP body read, a storage
/// stream an I/O error carries) is told apart from a real one, so the
/// node unwinds as cancelled instead of failing.
pub fn is_cancel(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut at = Some(error);
    while let Some(e) = at {
        if matches!(e.downcast_ref::<crate::error::WeftError>(), Some(crate::error::WeftError::Cancelled)) {
            return true;
        }
        // An I/O error hides what it wraps from `source`.
        if let Some(inner) = e.downcast_ref::<std::io::Error>().and_then(std::io::Error::get_ref) {
            if is_cancel(inner) {
                return true;
            }
        }
        at = e.source();
    }
    false
}

/// A stream that ends with an error the moment its run is cancelled
/// ([`guarded`]).
struct Guarded<S, E> {
    inner: S,
    cancelled: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    cancel_error: fn() -> E,
    ended: bool,
}

/// `stream`, ending with `cancel_error()` the moment `flag` is set rather
/// than whenever its source next answers. One allocation per stream, and
/// one poll of the flag per chunk read.
pub(crate) fn guarded<S, T, E>(stream: S, flag: Arc<CancellationFlag>, cancel_error: fn() -> E) -> impl futures::Stream<Item = Result<T, E>> + Send
where
    S: futures::Stream<Item = Result<T, E>> + Send + Unpin,
{
    Guarded { inner: stream, cancelled: flag.cancelled_owned(), cancel_error, ended: false }
}

impl<S, T, E> futures::Stream for Guarded<S, E>
where
    S: futures::Stream<Item = Result<T, E>> + Unpin,
{
    type Item = Result<T, E>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.cancelled.as_mut().poll(cx).is_ready() {
            this.ended = true;
            return Poll::Ready(Some(Err((this.cancel_error)())));
        }
        std::pin::Pin::new(&mut this.inner).poll_next(cx)
    }
}

/// The cancelled error as a storage stream carries it.
pub(crate) fn cancelled_io() -> std::io::Error {
    std::io::Error::other(crate::error::WeftError::Cancelled)
}

/// The middleware every client a run is handed carries
/// (`RunContext::http`, `RunContext::client`, `RunContext::open`): a
/// request ends with the cancelled error the moment the run is cancelled,
/// and so does a read of its answer's body. A run never cancelled pays one
/// poll of its flag per send and per chunk.
struct EndsWithRun(Arc<CancellationFlag>);

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for EndsWithRun {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let resp = match self.0.race(next.run(req, extensions)).await {
            Ok(answered) => answered?,
            Err(cancelled) => return Err(reqwest_middleware::Error::Middleware(anyhow::Error::new(cancelled))),
        };
        Ok(body_ends_with_run(resp, self.0.clone()))
    }
}

/// `resp` with its body read through [`guarded`], everything else about
/// it kept (its stated size included).
fn body_ends_with_run(resp: reqwest::Response, flag: Arc<CancellationFlag>) -> reqwest::Response {
    type BoxError = Box<dyn std::error::Error + Send + Sync>;
    crate::net::with_body_stream(resp, |chunks| {
        let chunks = futures::StreamExt::map(chunks, |chunk| chunk.map_err(|e| -> BoxError { Box::new(e) }));
        guarded(chunks, flag, || -> BoxError { Box::new(crate::error::WeftError::Cancelled) })
    })
}

/// `client` with [`EndsWithRun`] on it, for the run `flag` belongs to.
pub(crate) fn ends_with_run(client: reqwest_middleware::ClientWithMiddleware, flag: Arc<CancellationFlag>) -> reqwest_middleware::ClientWithMiddleware {
    reqwest_middleware::ClientBuilder::from_client(client).with(EndsWithRun(flag)).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the `Notify::notified()` arm-then-check race under a
    // multi-threaded tokio runtime: spawn many `cancelled()` waiters and
    // `cancel()`s in pairs, where each `cancel()` happens AFTER the
    // waiter is spawned but BEFORE the waiter's underlying `Notified`
    // future is first polled. With the broken pattern (future created
    // then awaited without `enable()`, `notify_waiters` storing no
    // permit), the wait deadlocks. With `pin!` + `enable()` the wait
    // completes promptly. Stress-looped to surface any future regression
    // on the first failing CI run rather than waiting for the flake.
    crate::stress_test!(
        name: cancelled_does_not_miss_notify_under_multi_thread,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..200 {
                let flag = Arc::new(CancellationFlag::new());
                let f2 = flag.clone();
                let wait = tokio::spawn(async move { f2.cancelled().await });
                let cancel = tokio::spawn(async move { flag.cancel_because(CancelCause::User) });
                let result = tokio::time::timeout(std::time::Duration::from_millis(500), wait)
                    .await
                    .expect("cancelled() must not hang under multi-threaded runtime");
                result.expect("join ok");
                let _ = cancel.await;
            }
        }
    );

    /// `cancelled()` resolves immediately if the flag is already set.
    #[tokio::test]
    async fn cancelled_returns_immediately_when_already_set() {
        let flag = CancellationFlag::new();
        flag.cancel_because(CancelCause::User);
        flag.cancelled().await;
    }

    /// `race` hands back the work's output, or the cancelled error once
    /// the flag is set, dropping the work where it stands.
    #[tokio::test]
    async fn race_ends_the_work_with_the_run() {
        let flag = Arc::new(CancellationFlag::new());
        assert_eq!(flag.race(async { 7 }).await.unwrap(), 7);
        let waiting = tokio::spawn({
            let flag = flag.clone();
            async move { flag.race(std::future::pending::<()>()).await }
        });
        flag.cancel_because(CancelCause::User);
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), waiting).await.expect("it ended").unwrap();
        assert!(matches!(ended, Err(crate::error::WeftError::Cancelled)));
        assert!(matches!(flag.race(async { 7 }).await, Err(crate::error::WeftError::Cancelled)), "already cancelled");
    }

    /// A server that takes every connection and reads its request, then
    /// answers `/body` with a head and the first byte of a ten-byte body
    /// and nothing more, and anything else with nothing at all.
    async fn quiet_server() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0u8; 4096];
                let read = socket.read(&mut request).await.unwrap();
                if String::from_utf8_lossy(&request[..read]).starts_with("GET /body") {
                    socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nx").await.unwrap();
                }
                held.push(socket);
            }
        });
        format!("http://{addr}")
    }

    // A cancelled run ends a request its client sent, and a read of an
    // answer's body, with the cancelled error, however long the server
    // would have kept them waiting.
    crate::stress_test!(
        name: a_cancelled_run_ends_a_request_and_a_body_read,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let base = quiet_server().await;
            let flag = Arc::new(CancellationFlag::new());
            let client = ends_with_run(crate::access::client::plain_client(), flag.clone());
            let sent = tokio::spawn({
                let client = client.clone();
                let url = format!("{base}/slow");
                async move { crate::access::client::checked_send(client.get(url), "ask the slow server").await.map(|_| ()) }
            });
            flag.cancel_because(CancelCause::User);
            let ended = tokio::time::timeout(std::time::Duration::from_secs(5), sent).await.expect("the send ended").unwrap();
            assert!(matches!(ended, Err(crate::error::WeftError::Cancelled)), "{ended:?}");

            let flag = Arc::new(CancellationFlag::new());
            let client = ends_with_run(crate::access::client::plain_client(), flag.clone());
            let answer = client.get(format!("{base}/body")).send().await.unwrap();
            assert_eq!(answer.headers()["content-length"], "10", "the head is kept");
            let read = tokio::spawn(async move { answer.text().await });
            flag.cancel_because(CancelCause::User);
            let failed = tokio::time::timeout(std::time::Duration::from_secs(5), read).await.expect("the read ended").unwrap().unwrap_err();
            assert!(is_cancel(&failed), "{failed:?}");
            let refused = crate::access::client::body_error("read the answer", failed);
            assert!(matches!(refused, crate::error::WeftError::Cancelled), "{refused:?}");
        }
    );

    /// An answer read through a run's client still states its size.
    #[tokio::test]
    async fn an_answer_keeps_its_size_through_the_run_guard() {
        let base = quiet_server().await;
        let client = ends_with_run(crate::access::client::plain_client(), CancellationFlag::new_arc());
        let answer = client.get(format!("{base}/body")).send().await.unwrap();
        assert_eq!(answer.content_length(), Some(10));
    }

    /// A stream ends with the cancelled error once its run is cancelled,
    /// and passes its chunks through until then.
    #[tokio::test]
    async fn a_guarded_stream_ends_with_its_run() {
        use futures::StreamExt;
        let flag = Arc::new(CancellationFlag::new());
        let first = futures::stream::once(async { Ok::<_, std::io::Error>(1) });
        let mut stream = guarded(Box::pin(first.chain(futures::stream::pending())), flag.clone(), cancelled_io);
        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        flag.cancel_because(CancelCause::User);
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(is_cancel(&err), "{err:?}");
        assert!(stream.next().await.is_none(), "and ends there");
    }

    /// The first cause recorded is the one the terminal write reads; a
    /// second cancel for a different reason changes nothing, and an
    /// untripped flag carries none.
    #[test]
    fn first_cause_wins() {
        let flag = CancellationFlag::new();
        assert_eq!(flag.cause(), None);
        flag.cancel_because(CancelCause::CallerGone);
        flag.cancel_because(CancelCause::User);
        assert!(flag.is_cancelled());
        assert_eq!(flag.cause(), Some(CancelCause::CallerGone));
    }
}
