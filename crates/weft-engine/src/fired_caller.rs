//! The stand-in caller: what plays the person on the other end when a
//! route's program is FIRED rather than reached.
//!
//! A Route and every Reply behind it ask for the live caller, so with
//! nobody there the whole program is unrunnable. That made trying a
//! route cost a cluster, an activation and an image build before the
//! first value, which is minutes instead of seconds. This serves the
//! body the author typed and writes what the program answers into the
//! journal, so `weft follow` shows the exchange the way it shows a real
//! one.
//!
//! It is a real `CallerConnection`, not a special case threaded through
//! the nodes: the trigger, the Reply, the Stream and the Close all run
//! their ordinary code against it and never learn the difference. That
//! is the whole point, because a loop that exercises a DIFFERENT path
//! from production teaches you about the fake.
//!
//! HTTP only. A socket's whole shape is a conversation over time, and
//! there is nothing honest to invent for the caller's next message, so
//! `Socket` still needs a real client and says so.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use weft_core::caller::{
    CallerConnection, CallerError, CallerRuntimeConfig, CloseReason, HttpRequestParts,
    InboundMessage, LiveRequest, OutboundChunk, ResponseHead,
};
use weft_core::signal::Protocol;
use weft_core::Color;

use crate::caller_conn::CallerJournalSink;

/// A caller that was never there: the request came from `--fire` and
/// the answer goes to the journal.
pub struct FiredCaller {
    color: Color,
    config: CallerRuntimeConfig,
    parts: Arc<HttpRequestParts>,
    journal: Arc<dyn CallerJournalSink>,
    /// The journal offsets this exchange has used. The same counter a
    /// real connection keeps, so the rows read in the same order.
    offset: AtomicU64,
    started: AtomicBool,
    /// The terminate latch, as a watch so `disconnected()` wakes when
    /// the program ends the exchange.
    terminated: tokio::sync::watch::Sender<bool>,
}

impl FiredCaller {
    /// Build one for a fired run and record that the exchange opened.
    ///
    /// The request is the one the fire payload described. There is no
    /// body and never can be: nobody sent anything, so nothing came in,
    /// nothing is recorded as coming in, and the request the node reads
    /// carries an empty one. What the author typed as a body reaches
    /// the trigger as its wake payload and is read there, by the node
    /// that knows which of its own fields that is.
    pub fn open(
        color: Color,
        config: CallerRuntimeConfig,
        request: LiveRequest,
        journal: Arc<dyn CallerJournalSink>,
    ) -> Arc<Self> {
        let caller = Arc::new(Self {
            color,
            config,
            parts: Arc::new(HttpRequestParts {
                request,
                body: InboundMessage::Json(serde_json::Value::Null),
            }),
            journal,
            offset: AtomicU64::new(0),
            started: AtomicBool::new(false),
            terminated: tokio::sync::watch::Sender::new(false),
        });
        let at = caller.next_offset();
        caller.journal.connected(color, at, Protocol::Http);
        caller
    }

    fn next_offset(&self) -> u64 {
        self.offset.fetch_add(1, Ordering::SeqCst)
    }

    /// Record what the program sent. `head` rides the first item only,
    /// exactly as a real connection commits its status line once, so a
    /// program that sets a head too late is refused here rather than
    /// passing offline and failing against a real caller.
    fn record(
        &self,
        head: Option<ResponseHead>,
        chunk: Option<OutboundChunk>,
        terminal: bool,
    ) -> Result<(), CallerError> {
        if *self.terminated.borrow() {
            return Err(CallerError::AlreadyTerminated);
        }
        if head.is_some() && self.started.swap(true, Ordering::SeqCst) {
            return Err(CallerError::HeadAlreadySent);
        }
        self.started.store(true, Ordering::SeqCst);
        if terminal {
            self.terminated.send_replace(true);
        }
        // A bare close carries no chunk, and NOTHING is recorded for it:
        // over a real connection the body simply ends, so inventing an
        // empty message here would show a caller something the wire
        // never carries. That is worse than showing nothing, because a
        // reader believes it: it once taught a frontend that a stream
        // ends with a null line, and the contract built on that was
        // wrong against real HTTP. The exchange still reads as closed,
        // through the disconnect below.
        if let Some(chunk) = chunk {
            let at = self.next_offset();
            self.journal.outbound(self.color, at, &chunk, terminal);
        }
        if terminal {
            let at = self.next_offset();
            self.journal.disconnected(self.color, at, "the fired run answered");
        }
        Ok(())
    }
}

#[async_trait]
impl CallerConnection for FiredCaller {
    fn config(&self) -> &CallerRuntimeConfig {
        &self.config
    }

    /// Always attached, right up until the program ends the exchange.
    /// Nothing can drop: there is no socket to lose.
    fn is_connected(&self) -> bool {
        !*self.terminated.borrow()
    }

    async fn disconnected(&self) {
        let mut rx = self.terminated.subscribe();
        let _ = rx.wait_for(|terminated| *terminated).await;
    }

    fn wire_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// The one call that would WAIT against a real caller returns at
    /// once: the request is already here.
    async fn ensure_connected(&self) -> Result<(), CallerError> {
        Ok(())
    }

    fn handshake(&self) -> Arc<LiveRequest> {
        Arc::new(self.parts.request.clone())
    }

    async fn send_chunk(
        &self,
        head: Option<ResponseHead>,
        chunk: OutboundChunk,
    ) -> Result<(), CallerError> {
        self.record(head, Some(chunk), false)
    }

    async fn terminate(
        &self,
        head: Option<ResponseHead>,
        final_chunk: Option<OutboundChunk>,
        _close: Option<CloseReason>,
    ) -> Result<(), CallerError> {
        self.record(head, final_chunk, true)
    }

    async fn receive(&self, _cursor: &AtomicU64) -> Result<InboundMessage, CallerError> {
        Err(CallerError::WrongProtocol { protocol: "http" })
    }

    async fn request(
        &self,
        _msg: OutboundChunk,
        _cursor: &AtomicU64,
    ) -> Result<InboundMessage, CallerError> {
        Err(CallerError::WrongProtocol { protocol: "http" })
    }

    fn http_request(&self) -> Result<Arc<HttpRequestParts>, CallerError> {
        Ok(self.parts.clone())
    }

    fn inbound_now_offset(&self) -> u64 {
        0
    }

    fn inbound_attach_offset(&self) -> u64 {
        0
    }

    fn inbound_retained_floor(&self) -> u64 {
        0
    }

    fn last_inbound_offset(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records what a real journal would have written, so a test can
    /// read the exchange back the way `weft follow` does.
    #[derive(Default)]
    struct Rows(Mutex<Vec<String>>);

    impl CallerJournalSink for Rows {
        fn connected(&self, _: Color, at: u64, p: Protocol) {
            self.0.lock().unwrap().push(format!("{at} connected {}", p.as_wire_str()));
        }
        fn inbound(&self, _: Color, at: u64, msg: &InboundMessage) {
            self.0.lock().unwrap().push(format!("{at} inbound {msg:?}"));
        }
        fn outbound(&self, _: Color, at: u64, chunk: &OutboundChunk, terminal: bool) {
            self.0.lock().unwrap().push(format!("{at} outbound {chunk:?} terminal={terminal}"));
        }
        fn errored(&self, _: Color, at: u64, message: &str) {
            self.0.lock().unwrap().push(format!("{at} errored {message}"));
        }
        fn disconnected(&self, _: Color, at: u64, reason: &str) {
            self.0.lock().unwrap().push(format!("{at} disconnected {reason}"));
        }
    }

    fn caller(rows: &Arc<Rows>) -> Arc<FiredCaller> {
        FiredCaller::open(
            Color::nil(),
            CallerRuntimeConfig::from_config(
                &serde_json::from_value(serde_json::json!({ "path": "hello" }))
                    .expect("a route config with just a path"),
                Protocol::Http,
            ),
            LiveRequest { method: "POST".into(), path: "hello".into(), ..Default::default() },
            rows.clone(),
        )
    }

    /// The whole exchange lands in the journal, in order, with the
    /// answer marked terminal. That is what makes a fired route
    /// readable: the request went in, this came back.
    #[tokio::test]
    async fn the_exchange_is_journalled_the_way_a_real_one_is() {
        let rows = Arc::new(Rows::default());
        let caller = caller(&rows);
        assert!(caller.is_connected());
        assert!(!caller.wire_started(), "nothing has gone out yet");
        caller.ensure_connected().await.expect("the request is already here");

        caller
            .terminate(
                Some(ResponseHead::new(201)),
                Some(OutboundChunk::Json(serde_json::json!({ "greeting": "hello ada" }))),
                None,
            )
            .await
            .expect("the program answers");

        // Connected, answered, gone. No INBOUND row: a fired run has no
        // caller and nobody sent anything, so the conversation records
        // nothing coming in. What the author typed as the body reaches
        // the trigger as its wake payload and is recorded on the firing,
        // which is where it arrived.
        let rows = rows.0.lock().unwrap().clone();
        assert_eq!(rows.len(), 3, "{rows:#?}");
        assert!(rows[0].contains("connected http"), "{rows:#?}");
        assert!(!rows.iter().any(|r| r.contains("inbound")), "nobody spoke: {rows:#?}");
        assert!(rows[1].contains("outbound") && rows[1].contains("terminal=true"), "{rows:#?}");
        assert!(rows[2].contains("disconnected"), "{rows:#?}");
        assert!(!caller.is_connected(), "the exchange is over");
    }

    /// The node asks for the request through the same call a real
    /// caller serves, which is what lets the trigger run its ordinary
    /// code offline. It gets the request line, and an empty body:
    /// nobody sent one. That emptiness is the node's cue to look at its
    /// own wake payload, where the author's typed body actually is.
    #[test]
    fn the_request_arrives_with_no_body_behind_it() {
        let rows = Arc::new(Rows::default());
        let parts = caller(&rows).http_request().expect("an http caller has request parts");
        assert_eq!(parts.request.method, "POST");
        assert_eq!(parts.body, InboundMessage::Json(serde_json::Value::Null));
    }

    /// The rules a real connection enforces are enforced here too, so a
    /// program that only works offline cannot exist: a second answer is
    /// refused, and so is a head after the wire started.
    #[tokio::test]
    async fn it_refuses_what_a_real_caller_refuses() {
        let rows = Arc::new(Rows::default());
        let caller = caller(&rows);
        caller
            .send_chunk(None, OutboundChunk::Text("first".into()))
            .await
            .expect("a stream's first chunk");
        assert!(caller.wire_started());
        assert!(
            matches!(
                caller.send_chunk(Some(ResponseHead::new(200)), OutboundChunk::Text("x".into())).await,
                Err(CallerError::HeadAlreadySent)
            ),
            "the status line was committed by the first chunk"
        );
        caller.terminate(None, None, None).await.expect("the stream ends");
        assert!(
            matches!(caller.terminate(None, None, None).await, Err(CallerError::AlreadyTerminated)),
            "one terminal per exchange"
        );
    }

    /// A bare close records NO message, because a real connection
    /// carries none: the body just ends. Inventing an empty one here
    /// shipped once and taught a frontend that a stream ends with a null
    /// line, which is not true over HTTP, so the contract built on it
    /// was wrong. The exchange still reads as closed.
    #[tokio::test]
    async fn a_bare_close_invents_no_message() {
        let rows = Arc::new(Rows::default());
        let caller = caller(&rows);
        caller
            .send_chunk(None, OutboundChunk::Text("only this".into()))
            .await
            .expect("the one real chunk");
        caller.terminate(None, None, None).await.expect("ends with nothing to say");

        let rows = rows.0.lock().unwrap().clone();
        let outbound: Vec<_> = rows.iter().filter(|r| r.contains("outbound")).collect();
        assert_eq!(outbound.len(), 1, "only what the program actually sent: {rows:#?}");
        assert!(outbound[0].contains("only this"), "{rows:#?}");
        assert!(
            rows.iter().any(|r| r.contains("disconnected")),
            "the exchange still reads as closed: {rows:#?}"
        );
    }

    /// A socket is not fireable, so nothing here pretends to be one.
    #[tokio::test]
    async fn a_socket_read_is_the_wrong_protocol() {
        let rows = Arc::new(Rows::default());
        let caller = caller(&rows);
        let cursor = AtomicU64::new(0);
        assert!(matches!(
            caller.receive(&cursor).await,
            Err(CallerError::WrongProtocol { .. })
        ));
    }
}
