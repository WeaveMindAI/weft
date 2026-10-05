//! The line: one WebSocket from a process to the broker, kept open for
//! the life of the process, that every JSON call of every broker client
//! of that process travels on, and that the broker pushes news down.
//!
//! A call used to be an HTTPS request of its own, and a call the broker
//! holds open until something happens (a cancel, new journal rows, a task
//! ending) held one of the broker's request slots for its whole wait.
//! On Cloud Run a slot is a seat at an instance: twenty runs each holding
//! two waits filled a broker instance with requests doing nothing but
//! wait, and the platform turned the next calls away while it started
//! more. On the line a process holds one seat however much it asks or
//! waits, and a call pays for a frame instead of a request through the
//! platform's front door.
//!
//! Calls are numbered and answered out of order, so a call the broker
//! holds never delays one behind it. Each call carries the same headers
//! it would carry as a request (the bearer read fresh, the replica and
//! the role), and the broker runs it through the very handler the
//! request would have reached, so the line adds no second way to be
//! authorized.
//!
//! The broker pushes the notifications a caller may see (`Notice`): a
//! worker hears when its project's infra copies or connections change,
//! which is what lets it keep them in memory (`weft_task_store::held_copy`
//! over [`BrokerLink::subscribe`]). While the line is down nothing is
//! heard, and the subscription says so ([`Heard::Lost`]) so a copy kept on
//! it stops trusting itself until the line is back ([`Heard::Recheck`]).
//!
//! When the line breaks it comes back by itself. A call that had not been
//! written yet goes out on the next line if that opens within the call's
//! wait to send, and fails [`LineError::NotSent`] otherwise. One that had
//! been written is answered [`LineError::Dropped`], since it may have
//! landed. The callers decide what is safe to make again: a journal write
//! that was never sent is sent again (`client::never_sent`), a read of a
//! run's history is asked again on either (`client::read_until_answered`),
//! and any other call fails the way a request whose connection reset does.
//!
//! Byte streams (`ctx.storage` uploads and downloads) stay ordinary
//! requests: a large body on the line would hold up every call behind it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use weft_task_store::pg_signal::{Heard, Subscription};

use crate::token::TokenSource;

/// The broker's path for opening a line.
// SYNC: LINE_PATH <-> crates/weft-broker/src/line.rs (routes)
pub const LINE_PATH: &str = "/v1/line";

/// How long a call waits: to be written on the line (while the line is
/// down, or still opening), and, counted from the call, for its answer.
#[derive(Debug, Clone, Copy)]
pub struct CallWait {
    pub to_send: Duration,
    pub to_answer: Duration,
}

impl CallWait {
    /// Up to `wait` for both: a call that may wait out a broker that is
    /// restarting as long as it may wait for its answer.
    pub const fn within(wait: Duration) -> Self {
        Self { to_send: wait, to_answer: wait }
    }
}

/// How long a call waits, unless it says otherwise.
pub const DEFAULT_CALL_WAIT: CallWait = CallWait::within(Duration::from_secs(30));

/// The first wait before opening the line again after it could not be
/// opened; each further failure doubles it, up to [`RECONNECT_LONGEST`].
const RECONNECT_FIRST: Duration = Duration::from_millis(100);
const RECONNECT_LONGEST: Duration = Duration::from_secs(5);

/// How long one attempt to open the line may take: a broker instance that
/// is still starting answers within it, and an address that swallows the
/// attempt (no refusal, no answer) is given up on and tried again rather
/// than waited on for as long as the system's own connect timeout.
const CONNECT_WAIT: Duration = Duration::from_secs(10);

/// How long a call made on behalf of someone waiting right now (a live
/// caller, a person in the editor) waits to be written: long enough for one
/// attempt to open the line and the wait before the next, and short enough
/// that a broker that is down is said to them at once rather than after
/// the call's whole wait.
pub const REOPEN_WAIT: Duration = Duration::from_secs(CONNECT_WAIT.as_secs() + RECONNECT_LONGEST.as_secs());

/// How often the line proves it is alive while nothing else crosses it,
/// and how long it waits for the proof before it counts as dead. A line
/// whose far end vanished without a close (the instance was cut off)
/// would otherwise hold every call written on it until its own wait ran
/// out.
const PING_EVERY: Duration = Duration::from_secs(20);
const PONG_WAIT: Duration = Duration::from_secs(15);

/// How many notices a slow subscriber may fall behind by before it is
/// told to look again instead (`broadcast`'s lag).
const NOTICE_CAPACITY: usize = 1024;

/// Why a call on the line got no answer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LineError {
    /// The call never left this process: the line was down for the
    /// call's whole wait. Nothing landed, so it is safe to send again.
    #[error("the broker could not be reached for {waited:?} (line: {why})")]
    NotSent { waited: Duration, why: String },
    /// The line broke after the call was written: it may have landed.
    #[error("the line to the broker broke after the call was sent: {0}")]
    Dropped(String),
    /// The call was written and no answer came within its wait.
    #[error("the broker did not answer within {0:?}")]
    NoAnswer(Duration),
}

/// The broker's answer to one call: what the request would have been
/// answered with.
#[derive(Debug)]
pub struct Answer {
    pub status: reqwest::StatusCode,
    pub body: Vec<u8>,
}

/// One call's head, framed before its body
/// (`[u32 head length][head JSON][body]`, big-endian length).
// SYNC: CallHead / AnswerHead / Notice <-> crates/weft-broker/src/line.rs
#[derive(Debug, Serialize, Deserialize)]
pub struct CallHead {
    pub id: u64,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

/// One answer's head, framed like a call's.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnswerHead {
    pub id: u64,
    pub status: u16,
}

/// What the broker says on the line besides answers, as text frames.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Notice {
    /// Sent by the caller once the line is open: push the notifications
    /// this caller may see.
    Follow,
    /// The broker follows for this caller; `listening` is whether its own
    /// database listener hears right now.
    Following { listening: bool },
    /// One notification this caller may see.
    Signal { channel: String, payload: String },
    /// The broker's database listener lost its connection: nothing is
    /// heard until a `Recheck`.
    Lost,
    /// Notifications may have been missed: look again.
    Recheck,
}

/// Frame `head` and `body` for the line.
pub fn frame<H: Serialize>(head: &H, body: &[u8]) -> Vec<u8> {
    let head = serde_json::to_vec(head).expect("a frame head serializes");
    let mut out = Vec::with_capacity(4 + head.len() + body.len());
    out.extend_from_slice(&(head.len() as u32).to_be_bytes());
    out.extend_from_slice(&head);
    out.extend_from_slice(body);
    out
}

/// Split a framed message into its head and its body.
pub fn unframe<H: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<(H, &[u8])> {
    anyhow::ensure!(bytes.len() >= 4, "a line frame shorter than its length prefix");
    let len = u32::from_be_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
    anyhow::ensure!(bytes.len() >= 4 + len, "a line frame shorter than its head");
    let head = serde_json::from_slice(&bytes[4..4 + len])?;
    Ok((head, &bytes[4 + len..]))
}

/// The notification channels a line may carry, by name: what turns a
/// name the broker sent back into the `&'static str` a [`Heard`] holds.
// SYNC: LINE_CHANNELS <-> crates/weft-broker/src/line.rs (audience), crates/weft-dispatcher/src/infra_node.rs (infra_node_status_notify), crates/weft-dispatcher/src/project_store.rs (project_declared_infra_notify), crates/weft-access-store/src/lib.rs (access_notify)
pub const LINE_CHANNELS: &[&str] = &[INFRA_STATUS_CHANNEL, ACCESS_CHANNEL];

/// Announced with a project's id when one of its infra copies comes,
/// goes, changes status or answers at another address, and when the
/// project is registered with another program (which may declare other
/// infra).
pub const INFRA_STATUS_CHANNEL: &str = "weft_infra_status";

/// Announced when a connection, an install's pick of one, or a value an
/// instance provides changes or goes: with its project's id, or
/// `tenant:<tenant>` for a connection shared across the tenant's projects.
pub const ACCESS_CHANNEL: &str = "weft_access";

/// A process's way to the broker. Every broker client of the process
/// holds the same one, so the process has one line, opened on the first
/// call and kept.
#[derive(Clone)]
pub struct BrokerLink {
    inner: Arc<LinkInner>,
}

struct LinkInner {
    base_url: String,
    token: TokenSource,
    /// Byte streams, which stay requests (see the module doc).
    http: reqwest::Client,
    line: OnceLock<Arc<Line>>,
    /// Dropped with the last [`BrokerLink`], which tells the line's task
    /// to close the line: nothing will call on it again.
    _closing: tokio::sync::watch::Sender<()>,
    closed: tokio::sync::watch::Receiver<()>,
}

impl BrokerLink {
    pub fn new(base_url: impl Into<String>, token: TokenSource) -> Self {
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_CALL_WAIT.to_answer)
            .build()
            .expect("reqwest client builds");
        let (closing, closed) = tokio::sync::watch::channel(());
        Self {
            inner: Arc::new(LinkInner {
                base_url: base_url.into().trim_end_matches('/').to_string(),
                token,
                http,
                line: OnceLock::new(),
                _closing: closing,
                closed,
            }),
        }
    }

    /// The broker's base URL.
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    pub fn token(&self) -> &TokenSource {
        &self.inner.token
    }

    /// The client for the calls that stay requests (byte streams).
    pub fn http(&self) -> &reqwest::Client {
        &self.inner.http
    }

    /// Call `path` with the JSON `body`, waiting as `wait` says.
    pub async fn call(&self, path: &str, body: Vec<u8>, wait: CallWait) -> Result<Answer, anyhow::Error> {
        let bearer = self.inner.token.read(&self.inner.base_url).await?;
        let mut headers = vec![
            ("authorization".to_string(), format!("Bearer {bearer}")),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        headers.extend(self.inner.token.headers().into_iter().map(|(k, v)| (k.to_string(), v)));
        Ok(self.line().call(path, headers, body, wait).await?)
    }

    /// What the broker pushes on this process's line (see the module doc).
    pub fn subscribe(&self) -> Subscription {
        let line = self.line();
        Subscription::with_listening(line.notices.subscribe(), line.listening.clone())
    }

    fn line(&self) -> &Arc<Line> {
        self.inner.line.get_or_init(|| Line::open(Arc::downgrade(&self.inner), self.inner.closed.clone()))
    }
}

/// One call waiting on the line.
struct Waiting {
    /// The framed call until it is written, `None` once it is.
    frame: Option<Vec<u8>>,
    answer: oneshot::Sender<Result<Answer, LineError>>,
}

/// Takes a call off the line when its caller stops waiting for it, however
/// it stops (an answer already took it off; that is fine).
struct Forget<'a> {
    calls: &'a Mutex<HashMap<u64, Waiting>>,
    id: u64,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        self.calls.lock().expect("line calls").remove(&self.id);
    }
}

struct Line {
    calls: Mutex<HashMap<u64, Waiting>>,
    /// The ids of calls to write, in order.
    outbox: mpsc::UnboundedSender<u64>,
    next_id: AtomicU64,
    notices: broadcast::Sender<Heard>,
    /// Whether what the line pushes can be trusted right now (see
    /// `pg_signal::Subscription::listening`).
    listening: Arc<AtomicBool>,
    /// Why the line is down, while it is.
    down: Mutex<Option<String>>,
}

impl Line {
    fn open(link: Weak<LinkInner>, closed: tokio::sync::watch::Receiver<()>) -> Arc<Self> {
        let (outbox, ids) = mpsc::unbounded_channel();
        let (notices, _) = broadcast::channel(NOTICE_CAPACITY);
        let line = Arc::new(Self {
            calls: Mutex::new(HashMap::new()),
            outbox,
            next_id: AtomicU64::new(1),
            notices,
            listening: Arc::new(AtomicBool::new(false)),
            down: Mutex::new(Some("not opened yet".into())),
        });
        tokio::spawn(keep_open(link, Arc::downgrade(&line), ids, closed));
        line
    }

    async fn call(&self, path: &str, headers: Vec<(String, String)>, body: Vec<u8>, wait: CallWait) -> Result<Answer, LineError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = frame(&CallHead { id, path: path.to_string(), headers }, &body);
        let (answer, answered) = oneshot::channel();
        self.calls.lock().expect("line calls").insert(id, Waiting { frame: Some(frame), answer });
        // A caller that stops waiting (its own timeout, a cancelled task)
        // takes its call off the line, so an abandoned write is never sent
        // later behind its back.
        let _forget = Forget { calls: &self.calls, id };
        // The receiving end lives as long as the line's task, which lives
        // as long as this line: a send can only fail once neither does.
        let _ = self.outbox.send(id);
        let start = tokio::time::Instant::now();
        let to_send = wait.to_send.min(wait.to_answer);
        tokio::pin!(answered);
        let landed = |result: Result<Result<Answer, LineError>, oneshot::error::RecvError>| {
            result.unwrap_or_else(|_| Err(LineError::Dropped("the line stopped".into())))
        };
        tokio::select! {
            result = &mut answered => return landed(result),
            () = tokio::time::sleep_until(start + to_send) => {}
        }
        {
            // Under the lock the writer takes frames with, so a call is
            // either still unwritten here, and never will be, or written.
            let mut calls = self.calls.lock().expect("line calls");
            if calls.get(&id).is_some_and(|waiting| waiting.frame.is_some()) {
                calls.remove(&id);
                return Err(LineError::NotSent {
                    waited: to_send,
                    why: self.down.lock().expect("line down").clone().unwrap_or_else(|| "busy".into()),
                });
            }
        }
        match tokio::time::timeout_at(start + wait.to_answer, &mut answered).await {
            Ok(result) => landed(result),
            Err(_) => Err(LineError::NoAnswer(wait.to_answer)),
        }
    }

    /// The line broke: every written call may have landed and is told so;
    /// the unwritten ones wait for the next line.
    fn broke(&self, why: &str) {
        self.listening.store(false, Ordering::Release);
        let _ = self.notices.send(Heard::Lost);
        *self.down.lock().expect("line down") = Some(why.to_string());
        let mut calls = self.calls.lock().expect("line calls");
        let written: Vec<u64> = calls.iter().filter(|(_, w)| w.frame.is_none()).map(|(id, _)| *id).collect();
        for id in written {
            if let Some(waiting) = calls.remove(&id) {
                let _ = waiting.answer.send(Err(LineError::Dropped(why.to_string())));
            }
        }
    }

    fn answered(&self, id: u64, answer: Answer) {
        if let Some(waiting) = self.calls.lock().expect("line calls").remove(&id) {
            let _ = waiting.answer.send(Ok(answer));
        }
    }

    /// The frame to write for `id`, marking it written; `None` when its
    /// caller already gave up. From here on the call counts as sent, even
    /// if the write then fails: part of it may have reached the broker.
    fn take_frame(&self, id: u64) -> Option<Vec<u8>> {
        self.calls.lock().expect("line calls").get_mut(&id).and_then(|w| w.frame.take())
    }

    fn heard(&self, notice: Notice) {
        match notice {
            Notice::Following { listening: true } | Notice::Recheck => {
                self.listening.store(true, Ordering::Release);
                let _ = self.notices.send(Heard::Recheck);
            }
            Notice::Following { listening: false } | Notice::Lost => {
                self.listening.store(false, Ordering::Release);
                let _ = self.notices.send(Heard::Lost);
            }
            Notice::Signal { channel, payload } => match LINE_CHANNELS.iter().find(|c| **c == channel) {
                Some(channel) => {
                    let _ = self.notices.send(Heard::Signal { channel, payload: payload.into() });
                }
                None => tracing::warn!(target: "weft_broker_client::line", %channel, "the broker pushed a channel this build does not know"),
            },
            Notice::Follow => tracing::warn!(target: "weft_broker_client::line", "the broker sent a follow request"),
        }
    }
}

/// Keep the line open for as long as anything holds it: open it, serve
/// it until it breaks, open it again. Once the process lets go of its
/// last [`BrokerLink`] (`closed`), the line is closed and stays closed.
async fn keep_open(
    link: Weak<LinkInner>,
    line: Weak<Line>,
    mut ids: mpsc::UnboundedReceiver<u64>,
    mut closed: tokio::sync::watch::Receiver<()>,
) {
    let mut retry = RECONNECT_FIRST;
    loop {
        let (Some(inner), Some(held)) = (link.upgrade(), line.upgrade()) else { return };
        // What opening needs, taken out so nothing holds the link while it
        // opens: the last link let go of mid-attempt closes the line.
        let (base_url, token) = (inner.base_url.clone(), inner.token.clone());
        drop(inner);
        let opened = tokio::select! {
            opened = tokio::time::timeout(CONNECT_WAIT, connect(&base_url, &token)) => {
                opened.unwrap_or_else(|_| Err(anyhow::anyhow!("opening it took more than {CONNECT_WAIT:?}")))
            }
            _ = closed.changed() => return,
        };
        match opened {
            Ok(socket) => {
                retry = RECONNECT_FIRST;
                *held.down.lock().expect("line down") = None;
                match serve(socket, &held, &mut ids, &mut closed).await {
                    Served::LetGo => return,
                    Served::Broke(why) => {
                        tracing::warn!(target: "weft_broker_client::line", reason = %why, "the line to the broker broke; opening it again");
                        held.broke(&why);
                    }
                }
            }
            Err(e) => {
                let why = format!("{e:#}");
                tracing::warn!(target: "weft_broker_client::line", error = %why, retry_in_ms = retry.as_millis() as u64, "could not open the line to the broker");
                *held.down.lock().expect("line down") = Some(why);
                drop(held);
                tokio::select! {
                    () = tokio::time::sleep(retry) => {}
                    _ = closed.changed() => return,
                }
                retry = (retry * 2).min(RECONNECT_LONGEST);
            }
        }
    }
}

/// How one open line ended.
enum Served {
    /// It broke, for this reason; it is opened again.
    Broke(String),
    /// The process let go of its last link: nothing will call on it again.
    LetGo,
}

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(base_url: &str, token: &TokenSource) -> anyhow::Result<Socket> {
    let url = line_url(base_url)?;
    let mut request = url.as_str().into_client_request()?;
    let bearer = token.read(base_url).await?;
    let headers = request.headers_mut();
    headers.insert("authorization", format!("Bearer {bearer}").parse()?);
    for (name, value) in token.headers() {
        headers.insert(name, value.parse()?);
    }
    let connector = match url.starts_with("wss://") {
        true => Some(tokio_tungstenite::Connector::Rustls(weft_core::net::tls_config().map_err(anyhow::Error::msg)?)),
        false => None,
    };
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        // The broker's answers are as large as what it reads back (a
        // run's whole journal); it is weft's own, so nothing caps them
        // that would not cap the same answer as a request.
        max_message_size: None,
        max_frame_size: None,
        ..Default::default()
    };
    let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(request, Some(config), true, connector).await?;
    Ok(socket)
}

/// The line's address for the broker at `base_url`.
fn line_url(base_url: &str) -> anyhow::Result<String> {
    let rest = base_url
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| base_url.strip_prefix("http://").map(|rest| format!("ws://{rest}")))
        .ok_or_else(|| anyhow::anyhow!("the broker's address '{base_url}' is neither http nor https"))?;
    Ok(format!("{rest}{LINE_PATH}"))
}

/// Serve one open line until it breaks, or until the process lets go of
/// its last link (`closed`).
///
/// Every write is bounded by [`PONG_WAIT`]: a broker that vanished without
/// closing leaves the socket's buffer full and a write waiting for minutes,
/// which would also keep the ping's deadline from ever being looked at.
async fn serve(
    socket: Socket,
    line: &Line,
    ids: &mut mpsc::UnboundedReceiver<u64>,
    closed: &mut tokio::sync::watch::Receiver<()>,
) -> Served {
    let (mut sink, mut stream) = socket.split();
    let follow = serde_json::to_string(&Notice::Follow).expect("a notice serializes");
    if let Err(why) = write(&mut sink, Message::Text(follow), "asking to follow").await {
        return Served::Broke(why);
    }
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    let mut pong_due: Option<tokio::time::Instant> = None;
    loop {
        let pong_deadline = pong_due.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));
        let written = tokio::select! {
            _ = closed.changed() => {
                // Nothing is waiting on the line any more; a polite close is
                // all that is left, and its failing changes nothing.
                let _ = write(&mut sink, Message::Close(None), "closing").await;
                return Served::LetGo;
            }
            id = ids.recv() => {
                let Some(id) = id else { return Served::LetGo };
                // A call whose caller gave up has no frame left to write.
                let Some(frame) = line.take_frame(id) else { continue };
                write(&mut sink, Message::Binary(frame), "writing a call").await
            }
            message = stream.next() => {
                pong_due = None;
                match message {
                    None => Err("the broker closed the line".into()),
                    Some(Err(e)) => Err(format!("reading: {e}")),
                    Some(Ok(Message::Binary(bytes))) => match unframe::<AnswerHead>(&bytes) {
                        Ok((head, body)) => match reqwest::StatusCode::from_u16(head.status) {
                            Ok(status) => {
                                line.answered(head.id, Answer { status, body: body.to_vec() });
                                Ok(())
                            }
                            Err(e) => Err(format!("an answer with status {}: {e}", head.status)),
                        },
                        Err(e) => Err(format!("an answer that does not read: {e:#}")),
                    },
                    Some(Ok(Message::Text(text))) => match serde_json::from_str::<Notice>(&text) {
                        Ok(notice) => {
                            line.heard(notice);
                            Ok(())
                        }
                        Err(e) => Err(format!("a notice that does not read ({text}): {e}")),
                    },
                    Some(Ok(Message::Close(frame))) => Err(format!("the broker closed the line ({frame:?})")),
                    Some(Ok(Message::Ping(payload))) => write(&mut sink, Message::Pong(payload), "answering a ping").await,
                    Some(Ok(_)) => Ok(()),
                }
            }
            _ = ping.tick() => {
                pong_due.get_or_insert_with(|| tokio::time::Instant::now() + PONG_WAIT);
                write(&mut sink, Message::Ping(Vec::new()), "pinging").await
            }
            _ = tokio::time::sleep_until(pong_deadline), if pong_due.is_some() => {
                Err(format!("the broker said nothing for {PONG_WAIT:?} after a ping"))
            }
        };
        if let Err(why) = written {
            return Served::Broke(why);
        }
    }
}

type Sink = futures::stream::SplitSink<Socket, Message>;

/// Write one message, within [`PONG_WAIT`]; `doing` names it in the reason
/// the line broke.
async fn write(sink: &mut Sink, message: Message, doing: &str) -> Result<(), String> {
    match tokio::time::timeout(PONG_WAIT, sink.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("{doing}: {e}")),
        Err(_) => Err(format!("{doing}: nothing could be written for {PONG_WAIT:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_reads_back_as_its_head_and_body() {
        let framed = frame(&CallHead { id: 7, path: "/v1/x".into(), headers: vec![("a".into(), "b".into())] }, b"{\"k\":1}");
        let (head, body) = unframe::<CallHead>(&framed).unwrap();
        assert_eq!((head.id, head.path.as_str(), head.headers.len()), (7, "/v1/x", 1));
        assert_eq!(body, b"{\"k\":1}");
        assert!(unframe::<CallHead>(&framed[..3]).is_err());
        assert!(unframe::<CallHead>(&framed[..6]).is_err());
    }

    #[test]
    fn the_line_lives_beside_the_broker_address() {
        assert_eq!(line_url("https://b.run.app").unwrap(), "wss://b.run.app/v1/line");
        assert_eq!(line_url("http://127.0.0.1:9").unwrap(), "ws://127.0.0.1:9/v1/line");
        assert!(line_url("b.run.app").is_err());
    }

    #[test]
    fn a_notice_reads_back() {
        for notice in [
            Notice::Follow,
            Notice::Following { listening: true },
            Notice::Signal { channel: INFRA_STATUS_CHANNEL.into(), payload: "p".into() },
            Notice::Lost,
            Notice::Recheck,
        ] {
            let text = serde_json::to_string(&notice).unwrap();
            assert_eq!(serde_json::from_str::<Notice>(&text).unwrap(), notice);
        }
    }
}

/// The broker's half of the line: every call framed on it is run through
/// the router the request would have reached, and its answer framed back.
/// The broker serves it (`weft-broker`'s `line`), and so does any test that
/// stands a fake broker up from routes ([`server::with_line`]).
#[cfg(feature = "line-server")]
pub mod server {
    use std::sync::Arc;

    use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
    use axum::routing::get;
    use axum::Router;
    use futures::{SinkExt, StreamExt};
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    use super::{frame, unframe, AnswerHead, CallHead, Notice, LINE_PATH};

    /// How many calls of one line run at once; the next waits for one to
    /// end, which holds the line's reads back rather than the broker's
    /// memory.
    const IN_FLIGHT: usize = 4096;

    /// How many bytes of calls one line may have running at once: one
    /// largest message. A caller is untrusted code, and a route's own body
    /// limit only refuses a call once it has been read whole, so without
    /// this bound one line could hold [`IN_FLIGHT`] largest messages. A call
    /// past it waits for room, and the line reads nothing meanwhile, its
    /// pings included: two of the largest calls on one line run one after
    /// the other, and a first one slower than the caller's ping wait breaks
    /// the line, failing its calls loudly.
    const BYTES_IN_FLIGHT: usize = MAX_MESSAGE;

    /// The largest message a line takes: the largest body any call does
    /// (a failed unrecorded run's whole record) and room for its head.
    pub const MAX_MESSAGE: usize = 128 * 1024 * 1024;

    /// What pushes notices down a followed line: handed the line's notice
    /// sender, it runs until the line goes (it is aborted then).
    pub type Follower = Box<dyn FnOnce(mpsc::Sender<Notice>) -> tokio::task::JoinHandle<()> + Send>;

    /// `api` with the line beside it, pushing no notices: a fake broker's
    /// routes, reachable by the clients that speak only the line.
    pub fn with_line(api: Router) -> Router {
        let served = api.clone();
        Router::new()
            .route(
                LINE_PATH,
                get(move |ws: WebSocketUpgrade| {
                    let api = served.clone();
                    async move { upgrade(ws).on_upgrade(move |socket| serve(socket, api, None)) }
                }),
            )
            .merge(api)
    }

    /// The upgrade, sized for the line's largest message.
    pub fn upgrade(ws: WebSocketUpgrade) -> WebSocketUpgrade {
        ws.max_message_size(MAX_MESSAGE).max_frame_size(MAX_MESSAGE)
    }

    /// Serve one line: run each call through `api` and frame its answer
    /// back, and once the caller asks to follow, start `follower`. Without
    /// one the line says it follows nothing (`Following { listening: false }`),
    /// so nothing kept on it is ever trusted.
    pub async fn serve(socket: WebSocket, api: Router, follower: Option<Follower>) {
        let (mut sink, mut stream) = socket.split();
        let (out, mut outgoing) = mpsc::channel::<Message>(1024);
        let writer = tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                if sink.send(message).await.is_err() {
                    return;
                }
            }
        });
        let calls = Arc::new(tokio::sync::Semaphore::new(IN_FLIGHT));
        // The line's calls, stopped with it: a call the broker holds (a wait
        // for journal rows, a claim) would otherwise go on waiting for a
        // caller that is gone.
        let mut running = tokio::task::JoinSet::new();
        let bytes = Arc::new(tokio::sync::Semaphore::new(BYTES_IN_FLIGHT));
        let mut follower = follower;
        // The tasks pushing notices down this line, stopped when it goes.
        let mut following: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        while let Some(message) = stream.next().await {
            match message {
                Ok(Message::Binary(message)) => {
                    let (head, body_at) = match unframe::<CallHead>(&message) {
                        Ok((head, body)) => (head, message.len() - body.len()),
                        Err(e) => {
                            // Only a client with a framing bug sends this, and
                            // no answer could name the call: closing the line
                            // fails its calls at once instead of leaving them
                            // to wait out their time.
                            tracing::warn!(target: "weft_broker_client::line", error = %format!("{e:#}"), "a call on the line does not read; closing it");
                            break;
                        }
                    };
                    let size = u32::try_from(message.len()).expect("a line message is at most MAX_MESSAGE");
                    let Ok(call) = calls.clone().acquire_owned().await else { break };
                    let Ok(held) = bytes.clone().acquire_many_owned(size).await else { break };
                    let (api, out) = (api.clone(), out.clone());
                    while running.try_join_next().is_some() {}
                    running.spawn(async move {
                        let (status, answer) = run(api, &head, message.slice(body_at..)).await;
                        let _ = out.send(Message::Binary(frame(&AnswerHead { id: head.id, status }, &answer).into())).await;
                        drop((call, held));
                    });
                }
                Ok(Message::Text(text)) => match serde_json::from_str::<Notice>(&text) {
                    Ok(Notice::Follow) if following.is_empty() => {
                        let (notices, mut heard) = mpsc::channel::<Notice>(1024);
                        let pushed = out.clone();
                        following.push(tokio::spawn(async move {
                            while let Some(notice) = heard.recv().await {
                                let text = serde_json::to_string(&notice).expect("a notice serializes");
                                if pushed.send(Message::Text(text.into())).await.is_err() {
                                    return;
                                }
                            }
                        }));
                        match follower.take() {
                            Some(follow) => following.push(follow(notices)),
                            None => {
                                let _ = notices.send(Notice::Following { listening: false }).await;
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(target: "weft_broker_client::line", error = %e, "a line sent a notice that does not read; closing it");
                        break;
                    }
                },
                Ok(Message::Ping(payload)) => {
                    let _ = out.send(Message::Pong(payload)).await;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        for task in following {
            task.abort();
        }
        running.shutdown().await;
        drop(out);
        let _ = writer.await;
    }

    async fn run(api: Router, head: &CallHead, body: bytes::Bytes) -> (u16, Vec<u8>) {
        if !head.path.starts_with("/v1/") || head.path == LINE_PATH {
            return (404, format!("no call at {}", head.path).into_bytes());
        }
        let mut request = axum::http::Request::post(head.path.as_str());
        for (name, value) in &head.headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let request = match request.body(axum::body::Body::from(body)) {
            Ok(request) => request,
            Err(e) => return (400, format!("a call that is not a request: {e}").into_bytes()),
        };
        let response = match api.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        let status = response.status().as_u16();
        match axum::body::to_bytes(response.into_body(), usize::MAX).await {
            Ok(bytes) => (status, bytes.to_vec()),
            Err(e) => (500, format!("reading the answer: {e}").into_bytes()),
        }
    }
}
