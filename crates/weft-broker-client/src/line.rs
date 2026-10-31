//! The line: one WebSocket from a process to the broker, open while the
//! process has something to ask or hear (it closes when idle, see below),
//! that every JSON call of every broker client of that process travels
//! on, and that the broker pushes news down.
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
//! A broker that cuts a request after a while (Cloud Run's limit on one
//! request) says so as the line opens (`Notice::Following`'s
//! `max_age_secs`), and the line tells its process when that cut comes
//! ([`BrokerLink::cut_at`]). A process can have its line replaced before
//! then ([`BrokerLink::replace`]): a second line opens beside it, and once
//! the broker follows on it, calls go out on it and what the first line
//! still had to say is read until its calls are answered and it closes.
//! The first line delivers until the second one follows, so nothing is
//! missed and that `Following` is no recheck: nobody looks again, and the
//! replacement reads nothing from the database. The exception is a first
//! line that had stopped hearing: its gap is looked at once a line hears
//! again, and a recheck it still sends while it retires is passed on. A
//! worker replaces its line while it works; a worker whose runs only wait lets nothing open a line
//! (the broker copy it lands on may have to read who it is), and stops
//! before the cut instead.
//!
//! When the line breaks it comes back by itself. A call that had not been
//! written yet goes out on the next line if that opens within the call's
//! wait to send, and fails [`LineError::NotSent`] otherwise. One that had
//! been written may have landed, and what becomes of it is what the call
//! says of itself where it is declared ([`BrokerCall`]): one whose second
//! arrival changes nothing ([`Twice::Harmless`]: a read, or a write the
//! broker recognises, like a batch of records, `weft_journal::record::Fate`)
//! is sent again on the next line, within the wait it already had, and any
//! other is answered [`LineError::Dropped`], which says it may or may not
//! have landed. A broker rolling out stops its copies, and every call a
//! stopping copy was running breaks this way.
//!
//! A caller that stops waiting for a call it already wrote tells the
//! broker (`Notice::Forget`), which stops working on it, the way a request
//! whose caller hung up stops its handler: a wait the broker holds for
//! nobody would otherwise go on reading the database for its whole hold.
//!
//! A line with nothing to do closes ([`LINE_IDLE`]) and opens again on the
//! next call: an open line is an open request on the broker, and one held
//! by a process with nothing to ask would keep a broker that scales to
//! zero up for as long as that process lives. While it is closed nothing
//! is heard, so what is kept on it is not trusted until it is back. A
//! process that must hear while it asks nothing (a worker driving runs,
//! which hears their answers and cancels) holds it open
//! ([`BrokerLink::stay_open`]).
//!
//! Byte streams (`ctx.storage` uploads and downloads) stay ordinary
//! requests: a large body on the line would hold up every call behind it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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

/// One call the broker answers, as it is declared beside its request
/// (`crate::protocol`): where it goes, and whether it may arrive twice.
/// Both are said at once, so no call goes out without saying the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerCall {
    pub path: &'static str,
    pub twice: Twice,
}

/// Whether a call may reach the broker twice: what decides if a call whose
/// line broke after it was written is sent again (see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Twice {
    /// Its second arrival changes nothing the first did not, and is
    /// answered the way its caller reads the first: a read, or a write the
    /// broker recognises or that sets what the first set. Sent again.
    Harmless,
    /// Anything else, and anything not shown to be harmless: a claim (the
    /// second answers that there is nothing left to claim), something
    /// queued or counted, a write that checks what it overwrites. Never
    /// sent again by the line.
    NotSafe,
}

/// How long a call waits: to be written on the line (while the line is
/// down, or still opening), and, counted from the call, for its answer. A
/// call sent again after its line broke waits again to be written, within
/// the same wait for its answer.
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

/// How soon a line whose broker copy steps aside tries again to move to a
/// current copy, when the last try failed. The line still answers
/// meanwhile, turning new calls away to be sent again.
const MOVE_AGAIN: Duration = Duration::from_secs(1);
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

/// How long a line with no call out and nothing holding it open stays
/// open before it closes (see the module doc), and how often that is
/// looked at.
pub const LINE_IDLE: Duration = Duration::from_secs(60);
const IDLE_LOOK: Duration = Duration::from_secs(5);

/// Why a call on the line got no answer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LineError {
    /// The call never left this process: the line was down for the
    /// call's whole wait. Nothing landed, so it is safe to send again.
    /// `busy` when the last try to open the line was turned away because
    /// every copy of the broker the platform may run was busy (HTTP 429):
    /// it passes once the load does.
    #[error("the broker could not be reached for {waited:?} (line: {why})")]
    NotSent { waited: Duration, why: String, busy: bool },
    /// The line broke after the call was written: it may or may not have
    /// landed. Only a call that is not safe to send twice
    /// ([`Twice::NotSafe`]) is answered so; a harmless one is sent again.
    #[error(
        "the line to the broker broke while the call was in flight (the broker copy that had it stopped, or the network \
         dropped it: {0}), so it may or may not have landed, and it is not safe to send twice: what made it (a run, a \
         command) can be run again"
    )]
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

/// What either end says on the line besides calls and answers, as text
/// frames: the caller's requests (`Follow`, `Forget`), and the broker's
/// news.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Notice {
    /// Sent by the caller once the line is open: push the notifications
    /// this caller may see.
    Follow,
    /// Sent by the caller: it stopped waiting for the call `id`, so the
    /// broker stops working on it.
    Forget { id: u64 },
    /// The broker follows for this caller. `listening` is whether its own
    /// database listener hears right now; `max_age_secs` how long the
    /// broker keeps one line before cutting it (a request limit), `None`
    /// when it keeps it for good.
    Following { listening: bool, max_age_secs: Option<u64> },
    /// One notification this caller may see.
    Signal { channel: String, payload: String },
    /// The broker's database listener lost its connection: nothing is
    /// heard until a `Recheck` that says it listens.
    Lost,
    /// Notifications may have been missed: look again. `listening` is
    /// whether the broker's database listener hears right now: a recheck
    /// may come while it does not (the install's news, heard while the
    /// database sleeps), and then nothing kept on the line is trusted yet.
    Recheck { listening: bool },
    /// The broker's database listener hears again after a quiet spell. What
    /// runs wait on (their answers, their cancels) reached it meanwhile as
    /// the install's news, so nothing needs looking at again; only what is
    /// kept on the line may follow again.
    Resumed,
    /// This copy of the broker is stepping aside for a newer one
    /// (`server::serve`): the calls running on the line are answered, and
    /// none more is run on it. The caller replaces the line, which opens
    /// on a current copy, and closes this one once it owes nothing.
    SteppingAside,
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
// SYNC: LINE_CHANNELS <-> crates/weft-broker/src/line.rs (audience), crates/weft-task-store/src/runs.rs (CANCEL_CHANNEL), crates/weft-task-store/src/parked_fires.rs (PARKED_FIRE_CHANNEL), triggers_notify_project, crates/weft-dispatcher/src/infra_node.rs (infra_node_status_notify), crates/weft-dispatcher/src/project_store.rs (project_declared_infra_notify), crates/weft-access-store/src/lib.rs (access_notify)
pub const LINE_CHANNELS: &[&str] = &[INFRA_STATUS_CHANNEL, ACCESS_CHANNEL, CANCEL_CHANNEL, TRIGGERS_CHANNEL, PARKED_FIRE_CHANNEL];

/// Announced with a tenant's id when one of its routes changes: a public
/// entry armed, re-armed or taken down, or how its activation stands (the
/// signal, project and activation groups' triggers). The dispatchers read
/// the tenant's routes again (`weft_dispatcher::held`); it rides no line.
// SYNC: ROUTES_CHANNEL <-> crates/weft-dispatcher/src/store/postgres.rs (routes_notify_tenant), crates/weft-dispatcher/src/activation_store.rs (trigger_activation_routes_notify)
pub const ROUTES_CHANNEL: &str = "weft_routes";

/// Announced with a project's id when one of its triggers changes: armed,
/// re-armed or taken down, its holder changed, or how its activation stands
/// (the signal and activation groups' triggers). Its workers read
/// the project's triggers again (`/v1/door/triggers`).
// SYNC: TRIGGERS_CHANNEL <-> crates/weft-dispatcher/src/store/postgres.rs (triggers_notify_project), crates/weft-dispatcher/src/activation_store.rs (trigger_activation_triggers_notify)
pub const TRIGGERS_CHANNEL: &str = "weft_triggers";

/// Announced with a project's id when one of its infra copies comes,
/// goes, changes status or answers at another address, and when the
/// project is registered with another program (which may declare other
/// infra).
pub const INFRA_STATUS_CHANNEL: &str = "weft_infra_status";

/// Announced when a connection, an install's pick of one, or a value an
/// instance provides changes or goes: with its project's id, or
/// `tenant:<tenant>` for a connection shared across the tenant's projects.
pub const ACCESS_CHANNEL: &str = "weft_access";

/// Announced with `<project id> <run id>` when a running run
/// is asked to stop: the worker driving it fires its flag
/// (`weft_task_store::runs`).
pub use weft_task_store::runs::CANCEL_CHANNEL;

/// Announced with `<project id> <run id>` when an answer is handed to
/// a running run (and with a project's id alone when an event waits
/// for its trigger, which rides no line): the worker driving the run reads
/// its answers (`weft_task_store::parked_fires`).
pub use weft_task_store::parked_fires::PARKED_FIRE_CHANNEL;

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
    /// How long the line stays open with nothing to do ([`LINE_IDLE`]).
    idle_after: Duration,
    /// Where the line's task runs ([`BrokerLink::on_runtime`]); the
    /// runtime of its first call when `None`.
    runtime: Option<tokio::runtime::Handle>,
    /// Dropped with the last [`BrokerLink`], which tells the line's task
    /// to close the line: nothing will call on it again.
    _closing: tokio::sync::watch::Sender<()>,
    closed: tokio::sync::watch::Receiver<()>,
}

impl BrokerLink {
    pub fn new(base_url: impl Into<String>, token: TokenSource) -> Self {
        let http = weft_core::net::http_client()
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
                idle_after: weft_core::time_scale::scaled(LINE_IDLE),
                runtime: None,
                _closing: closing,
                closed,
            }),
        }
    }

    /// Close the line after `after` with nothing to do instead of
    /// [`LINE_IDLE`]. Only on a link no clone of which was made yet.
    pub fn closing_when_idle_after(mut self, after: Duration) -> Self {
        Arc::get_mut(&mut self.inner).expect("set before the link is shared").idle_after = after;
        self
    }

    /// Run the line's task on `runtime`: a worker keeps its line and its
    /// writer lanes on a small runtime of their own, so their wake-ups
    /// never land on the threads serving calls. Only on a link no clone of
    /// which was made yet.
    pub fn on_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        Arc::get_mut(&mut self.inner).expect("set before the link is shared").runtime = Some(runtime);
        self
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

    /// Make `call` with the JSON `body`, waiting as `wait` says.
    pub async fn call(&self, call: BrokerCall, body: impl AsRef<[u8]>, wait: CallWait) -> Result<Answer, anyhow::Error> {
        self.call_as(call, "application/json", &[], body.as_ref(), wait).await
    }

    /// Make `call` with `body` as raw bytes (a batch of records,
    /// `weft_journal::frame`) and the extra `headers`, waiting as `wait`
    /// says.
    pub async fn call_bytes(&self, call: BrokerCall, headers: &[(&str, &str)], body: impl AsRef<[u8]>, wait: CallWait) -> Result<Answer, anyhow::Error> {
        self.call_as(call, "application/octet-stream", headers, body.as_ref(), wait).await
    }

    async fn call_as(&self, call: BrokerCall, content_type: &str, extra: &[(&str, &str)], body: &[u8], wait: CallWait) -> Result<Answer, anyhow::Error> {
        let bearer = self.inner.token.read(&self.inner.base_url).await?;
        let mut headers = vec![
            ("authorization".to_string(), format!("Bearer {bearer}")),
            ("content-type".to_string(), content_type.to_string()),
        ];
        headers.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        headers.extend(self.inner.token.headers().into_iter().map(|(k, v)| (k.to_string(), v)));
        Ok(self.line().call(call, headers, body, wait).await?)
    }

    /// What the broker pushes on this process's line (see the module doc).
    pub fn subscribe(&self) -> Subscription {
        let line = self.line();
        Subscription::with_listening(line.notices.subscribe(), line.listening.clone())
    }

    /// When the broker cuts the line open now, `None` while it is closed
    /// or when the broker keeps it for good. Told again whenever that
    /// changes, so a process can stop before the cut (see the module doc).
    pub fn cut_at(&self) -> tokio::sync::watch::Receiver<Option<Cut>> {
        self.line().cut.subscribe()
    }

    /// Replace the open line with a new one, missing nothing (see the
    /// module doc). Answers once the broker follows on the new line, which
    /// [`Self::cut_at`] then reports, or with why it could not: the line was
    /// not open, or the new one would not open or follow, and the open line
    /// stays as it was.
    pub async fn replace(&self) -> anyhow::Result<()> {
        let line = self.line();
        if let Some(why) = line.down.lock().expect("line down").clone() {
            anyhow::bail!("the line is not open: {why}");
        }
        let (done, replaced) = oneshot::channel();
        let _ = line.outbox.send(Out::Replace(done));
        replaced.await.unwrap_or_else(|_| Err("the line stopped".into())).map_err(anyhow::Error::msg)
    }

    /// Hold the line open while the guard lives, calls or not, so what the
    /// broker pushes is heard (see the module doc). A line closed for want
    /// of anything to do opens again at once.
    pub fn stay_open(&self) -> StayOpen {
        let line = self.line().clone();
        if line.keeps.fetch_add(1, Ordering::AcqRel) == 0 {
            let _ = line.outbox.send(Out::Wake);
        }
        StayOpen { line }
    }

    fn line(&self) -> &Arc<Line> {
        self.inner
            .line
            .get_or_init(|| Line::open(Arc::downgrade(&self.inner), self.inner.closed.clone(), self.inner.idle_after, self.inner.runtime.as_ref()))
    }
}

/// Holds the line open (see [`BrokerLink::stay_open`]).
pub struct StayOpen {
    line: Arc<Line>,
}

impl Drop for StayOpen {
    fn drop(&mut self) {
        self.line.keeps.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One call waiting on the line.
struct Waiting {
    /// The framed call until it is written, `None` once it is.
    frame: Option<Vec<u8>>,
    answer: oneshot::Sender<Result<Answer, LineError>>,
}

/// Takes a call off the line when its caller stops waiting for it, however
/// it stops (an answer already took it off; that is fine). One already
/// written is forgotten by the broker too.
struct Forget<'a> {
    line: &'a Line,
    id: u64,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        let removed = self.line.calls.lock().expect("line calls").remove(&self.id);
        if removed.is_some_and(|waiting| waiting.frame.is_none()) {
            let _ = self.line.outbox.send(Out::Forget(self.id));
        }
    }
}

/// What the line's task is asked to write, in order.
enum Out {
    /// The call with this id, unless its caller gave up first.
    Call(u64),
    /// That the caller stopped waiting for this written call.
    Forget(u64),
    /// Nothing: the line is wanted open (a [`StayOpen`] was taken).
    Wake,
    /// Replace the open line ([`BrokerLink::replace`]), answering how
    /// that went.
    Replace(oneshot::Sender<Result<(), String>>),
}

struct Line {
    calls: Mutex<HashMap<u64, Waiting>>,
    /// What to write, in order.
    outbox: mpsc::UnboundedSender<Out>,
    /// How many [`StayOpen`]s hold the line open.
    keeps: AtomicUsize,
    next_id: AtomicU64,
    notices: broadcast::Sender<Heard>,
    /// Whether what the line pushes can be trusted right now (see
    /// `pg_signal::Subscription::listening`).
    listening: Arc<AtomicBool>,
    /// Why the line is down, while it is.
    down: Mutex<Option<String>>,
    /// Whether the last try to open it was turned away busy
    /// ([`LineError::NotSent`]'s `busy`).
    turned_away: AtomicBool,
    /// When the broker cuts the open line, if it said it would.
    cut: tokio::sync::watch::Sender<Option<Cut>>,
}

/// When the broker cuts the line open now (`Notice::Following`'s
/// `max_age_secs`): `max_age` after it `opened`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cut {
    pub opened: tokio::time::Instant,
    pub max_age: Duration,
}

impl Line {
    fn open(link: Weak<LinkInner>, closed: tokio::sync::watch::Receiver<()>, idle_after: Duration, runtime: Option<&tokio::runtime::Handle>) -> Arc<Self> {
        let (outbox, ids) = mpsc::unbounded_channel();
        let (notices, _) = broadcast::channel(NOTICE_CAPACITY);
        let line = Arc::new(Self {
            calls: Mutex::new(HashMap::new()),
            outbox,
            keeps: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            notices,
            listening: Arc::new(AtomicBool::new(false)),
            down: Mutex::new(Some("not opened yet".into())),
            turned_away: AtomicBool::new(false),
            cut: tokio::sync::watch::channel(None).0,
        });
        let task = keep_open(link, Arc::downgrade(&line), ids, closed, idle_after);
        match runtime {
            Some(runtime) => drop(runtime.spawn(task)),
            None => drop(tokio::spawn(task)),
        }
        line
    }

    /// Make `call`, sending it again on the next line each time the line
    /// breaks after writing it, when it is harmless twice and still has
    /// time to be answered ([`after_try`]). Each try waits up to the call's
    /// wait to be written, and none past its wait for its answer, counted
    /// from the call.
    async fn call(&self, call: BrokerCall, headers: Vec<(String, String)>, body: &[u8], wait: CallWait) -> Result<Answer, LineError> {
        let answer_by = weft_core::deadline::after(tokio::time::Instant::now(), wait.to_answer);
        loop {
            let tried = tokio::time::Instant::now();
            let to_send = wait.to_send.min(answer_by.saturating_duration_since(tried));
            let sent = self.try_once(call.path, headers.clone(), body, to_send, answer_by, wait).await;
            match after_try(call.twice, sent, tokio::time::Instant::now() < answer_by, wait) {
                Next::Done(result) => return result,
                Next::Again => {
                    tracing::debug!(target: "weft_broker_client::line", path = call.path, "the line broke after a call harmless twice was written; sending it again");
                }
            }
        }
    }

    /// One try of a call to `path`: written on the line within `to_send`,
    /// and answered by `answer_by`.
    async fn try_once(
        &self,
        path: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
        to_send: Duration,
        answer_by: tokio::time::Instant,
        wait: CallWait,
    ) -> Result<Answer, LineError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = frame(&CallHead { id, path: path.to_string(), headers }, body);
        let (answer, answered) = oneshot::channel();
        self.calls.lock().expect("line calls").insert(id, Waiting { frame: Some(frame), answer });
        // A caller that stops waiting (its own timeout, a cancelled task)
        // takes its call off the line, so an abandoned write is never sent
        // later behind its back, and one already sent is dropped by the
        // broker too.
        let _forget = Forget { line: self, id };
        // The receiving end lives as long as the line's task, which lives
        // as long as this line: a send can only fail once neither does.
        let _ = self.outbox.send(Out::Call(id));
        let start = tokio::time::Instant::now();
        tokio::pin!(answered);
        let landed = |result: Result<Result<Answer, LineError>, oneshot::error::RecvError>| {
            result.unwrap_or_else(|_| Err(LineError::Dropped("the line stopped".into())))
        };
        tokio::select! {
            result = &mut answered => return landed(result),
            () = tokio::time::sleep_until(weft_core::deadline::after(start, to_send)) => {}
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
                    busy: self.turned_away.load(Ordering::Acquire),
                });
            }
        }
        match tokio::time::timeout_at(answer_by, &mut answered).await {
            Ok(result) => landed(result),
            Err(_) => Err(LineError::NoAnswer(wait.to_answer)),
        }
    }

    /// The line broke: every written call is told what became of it
    /// (`owed`); the unwritten ones wait for the next line.
    fn broke(&self, why: &str, owed: Owed) {
        self.listening.store(false, Ordering::Release);
        self.cut.send_replace(None);
        let _ = self.notices.send(Heard::Lost);
        *self.down.lock().expect("line down") = Some(why.to_string());
        self.unanswered(&self.written(), why, owed);
    }

    /// The line closed for want of anything to do: nothing is heard until
    /// it opens again.
    fn idled(&self) {
        self.listening.store(false, Ordering::Release);
        self.cut.send_replace(None);
        let _ = self.notices.send(Heard::Lost);
        *self.down.lock().expect("line down") = Some("closed while idle; it opens again on the next call".into());
    }

    /// Whether the line may close: no call is out and nothing holds it open.
    fn idle(&self) -> bool {
        self.keeps.load(Ordering::Acquire) == 0 && self.calls.lock().expect("line calls").is_empty()
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

    /// The broker follows on the line that `opened` at that instant, its
    /// listener hearing or not, cutting it after `max_age_secs`.
    fn followed(&self, listening: bool, max_age_secs: Option<u64>, opened: tokio::time::Instant) {
        self.cut.send_replace(max_age_secs.map(|secs| Cut { opened, max_age: Duration::from_secs(secs) }));
        self.listening.store(listening, Ordering::Release);
    }

    /// The broker follows on the line that replaced the open one. While the
    /// old one heard, it delivered until now, so nothing was missed and
    /// nobody looks again. If the old one had stopped hearing, what was
    /// pushed since is found only by looking again, which waiters do once
    /// a line hears: now, if the new broker's listener hears, else at that
    /// broker's own recheck. A new broker that does not hear means what is
    /// kept on the line is not trusted until it does. Says whether waiters
    /// were told to look again now.
    fn followed_instead(&self, listening: bool, max_age_secs: Option<u64>, opened: tokio::time::Instant) -> bool {
        let heard_until_now = self.listening.load(Ordering::Acquire);
        self.followed(listening, max_age_secs, opened);
        let told = match (heard_until_now, listening) {
            (_, false) => Heard::Lost,
            (false, true) => Heard::Recheck,
            (true, true) => return false,
        };
        let rechecked = matches!(told, Heard::Recheck);
        let _ = self.notices.send(told);
        rechecked
    }

    /// The calls written so far and not answered yet: what a line being
    /// replaced still owes.
    fn written(&self) -> std::collections::HashSet<u64> {
        self.calls.lock().expect("line calls").iter().filter(|(_, w)| w.frame.is_none()).map(|(id, _)| *id).collect()
    }

    /// A line broke, for `why`, with the calls `ids` still out on it:
    /// told they may have landed, or, when its copy stepped aside and closed
    /// it after answering every call it ran, that they never ran, which
    /// their callers send again as a busy broker's
    /// (`crate::client::turned_away`).
    fn unanswered(&self, ids: &std::collections::HashSet<u64>, why: &str, owed: Owed) {
        let mut calls = self.calls.lock().expect("line calls");
        for id in ids {
            if let Some(waiting) = calls.remove(id) {
                let error = match owed {
                    Owed::MayHaveLanded => LineError::Dropped(why.to_string()),
                    Owed::NeverRan => LineError::NotSent { waited: Duration::ZERO, why: why.to_string(), busy: true },
                };
                let _ = waiting.answer.send(Err(error));
            }
        }
    }

    /// Whether any of `owed` still waits for its answer.
    fn owes_any(&self, owed: &std::collections::HashSet<u64>) -> bool {
        let calls = self.calls.lock().expect("line calls");
        owed.iter().any(|id| calls.contains_key(id))
    }

    /// What the broker said on the line, which `opened` at that instant.
    fn heard(&self, notice: Notice, opened: tokio::time::Instant) {
        match notice {
            // The line follows again, or may have missed some: every waiter
            // looks again (a cancel announced while it did not follow is
            // found so), and copies kept on it are trusted only while the
            // broker hears. Set before the recheck goes out, so a waiter
            // reads it with the recheck.
            Notice::Following { listening, max_age_secs } => {
                self.followed(listening, max_age_secs, opened);
                let _ = self.notices.send(Heard::Recheck);
            }
            Notice::Recheck { listening } => {
                self.listening.store(listening, Ordering::Release);
                let _ = self.notices.send(Heard::Recheck);
            }
            // The broker hears again after a quiet spell, in which what
            // runs wait on reached it as the install's news: nothing to
            // look at again, but what is kept on the line may follow again.
            Notice::Resumed => {
                self.listening.store(true, Ordering::Release);
                let _ = self.notices.send(Heard::Resumed);
            }
            Notice::Lost => {
                self.listening.store(false, Ordering::Release);
                let _ = self.notices.send(Heard::Lost);
            }
            Notice::Signal { channel, payload } => match LINE_CHANNELS.iter().find(|c| **c == channel) {
                Some(channel) => {
                    let _ = self.notices.send(Heard::Signal { channel, payload: payload.into() });
                }
                None => tracing::warn!(target: "weft_broker_client::line", %channel, "the broker pushed a channel this build does not know"),
            },
            // The line it came on is replaced (`serve`).
            Notice::SteppingAside => {}
            Notice::Follow | Notice::Forget { .. } => {
                tracing::warn!(target: "weft_broker_client::line", ?notice, "the broker sent what only a caller sends")
            }
        }
    }
}

/// What the calls a line still owed when it ended became.
#[derive(Debug, Clone, Copy)]
enum Owed {
    /// It broke: each may have landed.
    MayHaveLanded,
    /// Its copy stepped aside and closed it once nothing ran on it: each
    /// arrived after it took no more call, so none ran.
    NeverRan,
}

/// What becomes of one try of a call ([`after_try`]).
#[derive(Debug)]
enum Next {
    /// Handed to its caller as it is.
    Done(Result<Answer, LineError>),
    /// Sent again on the next line.
    Again,
}

/// What becomes of one try of a call that came back `tried`. Only a call
/// the line broke under after writing it ([`LineError::Dropped`]) is sent
/// again, and only when it is harmless twice and `time_left` to be
/// answered; one out of time is answered as having had none
/// ([`LineError::NoAnswer`]), since its last try may still be on its way.
/// A call that is not safe twice keeps the drop, which says it may or may
/// not have landed. Every other answer and failure is the call's, as the
/// line gave it.
fn after_try(twice: Twice, tried: Result<Answer, LineError>, time_left: bool, wait: CallWait) -> Next {
    match (tried, twice) {
        (Err(LineError::Dropped(_)), Twice::Harmless) if time_left => Next::Again,
        (Err(LineError::Dropped(_)), Twice::Harmless) => Next::Done(Err(LineError::NoAnswer(wait.to_answer))),
        (tried, _) => Next::Done(tried),
    }
}

/// Keep the line open for as long as anything holds it: open it, serve
/// it until it breaks, open it again. Once the process lets go of its
/// last [`BrokerLink`] (`closed`), the line is closed and stays closed.
async fn keep_open(
    link: Weak<LinkInner>,
    line: Weak<Line>,
    mut outbox: mpsc::UnboundedReceiver<Out>,
    mut closed: tokio::sync::watch::Receiver<()>,
    idle_after: Duration,
) {
    let mut retry = RECONNECT_FIRST;
    // What woke a closed line, written first once it is open again.
    let mut first: Option<Out> = None;
    loop {
        let (Some(inner), Some(held)) = (link.upgrade(), line.upgrade()) else { return };
        // What opening needs, taken out so nothing holds the link while it
        // opens: the last link let go of mid-attempt closes the line.
        let (base_url, token) = (inner.base_url.clone(), inner.token.clone());
        drop(inner);
        // The broker's limit on one request counts from no earlier than
        // this.
        let opening = tokio::time::Instant::now();
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
                held.turned_away.store(false, Ordering::Release);
                let opens = Opens { base_url: &base_url, token: &token };
                match serve(socket, opening, &held, first.take(), &mut outbox, &mut closed, idle_after, opens).await {
                    Served::LetGo => return,
                    Served::Broke(why) => {
                        tracing::warn!(target: "weft_broker_client::line", reason = %why, "the line to the broker broke; opening it again");
                        held.broke(&why, Owed::MayHaveLanded);
                    }
                    Served::SteppedAside { why, retiring } => {
                        tracing::info!(target: "weft_broker_client::line", reason = %why, "the broker copy on the line stepped aside and closed it; opening it again");
                        // What a line being replaced still owed went to
                        // another copy, and may have landed there.
                        held.unanswered(&retiring, &why, Owed::MayHaveLanded);
                        held.broke(&why, Owed::NeverRan);
                    }
                    Served::Idle => {
                        held.idled();
                        drop(held);
                        // Closed until something is asked of it. A closed
                        // line has nothing to replace.
                        loop {
                            tokio::select! {
                                out = outbox.recv() => match out {
                                    Some(Out::Replace(done)) => {
                                        let _ = done.send(Err("the line is closed while idle".into()));
                                    }
                                    Some(out) => {
                                        first = Some(out);
                                        break;
                                    }
                                    None => return,
                                },
                                _ = closed.changed() => return,
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let why = format!("{e:#}");
                let busy = turned_away_busy(&e);
                if busy {
                    // Every copy of the broker in use: counted and said with
                    // the calls turned away, once a minute at most, rather
                    // than at every try (one every few seconds, per line).
                    crate::client::say_turned_away("the line to the broker");
                } else {
                    tracing::warn!(target: "weft_broker_client::line", error = %why, retry_in_ms = retry.as_millis() as u64, "could not open the line to the broker");
                }
                *held.down.lock().expect("line down") = Some(why);
                held.turned_away.store(busy, Ordering::Release);
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

/// Whether a try to open the line was turned away because every copy of
/// the broker the platform may run is busy: Cloud Run answers 429 when the
/// service is at its most copies and none has room, without handing the
/// request to any.
fn turned_away_busy(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<tokio_tungstenite::tungstenite::Error>(),
            Some(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status().as_u16() == 429
        )
    })
}

/// How one open line ended.
enum Served {
    /// It broke, for this reason; it is opened again.
    Broke(String),
    /// Its copy stepped aside and closed it, for `why`, after answering
    /// every call it ran: it is opened again, and the calls written on it
    /// and not answered never ran ([`Owed::NeverRan`]); those a line it was
    /// replacing still owed (`retiring`) went to another copy.
    SteppedAside { why: String, retiring: std::collections::HashSet<u64> },
    /// It had nothing to do for [`LINE_IDLE`] and closed; it opens again
    /// when something is asked of it.
    Idle,
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
    // The broker's answers are as large as what it reads back (a run's
    // whole journal); it is weft's own, so nothing caps them that would
    // not cap the same answer as a request.
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().max_message_size(None).max_frame_size(None);
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

/// Where the line opens, for a line that replaces it.
#[derive(Clone, Copy)]
struct Opens<'a> {
    base_url: &'a str,
    token: &'a TokenSource,
}

type Stream = futures::stream::SplitStream<Socket>;

/// A line opened to replace the open one, until the broker follows on it.
struct Joining {
    sink: Sink,
    stream: Stream,
    opened: tokio::time::Instant,
    done: oneshot::Sender<Result<(), String>>,
    /// Its copy said it steps aside before following: once it serves,
    /// it is replaced in turn.
    stepping_aside: bool,
}

/// A replaced line, read until the calls written on it (`owed`) are
/// answered, then closed (`closing`) and read until the broker closes it:
/// what it says meanwhile was said before the new line followed.
struct Retiring {
    sink: Sink,
    stream: Stream,
    owed: std::collections::HashSet<u64>,
    closing: bool,
    /// Its copy said it steps aside: closed by the broker, what it still
    /// owes never ran.
    stepping_aside: bool,
    /// Waiters were told to look again when the new line followed: that
    /// look already covers any gap the old broker could still recheck for,
    /// so a recheck it sends is not passed on.
    rechecked: bool,
}

/// Serve one open line, which `opened` at that instant, until it breaks,
/// or until the process lets go of its last link (`closed`), replacing it
/// when asked ([`BrokerLink::replace`]).
///
/// Every write is bounded by [`PONG_WAIT`]: a broker that vanished without
/// closing leaves the socket's buffer full and a write waiting for minutes,
/// which would also keep the ping's deadline from ever being looked at.
#[allow(clippy::too_many_arguments)]
async fn serve(
    socket: Socket,
    mut opened: tokio::time::Instant,
    line: &Line,
    first: Option<Out>,
    outbox: &mut mpsc::UnboundedReceiver<Out>,
    closed: &mut tokio::sync::watch::Receiver<()>,
    idle_after: Duration,
    opens: Opens<'_>,
) -> Served {
    let (mut sink, mut stream) = socket.split();
    if let Err(why) = write(&mut sink, follow(), "asking to follow").await {
        return Served::Broke(why);
    }
    if let Some(out) = first {
        if let Err(why) = write_out(&mut sink, line, out).await {
            return Served::Broke(why);
        }
    }
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    let mut pong_due: Option<tokio::time::Instant> = None;
    let mut idle_look = tokio::time::interval(IDLE_LOOK.min(idle_after));
    idle_look.tick().await;
    // Since when the line has had nothing to do, while it has not.
    let mut idle_since: Option<tokio::time::Instant> = None;
    // A replacement under way: the new line opening, then waiting for the
    // broker to follow on it; the replaced line, until it is closed.
    type Opening<'o> = (std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Socket>> + Send + 'o>>, tokio::time::Instant, oneshot::Sender<Result<(), String>>);
    let mut opening: Option<Opening<'_>> = None;
    let mut joining: Option<Joining> = None;
    let mut retiring: Option<Retiring> = None;
    // The open line's copy said it steps aside: the line is replaced by
    // one on a current copy as soon as no replacement is under way, so no
    // call waits out the ones still running on it, and again if that
    // replacement fails (no sooner than [`MOVE_AGAIN`] after the last).
    let mut stepping_aside = false;
    let mut moved_at: Option<tokio::time::Instant> = None;
    loop {
        let pong_deadline = pong_due.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));
        let written = tokio::select! {
            _ = closed.changed() => {
                // Nothing is waiting on the line any more; a polite close is
                // all that is left, and its failing changes nothing.
                let _ = write(&mut sink, Message::Close(None), "closing").await;
                if let Some(retiring) = retiring.as_mut() {
                    let _ = write(&mut retiring.sink, Message::Close(None), "closing").await;
                }
                return Served::LetGo;
            }
            out = outbox.recv() => match out {
                None => return Served::LetGo,
                // One replacement at a time.
                Some(Out::Replace(done)) if opening.is_some() || joining.is_some() || retiring.is_some() => {
                    let _ = done.send(Err("the line is being replaced already".into()));
                    Ok(())
                }
                Some(Out::Replace(done)) => {
                    opening = Some((reopening(opens), tokio::time::Instant::now(), done));
                    Ok(())
                }
                // A call written on the replaced line is forgotten there.
                Some(Out::Forget(id)) if retiring.as_ref().is_some_and(|retiring| retiring.owed.contains(&id)) => {
                    let retiring = retiring.as_mut().expect("looked at");
                    let _ = write_out(&mut retiring.sink, line, Out::Forget(id)).await;
                    Ok(())
                }
                Some(out) => write_out(&mut sink, line, out).await,
            },
            opened_new = async { opening.as_mut().expect("armed").0.as_mut().await }, if opening.is_some() => {
                let (_, at, done) = opening.take().expect("armed");
                match opened_new {
                    Ok(socket) => {
                        let (mut new_sink, new_stream) = socket.split();
                        match write(&mut new_sink, follow(), "asking to follow").await {
                            Ok(()) => joining = Some(Joining { sink: new_sink, stream: new_stream, opened: at, done, stepping_aside: false }),
                            Err(why) => drop(done.send(Err(why))),
                        }
                    }
                    Err(e) => drop(done.send(Err(format!("{e:#}")))),
                }
                Ok(())
            }
            message = async { joining.as_mut().expect("armed").stream.next().await }, if joining.is_some() => {
                let new = joining.as_mut().expect("armed");
                let failed = match message {
                    // The broker follows on the new line: calls go out on it
                    // from now on, and the old one is read until it owes
                    // nothing.
                    Some(Ok(Message::Text(text))) => match serde_json::from_str::<Notice>(&text) {
                        Ok(Notice::Following { listening, max_age_secs }) => {
                            let new = joining.take().expect("armed");
                            let owed = line.written();
                            let old_sink = std::mem::replace(&mut sink, new.sink);
                            let old_stream = std::mem::replace(&mut stream, new.stream);
                            opened = new.opened;
                            pong_due = None;
                            let rechecked = line.followed_instead(listening, max_age_secs, opened);
                            retiring = Some(Retiring { sink: old_sink, stream: old_stream, owed, closing: false, stepping_aside, rechecked });
                            stepping_aside = new.stepping_aside;
                            let _ = new.done.send(Ok(()));
                            None
                        }
                        // A copy that stepped aside already says so before
                        // it follows: the line it opens is replaced in turn.
                        Ok(Notice::SteppingAside) => {
                            new.stepping_aside = true;
                            None
                        }
                        // The broker says nothing else before it follows.
                        Ok(_) => None,
                        Err(e) => Some(format!("a notice that does not read ({text}): {e}")),
                    },
                    Some(Ok(Message::Ping(payload))) => write(&mut new.sink, Message::Pong(payload), "answering a ping").await.err(),
                    Some(Ok(Message::Close(frame))) => Some(format!("the broker closed the new line ({frame:?})")),
                    Some(Ok(_)) => None,
                    Some(Err(e)) => Some(format!("reading the new line: {e}")),
                    None => Some("the broker closed the new line".into()),
                };
                if let Some(why) = failed {
                    let _ = joining.take().expect("armed").done.send(Err(why));
                }
                Ok(())
            }
            message = async { retiring.as_mut().expect("armed").stream.next().await }, if retiring.is_some() => {
                let old = retiring.as_mut().expect("armed");
                let closed_by_broker = matches!(message, Some(Ok(Message::Close(_)))) && !old.closing;
                let broke = match message {
                    Some(Ok(Message::Binary(bytes))) => read_answer(line, &bytes).err(),
                    // What it pushes still counts, and so does a recheck: the
                    // gap it covers may have opened before the new line
                    // followed, and only the old broker knows of it, unless
                    // waiters already looked again when the new line
                    // followed, which covers that gap. How the old broker
                    // listens no longer counts.
                    Some(Ok(Message::Text(text))) => match serde_json::from_str::<Notice>(&text) {
                        Ok(notice @ Notice::Signal { .. }) => {
                            line.heard(notice, opened);
                            None
                        }
                        Ok(Notice::Recheck { .. }) => {
                            if !old.rechecked {
                                let _ = line.notices.send(Heard::Recheck);
                            }
                            None
                        }
                        Ok(_) => None,
                        Err(e) => Some(format!("a notice that does not read ({text}): {e}")),
                    },
                    Some(Ok(Message::Ping(payload))) if !old.closing => write(&mut old.sink, Message::Pong(payload), "answering a ping").await.err(),
                    Some(Ok(Message::Close(_))) | None if old.closing => Some(String::new()),
                    Some(Ok(Message::Close(frame))) => Some(format!("the broker closed the replaced line ({frame:?})")),
                    Some(Ok(_)) => None,
                    Some(Err(e)) => Some(format!("reading the replaced line: {e}")),
                    None => Some("the broker closed the replaced line".into()),
                };
                if let Some(why) = broke {
                    // A copy that stepped aside closes the line once it ran
                    // everything it took: what it still owes never ran.
                    // Anything else that ends it may have landed.
                    let owed = match closed_by_broker && old.stepping_aside {
                        true => Owed::NeverRan,
                        false => Owed::MayHaveLanded,
                    };
                    line.unanswered(&old.owed, &why, owed);
                    retiring = None;
                }
                Ok(())
            }
            _ = idle_look.tick() => {
                // Looked at from this task alone, which is the one that
                // writes calls: none can be written between the look and
                // the close, and one asked for meanwhile waits in the
                // outbox and opens the line again.
                match (line.idle(), idle_since) {
                    (false, _) => idle_since = None,
                    (true, None) => idle_since = Some(tokio::time::Instant::now()),
                    (true, Some(since)) if since.elapsed() >= idle_after => {
                        let _ = write(&mut sink, Message::Close(None), "closing while idle").await;
                        return Served::Idle;
                    }
                    (true, Some(_)) => {}
                }
                Ok(())
            }
            message = stream.next() => {
                pong_due = None;
                match message {
                    None => Err("the broker closed the line".into()),
                    Some(Err(e)) => Err(format!("reading: {e}")),
                    Some(Ok(Message::Binary(bytes))) => read_answer(line, &bytes),
                    Some(Ok(Message::Text(text))) => match serde_json::from_str::<Notice>(&text) {
                        // Its copy steps aside: the line is replaced (below).
                        Ok(Notice::SteppingAside) => {
                            stepping_aside = true;
                            Ok(())
                        }
                        Ok(notice) => {
                            line.heard(notice, opened);
                            Ok(())
                        }
                        Err(e) => Err(format!("a notice that does not read ({text}): {e}")),
                    },
                    // A copy that stepped aside closes the line once it ran
                    // everything it took.
                    Some(Ok(Message::Close(frame))) if stepping_aside => {
                        let why = format!("the broker copy stepped aside and closed the line ({frame:?})");
                        return Served::SteppedAside { why, retiring: retiring.map(|old| old.owed).unwrap_or_default() };
                    }
                    Some(Ok(Message::Close(frame))) => Err(format!("the broker closed the line ({frame:?})")),
                    Some(Ok(Message::Ping(payload))) => write(&mut sink, Message::Pong(payload), "answering a ping").await,
                    Some(Ok(_)) => Ok(()),
                }
            }
            _ = ping.tick() => {
                pong_due.get_or_insert_with(|| tokio::time::Instant::now() + PONG_WAIT);
                write(&mut sink, Message::Ping(Default::default()), "pinging").await
            }
            _ = tokio::time::sleep_until(pong_deadline), if pong_due.is_some() => {
                Err(format!("the broker said nothing for {PONG_WAIT:?} after a ping"))
            }
        };
        if let Err(why) = written {
            return Served::Broke(why);
        }
        // The replaced line owes nothing any more: it is closed, and read
        // until the broker closes it too.
        if let Some(old) = retiring.as_mut().filter(|old| !old.closing && !line.owes_any(&old.owed)) {
            old.closing = true;
            if write(&mut old.sink, Message::Close(None), "closing the replaced line").await.is_err() {
                retiring = None;
            }
        }
        // Nobody waits for this replacement's answer: it serves the line
        // itself.
        let replacing = opening.is_some() || joining.is_some() || retiring.is_some();
        if stepping_aside && !replacing && moved_at.is_none_or(|at| at.elapsed() >= MOVE_AGAIN) {
            moved_at = Some(tokio::time::Instant::now());
            opening = Some((reopening(opens), tokio::time::Instant::now(), oneshot::channel().0));
        }
    }
}

/// A line opening to replace the open one, within [`CONNECT_WAIT`].
fn reopening<'o>(opens: Opens<'o>) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Socket>> + Send + 'o>> {
    let connecting = tokio::time::timeout(CONNECT_WAIT, connect(opens.base_url, opens.token));
    Box::pin(async move { connecting.await.unwrap_or_else(|_| Err(anyhow::anyhow!("opening it took more than {CONNECT_WAIT:?}"))) })
}

/// The `Follow` a line opens with.
fn follow() -> Message {
    Message::Text(serde_json::to_string(&Notice::Follow).expect("a notice serializes").into())
}

/// Hand the answer framed in `bytes` to its call.
fn read_answer(line: &Line, bytes: &[u8]) -> Result<(), String> {
    let (head, body) = unframe::<AnswerHead>(bytes).map_err(|e| format!("an answer that does not read: {e:#}"))?;
    let status = reqwest::StatusCode::from_u16(head.status).map_err(|e| format!("an answer with status {}: {e}", head.status))?;
    line.answered(head.id, Answer { status, body: body.to_vec() });
    Ok(())
}

type Sink = futures::stream::SplitSink<Socket, Message>;

/// Write what the outbox asked for.
async fn write_out(sink: &mut Sink, line: &Line, out: Out) -> Result<(), String> {
    match out {
        // A call whose caller gave up has no frame left to write.
        Out::Call(id) => match line.take_frame(id) {
            Some(frame) => write(sink, Message::Binary(frame.into()), "writing a call").await,
            None => Ok(()),
        },
        Out::Forget(id) => {
            let forget = serde_json::to_string(&Notice::Forget { id }).expect("a notice serializes");
            write(sink, Message::Text(forget.into()), "forgetting a call").await
        }
        Out::Wake => Ok(()),
        Out::Replace(_) => unreachable!("a replacement is asked of the open line, never written on it"),
    }
}

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

    /// What a test has one line of a [`FakeBroker`] do.
    enum Push {
        Notice(Notice),
        /// Answer the call it holds.
        AnswerHeld,
    }

    /// A fake broker: each line it accepts follows at once (listening, cut
    /// after an hour) and answers every call with the number of the line
    /// it came on, except a call to `/v1/hold`, which it holds (counted in
    /// `holding`) until told to answer it, and the first call to a path
    /// under `/v1/drop/`, whose line it drops without answering, the way a
    /// broker copy that stops does. `closed` counts the lines the caller
    /// closed, `lines` reaches each line by its number, and `arrived` is
    /// the path of every call that reached it, in order.
    struct FakeBroker {
        address: String,
        closed: Arc<AtomicUsize>,
        holding: Arc<AtomicUsize>,
        lines: Arc<Mutex<HashMap<usize, mpsc::UnboundedSender<Push>>>>,
        arrived: Arc<Mutex<Vec<String>>>,
    }

    impl FakeBroker {
        fn push(&self, line: usize, push: Push) {
            self.lines.lock().unwrap()[&line].send(push).unwrap();
        }

        /// How many calls to `path` reached the broker.
        fn arrivals(&self, path: &str) -> usize {
            self.arrived.lock().unwrap().iter().filter(|arrived| *arrived == path).count()
        }
    }

    async fn fake_broker() -> FakeBroker {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker = FakeBroker {
            address: format!("http://{}", listener.local_addr().unwrap()),
            closed: Arc::default(),
            holding: Arc::default(),
            lines: Arc::default(),
            arrived: Arc::default(),
        };
        let (closed, holding, lines, arrived) = (broker.closed.clone(), broker.holding.clone(), broker.lines.clone(), broker.arrived.clone());
        tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let (pushes, mut pushed) = mpsc::unbounded_channel();
                let number = {
                    let mut lines = lines.lock().unwrap();
                    let number = lines.len() + 1;
                    lines.insert(number, pushes);
                    number
                };
                let (closed, holding, arrived) = (closed.clone(), holding.clone(), arrived.clone());
                tokio::spawn(async move {
                    let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                    let mut held = None;
                    loop {
                        tokio::select! {
                            message = socket.next() => match message {
                                Some(Ok(Message::Text(text))) if serde_json::from_str::<Notice>(&text).unwrap() == Notice::Follow => {
                                    let following = Notice::Following { listening: true, max_age_secs: Some(3600) };
                                    socket.send(Message::Text(serde_json::to_string(&following).unwrap().into())).await.unwrap();
                                }
                                Some(Ok(Message::Binary(bytes))) => {
                                    let (head, _) = unframe::<CallHead>(&bytes).unwrap();
                                    let first = {
                                        let mut arrived = arrived.lock().unwrap();
                                        arrived.push(head.path.clone());
                                        arrived.iter().filter(|path| **path == head.path).count() == 1
                                    };
                                    if first && head.path.starts_with("/v1/drop/") {
                                        return;
                                    }
                                    if head.path == "/v1/hold" {
                                        held = Some(head.id);
                                        holding.fetch_add(1, Ordering::SeqCst);
                                        continue;
                                    }
                                    let answer = frame(&AnswerHead { id: head.id, status: 200 }, number.to_string().as_bytes());
                                    socket.send(Message::Binary(answer.into())).await.unwrap();
                                }
                                Some(Ok(Message::Close(_))) => {
                                    closed.fetch_add(1, Ordering::SeqCst);
                                    return;
                                }
                                Some(Ok(_)) => {}
                                Some(Err(_)) | None => return,
                            },
                            Some(push) = pushed.recv() => match push {
                                Push::Notice(notice) => {
                                    socket.send(Message::Text(serde_json::to_string(&notice).unwrap().into())).await.unwrap();
                                }
                                Push::AnswerHeld => {
                                    let id = held.take().expect("a call is held");
                                    let answer = frame(&AnswerHead { id, status: 200 }, number.to_string().as_bytes());
                                    socket.send(Message::Binary(answer.into())).await.unwrap();
                                }
                            },
                        }
                    }
                });
            }
        });
        broker
    }

    async fn until(what: &str, done: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    fn worker_link(broker: &FakeBroker) -> BrokerLink {
        BrokerLink::new(broker.address.clone(), TokenSource::worker(Arc::new(weft_platform_traits::FixedToken("t".into())), "w-1"))
    }

    /// A call the fake broker answers with its line's number.
    const ANY: BrokerCall = BrokerCall { path: "/v1/x", twice: Twice::NotSafe };

    /// A call the fake broker holds until told to answer it.
    const HOLD: BrokerCall = BrokerCall { path: "/v1/hold", twice: Twice::NotSafe };

    async fn line_number(link: &BrokerLink) -> String {
        String::from_utf8(link.call(ANY, Vec::new(), DEFAULT_CALL_WAIT).await.unwrap().body).unwrap()
    }

    // A replaced line: calls go out on the new one, the old one closes once
    // it owes nothing, the cut is counted from the new one, and nobody is
    // told to look again (its `Following` is no recheck).
    weft_core::stress_test! {
        name: a_replaced_line_carries_on_and_tells_nobody_to_look_again,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let broker = fake_broker().await;
            let link = worker_link(&broker);
            let mut heard = link.subscribe();
            let _open = link.stay_open();
            assert_eq!(line_number(&link).await, "1");
            assert!(matches!(heard.next().await.unwrap(), Heard::Recheck), "a line that opens is a recheck");
            let first_cut = link.cut_at().borrow().expect("cut after an hour");

            link.replace().await.expect("replaced");
            assert_eq!(line_number(&link).await, "2", "calls go out on the new line");
            let cut = link.cut_at().borrow().expect("cut after an hour");
            assert!(cut.opened > first_cut.opened, "the cut counts from the new line");
            until("the replaced line is closed", || broker.closed.load(Ordering::SeqCst) == 1).await;
            let told = tokio::time::timeout(Duration::from_millis(200), heard.next()).await;
            assert!(told.is_err(), "nothing told to look again: {told:?}");
            assert!(link.subscribe().listening(), "what is kept on the line is still trusted");
        }
    }

    // A line replaced while its broker's listener did not hear: what was
    // pushed meanwhile is looked at again once the new line hears.
    weft_core::stress_test! {
        name: a_line_replaced_while_deaf_tells_everyone_to_look_again,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let broker = fake_broker().await;
            let link = worker_link(&broker);
            let mut heard = link.subscribe();
            let _open = link.stay_open();
            assert_eq!(line_number(&link).await, "1");
            assert!(matches!(heard.next().await.unwrap(), Heard::Recheck), "a line that opens is a recheck");
            broker.push(1, Push::Notice(Notice::Lost));
            assert!(matches!(heard.next().await.unwrap(), Heard::Lost));

            link.replace().await.expect("replaced");
            let told = tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("told");
            assert!(matches!(told, Ok(Heard::Recheck)), "told to look again: {told:?}");
            assert!(link.subscribe().listening(), "the new line hears");
        }
    }

    // A recheck the replaced line sends while it is read to its end is
    // passed on: the gap it covers may predate the new line.
    weft_core::stress_test! {
        name: a_recheck_on_the_replaced_line_is_passed_on,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let broker = fake_broker().await;
            let link = worker_link(&broker);
            let mut heard = link.subscribe();
            let _open = link.stay_open();
            assert_eq!(line_number(&link).await, "1");
            assert!(matches!(heard.next().await.unwrap(), Heard::Recheck), "a line that opens is a recheck");
            let held = tokio::spawn({
                let link = link.clone();
                async move { link.call(HOLD, Vec::new(), DEFAULT_CALL_WAIT).await.map(|answer| answer.body) }
            });
            until("the broker holds the call", || broker.holding.load(Ordering::SeqCst) == 1).await;

            link.replace().await.expect("replaced");
            broker.push(1, Push::Notice(Notice::Recheck { listening: false }));
            let told = tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("told");
            assert!(matches!(told, Ok(Heard::Recheck)), "told to look again: {told:?}");
            assert!(link.subscribe().listening(), "how the old broker listens no longer counts");

            broker.push(1, Push::AnswerHeld);
            assert_eq!(held.await.unwrap().unwrap(), b"1", "the old line answers what it owed");
            until("the replaced line is closed", || broker.closed.load(Ordering::SeqCst) == 1).await;
        }
    }

    // A line replaced while deaf already told everyone to look again when
    // the new line followed, which covers any gap the old broker knows of:
    // a recheck the replaced line sends afterwards is one read too many,
    // and is not passed on.
    weft_core::stress_test! {
        name: a_recheck_after_the_swap_already_rechecked_is_not_passed_on,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let broker = fake_broker().await;
            let link = worker_link(&broker);
            let mut heard = link.subscribe();
            let _open = link.stay_open();
            assert_eq!(line_number(&link).await, "1");
            assert!(matches!(heard.next().await.unwrap(), Heard::Recheck), "a line that opens is a recheck");
            broker.push(1, Push::Notice(Notice::Lost));
            assert!(matches!(heard.next().await.unwrap(), Heard::Lost));
            let held = tokio::spawn({
                let link = link.clone();
                async move { link.call(HOLD, Vec::new(), DEFAULT_CALL_WAIT).await.map(|answer| answer.body) }
            });
            until("the broker holds the call", || broker.holding.load(Ordering::SeqCst) == 1).await;

            link.replace().await.expect("replaced");
            let told = tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("told");
            assert!(matches!(told, Ok(Heard::Recheck)), "told to look again at the swap: {told:?}");

            broker.push(1, Push::Notice(Notice::Recheck { listening: true }));
            broker.push(1, Push::AnswerHeld);
            assert_eq!(held.await.unwrap().unwrap(), b"1", "the old line answers what it owed, after its recheck");
            let told = tokio::time::timeout(Duration::from_millis(200), heard.next()).await;
            assert!(told.is_err(), "nothing told to look again a second time: {told:?}");
        }
    }

    fn answered() -> Result<Answer, LineError> {
        Ok(Answer { status: reqwest::StatusCode::OK, body: b"1".to_vec() })
    }

    fn dropped() -> Result<Answer, LineError> {
        Err(LineError::Dropped("the broker closed the line".into()))
    }

    /// A call the line broke under after writing it is sent again only
    /// when it is harmless twice and still has time; a harmless one out of
    /// time had no answer, and one that is not safe keeps the drop. Nothing
    /// else is ever sent again by the line.
    #[test]
    fn only_a_dropped_call_harmless_twice_is_sent_again() {
        let wait = DEFAULT_CALL_WAIT;
        assert!(matches!(after_try(Twice::Harmless, dropped(), true, wait), Next::Again));
        assert!(matches!(after_try(Twice::Harmless, dropped(), false, wait), Next::Done(Err(LineError::NoAnswer(_)))));
        assert!(matches!(after_try(Twice::NotSafe, dropped(), true, wait), Next::Done(Err(LineError::Dropped(_)))));
        for twice in [Twice::Harmless, Twice::NotSafe] {
            let not_sent = Err(LineError::NotSent { waited: wait.to_send, why: "down".into(), busy: false });
            assert!(matches!(after_try(twice, not_sent, true, wait), Next::Done(Err(LineError::NotSent { .. }))));
            assert!(matches!(after_try(twice, Err(LineError::NoAnswer(wait.to_answer)), true, wait), Next::Done(Err(LineError::NoAnswer(_)))));
            assert!(matches!(after_try(twice, answered(), true, wait), Next::Done(Ok(_))));
        }
    }

    /// The drop a call that is not safe twice fails with says what
    /// happened and what to do, and keeps why the line broke.
    #[test]
    fn a_dropped_call_says_it_may_have_landed_and_can_be_run_again() {
        let said = LineError::Dropped("reading: peer closed connection without sending TLS close_notify".into()).to_string();
        assert!(said.contains("may or may not have landed"), "{said}");
        assert!(said.contains("can be run again"), "{said}");
        assert!(said.contains("peer closed connection without sending TLS close_notify"), "{said}");
    }

    // A broker copy that stops with a call on it: a call harmless twice is
    // answered on the next line, and one that is not safe fails saying it
    // may have landed, having reached the broker once.
    weft_core::stress_test! {
        name: a_call_harmless_twice_is_sent_again_when_its_line_drops_and_another_is_not,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            const READ: BrokerCall = BrokerCall { path: "/v1/drop/read", twice: Twice::Harmless };
            const WRITE: BrokerCall = BrokerCall { path: "/v1/drop/write", twice: Twice::NotSafe };
            let broker = fake_broker().await;
            let link = worker_link(&broker);

            let answer = link.call(READ, Vec::new(), DEFAULT_CALL_WAIT).await.expect("answered on the next line");
            assert_eq!(answer.body, b"2", "answered on the line that replaced the dropped one");
            assert_eq!(broker.arrivals(READ.path), 2, "sent once on each line");

            let failed = link.call(WRITE, Vec::new(), DEFAULT_CALL_WAIT).await.expect_err("dropped");
            assert!(
                failed.chain().any(|cause| matches!(cause.downcast_ref::<LineError>(), Some(LineError::Dropped(_)))),
                "{failed:#}"
            );
            assert!(format!("{failed:#}").contains("may or may not have landed"), "{failed:#}");
            assert_eq!(line_number(&link).await, "3", "the line came back");
            assert_eq!(broker.arrivals(WRITE.path), 1, "never sent again");
        }
    }

    #[test]
    fn a_notice_reads_back() {
        for notice in [
            Notice::Follow,
            Notice::Forget { id: 7 },
            Notice::Following { listening: true, max_age_secs: Some(3600) },
            Notice::Signal { channel: INFRA_STATUS_CHANNEL.into(), payload: "p".into() },
            Notice::Lost,
            Notice::Recheck { listening: false },
            Notice::Resumed,
            Notice::SteppingAside,
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
        with_line_stepping_aside(api, weft_task_store::aside::StepAside::new())
    }

    /// [`with_line`] for a fake broker copy that steps aside when `aside`
    /// says so ([`serve`]).
    pub fn with_line_stepping_aside(api: Router, aside: weft_task_store::aside::StepAside) -> Router {
        let served = api.clone();
        Router::new()
            .route(
                LINE_PATH,
                get(move |ws: WebSocketUpgrade| {
                    let (api, aside) = (served.clone(), aside.clone());
                    async move { upgrade(ws).on_upgrade(move |socket| serve(socket, api, None, aside)) }
                }),
            )
            .merge(api)
    }

    /// How much the broker reads of a line at a time. A line is mostly
    /// idle, and the read buffer is allocated in full as the line opens
    /// (tungstenite's own default is 128 KiB), so an idle line costs about
    /// this plus a few KiB of tasks and channels: with up to
    /// `deploy/terraform/gcp/serverless.tf`'s `broker_concurrency` lines on
    /// one copy, that is what keeps them all in a copy's memory. A larger
    /// message is read in more pieces; it still arrives whole.
    // SYNC: LINE_READ_BUFFER <-> deploy/terraform/gcp/serverless.tf (local.broker_concurrency)
    pub const LINE_READ_BUFFER: usize = 16 * 1024;

    /// The upgrade, sized for the line's largest message, read
    /// [`LINE_READ_BUFFER`] at a time.
    pub fn upgrade(ws: WebSocketUpgrade) -> WebSocketUpgrade {
        ws.max_message_size(MAX_MESSAGE).max_frame_size(MAX_MESSAGE).read_buffer_size(LINE_READ_BUFFER)
    }

    /// Serve one line: run each call through `api` and frame its answer
    /// back, and once the caller asks to follow, start `follower`. Without
    /// one the line says it follows nothing (`Following { listening: false }`),
    /// so nothing kept on it is ever trusted.
    ///
    /// Once this copy of the broker steps aside (`aside`,
    /// `weft_task_store::aside`), it says so on the line
    /// ([`Notice::SteppingAside`]), and the caller replaces the line with
    /// one on a current copy, moving its new calls there. This line takes
    /// no new call: one that arrives anyway (written before the caller
    /// heard) is answered [`crate::protocol::STEPPING_ASIDE`] with nothing
    /// of it run, which its caller sends again (`crate::client::turned_away`).
    /// The calls running are answered, and the caller closes the line once
    /// it owes nothing, so no call it wrote goes unanswered. A caller that
    /// does not close it (its replacement would not open) has it closed
    /// once nothing runs and [`weft_task_store::aside::GRACE`] has passed
    /// since the step aside.
    pub async fn serve(socket: WebSocket, api: Router, follower: Option<Follower>, aside: weft_task_store::aside::StepAside) {
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
        // Each running call by its id, so a caller that stops waiting
        // stops it (`Notice::Forget`), the way a request whose caller hung
        // up stops its handler. A call takes itself out when it ends.
        let by_id: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::task::AbortHandle>>> = Arc::default();
        let bytes = Arc::new(tokio::sync::Semaphore::new(BYTES_IN_FLIGHT));
        let mut follower = follower;
        // The tasks pushing notices down this line, stopped when it goes.
        let mut following: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        // Once the copy stepped aside: when the line is closed from this
        // end, if its caller has not closed it by then.
        let mut stepping_aside: Option<tokio::time::Instant> = None;
        loop {
            // Biased: once the copy stepped aside, no call read after that
            // is run, whichever was ready first.
            let message = tokio::select! {
                biased;
                () = aside.stepped(), if stepping_aside.is_none() => {
                    stepping_aside = Some(tokio::time::Instant::now() + weft_task_store::aside::GRACE);
                    let said = serde_json::to_string(&Notice::SteppingAside).expect("a notice serializes");
                    let _ = out.send(Message::Text(said.into())).await;
                    continue;
                }
                Some(_) = running.join_next(), if stepping_aside.is_some() => continue,
                () = tokio::time::sleep_until(stepping_aside.unwrap_or_else(tokio::time::Instant::now)), if stepping_aside.is_some() && running.is_empty() => break,
                message = stream.next() => message,
            };
            let Some(message) = message else { break };
            match message {
                Ok(Message::Binary(message)) if stepping_aside.is_some() => {
                    let Ok((head, _)) = unframe::<CallHead>(&message) else { break };
                    let answer = frame(&AnswerHead { id: head.id, status: 503 }, crate::protocol::STEPPING_ASIDE.as_bytes());
                    let _ = out.send(Message::Binary(answer.into())).await;
                }
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
                    let (api, out, ended) = (api.clone(), out.clone(), by_id.clone());
                    while running.try_join_next().is_some() {}
                    let id = head.id;
                    // Spawned under the lock its end takes, so a call that
                    // ends at once still finds itself there to take out.
                    let mut ids = by_id.lock().expect("line calls by id");
                    let handle = running.spawn(async move {
                        let (status, answer) = run(api, &head, message.slice(body_at..)).await;
                        ended.lock().expect("line calls by id").remove(&head.id);
                        let _ = out.send(Message::Binary(frame(&AnswerHead { id: head.id, status }, &answer).into())).await;
                        drop((call, held));
                    });
                    ids.insert(id, handle);
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
                                let _ = notices.send(Notice::Following { listening: false, max_age_secs: None }).await;
                            }
                        }
                    }
                    Ok(Notice::Forget { id }) => {
                        if let Some(call) = by_id.lock().expect("line calls by id").remove(&id) {
                            call.abort();
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
        if stepping_aside.is_some() {
            let _ = out.send(Message::Close(None)).await;
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
