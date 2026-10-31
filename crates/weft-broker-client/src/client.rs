//! HTTP-backed implementations of the trait surfaces defined in
//! `weft-journal::traits` and `weft-task-store::traits`. Drop-in
//! replacements for the Postgres clients for everything that reaches the
//! database through the broker (workers, the listener, the supervisor).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use weft_core::RunId;
use weft_journal::{BatchError, RecordClient};
use weft_task_store::tasks::{DedupOutcome, NewTask, TaskOutcome};
use weft_task_store::{InfraReader, TaskStoreClient};

use crate::line::{Answer, BrokerCall, BrokerLink, CallWait, LineError, DEFAULT_CALL_WAIT};
use crate::protocol::*;

/// Every call of every client below, over the process's one line to the
/// broker (`crate::line`), with the status read the way each kind of call
/// reads it.
#[derive(Clone)]
struct HttpCore {
    link: BrokerLink,
}

/// How much longer than the hold the line waits for a held answer, so it
/// never gives up on a hold the broker is about to end.
const HOLD_GRACE: Duration = Duration::from_secs(5);

impl HttpCore {
    fn new(link: BrokerLink) -> Self {
        Self { link }
    }

    async fn post<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        call: BrokerCall,
        body: &Req,
    ) -> Result<Res> {
        let answer = self.post_raw(call, body, DEFAULT_CALL_WAIT).await?;
        Self::parse_success(answer, call.path)
    }

    /// A call the broker holds open for up to `hold`, however long that is:
    /// a line has no idle timeout. The hold starts once the call is
    /// written, which may take up to the usual wait to send, so the line
    /// waits that, the hold and [`HOLD_GRACE`], and never gives up before
    /// the broker's own hold ends.
    async fn post_held<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        call: BrokerCall,
        body: &Req,
        hold: Duration,
    ) -> Result<Res> {
        let to_send = DEFAULT_CALL_WAIT.to_send;
        let wait = CallWait { to_send, to_answer: to_send.saturating_add(hold).saturating_add(HOLD_GRACE) };
        let answer = self.post_raw(call, body, wait).await?;
        Self::parse_success(answer, call.path)
    }

    /// Single call core: the body as JSON, sent on the line, the raw
    /// answer back, sent again for as long as the broker turns it away
    /// busy with nothing of it run ([`while_busy`]), and, when the call is
    /// harmless twice, after its line broke under it (the line does that,
    /// `crate::line::BrokerCall`). Status interpretation lives in the thin
    /// wrappers (`post` = 2xx-or-error, `post_or_404` = 404 is "no row",
    /// `post_fenced` = 410 / 409 are the two stale `WriteOutcome`s).
    async fn post_raw<Req: Serialize>(&self, call: BrokerCall, body: &Req, wait: CallWait) -> Result<Answer> {
        let path = call.path;
        let body = serde_json::to_vec(body).with_context(|| format!("serialize {path}"))?;
        while_busy(path, turned_away, || self.link.call(call, &body, wait)).await.with_context(|| format!("POST {path}"))
    }

    /// Shared 2xx gate + JSON parse for the status-interpreting
    /// wrappers.
    fn parse_success<Res: for<'de> serde::Deserialize<'de>>(answer: Answer, path: &str) -> Result<Res> {
        if !answer.status.is_success() {
            return Err(refusal(path, &answer));
        }
        serde_json::from_slice(&answer.body).with_context(|| format!("parse {path}"))
    }

    /// Variant of `post` for content-addressed reads where 404 means
    /// "no row exists for this key" (a real "not found", NOT a race).
    /// Returns `Ok(None)` on 404 and `Ok(Some(_))` on 2xx; any other
    /// non-2xx is an error. Distinct from `post_fenced` because a
    /// content-addressed read CANNOT race: the row either exists
    /// under the requested key or it doesn't.
    pub async fn post_or_404<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        call: BrokerCall,
        body: &Req,
    ) -> Result<Option<Res>> {
        let answer = self.post_raw(call, body, DEFAULT_CALL_WAIT).await?;
        if answer.status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::parse_success(answer, call.path)?))
    }

    /// Variant of `post` for the fenced lifecycle writes: HTTP 410
    /// (this process lost the project) is `WriteOutcome::Displaced`, HTTP
    /// 409 (the target is gone while the process still owns the project)
    /// is `WriteOutcome::Gone`, and both are answers rather than
    /// errors so the caller decides what each means for its command.
    // SYNC: the two status codes <-> crates/weft-broker/src/handlers.rs
    //       (stale_write, the one place that emits them)
    pub async fn post_fenced<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        call: BrokerCall,
        body: &Req,
    ) -> Result<WriteOutcome<Res>> {
        let answer = self.post_raw(call, body, DEFAULT_CALL_WAIT).await?;
        match answer.status {
            reqwest::StatusCode::GONE => Ok(WriteOutcome::Displaced),
            reqwest::StatusCode::CONFLICT => Ok(WriteOutcome::Gone),
            _ => Ok(WriteOutcome::Applied(Self::parse_success(answer, call.path)?)),
        }
    }
}

/// The broker answered a non-2xx status. Typed so a caller can tell
/// one refusal from another (`anyhow::Error::downcast_ref`) instead of
/// reading the message.
#[derive(Debug, thiserror::Error)]
#[error("broker {path} returned {status}: {body}")]
pub struct BrokerRefused {
    pub path: String,
    pub status: reqwest::StatusCode,
    pub body: String,
}

/// A non-2xx answer to `path`, typed: [`WorkerOutdated`] when the broker
/// could not read the call's shape, [`BrokerRefused`] for every other
/// status. Every client below builds its refusals here.
pub fn refusal(path: &str, answer: &Answer) -> anyhow::Error {
    let body = String::from_utf8_lossy(&answer.body).into_owned();
    match answer.status {
        UNREADABLE_SHAPE => WorkerOutdated { path: path.to_string(), said: body }.into(),
        status => BrokerRefused { path: path.to_string(), status, body }.into(),
    }
}

/// The broker could not read a call this worker sent ([`UNREADABLE_SHAPE`]):
/// the broker runs a newer weft than the one that built this worker's
/// program, and the project has to be resynced before the call can work.
/// Asking again repeats the same answer.
#[derive(Debug, thiserror::Error)]
#[error(
    "this worker was built by an older weft than the one running now, and the broker cannot read its call to {path}: \
     run `weft resync` on this project to rebuild it (the broker said: {said})"
)]
pub struct WorkerOutdated {
    pub path: String,
    pub said: String,
}

impl BrokerRefused {
    /// The broker, or the database behind it, could not answer for now
    /// (`503`, or a gateway in front of it saying the same): asking
    /// again later may land. Any other status is the broker's answer to
    /// this call, which asking again repeats.
    pub fn is_outage(&self) -> bool {
        matches!(self.status, reqwest::StatusCode::BAD_GATEWAY | reqwest::StatusCode::SERVICE_UNAVAILABLE | reqwest::StatusCode::GATEWAY_TIMEOUT)
    }
}

/// Outcome of a fenced lifecycle write. `Applied(_)`: the write
/// landed and the caller can rely on its effect. The two stale
/// outcomes are deliberately distinct because the caller must do
/// different things with them:
/// - `Displaced` (HTTP 410): this process no longer owns the project (the
///   `infra_owner` lease moved to a sibling). The caller stops touching
///   the project and leaves the command UNCOMPLETED, so the new owner
///   re-runs it; `command_complete` is displaced for the same reason.
/// - `Gone` (HTTP 409): the target of the write is not there any more
///   while this process still owns the project: the infra_node row was
///   removed, the unit left the roster, or the command is already
///   completed. The caller's work for THAT target is moot; the rest of
///   the command proceeds and completes normally.
/// Conflating the two (one "raced" answer) once let a terminate whose
/// row vanished mid-flight return early, complete as succeeded, and
/// never delete the copy's workloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome<T> {
    Applied(T),
    Displaced,
    Gone,
}

impl<T> WriteOutcome<T> {
    pub fn is_applied(&self) -> bool {
        matches!(self, WriteOutcome::Applied(_))
    }
}

/// `wait` as a held call's `wait_ms`, in milliseconds. A wait too long to
/// count in them is sent as the most they hold, which the broker reads as
/// waiting for good (`weft_core::deadline::after`).
fn wait_ms(wait: Duration) -> u64 {
    u64::try_from(wait.as_millis()).unwrap_or(u64::MAX)
}

/// Whether a call was turned away because the broker was busy, with
/// nothing of it run, so sending it again repeats nothing: the broker
/// answered that it could not get a database connection for it
/// ([`NOT_DONE`]), or the line could not open for the call's whole wait
/// because every copy of the broker the platform may run was busy
/// (`LineError::NotSent` with `busy`). The predicate [`while_busy`] takes
/// for any call made on the line.
// SYNC: NOT_DONE <-> crates/weft-broker/src/handlers.rs unavailable_or_internal
pub fn turned_away(sent: &Result<Answer>) -> bool {
    match sent {
        Ok(answer) => answered_busy(answer.status, &answer.body),
        Err(e) => e.chain().any(|cause| matches!(cause.downcast_ref::<LineError>(), Some(LineError::NotSent { busy: true, .. }))),
    }
}

/// Whether a call was turned away busy with nothing of it run: the broker
/// answered that it could not get a database connection for it
/// ([`NOT_DONE`]), or that its copy is stepping aside for a newer one
/// ([`STEPPING_ASIDE`]: sent again, the call goes out on a line to a
/// current copy), or the platform in front of it answered 429 because
/// every copy of the broker it may run was busy and it handed the call to
/// none (Cloud Run at its most copies). The broker never answers 429
/// itself, so a 429 is always the platform's.
// SYNC: NOT_DONE <-> crates/weft-broker/src/handlers.rs unavailable_or_internal
pub fn answered_busy(status: reqwest::StatusCode, body: &[u8]) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || (status == reqwest::StatusCode::SERVICE_UNAVAILABLE && (body == NOT_DONE.as_bytes() || body == STEPPING_ASIDE.as_bytes()))
}

/// The first wait before asking again after the broker could not answer
/// a read; each further failure doubles it, up to
/// [`READ_RETRY_LONGEST`].
const READ_RETRY_FIRST: Duration = Duration::from_millis(200);
const READ_RETRY_LONGEST: Duration = Duration::from_secs(5);

/// Whether a failed broker call means the broker could not answer right
/// now: the line could not carry it or its answer, or the broker said so
/// (503, which it answers when its database is unreachable). Any other
/// status, including a 500 (a row that does not decode fails the same way
/// every time), or an answer that does not parse, would say the same
/// thing again, so it is not.
// SYNC: 503 = ask again <-> crates/weft-broker/src/handlers.rs unavailable_or_internal
pub fn broker_unavailable(e: &anyhow::Error) -> bool {
    if let Some(refused) = e.downcast_ref::<BrokerRefused>() {
        return refused.status == reqwest::StatusCode::SERVICE_UNAVAILABLE;
    }
    e.chain().any(|cause| cause.downcast_ref::<LineError>().is_some())
}

/// Run a read of a run's history until the broker answers it. Reading
/// again is free (nothing is written), and one failed read would
/// otherwise fail the whole run, so an unavailable broker is waited out
/// with a short backoff, every miss logged; a refusal still fails at
/// once. No deadline: the run is waiting on its own history, and a
/// broker that stays down shows in these warnings (`weft stop` ends the
/// run).
async fn read_until_answered<T, F, Fut>(path: &str, mut read: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut retry = READ_RETRY_FIRST;
    loop {
        match read().await {
            Err(e) if broker_unavailable(&e) => {
                tracing::warn!(
                    target: "weft_broker_client",
                    path,
                    error = %format!("{e:#}"),
                    retry_in_ms = retry.as_millis() as u64,
                    "the broker could not answer a read of this run's history; asking again"
                );
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(READ_RETRY_LONGEST);
            }
            answer => return answer,
        }
    }
}

/// How long a call turned away busy waits before it is sent again: at
/// first, then doubling up to the longest ([`busy_wait`]).
const BUSY_FIRST: Duration = Duration::from_millis(50);
const BUSY_LONGEST: Duration = Duration::from_secs(5);

/// The calls this process sent again because the broker was busy, said in
/// its log at most once a minute (`weft_task_store::throttle::Tally`).
static BUSY: weft_task_store::throttle::Tally = weft_task_store::throttle::Tally::new();

/// The wait after `last`: doubled, up to [`BUSY_LONGEST`].
fn busy_wait(last: Duration) -> Duration {
    (last * 2).min(BUSY_LONGEST)
}

/// `wait`, with up to half of it again at random, so callers turned away
/// together do not all come back on the same tick.
fn jittered(wait: Duration) -> Duration {
    let half = u64::try_from(wait.as_micros() / 2).unwrap_or(u64::MAX).max(1);
    wait + Duration::from_micros((uuid::Uuid::new_v4().as_u128() as u64) % half)
}

/// Count one call (or one try to open the line) the broker turned away
/// with nothing of it run, at `path`, and say the count in the log when
/// due: once at first, then once a minute with how many waited
/// ([`BUSY`]), whichever way they were turned away.
pub(crate) fn say_turned_away(path: &str) {
    if let Some(waits) = BUSY.note(std::time::Instant::now()) {
        tracing::warn!(
            target: "weft_broker_client",
            path, waits,
            "the broker turned calls away with nothing of them run (its database is busy or not answering it, or every \
             copy of it is in use), so they wait and are sent again. The broker's log says which; `weft status` shows when \
             the install is short of database connections"
        );
    }
}

/// Make a call, and send it again for as long as `turned_away` says the
/// broker turned it away with nothing of it run. The predicate is the
/// caller's; for a call made on the line it is this module's
/// [`turned_away`]. A turned-away call waits and is sent again, never
/// failed, however long it lasts: either the install is running as fast
/// as its database (or the broker's copies) let it, or the broker's
/// database is not answering it for now, and the broker's own log says
/// which (`weft_task_store::throttle`). The waits are said in this log,
/// once at first and then once a minute with how many calls waited. Any
/// other answer, or failure, is the call's.
pub async fn while_busy<T, F, Fut>(path: &str, turned_away: impl Fn(&T) -> bool, mut call: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let mut wait = BUSY_FIRST;
    loop {
        let sent = call().await;
        if !turned_away(&sent) {
            return sent;
        }
        say_turned_away(path);
        tokio::time::sleep(jittered(wait)).await;
        wait = busy_wait(wait);
    }
}

// ---------- Records ----------

/// A worker's writer lanes' way to the record, and its reads of it.
pub struct BrokerRecordClient {
    http: HttpCore,
}

impl BrokerRecordClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }
}

#[async_trait]
impl RecordClient for BrokerRecordClient {
    /// The batch's bytes are the call's body as they are: a batch is
    /// framed once, by its lane (`weft_journal::frame`), never as JSON.
    async fn record_batch(&self, batch: Vec<u8>) -> std::result::Result<weft_journal::frame::BatchAnswer, BatchError> {
        let answer = self
            .http
            .link
            .call_bytes(JOURNAL_RECORD, &[(BATCH_FATES_HEADER, BATCH_FATES)], batch, DEFAULT_CALL_WAIT)
            .await
            .map_err(BatchError::Unanswered)?;
        if answer.status.is_success() {
            return serde_json::from_slice(&answer.body)
                .map_err(|e| BatchError::Refused(format!("the broker's answer to a batch does not read: {e}")));
        }
        Err(batch_refusal(JOURNAL_RECORD_PATH, &answer))
    }

    async fn record_of(&self, run_id: RunId, after: Option<i32>) -> Result<weft_journal::record::RawRecord> {
        let req = RecordOfRequest { run_id, after };
        let resp: RecordOfResponse = read_until_answered(RECORD_OF.path, || self.http.post(RECORD_OF, &req)).await?;
        Ok(resp.record)
    }

    async fn give_up(&self, run_id: RunId, why: String) -> std::result::Result<(), BatchError> {
        let answer = self
            .http
            .post_raw(RUN_GIVE_UP, &RunGiveUpRequest { run_id, error: why }, DEFAULT_CALL_WAIT)
            .await
            .map_err(BatchError::Unanswered)?;
        if answer.status.is_success() {
            return Ok(());
        }
        Err(batch_refusal(RUN_GIVE_UP.path, &answer))
    }
}

/// A non-2xx answer to a call about a run's record, as the writer lanes
/// read it: an outage answered nothing about it; any other status is the
/// broker's answer to it, which sending it again repeats.
fn batch_refusal(path: &str, answer: &Answer) -> BatchError {
    let refused = refusal(path, answer);
    match refused.downcast_ref::<BrokerRefused>().is_some_and(BrokerRefused::is_outage) {
        true => BatchError::Unanswered(refused),
        false => BatchError::Refused(refused.to_string()),
    }
}

// ---------- TaskStore ----------

pub struct BrokerTaskStoreClient {
    http: HttpCore,
}

impl BrokerTaskStoreClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }
}

#[async_trait]
impl TaskStoreClient for BrokerTaskStoreClient {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome> {
        let req = TaskEnqueueDedupRequest { spec };
        let resp: TaskEnqueueDedupResponse =
            self.http.post(TASK_ENQUEUE_DEDUP, &req).await?;
        Ok(if resp.inserted { DedupOutcome::Inserted(resp.id) } else { DedupOutcome::AlreadyLive(resp.id) })
    }

    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome> {
        let req = TaskWaitTerminalRequest { task_id, wait_ms: wait_ms(timeout) };
        let resp: TaskWaitTerminalResponse = self.http.post_held(TASK_WAIT_TERMINAL, &req, timeout).await?;
        Ok(resp.into_outcome())
    }
}

// ---------- Signals (listener-only rehydrate) ----------

pub struct BrokerSignalClient {
    http: HttpCore,
}

impl BrokerSignalClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Every signal the listener must hold, of `project` alone when it
    /// names one.
    pub async fn list_held(&self, project: Option<uuid::Uuid>) -> Result<Vec<SignalRowWire>> {
        let resp: SignalListHeldResponse =
            self.http.post(SIGNAL_LIST_HELD, &SignalListHeldRequest { project }).await?;
        Ok(resp.rows)
    }

    /// Put an entry's event in its trigger's queue: its worker could not be
    /// reached (`/v1/door/park_fire`). `held_by`: the holder it was picked
    /// up under.
    pub async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> Result<DoorParked> {
        self.http
            .post(DOOR_PARK_FIRE, &DoorParkRequest { token: token.to_string(), fire: fire.clone(), held_by: held_by.map(str::to_string) })
            .await
    }

    /// Where an event of the signal `token` goes; `None` when it is gone.
    pub async fn fire_target(&self, token: &str) -> Result<Option<SignalFireTarget>> {
        self.http.post(SIGNAL_FIRE_TARGET, &SignalFireTargetRequest { token: token.to_string() }).await
    }

    /// One held signal by token, `None` when none is held under it.
    pub async fn get_held(&self, token: &str) -> Result<Option<SignalRowWire>> {
        let resp: SignalGetHeldResponse =
            self.http.post(SIGNAL_GET_HELD, &SignalGetHeldRequest { token: token.to_string() }).await?;
        Ok(resp.row)
    }

    /// Write a signal kind's durable state (see
    /// [`SignalWriteKindStateRequest`]). Whether it landed.
    pub async fn write_kind_state(&self, token: &str, kind_state: Value, from_seq: i64) -> Result<bool> {
        let resp: SignalWriteKindStateResponse = self
            .http
            .post(
                SIGNAL_WRITE_KIND_STATE,
                &SignalWriteKindStateRequest { token: token.to_string(), kind_state, from_seq },
            )
            .await?;
        Ok(resp.written)
    }

    /// One look of a holder (see [`SignalHoldRequest`]).
    pub async fn hold(&self, req: &SignalHoldRequest) -> Result<SignalHoldResponse> {
        self.http.post(SIGNAL_HOLD, req).await
    }

    /// Record what a signal's kind decides about holding it (see
    /// [`SignalSetHoldsRequest`]).
    pub async fn set_holds(&self, token: &str, holds: bool) -> Result<()> {
        let _: Value = self.http.post(SIGNAL_SET_HOLDS, &SignalSetHoldsRequest { token: token.to_string(), holds }).await?;
        Ok(())
    }

    /// Give up on a signal for good (see [`SignalGiveUpRequest`]).
    pub async fn give_up(&self, req: &SignalGiveUpRequest) -> Result<()> {
        let _: Value = self.http.post(SIGNAL_GIVE_UP, req).await?;
        Ok(())
    }

    /// Give up the claims on held signals (see [`SignalLetGoRequest`]).
    pub async fn let_go(&self, req: &SignalLetGoRequest) -> Result<()> {
        let _: Value = self.http.post(SIGNAL_LET_GO, req).await?;
        Ok(())
    }

    /// Say a holder's load, letting go of what it hands over (see
    /// [`SignalHolderLoadRequest`]).
    pub async fn holder_load(&self, req: &SignalHolderLoadRequest) -> Result<()> {
        let _: Value = self.http.post(SIGNAL_HOLDER_LOAD, req).await?;
        Ok(())
    }
}

// ---------- Provider events (listener serving surface) ----------

/// The listener's client for serving event subscriptions: resolve a
/// connection's event source, keep a provider-side subscription
/// alive, tear it down at unregister.
pub struct BrokerEventsClient {
    http: HttpCore,
}

impl BrokerEventsClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    pub async fn listener_resolve(
        &self,
        req: &ListenerResolveRequest,
    ) -> Result<ListenerResolvedSource> {
        self.http.post(LISTENER_RESOLVE, req).await
    }

    pub async fn subscription_ensure(
        &self,
        req: &SubscriptionEnsureRequest,
    ) -> Result<SubscriptionEnsureResponse> {
        self.http.post(SUBSCRIPTION_ENSURE, req).await
    }

    pub async fn subscription_drop(&self, req: &SubscriptionDropRequest) -> Result<()> {
        let _: serde_json::Value = self.http.post(SUBSCRIPTION_DROP, req).await?;
        Ok(())
    }
}

// ---------- Infra ----------

pub struct BrokerInfraClient {
    http: HttpCore,
}

impl BrokerInfraClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Ask for `project`'s infra health to be looked at now: what a machine
    /// running its infra says when how its units stand changed.
    pub async fn ask_for_a_look(&self, project: Uuid) -> Result<()> {
        let _: InfraLookResponse = self.http.post(INFRA_LOOK, &InfraLookRequest { project_id: project }).await?;
        Ok(())
    }

    /// What changed of this copy's values (`/v1/infra/pushed`), from the
    /// agent beside it: the link's identity names the copy. A refusal comes
    /// back as `BrokerRefused`, carrying the broker's reason.
    pub async fn push_values(&self, values: &weft_core::infra::bake::PushedValues) -> Result<()> {
        let _: serde_json::Value = self.http.post(INFRA_PUSHED, values).await?;
        Ok(())
    }
}

#[async_trait]
impl InfraReader for BrokerInfraClient {
    async fn endpoint_address(
        &self,
        run_id: weft_core::RunId,
        _run_instance: Option<&weft_core::instance::InstanceId>,
        infra: &weft_core::infra::InfraHandle,
    ) -> Result<Option<weft_core::infra::EndpointAddress>> {
        let req = InfraEndpointUrlRequest { run_id, infra: infra.clone() };
        let resp: InfraEndpointUrlResponse =
            self.http.post(INFRA_ENDPOINT_URL, &req).await?;
        Ok(resp.address)
    }

    async fn baked_outputs(
        &self,
        run_id: weft_core::RunId,
        _run_instance: Option<&weft_core::instance::InstanceId>,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
        let req = InfraBakedRequest { run_id, place: place.to_string(), instance: copy.cloned() };
        let resp: InfraBakedResponse = self.http.post(INFRA_BAKED, &req).await?;
        Ok(resp.saved)
    }
}

// ---------- Connections ----------

/// Worker-side client for the connection endpoints: resolve a
/// connection for one firing, release it when the node finishes. (Cost
/// rows ride the run's record batches, not an endpoint of their own.)
pub struct BrokerAccessClient {
    http: HttpCore,
}

impl BrokerAccessClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    pub async fn resolve_connection(
        &self,
        req: &ResolveConnectionRequest,
    ) -> Result<ResolveConnectionResponse> {
        self.http.post(ACCESS_RESOLVE, req).await
    }

    pub async fn release_connection(
        &self,
        req: &ReleaseConnectionRequest,
    ) -> Result<ReleaseConnectionResponse> {
        self.http.post(ACCESS_CLOSE, req).await
    }

    pub async fn mint_instance_token(
        &self,
        req: &ProgramMintInstanceTokenRequest,
    ) -> Result<weft_core::program::MintedInstanceToken> {
        self.http.post(MINT_INSTANCE_TOKEN, req).await
    }

    pub async fn publish_access(
        &self,
        req: &PublishAccessRequest,
    ) -> Result<PublishAccessResponse> {
        self.http.post(ACCESS_PUBLISH, req).await
    }

    pub async fn published_access(
        &self,
        req: &PublishedAccessRequest,
    ) -> Result<PublishedAccessResponse> {
        self.http.post(ACCESS_PUBLISHED, req).await
    }
}

// ---------- The worker's door ----------

/// What a worker's door asks the broker (`/v1/door/*`, and the caller
/// check of a gated route).
pub struct BrokerDoorClient {
    http: HttpCore,
}

impl BrokerDoorClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    pub async fn triggers(&self) -> Result<DoorTriggers> {
        self.http.post(DOOR_TRIGGERS, &DoorTriggersRequest::default()).await
    }

    pub async fn run_facts(&self, instance: Option<&weft_core::instance::InstanceId>) -> Result<DoorRunFacts> {
        self.http.post(DOOR_RUN_FACTS, &DoorRunFactsRequest { instance: instance.cloned() }).await
    }

    /// Put `fire` in the queue of the trigger `token`; `held_by`: the
    /// holder it was picked up under.
    pub async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> Result<DoorParked> {
        self.http
            .post(DOOR_PARK_FIRE, &DoorParkRequest { token: token.to_string(), fire: fire.clone(), held_by: held_by.map(str::to_string) })
            .await
    }

    /// The instance an instance token names; `Ok(None)` when the broker
    /// refuses it (it names nobody of this project).
    pub async fn instance_token(&self, token: &str) -> Result<Option<DoorInstanceToken>> {
        let answer = self
            .http
            .post_raw(DOOR_INSTANCE_TOKEN, &DoorInstanceTokenRequest { token: token.to_string() }, DEFAULT_CALL_WAIT)
            .await?;
        if answer.status == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(None);
        }
        HttpCore::parse_success(answer, DOOR_INSTANCE_TOKEN.path).map(Some)
    }

    pub async fn tick(&self, request: &DoorTickRequest) -> Result<DoorTick> {
        self.http.post(DOOR_TICK, request).await
    }

    /// What a caller of a gated route proved; `Ok(None)` when the broker
    /// refused them (the reason is in its log).
    pub async fn verify_caller(&self, request: &CallerVerifyRequest) -> Result<Option<CallerVerified>> {
        let answer = self.http.post_raw(CALLER_VERIFY, request, DEFAULT_CALL_WAIT).await?;
        if answer.status == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(None);
        }
        HttpCore::parse_success::<CallerVerified>(answer, CALLER_VERIFY.path).map(Some)
    }
}

// ---------- A worker's runs ----------

/// What a worker asks the broker about the runs it drives: claiming one it
/// was handed, letting one go, the answers and cancels waiting for them.
pub struct BrokerRunClient {
    http: HttpCore,
}

impl BrokerRunClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Claim queued run `run_id` (`RunClaimRequest`); `None` when it
    /// is not queued. A claim the broker could not even start is sent
    /// again.
    pub async fn claim(&self, run_id: RunId) -> Result<Option<weft_journal::record::Claimed>> {
        let req = RunClaimRequest { run_id, reads: RecordReads::ByPage };
        let resp: RunClaimResponse = self.http.post(RUN_CLAIM, &req).await?;
        Ok(resp.claimed)
    }

    /// This worker no longer drives `run_id`, its whole record
    /// written, for `why` (`DoorLetGoRequest`).
    pub async fn let_go(&self, run_id: RunId, why: LetGo) -> Result<()> {
        let _: DoorLetGo = self.http.post(DOOR_LET_GO, &DoorLetGoRequest { run_id, why }).await?;
        Ok(())
    }

    /// The answers waiting for `run_id`'s waits, but the waits in
    /// `taken`, read once (`RunAnswersRequest`).
    pub async fn answers(&self, run_id: RunId, taken: &[String]) -> Result<Vec<RunAnswer>> {
        let req = RunAnswersRequest { run_id, taken: taken.to_vec() };
        let resp: RunAnswersResponse = read_until_answered(RUN_ANSWERS.path, || self.http.post(RUN_ANSWERS, &req)).await?;
        Ok(resp.answers)
    }

    /// The cancels and the answers waiting for every run this worker
    /// drives, in one read (`RunRecheckedRequest`): what it asks once its
    /// line may have missed some.
    pub async fn rechecked(&self) -> Result<RunRecheckedResponse> {
        let req = RunRecheckedRequest::default();
        read_until_answered(RUN_RECHECKED.path, || self.http.post(RUN_RECHECKED, &req)).await
    }
}

// ---------- Run steering (worker tags/stops runs) ----------

/// The worker's door to steering runs: tag its own run, stop
/// its siblings by tag. Two endpoints, both worker-only and process-bound
/// on the broker side (`/v1/run/tag`, `/v1/run/stop_tagged`).
pub struct BrokerSteeringClient {
    http: HttpCore,
}

impl BrokerSteeringClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Tag `run_id` with `tags`. Synchronous: on return the tag rows
    /// exist (or the call failed), which is what lets a following
    /// `stop_tagged` anchor on them.
    pub async fn tag_run(&self, run_id: RunId, tags: Vec<String>) -> Result<()> {
        let req = RunTagRequest { run_id, tags };
        let _: RunTagResponse = self.http.post(RUN_TAG, &req).await?;
        Ok(())
    }

    /// Ask that every live run of `run_id`'s project carrying
    /// `tag` be stopped. Returns once the stop is durably queued; the
    /// dispatcher carries it out.
    pub async fn stop_tagged(&self, run_id: RunId, tag: String, stop_self: weft_core::StopSelf) -> Result<RunStopTaggedResponse> {
        let req = RunStopTaggedRequest { run_id, tag, stop_self };
        self.http.post(RUN_STOP_TAGGED, &req).await
    }
}

// ---------- Project (worker fetches own definition) ----------

/// Worker-side client for `/v1/project/fetch_definition`. Used at
/// run claim time to pull the runtime `ProjectDefinition`
/// keyed by `(project_id, expected_hash)`. The lookup is content-
/// addressed against the append-only `project_definition` history
/// table: the row either exists for the requested hash (200), or it
/// does not (404). There is no "raced" case for this endpoint
/// because the read does not contend with any writer.
pub struct BrokerProjectClient {
    http: HttpCore,
}

impl BrokerProjectClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Fetch the project's `ProjectDefinition` JSON keyed by hash.
    /// Returns `Some(resp)` on 200, `None` on 404 (no row for the
    /// requested key, a real "not found"), `Err` on every other
    /// failure (5xx, IO, parse). Callers turn `None` into a loud
    /// error: the dispatcher should never enqueue a task with a
    /// hash that has no history row, so a 404 here is a real bug
    /// upstream, not a recoverable race.
    pub async fn fetch_definition(
        &self,
        project_id: Uuid,
        expected_hash: &str,
    ) -> Result<Option<ProjectFetchDefinitionResponse>> {
        let req = ProjectFetchDefinitionRequest {
            project_id,
            expected_hash: expected_hash.to_string(),
        };
        self.http
            .post_or_404(PROJECT_FETCH_DEFINITION, &req)
            .await
    }
}

// ---------- Supervisor ----------

/// Broker client for the infra-supervisor. Talks to the
/// `/v1/supervisor/*` endpoints under the InfraSupervisor role.
pub struct BrokerSupervisorClient {
    http: HttpCore,
}

impl BrokerSupervisorClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Sync this supervisor's project ownership: renew its existing
    /// leases, claim a batch more unowned projects' infra (the exclusive
    /// `infra_owner` lease), and return the full set it now owns plus the
    /// ones this tick took on. The supervisor acts ONLY on the owned
    /// projects.
    pub async fn sync_ownership(&self, req: &SupervisorSyncOwnershipRequest) -> Result<SupervisorSyncOwnershipResponse> {
        self.http.post(SUPERVISOR_SYNC_OWNERSHIP, req).await
    }

    /// Pure read of the projects this process owns (no claim/renew). The work
    /// loops use this; ownership breadth changes only via `sync_ownership`.
    pub async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>> {
        let req = SupervisorOwnedProjectsRequest {
            replica: replica.to_string(),
        };
        let resp: SupervisorOwnedProjectsResponse = self
            .http
            .post(SUPERVISOR_OWNED_PROJECTS, &req)
            .await?;
        Ok(resp.owned)
    }

    /// Which of `copies` of `project` are gone for good (their copy
    /// ids), or `None` when `replica` does not hold the project's lease
    /// and so may not judge them; see [`SupervisorGoneCopiesRequest`].
    pub async fn gone_copies(
        &self,
        replica: &str,
        project: Uuid,
        copies: &[weft_core::infra::NodeRef],
    ) -> Result<Option<Vec<String>>> {
        let req = SupervisorGoneCopiesRequest { replica: replica.to_string(), project, copies: copies.to_vec() };
        match self.http.post_fenced::<_, SupervisorGoneCopiesResponse>(SUPERVISOR_GONE_COPIES, &req).await? {
            WriteOutcome::Applied(resp) => Ok(Some(resp.gone)),
            WriteOutcome::Displaced => Ok(None),
            WriteOutcome::Gone => Err(anyhow::anyhow!("{}: the broker answered 409, which a judgment never does", SUPERVISOR_GONE_COPIES.path)),
        }
    }

    pub async fn infra_nodes(&self, project_id: Uuid) -> Result<Vec<SupervisorInfraNode>> {
        let req = SupervisorInfraNodesRequest {
            project_id,
        };
        let resp: SupervisorInfraNodesResponse =
            self.http.post(SUPERVISOR_INFRA_NODES, &req).await?;
        Ok(resp.nodes)
    }

    /// Where each health rule of the project's copies stands, and how the
    /// commands they queued ended.
    pub async fn health_state(&self, project_id: Uuid) -> Result<SupervisorHealthStateResponse> {
        self.http.post(SUPERVISOR_HEALTH_STATE, &SupervisorHealthStateRequest { project_id }).await
    }

    /// Replace where a project's health rules stand (see
    /// [`SupervisorSetHealthStateRequest`]).
    pub async fn set_health_state(
        &self,
        req: &SupervisorSetHealthStateRequest,
    ) -> Result<WriteOutcome<SupervisorSetHealthStateResponse>> {
        self.http.post_fenced(SUPERVISOR_SET_HEALTH_STATE, req).await
    }

    /// The oldest waiting command of a project this supervisor owns that
    /// no older waiting command reaches a copy of, other than the ones it
    /// runs already (`busy_commands`), holding up to `wait` for one to be
    /// issued when none is waiting, or for a cancel of a busy one it has
    /// not heard yet (none of `cancelling`).
    pub async fn claim_command(
        &self,
        claimer_replica: &str,
        busy_commands: &[i64],
        cancelling: &[i64],
        wait: Duration,
    ) -> Result<SupervisorClaim> {
        let req = SupervisorClaimCommandRequest {
            claimer_replica: claimer_replica.to_string(),
            busy_commands: busy_commands.to_vec(),
            cancelling: cancelling.to_vec(),
            wait_ms: wait_ms(wait),
        };
        self.http.post_held(SUPERVISOR_CLAIM_COMMAND, &req, wait).await
    }

    pub async fn event_record(
        &self,
        project_id: Uuid,
        node_id: Option<&str>,
        instance: Option<&weft_core::instance::InstanceId>,
        event: crate::protocol::InfraEvent,
    ) -> Result<i64> {
        let (kind, payload) = event.into_record();
        let req = SupervisorEventRecordRequest {
            project_id,
            node_id: node_id.map(|s| s.to_string()),
            instance: instance.cloned(),
            kind,
            payload,
        };
        let resp: SupervisorEventRecordResponse =
            self.http.post(SUPERVISOR_EVENT_RECORD, &req).await?;
        Ok(resp.id)
    }

    pub async fn set_status(&self, req: &SupervisorSetStatusRequest) -> Result<WriteOutcome<SupervisorSetStatusResponse>> {
        self.http.post_fenced(SUPERVISOR_SET_STATUS, req).await
    }

    /// Record what the command `command_id` (an apply, a stop, a
    /// terminate) waits on for this copy.
    pub async fn set_waiting(
        &self,
        replica: &str,
        command_id: i64,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        waiting: &str,
    ) -> Result<WriteOutcome<SupervisorSetWaitingResponse>> {
        let req = SupervisorSetWaitingRequest {
            replica: replica.to_string(),
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            waiting: waiting.to_string(),
        };
        self.http
            .post_fenced::<_, SupervisorSetWaitingResponse>(SUPERVISOR_SET_WAITING, &req)
            .await
    }

    /// `Raced` when the caller no longer owns the project (ownership
    /// moved mid-Terminate): the supervisor aborts and leaves the
    /// command for the new owner. `Applied(removed)` otherwise.
    pub async fn remove_node(
        &self,
        replica: &str,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        command_id: i64,
    ) -> Result<WriteOutcome<SupervisorRemoveNodeResponse>> {
        let req = SupervisorRemoveNodeRequest {
            replica: replica.to_string(),
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            command_id,
        };
        self.http
            .post_fenced::<_, SupervisorRemoveNodeResponse>(SUPERVISOR_REMOVE_NODE, &req)
            .await
    }

    pub async fn command_complete(
        &self,
        replica: &str,
        command_id: i64,
        error: Option<&str>,
        cancelled: bool,
    ) -> Result<WriteOutcome<SupervisorCommandCompleteResponse>> {
        let req = SupervisorCommandCompleteRequest {
            replica: replica.to_string(),
            command_id,
            error: error.map(|s| s.to_string()),
            cancelled,
        };
        self.http
            .post_fenced::<_, SupervisorCommandCompleteResponse>(
                SUPERVISOR_COMMAND_COMPLETE,
                &req,
            )
            .await
    }

    /// The project's uncompleted supervisor commands (apply / stop /
    /// terminate), each as the copies it acts on. The health loop stands
    /// down for those copies while they are here, so it never fights the
    /// action.
    pub async fn infra_commands_in_flight(&self, project_id: Uuid) -> Result<Vec<InFlightCommand>> {
        let req = SupervisorInfraCommandInFlightRequest {
            project_id,
        };
        let resp: SupervisorInfraCommandInFlightResponse = self
            .http
            .post(SUPERVISOR_INFRA_COMMAND_IN_FLIGHT, &req)
            .await?;
        Ok(resp.commands)
    }

    pub async fn set_applied(
        &self,
        replica: &str,
        command_id: i64,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        applied_spec_hash: &str,
        addresses: AppliedEndpoints,
        keep_disks: Vec<String>,
        health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
        notes: Vec<String>,
        units: std::collections::BTreeMap<String, crate::protocol::UnitRuntime>,
    ) -> Result<WriteOutcome<SupervisorSetAppliedResponse>> {
        let req = SupervisorSetAppliedRequest {
            replica: replica.to_string(),
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            copy_id: copy_id.to_string(),
            applied_spec_hash: applied_spec_hash.to_string(),
            addresses,
            keep_disks,
            notes,
            units,
            health_rules,
        };
        self.http
            .post_fenced::<_, SupervisorSetAppliedResponse>(SUPERVISOR_SET_APPLIED, &req)
            .await
    }

    /// Pre-apply commitment: writes the infra_node row at
    /// Provisioning status with the locked-in copy_id + keep_disks.
    /// A subsequent apply failure leaves a visible row the user can
    /// Terminate. Apply success flips to Running via `set_applied`.
    pub async fn set_provisioning(
        &self,
        replica: &str,
        command_id: i64,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        keep_disks: Vec<String>,
        health_rules: Option<Vec<weft_core::infra::health::HealthRule>>,
        units: std::collections::BTreeMap<String, crate::protocol::UnitRuntime>,
    ) -> Result<WriteOutcome<SupervisorSetProvisioningResponse>> {
        let req = SupervisorSetProvisioningRequest {
            replica: replica.to_string(),
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            copy_id: copy_id.to_string(),
            keep_disks,
            units,
            health_rules,
        };
        self.http
            .post_fenced::<_, SupervisorSetProvisioningResponse>(
                SUPERVISOR_SET_PROVISIONING,
                &req,
            )
            .await
    }

    pub async fn project_image_tags(
        &self,
        project_id: Uuid,
        node_id: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        let req = SupervisorProjectImageTagsRequest {
            project_id,
            node_id: node_id.to_string(),
        };
        let resp: SupervisorProjectImageTagsResponse = self
            .http
            .post(SUPERVISOR_PROJECT_IMAGE_TAGS, &req)
            .await?;
        Ok(resp.tags)
    }

    /// Enqueue a dispatcher-targeted lifecycle command
    /// (`Deactivate(...)` | `Reactivate`). Used by the health rules'
    /// actions in the supervisor. Verb-and-payload are
    /// typed via `LifecycleSpec`, so the supervisor cannot
    /// accidentally enqueue a supervisor-owned verb.
    pub async fn enqueue_lifecycle(
        &self,
        project_id: Uuid,
        spec: crate::protocol::LifecycleSpec,
    ) -> Result<i64> {
        let req = SupervisorEnqueueLifecycleRequest {
            project_id,
            spec,
        };
        let resp: SupervisorEnqueueLifecycleResponse = self
            .http
            .post(SUPERVISOR_ENQUEUE_LIFECYCLE, &req)
            .await?;
        Ok(resp.command_id)
    }
}

/// Worker-callable broker client. Used by the engine to hand off an
/// InfraSpec to the supervisor and wait for it to settle. The worker
/// never reads prior applied state, never compiles, never decides
/// skip/fresh/replace; the supervisor owns the whole apply pipeline.
pub struct BrokerInfraStateClient {
    http: HttpCore,
}

impl BrokerInfraStateClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    pub async fn enqueue_apply(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        spec_json: serde_json::Value,
        run_id: weft_core::RunId,
    ) -> Result<i64> {
        let req = InfraEnqueueApplyRequest {
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            spec_json,
            run_id,
        };
        let resp: InfraEnqueueApplyResponse =
            self.http.post(INFRA_ENQUEUE_APPLY, &req).await?;
        Ok(resp.command_id)
    }

    /// Save what the infra setup `run_id` sent out on the baked
    /// outputs of its node at `place` (`InfraBakeRequest`).
    pub async fn save_bake(
        &self,
        run_id: weft_core::RunId,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
        values: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        let req = InfraBakeRequest { run_id, place: place.to_string(), instance: copy.cloned(), values };
        let _: serde_json::Value = self.http.post(INFRA_BAKE, &req).await?;
        Ok(())
    }

    /// The apply command's state, holding up to `wait` for it to
    /// complete.
    pub async fn wait_apply(
        &self,
        project_id: Uuid,
        command_id: i64,
        wait: Duration,
    ) -> Result<InfraWaitApplyResponse> {
        let req = InfraWaitApplyRequest { project_id, command_id, wait_ms: wait_ms(wait) };
        self.http.post_held(INFRA_WAIT_APPLY, &req, wait).await
    }
}

#[cfg(test)]
mod read_retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn refused(status: reqwest::StatusCode) -> anyhow::Error {
        BrokerRefused { path: "/v1/journal/wait".into(), status, body: String::new() }.into()
    }

    #[test]
    fn only_a_503_or_no_answer_is_unavailable() {
        assert!(broker_unavailable(&refused(reqwest::StatusCode::SERVICE_UNAVAILABLE)));
        assert!(!broker_unavailable(&refused(reqwest::StatusCode::INTERNAL_SERVER_ERROR)));
        assert!(!broker_unavailable(&refused(reqwest::StatusCode::FORBIDDEN)));
        assert!(!broker_unavailable(&refused(reqwest::StatusCode::NOT_FOUND)));
        assert!(!broker_unavailable(&anyhow::anyhow!("read SA token")));
    }

    /// A broker that fails twice and then answers: the read waits it out
    /// and hands back the answer.
    #[tokio::test(start_paused = true)]
    async fn a_read_waits_out_an_unavailable_broker() {
        let calls = AtomicU32::new(0);
        let answer = read_until_answered("/v1/journal/wait", || async {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(refused(reqwest::StatusCode::SERVICE_UNAVAILABLE)),
                _ => Ok(7),
            }
        })
        .await
        .unwrap();
        assert_eq!(answer, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    fn answer(status: reqwest::StatusCode, body: &str) -> Result<Answer> {
        Ok(Answer { status, body: body.as_bytes().to_vec() })
    }

    fn not_sent(busy: bool) -> Result<Answer> {
        Err(anyhow::Error::from(LineError::NotSent { waited: Duration::from_secs(30), why: "429".into(), busy }).context("POST /v1/x"))
    }

    /// Only a call nothing of which ran is sent again: the broker's own
    /// "nothing was done", or a line every copy of the broker turned away
    /// busy. Any other 503 may have half happened; a line down for any
    /// other reason is the call's failure.
    #[test]
    fn only_a_call_turned_away_busy_is_sent_again() {
        assert!(turned_away(&answer(reqwest::StatusCode::SERVICE_UNAVAILABLE, NOT_DONE)));
        assert!(turned_away(&not_sent(true)));
        assert!(turned_away(&answer(reqwest::StatusCode::TOO_MANY_REQUESTS, "Rate exceeded.")), "the platform handed it to no copy");
        assert!(!turned_away(&answer(reqwest::StatusCode::SERVICE_UNAVAILABLE, "the database connection dropped")));
        assert!(!turned_away(&answer(reqwest::StatusCode::INTERNAL_SERVER_ERROR, NOT_DONE)));
        assert!(!turned_away(&answer(reqwest::StatusCode::OK, "{}")));
        assert!(!turned_away(&not_sent(false)));
    }

    /// The wait between tries doubles up to its longest, and its jitter
    /// only ever adds, by less than half.
    #[test]
    fn a_busy_wait_doubles_up_to_its_longest() {
        let mut wait = BUSY_FIRST;
        let mut waits = vec![wait];
        while wait < BUSY_LONGEST {
            wait = busy_wait(wait);
            waits.push(wait);
        }
        assert_eq!(waits.first(), Some(&Duration::from_millis(50)));
        assert_eq!(busy_wait(BUSY_LONGEST), BUSY_LONGEST, "never past the longest");
        assert!(waits.windows(2).all(|w| w[1] == (w[0] * 2).min(BUSY_LONGEST)));
        for _ in 0..100 {
            let j = jittered(BUSY_FIRST);
            assert!(j >= BUSY_FIRST && j < BUSY_FIRST + BUSY_FIRST / 2, "{j:?}");
        }
    }

    /// A call turned away busy is sent again for as long as it takes, here
    /// well past the twenty seconds a call used to be given; one that
    /// failed otherwise is not sent again.
    #[tokio::test(start_paused = true)]
    async fn a_call_turned_away_busy_waits_until_it_goes_through() {
        let calls = AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        let sent = while_busy("/v1/task/complete", turned_away, || async {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0..20 => answer(reqwest::StatusCode::SERVICE_UNAVAILABLE, NOT_DONE),
                20 => not_sent(true),
                _ => answer(reqwest::StatusCode::OK, "7"),
            }
        })
        .await
        .unwrap();
        assert_eq!((sent.body, calls.load(Ordering::SeqCst)), (b"7".to_vec(), 22));
        assert!(started.elapsed() > Duration::from_secs(60), "it waited as long as the broker was busy");
        let calls = AtomicU32::new(0);
        let sent = while_busy("/v1/task/complete", turned_away, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            answer(reqwest::StatusCode::SERVICE_UNAVAILABLE, "the database connection dropped")
        })
        .await
        .unwrap();
        assert_eq!(sent.status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "a 503 that may have half happened is not sent again");
    }

    /// A refusal would say the same thing again: it fails at once.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_fails_at_once() {
        let calls = AtomicU32::new(0);
        let answer: Result<u32> = read_until_answered("/v1/journal/wait", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(refused(reqwest::StatusCode::FORBIDDEN))
        })
        .await;
        assert!(answer.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod refusal_tests {
    use super::*;

    fn answer(status: reqwest::StatusCode, body: &str) -> Answer {
        Answer { status, body: body.as_bytes().to_vec() }
    }

    /// The broker's answer to a call it cannot read is one error, saying
    /// the project needs a resync, on every client and at any depth of
    /// context; it is not an outage, so nothing waits it out.
    #[test]
    fn a_call_the_broker_cannot_read_says_the_project_needs_a_resync() {
        let said = unreadable_shape("/v1/caller/verify: invalid type: map, expected a sequence");
        let e = refusal("/v1/caller/verify", &answer(UNREADABLE_SHAPE, &said)).context("verify a caller");
        let typed = e.chain().find_map(|cause| cause.downcast_ref::<WorkerOutdated>()).expect("the typed error");
        assert_eq!(typed.path, "/v1/caller/verify");
        assert!(format!("{e:#}").contains("run `weft resync` on this project"), "{e:#}");
        assert!(!broker_unavailable(&e));
        assert!(refusal("/v1/caller/verify", &answer(reqwest::StatusCode::BAD_REQUEST, "no")).downcast_ref::<WorkerOutdated>().is_none());
    }

    /// A writer lane's batch the broker cannot read fails its runs with
    /// the same words, never as an outage to send again.
    #[test]
    fn a_batch_the_broker_cannot_read_is_refused_with_the_resync() {
        match batch_refusal(JOURNAL_RECORD_PATH, &answer(UNREADABLE_SHAPE, &unreadable_shape("x"))) {
            BatchError::Refused(why) => assert!(why.contains("`weft resync`"), "{why}"),
            other => panic!("refused, got {other:?}"),
        }
    }
}
