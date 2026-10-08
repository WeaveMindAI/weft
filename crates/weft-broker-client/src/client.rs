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

use weft_core::ExecutionId;
use weft_journal::{BatchError, RecordClient};
use weft_task_store::tasks::{DedupOutcome, NewTask, TaskOutcome};
use weft_task_store::{InfraReader, TaskStoreClient};

use crate::line::{Answer, BrokerLink, CallWait, LineError, DEFAULT_CALL_WAIT};
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
        path: &str,
        body: &Req,
    ) -> Result<Res> {
        let answer = self.post_raw(path, body, DEFAULT_CALL_WAIT).await?;
        Self::parse_success(answer, path)
    }

    /// A call the broker holds open for up to `hold` (never more than
    /// `MAX_HOLD`, which it enforces). The hold starts once the call is
    /// written, which may take up to the usual wait to send, so the line
    /// waits that, the hold and [`HOLD_GRACE`], and never gives up before
    /// the broker's own hold ends.
    async fn post_held<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
        hold: Duration,
    ) -> Result<Res> {
        let to_send = DEFAULT_CALL_WAIT.to_send;
        let wait = CallWait { to_send, to_answer: to_send + hold + HOLD_GRACE };
        let answer = self.post_raw(path, body, wait).await?;
        Self::parse_success(answer, path)
    }

    /// Single call core: the body as JSON, sent on the line, the raw
    /// answer back. Status interpretation lives in the thin wrappers
    /// (`post` = 2xx-or-error, `post_or_404` = 404 is "no row",
    /// `post_fenced` = 410 / 409 are the two stale `WriteOutcome`s).
    async fn post_raw<Req: Serialize>(&self, path: &str, body: &Req, wait: CallWait) -> Result<Answer> {
        let body = serde_json::to_vec(body).with_context(|| format!("serialize {path}"))?;
        self.link.call(path, body, wait).await.with_context(|| format!("POST {path}"))
    }

    /// Shared 2xx gate + JSON parse for the status-interpreting
    /// wrappers.
    fn parse_success<Res: for<'de> serde::Deserialize<'de>>(answer: Answer, path: &str) -> Result<Res> {
        if !answer.status.is_success() {
            let body = String::from_utf8_lossy(&answer.body).into_owned();
            return Err(BrokerRefused { path: path.to_string(), status: answer.status, body }.into());
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
        path: &str,
        body: &Req,
    ) -> Result<Option<Res>> {
        let answer = self.post_raw(path, body, DEFAULT_CALL_WAIT).await?;
        if answer.status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::parse_success(answer, path)?))
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
        path: &str,
        body: &Req,
    ) -> Result<WriteOutcome<Res>> {
        let answer = self.post_raw(path, body, DEFAULT_CALL_WAIT).await?;
        match answer.status {
            reqwest::StatusCode::GONE => Ok(WriteOutcome::Displaced),
            reqwest::StatusCode::CONFLICT => Ok(WriteOutcome::Gone),
            _ => Ok(WriteOutcome::Applied(Self::parse_success(answer, path)?)),
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

/// Wait up to `wait` in holds the broker grants (each at most
/// `MAX_HOLD`, which it enforces), asking again after a hold that ended
/// without the answer `done` looks for. Hands back the last answer. A
/// `hold` that waits out an unavailable broker ([`read_until_answered`])
/// stretches past `wait` by as long as the broker stays down.
async fn held<T, F, Fut>(wait: Duration, mut hold: F, done: impl Fn(&T) -> bool) -> Result<T>
where
    F: FnMut(Duration) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let answer = hold(left.min(weft_task_store::pg_signal::MAX_HOLD)).await?;
        if done(&answer) || left <= weft_task_store::pg_signal::MAX_HOLD {
            return Ok(answer);
        }
    }
}

/// Whether the broker answered that the call did nothing at all
/// ([`NOT_DONE`]), so sending it again repeats nothing.
// SYNC: NOT_DONE <-> crates/weft-broker/src/handlers.rs unavailable_or_internal
fn did_nothing(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<BrokerRefused>()
            .is_some_and(|refused| refused.status == reqwest::StatusCode::SERVICE_UNAVAILABLE && refused.body == NOT_DONE)
    })
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
fn broker_unavailable(e: &anyhow::Error) -> bool {
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

/// How long a call the broker answered [`NOT_DONE`] is sent again for, at
/// first and at most between tries, and in all: the broker's database
/// connections were all busy, which passes in moments, and one busy this
/// long is a broker in trouble the caller hears about.
const NOT_DONE_FIRST: Duration = Duration::from_millis(50);
const NOT_DONE_LONGEST: Duration = Duration::from_secs(2);
const NOT_DONE_FOR: Duration = Duration::from_secs(20);

/// Make a call, and send it again for as long as the broker answers that it
/// did nothing ([`did_nothing`]), within [`NOT_DONE_FOR`]: a claim or a
/// task's end the broker could not even start is safe to send again, and
/// failing it instead would fail a caller's run, or leave a finished task
/// claimed until its lease runs out and it runs again.
async fn until_done<T, F, Fut>(path: &str, mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let started = tokio::time::Instant::now();
    let mut wait = NOT_DONE_FIRST;
    loop {
        match call().await {
            Err(e) if did_nothing(&e) && started.elapsed() < NOT_DONE_FOR => {
                tracing::warn!(target: "weft_broker_client", path, retry_in_ms = wait.as_millis() as u64, "the broker did nothing with a call (its database connections were busy); sending it again");
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(NOT_DONE_LONGEST);
            }
            answer => return answer,
        }
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
            .call_bytes(JOURNAL_RECORD_PATH, batch, DEFAULT_CALL_WAIT)
            .await
            .map_err(BatchError::Unanswered)?;
        if answer.status.is_success() {
            return serde_json::from_slice(&answer.body)
                .map_err(|e| BatchError::Refused(format!("the broker's answer to a batch does not read: {e}")));
        }
        let refused = BrokerRefused {
            path: JOURNAL_RECORD_PATH.to_string(),
            status: answer.status,
            body: String::from_utf8_lossy(&answer.body).into_owned(),
        };
        // An outage answered nothing about the batch; any other status is
        // the broker's answer to it, which sending it again repeats.
        Err(if refused.is_outage() { BatchError::Unanswered(refused.into()) } else { BatchError::Refused(refused.to_string()) })
    }

    async fn record_of(&self, execution_id: ExecutionId) -> Result<weft_journal::record::RawRecord> {
        let path = "/v1/journal/record_of";
        let req = RecordOfRequest { execution_id };
        let resp: RecordOfResponse = read_until_answered(path, || self.http.post(path, &req)).await?;
        Ok(resp.record)
    }

    async fn give_up(&self, execution_id: ExecutionId, why: String) -> std::result::Result<(), BatchError> {
        let path = "/v1/run/give_up";
        let answer = self
            .http
            .post_raw(path, &RunGiveUpRequest { execution_id, error: why }, DEFAULT_CALL_WAIT)
            .await
            .map_err(BatchError::Unanswered)?;
        if answer.status.is_success() {
            return Ok(());
        }
        let refused = BrokerRefused { path: path.to_string(), status: answer.status, body: String::from_utf8_lossy(&answer.body).into_owned() };
        Err(if refused.is_outage() { BatchError::Unanswered(refused.into()) } else { BatchError::Refused(refused.to_string()) })
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
            self.http.post("/v1/task/enqueue_dedup", &req).await?;
        Ok(if resp.inserted { DedupOutcome::Inserted(resp.id) } else { DedupOutcome::AlreadyLive(resp.id) })
    }

    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome> {
        held(
            timeout,
            |hold| async move {
                let req = TaskWaitTerminalRequest { task_id, wait_ms: hold.as_millis() as u64 };
                let resp: TaskWaitTerminalResponse =
                    self.http.post_held("/v1/task/wait_terminal", &req, hold).await?;
                Ok(resp.into_outcome())
            },
            |outcome| outcome.status.is_terminal(),
        )
        .await
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
            self.http.post("/v1/signal/list_held", &SignalListHeldRequest { project }).await?;
        Ok(resp.rows)
    }

    /// Put an entry's event in its trigger's queue: its worker could not be
    /// reached (`/v1/door/park_fire`). `held_by`: the holder it was picked
    /// up under.
    pub async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> Result<DoorParked> {
        self.http
            .post("/v1/door/park_fire", &DoorParkRequest { token: token.to_string(), fire: fire.clone(), held_by: held_by.map(str::to_string) })
            .await
    }

    /// Where an event of the signal `token` goes; `None` when it is gone.
    pub async fn fire_target(&self, token: &str) -> Result<Option<SignalFireTarget>> {
        self.http.post("/v1/signal/fire_target", &SignalFireTargetRequest { token: token.to_string() }).await
    }

    /// One held signal by token, `None` when none is held under it.
    pub async fn get_held(&self, token: &str) -> Result<Option<SignalRowWire>> {
        let resp: SignalGetHeldResponse =
            self.http.post("/v1/signal/get_held", &SignalGetHeldRequest { token: token.to_string() }).await?;
        Ok(resp.row)
    }

    /// Write a signal kind's durable state (see
    /// [`SignalWriteKindStateRequest`]). Whether it landed.
    pub async fn write_kind_state(&self, token: &str, kind_state: Value, from_seq: i64) -> Result<bool> {
        let resp: SignalWriteKindStateResponse = self
            .http
            .post(
                "/v1/signal/write_kind_state",
                &SignalWriteKindStateRequest { token: token.to_string(), kind_state, from_seq },
            )
            .await?;
        Ok(resp.written)
    }

    /// One look of a holder (see [`SignalHoldRequest`]).
    pub async fn hold(&self, req: &SignalHoldRequest) -> Result<SignalHoldResponse> {
        self.http.post("/v1/signal/hold", req).await
    }

    /// Record what a signal's kind decides about holding it (see
    /// [`SignalSetHoldsRequest`]).
    pub async fn set_holds(&self, token: &str, holds: bool) -> Result<()> {
        let _: Value = self.http.post("/v1/signal/set_holds", &SignalSetHoldsRequest { token: token.to_string(), holds }).await?;
        Ok(())
    }

    /// Give up the claims on held signals (see [`SignalLetGoRequest`]).
    pub async fn let_go(&self, req: &SignalLetGoRequest) -> Result<()> {
        let _: Value = self.http.post("/v1/signal/let_go", req).await?;
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
        self.http.post("/v1/access/listener-resolve", req).await
    }

    pub async fn listener_infra_address(&self, req: &ListenerInfraAddressRequest) -> Result<ListenerInfraAddress> {
        self.http.post("/v1/infra/listener-address", req).await
    }

    pub async fn subscription_ensure(
        &self,
        req: &SubscriptionEnsureRequest,
    ) -> Result<SubscriptionEnsureResponse> {
        self.http.post("/v1/access/subscription/ensure", req).await
    }

    pub async fn subscription_drop(&self, req: &SubscriptionDropRequest) -> Result<()> {
        let _: serde_json::Value = self.http.post("/v1/access/subscription/drop", req).await?;
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
        let _: InfraLookResponse = self.http.post("/v1/infra/look", &InfraLookRequest { project_id: project }).await?;
        Ok(())
    }

    /// What changed of this copy's values (`/v1/infra/pushed`), from the
    /// agent beside it: the link's identity names the copy. A refusal comes
    /// back as `BrokerRefused`, carrying the broker's reason.
    pub async fn push_values(&self, values: &weft_core::infra::bake::PushedValues) -> Result<()> {
        let _: serde_json::Value = self.http.post("/v1/infra/pushed", values).await?;
        Ok(())
    }
}

#[async_trait]
impl InfraReader for BrokerInfraClient {
    async fn endpoint_address(
        &self,
        execution_id: weft_core::ExecutionId,
        _run_instance: Option<&weft_core::instance::InstanceId>,
        infra: &weft_core::infra::InfraHandle,
    ) -> Result<Option<weft_core::infra::EndpointAddress>> {
        let req = InfraEndpointUrlRequest { execution_id, infra: infra.clone() };
        let resp: InfraEndpointUrlResponse =
            self.http.post("/v1/infra/endpoint_url", &req).await?;
        Ok(resp.address)
    }

    async fn baked_outputs(
        &self,
        execution_id: weft_core::ExecutionId,
        _run_instance: Option<&weft_core::instance::InstanceId>,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
        let req = InfraBakedRequest { execution_id, place: place.to_string(), instance: copy.cloned() };
        let resp: InfraBakedResponse = self.http.post("/v1/infra/baked", &req).await?;
        Ok(resp.saved)
    }
}

// ---------- Connections + cost recording ----------

/// Worker-side client for the connection endpoints: resolve a
/// connection for one firing, release it when the node finishes. (Cost
/// records ride the generic task rail, not a dedicated endpoint.)
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
        self.http.post("/v1/access/resolve", req).await
    }

    pub async fn release_connection(
        &self,
        req: &ReleaseConnectionRequest,
    ) -> Result<ReleaseConnectionResponse> {
        self.http.post("/v1/access/close", req).await
    }

    pub async fn mint_instance_token(
        &self,
        req: &ProgramMintInstanceTokenRequest,
    ) -> Result<weft_core::program::MintedInstanceToken> {
        self.http.post("/v1/program/mint_instance_token", req).await
    }

    pub async fn publish_access(
        &self,
        req: &PublishAccessRequest,
    ) -> Result<PublishAccessResponse> {
        self.http.post("/v1/access/publish", req).await
    }

    pub async fn published_access(
        &self,
        req: &PublishedAccessRequest,
    ) -> Result<PublishedAccessResponse> {
        self.http.post("/v1/access/published", req).await
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
        self.http.post("/v1/door/triggers", &DoorTriggersRequest::default()).await
    }

    pub async fn run_facts(&self, instance: Option<&weft_core::instance::InstanceId>) -> Result<DoorRunFacts> {
        self.http.post("/v1/door/run_facts", &DoorRunFactsRequest { instance: instance.cloned() }).await
    }

    /// Put `fire` in the queue of the trigger `token`; `held_by`: the
    /// holder it was picked up under.
    pub async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> Result<DoorParked> {
        self.http
            .post("/v1/door/park_fire", &DoorParkRequest { token: token.to_string(), fire: fire.clone(), held_by: held_by.map(str::to_string) })
            .await
    }

    /// The instance an instance token names; `Ok(None)` when the broker
    /// refuses it (it names nobody of this project).
    pub async fn instance_token(&self, token: &str) -> Result<Option<DoorInstanceToken>> {
        let answer = self
            .http
            .post_raw("/v1/door/instance_token", &DoorInstanceTokenRequest { token: token.to_string() }, DEFAULT_CALL_WAIT)
            .await?;
        if answer.status == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(None);
        }
        HttpCore::parse_success(answer, "/v1/door/instance_token").map(Some)
    }

    pub async fn tick(&self, request: &DoorTickRequest) -> Result<DoorTick> {
        self.http.post("/v1/door/tick", request).await
    }

    /// What a caller of a gated route proved; `Ok(None)` when the broker
    /// refused them (the reason is in its log).
    pub async fn verify_caller(&self, request: &CallerVerifyRequest) -> Result<Option<CallerVerified>> {
        let answer = self.http.post_raw("/v1/caller/verify", request, DEFAULT_CALL_WAIT).await?;
        if answer.status == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(None);
        }
        HttpCore::parse_success::<CallerVerified>(answer, "/v1/caller/verify").map(Some)
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

    /// Claim queued run `execution_id` (`RunClaimRequest`); `None` when it
    /// is not queued. A claim the broker could not even start is sent
    /// again.
    pub async fn claim(&self, execution_id: ExecutionId) -> Result<Option<weft_journal::record::Claimed>> {
        let path = "/v1/run/claim";
        let req = RunClaimRequest { execution_id };
        let resp: RunClaimResponse = until_done(path, || self.http.post(path, &req)).await?;
        Ok(resp.claimed)
    }

    /// This worker no longer drives `execution_id`, its whole record
    /// written, for `why` (`DoorLetGoRequest`).
    pub async fn let_go(&self, execution_id: ExecutionId, why: LetGo) -> Result<()> {
        let _: DoorLetGo = self.http.post("/v1/door/let_go", &DoorLetGoRequest { execution_id, why }).await?;
        Ok(())
    }

    /// The answers waiting for `execution_id`'s waits, but the waits in
    /// `taken`, held up to `wait` until one is there (`RunAnswersRequest`).
    pub async fn answers(&self, execution_id: ExecutionId, taken: &[String], wait: Duration) -> Result<Vec<RunAnswer>> {
        let path = "/v1/run/answers";
        held(
            wait,
            |hold| async move {
                let req = RunAnswersRequest { execution_id, wait_ms: hold.as_millis() as u64, taken: taken.to_vec() };
                let resp: RunAnswersResponse = read_until_answered(path, || self.http.post_held(path, &req, hold)).await?;
                Ok(resp.answers)
            },
            |answers| !answers.is_empty(),
        )
        .await
    }

    /// The cancels waiting for the runs this worker drives (`RunCancelsRequest`).
    pub async fn cancels(&self) -> Result<Vec<RunCancel>> {
        let path = "/v1/run/cancels";
        let req = RunCancelsRequest::default();
        let resp: RunCancelsResponse = read_until_answered(path, || self.http.post(path, &req)).await?;
        Ok(resp.cancels)
    }
}

// ---------- Execution steering (worker tags/stops runs) ----------

/// The worker's door to steering executions: tag its own run, stop
/// its siblings by tag. Two endpoints, both worker-only and process-bound
/// on the broker side (`/v1/execution/tag`, `/v1/execution/stop_tagged`).
pub struct BrokerExecutionClient {
    http: HttpCore,
}

impl BrokerExecutionClient {
    pub fn new(link: BrokerLink) -> Arc<Self> {
        Arc::new(Self { http: HttpCore::new(link) })
    }

    /// Tag `execution_id` with `tags`. Synchronous: on return the tag rows
    /// exist (or the call failed), which is what lets a following
    /// `stop_tagged` anchor on them.
    pub async fn tag_execution(&self, execution_id: ExecutionId, tags: Vec<String>) -> Result<()> {
        let req = ExecutionTagRequest { execution_id, tags };
        let _: ExecutionTagResponse = self.http.post("/v1/execution/tag", &req).await?;
        Ok(())
    }

    /// Ask that every live execution of `execution_id`'s project carrying
    /// `tag` be stopped. Returns once the stop is durably queued; the
    /// dispatcher carries it out.
    pub async fn stop_tagged(&self, execution_id: ExecutionId, tag: String, stop_self: weft_core::StopSelf) -> Result<ExecutionStopTaggedResponse> {
        let req = ExecutionStopTaggedRequest { execution_id, tag, stop_self };
        self.http.post("/v1/execution/stop_tagged", &req).await
    }
}

// ---------- Project (worker fetches own definition) ----------

/// Worker-side client for `/v1/project/fetch_definition`. Used at
/// execution claim time to pull the runtime `ProjectDefinition`
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
            .post_or_404("/v1/project/fetch_definition", &req)
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
    pub async fn sync_ownership(&self, replica: &str, held_projects: &[Uuid]) -> Result<SupervisorSyncOwnershipResponse> {
        let req = SupervisorSyncOwnershipRequest { replica: replica.to_string(), held_projects: held_projects.to_vec() };
        let resp: SupervisorSyncOwnershipResponse = self
            .http
            .post("/v1/supervisor/sync_ownership", &req)
            .await?;
        Ok(resp)
    }

    /// Pure read of the projects this process owns (no claim/renew). The work
    /// loops use this; ownership breadth changes only via `sync_ownership`.
    pub async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>> {
        let req = SupervisorOwnedProjectsRequest {
            replica: replica.to_string(),
        };
        let resp: SupervisorOwnedProjectsResponse = self
            .http
            .post("/v1/supervisor/owned_projects", &req)
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
        let path = "/v1/supervisor/gone_copies";
        match self.http.post_fenced::<_, SupervisorGoneCopiesResponse>(path, &req).await? {
            WriteOutcome::Applied(resp) => Ok(Some(resp.gone)),
            WriteOutcome::Displaced => Ok(None),
            WriteOutcome::Gone => Err(anyhow::anyhow!("{path}: the broker answered 409, which a judgment never does")),
        }
    }

    pub async fn infra_nodes(&self, project_id: Uuid) -> Result<Vec<SupervisorInfraNode>> {
        let req = SupervisorInfraNodesRequest {
            project_id,
        };
        let resp: SupervisorInfraNodesResponse =
            self.http.post("/v1/supervisor/infra_nodes", &req).await?;
        Ok(resp.nodes)
    }

    pub async fn health_protocols(
        &self,
        project_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let req = SupervisorHealthProtocolsRequest {
            project_id,
        };
        let resp: SupervisorHealthProtocolsResponse = self
            .http
            .post("/v1/supervisor/health_protocols", &req)
            .await?;
        Ok(resp.protocols)
    }

    /// The oldest waiting command of a project this supervisor owns that
    /// no older waiting command reaches a copy of, other than the ones it
    /// runs already (`busy_commands`), holding up to `wait` for one to be
    /// issued when none is waiting.
    pub async fn claim_command(
        &self,
        claimer_replica: &str,
        busy_commands: &[i64],
        wait: Duration,
    ) -> Result<SupervisorClaim> {
        held(
            wait,
            |hold| {
                let req = SupervisorClaimCommandRequest {
                    claimer_replica: claimer_replica.to_string(),
                    busy_commands: busy_commands.to_vec(),
                    wait_ms: hold.as_millis() as u64,
                };
                async move {
                    self.http.post_held("/v1/supervisor/claim_command", &req, hold).await
                }
            },
            |claim| !matches!(claim, SupervisorClaim::Nothing),
        )
        .await
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
            self.http.post("/v1/supervisor/event_record", &req).await?;
        Ok(resp.id)
    }

    pub async fn set_status(
        &self,
        replica: &str,
        command_id: Option<i64>,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: Option<&str>,
        status: crate::protocol::InfraNodeStatus,
        failure_stage: Option<crate::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<WriteOutcome<SupervisorSetStatusResponse>> {
        let req = SupervisorSetStatusRequest {
            replica: replica.to_string(),
            command_id,
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            unit: unit.map(|s| s.to_string()),
            status,
            failure_stage,
            failure_message: failure_message.map(|s| s.to_string()),
        };
        self.http
            .post_fenced::<_, SupervisorSetStatusResponse>("/v1/supervisor/set_status", &req)
            .await
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
            .post_fenced::<_, SupervisorSetWaitingResponse>("/v1/supervisor/set_waiting", &req)
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
            .post_fenced::<_, SupervisorRemoveNodeResponse>("/v1/supervisor/remove_node", &req)
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
                "/v1/supervisor/command_complete",
                &req,
            )
            .await
    }

    /// Whether the user requested cancellation of a claimed command.
    /// Polled by the executing supervisor between platform calls.
    pub async fn command_cancel_requested(&self, command_id: i64) -> Result<bool> {
        let req = crate::protocol::SupervisorCommandCancelRequestedRequest { command_id };
        let resp: crate::protocol::SupervisorCommandCancelRequestedResponse = self
            .http
            .post("/v1/supervisor/command_cancel_requested", &req)
            .await?;
        Ok(resp.cancel_requested)
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
            .post("/v1/supervisor/infra_command_in_flight", &req)
            .await?;
        Ok(resp.commands)
    }

    pub async fn trigger_deps(
        &self,
        project_id: Uuid,
    ) -> Result<Vec<SupervisorTriggerDep>> {
        let req = SupervisorTriggerDepsRequest {
            project_id,
        };
        let resp: SupervisorTriggerDepsResponse =
            self.http.post("/v1/supervisor/trigger_deps", &req).await?;
        Ok(resp.deps)
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
        };
        self.http
            .post_fenced::<_, SupervisorSetAppliedResponse>("/v1/supervisor/set_applied", &req)
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
        };
        self.http
            .post_fenced::<_, SupervisorSetProvisioningResponse>(
                "/v1/supervisor/set_provisioning",
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
            .post("/v1/supervisor/project_image_tags", &req)
            .await?;
        Ok(resp.tags)
    }

    /// Enqueue a dispatcher-targeted lifecycle command
    /// (`Deactivate(...)` | `Reactivate`). Used by HealthProtocol
    /// action dispatch in the supervisor. Verb-and-payload are
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
            .post("/v1/supervisor/enqueue_lifecycle", &req)
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
    ) -> Result<i64> {
        let req = InfraEnqueueApplyRequest {
            project_id,
            node_id: node_id.to_string(),
            instance: instance.cloned(),
            spec_json,
        };
        let resp: InfraEnqueueApplyResponse =
            self.http.post("/v1/infra/enqueue_apply", &req).await?;
        Ok(resp.command_id)
    }

    /// Save what the infra setup `execution_id` sent out on the baked
    /// outputs of its node at `place` (`InfraBakeRequest`).
    pub async fn save_bake(
        &self,
        execution_id: weft_core::ExecutionId,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
        values: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        let req = InfraBakeRequest { execution_id, place: place.to_string(), instance: copy.cloned(), values };
        let _: serde_json::Value = self.http.post("/v1/infra/bake", &req).await?;
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
        held(
            wait,
            |hold| {
                let req = InfraWaitApplyRequest {
                    project_id,
                    command_id,
                    wait_ms: hold.as_millis() as u64,
                };
                async move { self.http.post_held("/v1/infra/wait_apply", &req, hold).await }
            },
            |resp: &InfraWaitApplyResponse| resp.completed,
        )
        .await
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

    /// Only the broker's own "nothing was done" makes a write safe to send
    /// again: any other 503 may have half happened.
    #[test]
    fn only_nothing_done_is_safe_to_send_again() {
        let not_done: anyhow::Error =
            BrokerRefused { path: "/v1/task/complete".into(), status: reqwest::StatusCode::SERVICE_UNAVAILABLE, body: NOT_DONE.into() }.into();
        assert!(did_nothing(&not_done));
        assert!(!did_nothing(&refused(reqwest::StatusCode::SERVICE_UNAVAILABLE)));
        assert!(!did_nothing(&refused(reqwest::StatusCode::INTERNAL_SERVER_ERROR)));
    }

    /// A call the broker did nothing with is sent again until it is done;
    /// one that failed otherwise is not.
    #[tokio::test(start_paused = true)]
    async fn a_call_that_did_nothing_is_sent_again() {
        let calls = AtomicU32::new(0);
        let answer = until_done("/v1/task/complete", || async {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(BrokerRefused { path: "/v1/task/complete".into(), status: reqwest::StatusCode::SERVICE_UNAVAILABLE, body: NOT_DONE.into() }.into()),
                _ => Ok(7),
            }
        })
        .await
        .unwrap();
        assert_eq!((answer, calls.load(Ordering::SeqCst)), (7, 3));
        let calls = AtomicU32::new(0);
        let failed: Result<u32> = until_done("/v1/task/complete", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(refused(reqwest::StatusCode::SERVICE_UNAVAILABLE))
        })
        .await;
        assert!(failed.is_err());
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
