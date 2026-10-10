//! The worker: the project's compiled program, serving HTTP.
//!
//! Weft calls a worker for each run it hands one; the worker never goes
//! looking for work. Three ways in, one drive:
//!
//! - `POST /_weft/run/<execution_id>`: carry on this queued run. The
//!   worker claims it (its `run` row is then this worker's, under a raised
//!   epoch), drives it from its record, and answers when it ends or lets
//!   go of it. A run that is not queued (another worker claimed it, it
//!   ended) claims nothing and says so.
//! - `POST /_weft/fire`: a trigger's event (a listener's tick, a webhook
//!   the install took, a parked fire replayed), checked at the door the
//!   way a caller is (`door`): parked in its trigger's queue when the
//!   trigger is parked or what it reads is not ready, else its run born
//!   and driven here. The answer's first line says which, and stays open
//!   until a started run ends.
//! - any other path: a caller at this worker's door (`door`), checked
//!   here, its run born here and driven with the caller's connection
//!   attached (`caller_conn`).
//!
//! Every run writes its record through its one handle on the process's
//! writer lanes (`crate::journal_writer`). While it drives anything, the
//! worker keeps its line to the broker open, which pushes the cancels of
//! the runs it drives, so a `weft stop` reaches it at once. When the
//! platform stops the replica, every run it drives has to leave it, and
//! goes the way a run that pauses does: one that can be suspended starts
//! no new step and, once the steps it runs have ended, is handed back
//! whole for another worker to carry on from its record (a step still
//! running `HAND_BACK_GRACE` later is stopped where it is, and the next
//! worker fails it as cut short); one that cannot (unrecorded, tied to
//! the caller on the line, or with a bus open) carries on as if nothing
//! was asked, getting as far as it can before the platform stops the
//! process, which ends it the way a crash would. The worker writes what
//! is left of their records as each one ends. On a
//! platform that stops a worker no call holds open, a run that outlives
//! its caller has to leave the same way once its caller leaves, and
//! carries on under the call weft holds open for its resume.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use weft_core::cancellation::CancellationFlag;
use weft_core::door_fire::RunAnswer;
use weft_core::caller::{InboundMessage, OutboundChunk};
use weft_core::{ExecutionId, NodeCatalog};
use weft_journal::ExecEvent;

use crate::context::EngineClients;
use crate::execution_driver::{run_one_execution, BornWith, ExecutionOutcome, StartsFrom};
use crate::journal_writer::{DriveJournal, RunSpec};
use crate::plan::{ProgramTables, RunGate, Unready};

/// Who may call this worker's own endpoints (`/_weft/...`): weft, which
/// presents the project's key (`ProjectSecret::worker_door_key`) as its
/// bearer in [`weft_platform_traits::WORKER_AUTH_HEADER`]. Anybody may
/// reach the worker itself (its callers do, straight).
#[derive(Clone)]
pub struct WeftCredential(Arc<Vec<u8>>);

impl WeftCredential {
    /// The key weft's calls carry to the workers of the project whose
    /// secret is `secret`.
    pub fn of(secret: &weft_core::caller_token::ProjectSecret) -> Self {
        Self(Arc::new(hex::decode(secret.worker_door_key()).expect("a derived key is hex")))
    }


    /// Whether a call with these headers may use the worker's own
    /// endpoints.
    pub fn admits_headers(&self, headers: &HeaderMap) -> bool {
        let presented = headers
            .get(weft_platform_traits::WORKER_AUTH_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(weft_platform_traits::worker_auth_key)
            .and_then(|v| hex::decode(v).ok());
        presented.is_some_and(|p| weft_core::signed_token::constant_time_eq(&p, &self.0))
    }
}


/// This worker's identity for its calls to the broker, from
/// `WEFT_WORKER_IDENTITY` (`weft_platform_gcp::identity_from_env`).
pub fn identity_from_env() -> Result<Arc<dyn weft_platform_traits::IdentityTokens>> {
    weft_platform_gcp::identity_from_env("WEFT_WORKER_IDENTITY")
}

/// What starts a worker, besides its catalog and its broker clients.
pub struct WorkerConfig {
    pub project_id: uuid::Uuid,
    pub tenant_id: String,
    /// This process's replica id: names the runs it drives, and the
    /// records it writes.
    pub replica: String,
    /// The program this worker runs (its image's binary hash): its door
    /// serves the routes armed for it.
    pub binary_hash: String,
    /// What the install allows at its public edge.
    pub edge: crate::door::Edge,
    /// The project's secret: what socket tickets and the callers this
    /// worker passes on are signed with.
    pub secret: weft_core::caller_token::ProjectSecret,
    pub port: u16,
    /// The platform's hard cap on one stretch of a run, when it has one: a
    /// run reaching it is stopped, saying a pause gives it a fresh one.
    pub run_cap: Option<std::time::Duration>,
    /// Set by a platform that may stop a worker no call holds open (Cloud
    /// Run): a run that outlives its caller is handed back once
    /// its caller leaves, and carries on under weft's own held call.
    pub resume_when_caller_leaves: bool,
}

/// How long a run on a worker told to stop has to end the steps
/// it is running and hand itself back whole, before those steps are
/// stopped where they are and the run is handed back without them. Half
/// of the ten seconds Cloud Run waits between its stop and its kill, so
/// the run's last rows and its hand-back still land.
const HAND_BACK_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// How long before the platform's cap a run stops itself, so the ending
/// it writes is its own rather than a cut connection.
const RUN_CAP_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);

/// How many locks the cancel registry is cut into, by the random tail of
/// a run's id: runs registering and leaving at once rarely wait on each
/// other.
const CANCEL_SHARDS: usize = 16;

/// The cancellation flag of every run this worker drives: a cancel looks
/// up the run and fires its flag. Registered and removed inline, by the
/// run's own drive ([`Registered`]).
struct Cancels {
    shards: [std::sync::Mutex<HashMap<ExecutionId, Arc<CancellationFlag>>>; CANCEL_SHARDS],
    /// Poked whenever a run comes or goes, so the cancel watch holds the
    /// line open exactly while there are some.
    changed: tokio::sync::Notify,
    /// Told whenever a run leaves, for a claim waiting on its run's last
    /// drive here to end ([`Self::claim_once_left`]).
    left: tokio::sync::Notify,
}

impl Cancels {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shards: std::array::from_fn(|_| std::sync::Mutex::new(HashMap::new())),
            changed: tokio::sync::Notify::new(),
            left: tokio::sync::Notify::new(),
        })
    }

    fn shard(&self, execution_id: ExecutionId) -> &std::sync::Mutex<HashMap<ExecutionId, Arc<CancellationFlag>>> {
        let tail = u64::from_be_bytes(execution_id.as_bytes()[8..16].try_into().expect("eight bytes"));
        &self.shards[(tail % CANCEL_SHARDS as u64) as usize]
    }

    /// `execution_id` is driven here from now on, under a fresh cancel,
    /// until the answer is dropped; `None` when it already is. The one
    /// door a run takes onto this worker, however it came (a claim, a
    /// caller, a fire): checked and taken in one step, so a run delivered
    /// twice at once is driven once, and a cancel announced from here on
    /// finds it.
    fn claim(self: &Arc<Self>, execution_id: ExecutionId) -> Option<Registered> {
        let flag = CancellationFlag::new_arc();
        {
            let mut shard = self.shard(execution_id).lock().expect("cancel registry");
            if shard.contains_key(&execution_id) {
                return None;
            }
            shard.insert(execution_id, flag.clone());
        }
        self.changed.notify_one();
        Some(Registered { cancels: self.clone(), execution_id, flag })
    }

    /// [`Self::claim`] a queued run, once a drive of it on this worker has
    /// ended. A run let go of may be queued again in the same breath (an
    /// answer that came while it ran), and its delivery can reach this
    /// worker before the drive that let go of it has left. A second
    /// delivery of a run this worker drives waits out that drive the same
    /// way, and then finds nothing queued.
    async fn claim_once_left(self: &Arc<Self>, execution_id: ExecutionId) -> Registered {
        loop {
            let left = self.left.notified();
            tokio::pin!(left);
            left.as_mut().enable();
            if let Some(registered) = self.claim(execution_id) {
                return registered;
            }
            left.await;
        }
    }

    fn contains(&self, execution_id: ExecutionId) -> bool {
        self.shard(execution_id).lock().expect("cancel registry").contains_key(&execution_id)
    }

    /// Fire `execution_id`'s flag for `cause`. A run not driven here ended
    /// on its own before the cancel landed (or another worker drives it):
    /// nothing to do.
    fn cancel(&self, execution_id: ExecutionId, cause: weft_core::exec::CancelCause) {
        let flag = self.shard(execution_id).lock().expect("cancel registry").get(&execution_id).cloned();
        if let Some(flag) = flag {
            tracing::info!(target: "weft_engine::worker", %execution_id, %cause, "firing the run's cancel");
            flag.cancel_because(cause);
        }
    }

    /// How many runs this worker drives.
    fn count(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().expect("cancel registry").len()).sum()
    }
}

/// A run's place in [`Cancels`], removed when its drive ends however it
/// ends (an unwind included). Removes only its own flag: a resume of the
/// same run may register its own the moment this one is released.
struct Registered {
    cancels: Arc<Cancels>,
    execution_id: ExecutionId,
    flag: Arc<CancellationFlag>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        // Never panics, even on a poisoned lock: this runs during unwinds.
        if let Ok(mut shard) = self.cancels.shard(self.execution_id).lock() {
            if shard.get(&self.execution_id).is_some_and(|flag| Arc::ptr_eq(flag, &self.flag)) {
                shard.remove(&self.execution_id);
            }
        }
        self.cancels.changed.notify_one();
        self.cancels.left.notify_waiters();
    }
}

/// One worker process: what every drive on it shares.
///
/// The `ProjectDefinition` is not held here: each run names its own
/// definition hash, fetched from the broker and cached by hash.
#[derive(Clone)]
struct Worker {
    project_id: uuid::Uuid,
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    replica: String,
    tenant_id: String,
    cancels: Arc<Cancels>,
    /// What this worker keeps of its programs and of the runs its triggers
    /// start (`crate::plan`).
    plans: Arc<crate::plan::Plans>,
    /// Every drive this worker runs, the gate shutdown waits on.
    background: Arc<weft_core::in_flight::InFlight>,
    /// Fired when the platform tells the worker to stop: every run it
    /// drives has to leave (see the module doc), and no run is born here.
    stopping: tokio_util::sync::CancellationToken,
    /// Fired [`HAND_BACK_GRACE`] after [`Self::stopping`]: the steps still
    /// running are stopped where they are and their runs handed back.
    stopping_overdue: tokio_util::sync::CancellationToken,
    run_cap: Option<std::time::Duration>,
    /// See [`WorkerConfig::resume_when_caller_leaves`].
    resume_when_caller_leaves: bool,
    /// Where callers arrive (`crate::door`).
    door: Arc<crate::door::Door>,
}

/// How a drive ended, as the run's deliverer hears it.
fn answer_of(drove: Result<()>) -> RunAnswer {
    match drove {
        Ok(()) => RunAnswer::Completed,
        Err(e) => RunAnswer::Failed { error: format!("{e:#}") },
    }
}

/// The stand-in caller of a run started by hand through a trigger that
/// answers a caller (`ExecutionStarted::stand_in`): nobody is on the line,
/// so the stand-in serves the trigger's spec and the opening request.
struct StandIn {
    spec: weft_core::primitive::SignalSpec,
    request: weft_core::caller::LiveRequest,
}

impl StandIn {
    /// The stand-in caller a run started by hand through a trigger that
    /// answers a caller serves (`ExecutionStarted::stand_in`): its request
    /// is the firing kick's payload. `None` for any other run: a caller on
    /// the line was on the worker that bore its run, and none follows it.
    fn of(events: &[ExecEvent]) -> Result<Option<Self>> {
        let Some(spec) = events.iter().find_map(|event| match event {
            ExecEvent::ExecutionStarted { stand_in, .. } => stand_in.clone(),
            _ => None,
        }) else {
            return Ok(None);
        };
        let payload = events
            .iter()
            .find_map(|event| match event {
                ExecEvent::NodeKicked { firing: true, payload, .. } => Some(payload.clone().unwrap_or_default()),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("a run started by hand through a caller's trigger has no firing kick to serve"))?;
        let request = serde_json::from_value(payload).context("the firing kick of a run started by hand through a caller's trigger is not a request")?;
        Ok(Some(Self { spec, request }))
    }

    /// The exchange its run serves through the stand-in, recorded on the
    /// run's record like a real caller's. Only an HTTP trigger can be
    /// fired: a socket's shape is a conversation over time, and there is
    /// nothing honest to invent for the caller's next message.
    fn exchange(self, execution_id: ExecutionId, journal: Arc<DriveJournal>, replica: &str) -> Result<crate::execution_driver::Exchange> {
        let (protocol, cfg) = weft_core::signal::live_connection(&self.spec).map_err(|e| anyhow::anyhow!("the caller of run {execution_id}: {e}"))?;
        if protocol != weft_core::signal::Protocol::Http {
            anyhow::bail!(
                "a Socket cannot be fired: its shape is a conversation over time, and there is \
                 nothing honest to invent for the caller's next message. Point a real client at \
                 it (`weft activate` prints the URL)"
            );
        }
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> = ExchangeJournal::new(journal, replica.to_string(), cfg.journal_policy());
        // The stand-in serves the REQUEST and records the answer, and that
        // is all it can honestly do: a fired run's body, when the author
        // typed one, is a field of the trigger's own wake payload, and the
        // node reads it there.
        let fired = crate::fired_caller::FiredCaller::open(
            execution_id,
            weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol),
            self.request,
            sink.clone(),
        );
        Ok(crate::execution_driver::Exchange { conn: fired, live: None, sink })
    }
}

/// What a drive needs about the run it drives, wherever the run came
/// from: a claim, or this worker's door.
struct RunStart {
    execution_id: ExecutionId,
    /// The run's one handle on its record.
    journal: Arc<DriveJournal>,
    /// The tables of the program the run executes.
    program: Arc<ProgramTables>,
    /// What the run starts from: its record as claimed, or the plan it was
    /// born from (its birth already handed to `journal`).
    starts_from: StartsFrom,
    /// The run's caller: the one on the line (a call at this worker's
    /// door), or the stand-in of a run started by hand through a trigger
    /// that answers a caller.
    exchange: Option<crate::execution_driver::Exchange>,
    /// Its place in the cancel registry, and so its cancel: taken before
    /// anything of the run starts (`Cancels::claim`).
    registered: Registered,
    /// Its place at the door (a run born at this worker's door, or a fire
    /// it started), given up when the run's own work ends.
    slot: Option<crate::door::Slot>,
}

impl Worker {
    /// Claim queued run `execution_id` and carry it on to its end, on a
    /// task of its own so whoever handed it here can go away without
    /// stopping it (a run stops itself before the platform's cap, see
    /// `RUN_CAP_MARGIN`). `None` once this worker is stopping: a run
    /// claimed then would only be handed back, and weft hands it to another
    /// worker instead.
    async fn run_execution_id(&self, execution_id: ExecutionId) -> Result<Option<RunAnswer>> {
        // Registered before the claim: a cancel announced once the claim
        // committed finds the run here.
        let registered = tokio::select! {
            biased;
            () = self.stopping.cancelled() => return Ok(None),
            registered = self.cancels.claim_once_left(execution_id) => registered,
        };
        // The claim makes the run this worker's on record, which its lease
        // keeps: the registration counts it where the door's tick looks, so
        // the tick goes out now, and the claim does not wait for it.
        self.door.wake();
        let Some(claimed) = self.clients.runs.claim(execution_id).await.context("claim the run")? else {
            return Ok(Some(RunAnswer::NothingToRun));
        };
        if let Some(cause) = claimed.cancel_requested.clone() {
            registered.flag.cancel_because(cause);
        }
        let worker = self.clone();
        let token = self.background.token();
        let drive = tokio::spawn(async move {
            let _token = token;
            answer_of(worker.carry_on(execution_id, claimed, registered).await)
        });
        drive.await.map(Some).context("the drive panicked outside its guard")
    }

    /// Carry on a run this worker just claimed, from its record.
    async fn carry_on(
        &self,
        execution_id: ExecutionId,
        claimed: weft_journal::record::Claimed,
        registered: Registered,
    ) -> Result<()> {
        let next_seq = claimed.record.rows.last().map_or(0, |row| row.seq + 1);
        let first = claimed.record.events(execution_id).map_err(anyhow::Error::msg);
        let settings = first.as_ref().ok().and_then(|events| birth_settings(events)).unwrap_or_default();
        let project = self.plans.project(&self.clients, &claimed.definition_hash).await;
        let keep_for = project.as_ref().map_or(weft_core::run_settings::KeepFor::WEFT_DEFAULT, |project| project.defaults.keep_for());
        let journal = self.clients.writer.run(RunSpec {
            execution_id,
            settings,
            keep_for: settings.kept_for(keep_for),
            epoch: claimed.epoch,
            next_seq,
            redaction: Default::default(),
        });
        // A record this worker cannot read, a program it cannot fetch, or a
        // stand-in it cannot serve still ends the run: it is this worker's
        // now.
        let opened = project.and_then(|project| {
            let events = first?;
            let exchange = StandIn::of(&events)?.map(|stand_in| stand_in.exchange(execution_id, journal.clone(), &self.replica)).transpose()?;
            let selection = events.iter().find_map(|event| match event {
                ExecEvent::ExecutionStarted { selection, .. } => Some(selection.clone()),
                _ => None,
            });
            let program = self.plans.program(&project, &claimed.definition_hash, selection.flatten());
            Ok((program, events, exchange))
        });
        let (program, events, exchange) = match opened {
            Ok(opened) => opened,
            Err(e) => return Err(crate::execution_driver::fail_run(&self.clients.writer, &journal, execution_id, &self.replica, e).await),
        };
        self.drive(RunStart {
            execution_id,
            journal,
            program,
            starts_from: StartsFrom::Record(events),
            exchange,
            registered,
            slot: None,
        })
        .await
    }

    /// Drive one run to its end or until it lets go: one claimed, or one
    /// born at this worker's door.
    async fn drive(&self, start: RunStart) -> Result<()> {
        let RunStart { execution_id, journal, program, starts_from, exchange, registered, slot } = start;
        let cancellation = registered.flag.clone();
        // Every run is asked to leave this worker when it is told to stop,
        // or when its caller left: it suspends if it can and is carried on
        // by the next worker, and carries on here if it cannot, until the
        // process goes (the driver decides both). The ask ends with the
        // drive.
        let hand_back = crate::execution_driver::HandBack { asked: self.stopping.child_token(), overdue: self.stopping_overdue.clone() };
        let _asked_ends = hand_back.asked.clone().drop_guard();
        // On a platform that stops a worker no call holds open, a run has to
        // leave this worker once its caller is gone (hung up, or answered):
        // it suspends and carries on under the delivery weft holds open for
        // its resume, or stays on as long as the worker does if it cannot
        // (a tied run its caller hung up on is cancelled already, before the
        // caller reads as gone).
        let caller = exchange.as_ref().and_then(|exchange| exchange.live.clone());
        let leaves_with = caller.clone().filter(|_| self.resume_when_caller_leaves);
        let run = run_one_execution(
            self.catalog.clone(),
            &self.clients,
            crate::execution_driver::RunDrive { execution_id, journal, program, starts_from, cancellation: cancellation.clone(), exchange, hand_back: Some(&hand_back) },
            &self.replica,
            &self.tenant_id,
        );
        let outcome = self.while_driving(run, execution_id, &cancellation, caller.is_some(), leaves_with, &hand_back).await;
        // The run's own work is over: its place at the door (its entry's
        // runs at once, its run permit) is free once the slot goes, and a
        // failure is counted for `weft status`.
        if let Some(slot) = slot {
            if matches!(outcome, Err(_) | Ok(ExecutionOutcome::Failed { .. } | ExecutionOutcome::Stuck { .. })) {
                slot.failed(crate::now_unix() as i64);
            }
        }
        // A run parked or handed back with its whole record written is let
        // go of: it loses nothing when this worker goes away later, and
        // whoever carries it on owns it next. One the broker will not let
        // go of is ended by it, naming why: nobody would carry it on.
        let why = match &outcome {
            Ok(ExecutionOutcome::Stalled) => Some(weft_broker_client::protocol::LetGo::Parked),
            Ok(ExecutionOutcome::HandedBack) => Some(weft_broker_client::protocol::LetGo::HandedBack),
            _ => None,
        };
        if let Some(why) = why {
            if let Err(refused) = self.let_go(execution_id, why).await {
                self.clients.writer.give_up(execution_id, &format!("the run could not be let go of ({why:?}): {refused:#}")).await;
                return Err(refused);
            }
        }
        outcome.map(|_| ())
    }

    /// Let go of a run whose whole record is written, for `why`, asking
    /// until the broker answers: until then this worker owns the run on
    /// record, and nothing else may pick it up. An outage is asked about
    /// again; any other refusal is the broker's answer, and fails the
    /// drive with it, since asking again would hear the same.
    async fn let_go(&self, execution_id: ExecutionId, why: weft_broker_client::protocol::LetGo) -> Result<()> {
        // Whether an earlier ask may have landed with its answer lost.
        let mut maybe_landed = false;
        let mut wait = REASK_FIRST;
        loop {
            match self.clients.runs.let_go(execution_id, why).await {
                Ok(()) => return Ok(()),
                // The run is another worker's now: the earlier ask landed,
                // and the worker that took its resume owns it.
                Err(e) if maybe_landed && broker_conflict(&e) => {
                    tracing::info!(target: "weft_engine::worker", %execution_id, "the run was let go of by an earlier ask whose answer was lost");
                    return Ok(());
                }
                Err(e) if broker_outage(&e) => {
                    maybe_landed = true;
                    tracing::warn!(target: "weft_engine::worker", %execution_id, ?why, error = %format!("{e:#}"), "could not let go of the run yet; trying again");
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(REASK_LONGEST);
                }
                Err(e) => {
                    let e = e.context(format!("let go of run {execution_id} ({why:?})"));
                    tracing::error!(target: "weft_engine::worker", %execution_id, error = %format!("{e:#}"), "the broker refused to let go of the run");
                    return Err(e);
                }
            }
        }
    }
}

/// How the run whose record begins with `events` is kept, from its birth.
fn birth_settings(events: &[ExecEvent]) -> Option<weft_core::run_settings::RunSettings> {
    events.iter().find_map(|event| match event {
        ExecEvent::ExecutionStarted { settings, .. } => Some(*settings),
        _ => None,
    })
}

impl Worker {
    /// Drive `run` with the two things that watch it as arms of the same
    /// wait, so nothing outlives it: the platform's cap on one stretch of
    /// a run (a minute before it, the run stops itself with a cause that
    /// says what to do, instead of being cut mid-node with no ending), and,
    /// on a platform that stops a worker no call holds open, its caller
    /// leaving (`leaves_with`), which asks the run to let go of this
    /// worker. The cap's timer is only set once the run outlives the poll
    /// that started it, and never on a platform without a cap.
    async fn while_driving(
        &self,
        run: impl std::future::Future<Output = Result<ExecutionOutcome>>,
        execution_id: ExecutionId,
        cancellation: &CancellationFlag,
        live: bool,
        leaves_with: Option<Arc<crate::caller_conn::LiveCallerConnection>>,
        hand_back: &crate::execution_driver::HandBack,
    ) -> Result<ExecutionOutcome> {
        tokio::pin!(run);
        let cap = self.run_cap;
        let capped = async {
            match cap {
                Some(cap) => tokio::time::sleep(cap.saturating_sub(RUN_CAP_MARGIN)).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(capped);
        let caller_left = async {
            match &leaves_with {
                Some(conn) => weft_core::caller::CallerConnection::disconnected(conn.as_ref()).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(caller_left);
        let (mut cap_fired, mut left) = (false, false);
        loop {
            tokio::select! {
                biased;
                outcome = &mut run => return outcome,
                () = &mut capped, if !cap_fired => {
                    cap_fired = true;
                    if !cancellation.is_cancelled() {
                        let cap = cap.expect("armed only with a cap");
                        cancellation.cancel_because(weft_core::exec::CancelCause::Runtime { detail: run_cap_reached(execution_id, cap, live) });
                    }
                }
                () = &mut caller_left, if !left => {
                    left = true;
                    hand_back.asked.cancel();
                }
            }
        }
    }
}

/// The broker's refusal in `e`, if it answered with one.
fn broker_refusal(e: &anyhow::Error) -> Option<&weft_broker_client::BrokerRefused> {
    e.chain().find_map(|cause| cause.downcast_ref::<weft_broker_client::BrokerRefused>())
}

/// Whether the broker could not answer for now (unreachable, or saying it
/// cannot), as opposed to answering with a refusal.
fn broker_outage(e: &anyhow::Error) -> bool {
    broker_refusal(e).is_none_or(weft_broker_client::BrokerRefused::is_outage)
}

/// Whether the broker refused because the run is not the asking worker's.
fn broker_conflict(e: &anyhow::Error) -> bool {
    broker_refusal(e).is_some_and(|refused| refused.status == reqwest::StatusCode::CONFLICT)
}

// ----- Live caller connection wiring ---------------------------------

/// Journal sink that projects caller events to `ExecEvent::Caller*` rows,
/// handed to the exchange's lane on the run's journal writer, which keeps
/// them in the order they were handed (the window holding a conversation's
/// last messages before the row saying the caller hung up) and never makes
/// the connection's hot path wait. Live connections are non-durable: the
/// exchange is observable and replayable, not a resume-critical record.
pub(crate) struct ExchangeJournal {
    /// Itself, for the window clock it starts.
    me: std::sync::Weak<ExchangeJournal>,
    /// Whether the window clock runs ([`Self::start_clock`]).
    clock_started: std::sync::atomic::AtomicBool,
    lane: Arc<crate::journal_writer::DriveJournal>,
    replica: String,
    /// Why this conversation's journal stopped working, once it has.
    /// The next thing the program tries to send the caller fails with
    /// it, the same way a bus whose journal failed refuses the next
    /// send: a run that finishes looking clean while its exchange is
    /// missing from the record is the silent loss both exist to stop.
    degraded: Arc<std::sync::Mutex<Option<String>>>,
    /// What the journal keeps of this conversation: the same policy
    /// type a bus carries, so the two cannot drift on where content
    /// gets trimmed or on what "ephemeral" means.
    policy: weft_core::stream_journal::JournalPolicy,
    pending: std::sync::Mutex<PendingCallerWindow>,
}

/// Messages said since the last row went out.
#[derive(Default)]
struct PendingCallerWindow {
    /// One connection is one execution, so the execution is the same for
    /// every message; kept from the first one rather than passed to the
    /// flush.
    execution_id: Option<ExecutionId>,
    messages: Vec<weft_core::stream_journal::WindowedCallerMessage>,
    /// What the messages above already weigh, so a window closes on
    /// size as well as on time (see `JOURNAL_ROW_BYTES`).
    kept_bytes: usize,
    /// The exchange is over: the ticker can stop.
    closed: bool,
}

impl ExchangeJournal {
    /// The sink of one exchange, written through `lane`.
    pub(crate) fn new(
        lane: Arc<crate::journal_writer::DriveJournal>,
        replica: String,
        policy: weft_core::stream_journal::JournalPolicy,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            lane,
            replica,
            degraded: Arc::new(std::sync::Mutex::new(None)),
            policy,
            pending: std::sync::Mutex::new(PendingCallerWindow::default()),
            clock_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Start the window clock, once: with the first message the window
    /// holds, so an exchange that holds none (an HTTP call answered in one
    /// piece) never starts one. The clock stops when the exchange ends or
    /// when the connection drops the sink, whichever comes first, so a
    /// conversation never leaves a task behind.
    fn start_clock(&self) {
        if self.clock_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let weak = self.me.clone();
        let window = self.policy.window;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(window);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(sink) = weak.upgrade() else { return };
                if sink.closed() {
                    return;
                }
                sink.flush();
            }
        });
    }

    fn closed(&self) -> bool {
        self.pending.lock().expect("caller journal buffer").closed
    }

    /// Hold one message for the open window. What the journal keeps of
    /// it is decided here rather than at flush time, so an oversized
    /// payload is cut once and the buffer never holds more than the
    /// journal will.
    fn hold(
        &self,
        execution_id: ExecutionId,
        offset: u64,
        direction: weft_core::stream_journal::CallerDirection,
        payload: &weft_core::bus::WirePayload,
        terminal: bool,
    ) {
        self.start_clock();
        let kept = weft_core::stream_journal::record(payload, &self.policy);
        let weighs = kept.kept_bytes();
        // A window closes on whichever comes first, its clock or its
        // size: a chatty socket can otherwise put more in one second
        // than one journal row may carry, and the row is then refused
        // for ever. Flushed BEFORE this message joins, so it opens the
        // next window rather than overflowing this one.
        let full = {
            let pending = self.pending.lock().expect("caller journal buffer");
            self.policy.row_is_full(pending.kept_bytes, weighs)
        };
        if full {
            self.flush();
        }
        let mut pending = self.pending.lock().expect("caller journal buffer");
        pending.execution_id.get_or_insert(execution_id);
        pending.kept_bytes += weighs;
        pending.messages.push(weft_core::stream_journal::WindowedCallerMessage {
            offset,
            direction,
            payload: kept.payload,
            payload_byte_size: kept.byte_size,
            trimmed: kept.trimmed,
            terminal,
            at_unix: crate::now_unix(),
        });
    }

    /// Write the open window, if it holds anything.
    fn flush(&self) {
        let (execution_id, messages) = {
            let mut pending = self.pending.lock().expect("caller journal buffer");
            if pending.messages.is_empty() {
                return;
            }
            pending.kept_bytes = 0;
            (pending.execution_id, std::mem::take(&mut pending.messages))
        };
        let Some(execution_id) = execution_id else { return };
        let Some(window) = weft_core::stream_journal::aggregate_caller_window(messages) else {
            return;
        };
        self.emit(weft_journal::ExecEvent::CallerWindow {
            execution_id,
            first_offset: window.first_offset,
            last_offset: window.last_offset,
            messages: window.messages,
            totals: window.totals,
            at_unix: window.last_at_unix,
        });
    }

    /// A lifecycle row (connected, errored, disconnected) goes out on
    /// its own, so the open window is written first: the row stream
    /// must never say a thing happened before something it followed.
    /// Both land on the one queue, in this order, and the writer keeps
    /// them in it.
    fn emit_after_flush(&self, event: weft_journal::ExecEvent) {
        self.flush();
        self.emit(event);
    }

    /// Hand one row to the lane. A row the lane refuses (an earlier one
    /// failed to be written) degrades the exchange's record.
    fn emit(&self, event: weft_journal::ExecEvent) {
        if let Err(e) = self.lane.hand(&event, Some(&self.replica)) {
            self.degrade(&e);
        }
    }

    fn degrade(&self, e: &anyhow::Error) {
        tracing::error!(
            target: "weft_engine::caller_conn",
            error = %format!("{e:#}"),
            "caller journal write failed; the exchange is no longer being recorded"
        );
        let mut slot = self.degraded.lock().expect("caller journal degraded");
        // Keep the FIRST reason: it is the one that explains the gap, and
        // the ones after it are its echoes.
        slot.get_or_insert_with(|| format!("journal write failed: {e:#}"));
    }
}

/// Project an `InboundMessage` into the journal's payload vocabulary.
/// Bytes stay bytes: what the journal keeps of them is one rule for
/// every channel and it lives in `stream_journal`, not here, so a
/// conversation and a bus cannot answer it differently.
fn inbound_payload(msg: &InboundMessage) -> weft_core::bus::WirePayload {
    match msg {
        InboundMessage::Json(v) => weft_core::bus::WirePayload::Json(v.clone()),
        InboundMessage::Text(s) => {
            weft_core::bus::WirePayload::Json(serde_json::Value::String(s.clone()))
        }
        InboundMessage::Bytes(b) => {
            weft_core::bus::WirePayload::Bytes(bytes::Bytes::from(b.clone()))
        }
    }
}

fn outbound_payload(chunk: &OutboundChunk) -> weft_core::bus::WirePayload {
    match chunk {
        OutboundChunk::Json(v) => weft_core::bus::WirePayload::Json(v.clone()),
        OutboundChunk::Text(s) => {
            weft_core::bus::WirePayload::Json(serde_json::Value::String(s.clone()))
        }
        OutboundChunk::Bytes(b) => {
            weft_core::bus::WirePayload::Bytes(bytes::Bytes::from(b.clone()))
        }
    }
}

impl crate::caller_conn::CallerJournalSink for ExchangeJournal {
    fn degraded(&self) -> Option<String> {
        self.degraded.lock().expect("caller journal degraded").clone()
    }
    fn connected(&self, execution_id: ExecutionId, offset: u64, protocol: weft_core::signal::Protocol) {
        self.emit(weft_journal::ExecEvent::CallerConnected {
            execution_id,
            offset,
            protocol: protocol.as_wire_str().to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn inbound(&self, execution_id: ExecutionId, offset: u64, msg: &weft_core::caller::InboundMessage) {
        self.hold(
            execution_id,
            offset,
            weft_core::stream_journal::CallerDirection::Inbound,
            &inbound_payload(msg),
            false,
        );
    }
    fn outbound(
        &self,
        execution_id: ExecutionId,
        offset: u64,
        chunk: &weft_core::caller::OutboundChunk,
        terminal: bool,
    ) {
        self.hold(
            execution_id,
            offset,
            weft_core::stream_journal::CallerDirection::Outbound,
            &outbound_payload(chunk),
            terminal,
        );
    }
    fn errored(&self, execution_id: ExecutionId, offset: u64, message: &str) {
        self.emit_after_flush(weft_journal::ExecEvent::CallerErrored {
            execution_id,
            offset,
            message: message.to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn disconnected(&self, execution_id: ExecutionId, offset: u64, reason: &str) {
        // The exchange is over: write what is held, say so, and let the
        // window clock stop. Nothing said after this can be lost,
        // because nothing is said after this.
        self.emit_after_flush(weft_journal::ExecEvent::CallerDisconnected {
            execution_id,
            offset,
            reason: reason.to_string(),
            at_unix: crate::now_unix(),
        });
        self.pending.lock().expect("caller journal buffer").closed = true;
    }
    fn waits_for_commit(&self) -> bool {
        self.lane.keeping().is_durable()
    }
    fn committed(&self) -> futures::future::BoxFuture<'static, Result<(), String>> {
        if !self.waits_for_commit() {
            return Box::pin(async { Ok(()) });
        }
        self.flush();
        let lane = self.lane.clone();
        Box::pin(async move { lane.flush().await.map_err(|e| format!("{e:#}")) })
    }

    fn close(&self) {
        self.flush();
        self.pending.lock().expect("caller journal buffer").closed = true;
    }
}

/// Canceller over the worker's cancel registry. The connection server
/// fires it when a caller drops in a caller-tied (cancel) run, which is
/// registered by then: a run is registered as it is admitted, before its
/// caller is handed the exchange. A run no longer registered has ended, and there
/// is nothing to cancel.
struct RegistryCanceller {
    cancels: Arc<Cancels>,
}

impl crate::caller_conn::ExecutionCanceller for RegistryCanceller {
    fn cancel(&self, execution_id: ExecutionId, cause: weft_core::exec::CancelCause) {
        self.cancels.cancel(execution_id, cause);
    }
}

/// How long an ask that failed waits before it is made again, at first and
/// at most.
const REASK_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
const REASK_LONGEST: std::time::Duration = std::time::Duration::from_secs(30);

/// The cancel watch: the broker pushes every cancel of this worker's
/// project down its line (`weft_task_store::runs::CANCEL_CHANNEL`), and for
/// one this worker drives, the cancels waiting for its runs are asked for
/// and their flags fired. When the line may have missed some (it opened
/// again, or fell behind), they are asked for too. The line is held open
/// while anything is driven, so a cancel is heard however long a run goes
/// without calling the broker.
fn spawn_cancel_watch(worker: Worker) {
    // Subscribed before anything is driven, so no cancel is announced
    // before the watch hears.
    let mut heard = worker.clients.line.subscribe();
    tokio::spawn(async move {
        let mut held_open: Option<Box<dyn Send + Sync>> = None;
        loop {
            // Armed before the count is read, so a change between the read
            // and the wait still wakes it.
            let changed = worker.cancels.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let driving = worker.cancels.count() > 0;
            match (driving, held_open.is_some()) {
                (true, false) => held_open = Some(worker.clients.line.stay_open()),
                (false, true) => held_open = None,
                _ => {}
            }
            let next = tokio::select! {
                _ = &mut changed => continue,
                next = heard.next() => next,
            };
            match next {
                Ok(weft_task_store::pg_signal::Heard::Signal { channel, payload }) if channel == weft_task_store::runs::CANCEL_CHANNEL => {
                    let Some((project, execution_id)) = weft_task_store::runs::parse_cancel_payload(&payload) else {
                        tracing::error!(target: "weft_engine::worker", %payload, "a cancel announcement that names no run");
                        continue;
                    };
                    if project == worker.project_id && worker.cancels.contains(execution_id) {
                        fire_cancels(&worker).await;
                    }
                }
                Ok(weft_task_store::pg_signal::Heard::Recheck | weft_task_store::pg_signal::Heard::Resumed) if driving => fire_cancels(&worker).await,
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(target: "weft_engine::worker", error = %format!("{e:#}"), "the cancel watch stopped hearing the line");
                    return;
                }
            }
        }
    });
}

/// Ask for the cancels waiting for the runs this worker drives and fire
/// their flags. An ask that fails (the broker could not answer, or its
/// answer was lost) is made again for as long as this worker drives
/// anything: asking only reads, so the cancel is still there to find, and
/// nothing would announce it again.
async fn fire_cancels(worker: &Worker) {
    if let Err(e) = fire_cancels_once(worker).await {
        tracing::warn!(target: "weft_engine::worker", error = %format!("{e:#}"), "could not ask for the cancels heard; asking again");
        let worker = worker.clone();
        tokio::spawn(async move {
            let mut wait = REASK_FIRST;
            loop {
                tokio::time::sleep(wait).await;
                if worker.cancels.count() == 0 {
                    return;
                }
                match fire_cancels_once(&worker).await {
                    Ok(()) => return,
                    Err(e) => tracing::warn!(target: "weft_engine::worker", error = %format!("{e:#}"), "could not ask for the cancels heard; asking again"),
                }
                wait = (wait * 2).min(REASK_LONGEST);
            }
        });
    }
}

async fn fire_cancels_once(worker: &Worker) -> Result<()> {
    for cancel in worker.clients.runs.cancels().await? {
        worker.cancels.cancel(cancel.execution_id, cancel.cause);
    }
    Ok(())
}

/// Why a run was stopped at the platform's cap, and what to do.
fn run_cap_reached(execution_id: ExecutionId, cap: std::time::Duration, live: bool) -> String {
    let minutes = cap.as_secs() / 60;
    if live {
        format!(
            "execution {execution_id} stopped a minute before the {minutes} minute limit this platform puts on one \
             connection. A conversation meant to last longer keeps its state outside the run and \
             has its client reconnect, each connection its own run",
        )
    } else {
        format!(
            "execution {execution_id} stopped a minute before the {minutes} minute limit this platform puts on one \
             stretch of a run. A run that pauses whole (every branch waiting on a timer or a form) picks \
             back up with a fresh {minutes} minutes, so split long work with a pause",
        )
    }
}

fn new_worker(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: &WorkerConfig, door: Arc<crate::door::Door>) -> Worker {
    Worker {
        door,
        project_id: config.project_id,
        catalog,
        clients,
        replica: config.replica.clone(),
        tenant_id: config.tenant_id.clone(),
        cancels: Cancels::new(),
        plans: Arc::new(crate::plan::Plans::new(config.project_id)),
        background: weft_core::in_flight::InFlight::new("worker execution"),
        stopping: tokio_util::sync::CancellationToken::new(),
        stopping_overdue: tokio_util::sync::CancellationToken::new(),
        run_cap: config.run_cap,
        resume_when_caller_leaves: config.resume_when_caller_leaves,
    }
}

#[derive(Clone)]
struct ServerState {
    worker: Worker,
    weft_credential: WeftCredential,
}

async fn fire_handler(State(state): State<ServerState>, headers: HeaderMap, Json(fire): Json<weft_core::door_fire::DoorFire>) -> Response {
    if !state.weft_credential.admits_headers(&headers) {
        return (StatusCode::UNAUTHORIZED, "this worker answers the install only").into_response();
    }
    // A run born now would only have to leave at once: the firer hands it
    // to another copy.
    if state.worker.stopping.is_cancelled() {
        return (StatusCode::SERVICE_UNAVAILABLE, "this worker is stopping").into_response();
    }
    let (fired, drive) = match state.worker.admit_event(fire).await {
        Ok(fired) => fired,
        Err(refused) => return refused,
    };
    // The answer is its first line, and its body stays open until a
    // started run ends: a platform that gives a worker CPU only while a
    // request is in flight (Cloud Run's request-based billing), or stops
    // one it sees idle (a local install), counts the run as the work it is.
    let line = format!("{}\n", serde_json::to_string(&fired).expect("a fire's answer serializes"));
    let first = futures::stream::once(async move { Ok::<_, std::convert::Infallible>(axum::body::Bytes::from(line)) });
    let until_ended = futures::stream::once(async move {
        if let Some(drive) = drive {
            let _ = drive.await;
        }
        Ok(axum::body::Bytes::new())
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(axum::body::Body::from_stream(futures::StreamExt::chain(first, until_ended)))
        .expect("a fire's answer builds")
}

/// Up, and how many runs it drives: what tells a platform that stops idle
/// workers itself (a local install) that one with no call open is still at
/// work.
// SYNC: the health answer <-> crates/weft-platform-local/src/runner.rs (driving)
async fn healthz(State(state): State<ServerState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "driving": state.worker.cancels.count() }))
}

async fn run_handler(State(state): State<ServerState>, headers: HeaderMap, Path(execution_id): Path<String>) -> Response {
    if !state.weft_credential.admits_headers(&headers) {
        return (StatusCode::UNAUTHORIZED, "this worker answers the install only").into_response();
    }
    let Ok(execution_id) = execution_id.parse::<ExecutionId>() else {
        return (StatusCode::BAD_REQUEST, "not an execution id").into_response();
    };
    match state.worker.run_execution_id(execution_id).await {
        Ok(Some(answer)) => Json(answer).into_response(),
        Ok(None) => (StatusCode::SERVICE_UNAVAILABLE, "this worker is stopping").into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:#}")).into_response(),
    }
}

/// Serve this worker until the platform stops it: its recorded runs are
/// handed back (see the module doc) and it takes no new call, then it
/// winds down (`wind_down`): the runs it still drives end and their
/// records are written.
pub async fn serve(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: WorkerConfig) -> Result<()> {
    weft_core::net::install_crypto_provider();
    weft_core::time_scale::announce();
    let door = crate::door::Door::new(
        config.project_id,
        config.tenant_id.clone(),
        config.binary_hash.clone(),
        clients.door_broker.clone(),
        config.edge,
        config.secret.clone(),
        crate::door::RunPermits::from_env()?,
    );
    // The line is held open while runs are driven (the cancel watch) and
    // while records wait to be written (the writer's own calls); idle, it
    // closes as any line does, and the door's copy of its triggers reads
    // the broker until it is back (`weft_task_store::held_copy`).
    let worker = new_worker(catalog, clients, &config, door.clone());
    // The door ticks while this worker owns anything on record: a run it
    // drives, or a record still on its way.
    let (cancels, writer) = (worker.cancels.clone(), worker.clients.writer.clone());
    door.start_ticks(Box::new(move || cancels.count() > 0 || !writer.settled())).await?;
    spawn_cancel_watch(worker.clone());
    // What the runs share goes once nothing used it for the idle window.
    worker.clients.shared.sweep_every(std::time::Duration::from_secs(30));
    // SYNC: the `/_weft` prefix <-> weft_core::route::RESERVED_SEGMENT
    let own = Router::new()
        .route("/_weft/run/{execution_id}", post(run_handler))
        .route("/_weft/fire", post(fire_handler))
        .route("/_weft/healthz", get(healthz))
        .with_state(ServerState { worker: worker.clone(), weft_credential: WeftCredential::of(&config.secret) });
    let app = own.merge(crate::caller_conn::connection_router(crate::caller_conn::ConnServerState {
        door,
        weft_hop: WeftCredential::of(&config.secret),
        clock: worker.clients.clock.clone(),
        canceller: Arc::new(RegistryCanceller { cancels: worker.cancels.clone() }),
        starter: Arc::new(LiveStarter { worker: worker.clone() }),
    }));
    let app = app.layer(axum::middleware::from_fn_with_state(crate::busy::MemoryGuard::sampling(), crate::busy::refuse_when_busy));
    // Told to stop, the worker hands its recorded runs back and takes no
    // new call, while the calls it holds go on to their end.
    stop_on_signal(&worker);
    let stopping = worker.stopping.clone();
    crate::caller_conn::serve(
        app.layer(axum::middleware::map_response(mark_worker_answer)),
        config.port,
        async move { stopping.cancelled().await },
    )
    .await?;
    wind_down(&worker).await;
    Ok(())
}

/// Mark an answer as this worker's, whatever its status, so weft never
/// mistakes the program's own 404 for a worker that is gone.
// SYNC: WORKER_ANSWER_HEADER <-> crates/weft-platform-traits/src/runner.rs
pub(crate) async fn mark_worker_answer(mut answer: Response) -> Response {
    answer.headers_mut().insert(weft_platform_traits::WORKER_ANSWER_HEADER, axum::http::HeaderValue::from_static("1"));
    answer
}

/// Wait for the platform's stop (SIGTERM) or Ctrl+C.
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        },
        Err(e) => {
            tracing::error!(target: "weft_engine::worker", error = %e, "no SIGTERM handler; only Ctrl+C stops this worker cleanly");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Once the platform told the worker to stop ([`Worker::stopping`], which
/// hands every recorded run back): wait for the runs it still drives to
/// end and every record queued to be written. No deadline: what is waited
/// on is the work already under way, and the platform's kill is the bound.
async fn wind_down(worker: &Worker) {
    worker.background.wait_zero().await;
    // Every run's rows still queued go out before the process does: a
    // fast run that ended leaves its record trailing behind it.
    worker.clients.writer.written().await;
}

/// Fire [`Worker::stopping`] once the platform says to stop, and
/// [`Worker::stopping_overdue`] [`HAND_BACK_GRACE`] later.
fn stop_on_signal(worker: &Worker) {
    let (stopping, overdue) = (worker.stopping.clone(), worker.stopping_overdue.clone());
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(target: "weft_engine::worker", "told to stop: every run that can be suspended is handed back once its running steps end; the others carry on until the process goes");
        stopping.cancel();
        tokio::time::sleep(HAND_BACK_GRACE).await;
        overdue.cancel();
    });
}

/// Bears the run of a caller the door let in.
struct LiveStarter {
    worker: Worker,
}

#[async_trait::async_trait]
impl crate::caller_conn::LiveStarter for LiveStarter {
    async fn bear(&self, admitted: Box<crate::door::Admitted>) -> Result<crate::caller_conn::Born, Response> {
        let admitted = *admitted;
        let mut start = match self.worker.admit_caller(&admitted).await {
            Ok(start) => start,
            Err(refused) => {
                // No run started: the call is not counted against its
                // limits.
                admitted.slot.uncount();
                return Err(refused);
            }
        };
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> =
            ExchangeJournal::new(start.journal.clone(), self.worker.replica.clone(), admitted.live_config.journal_policy());
        let execution_id = start.execution_id;
        let worker = self.worker.clone();
        start.slot = Some(admitted.slot);
        let token = self.worker.background.token();
        Ok(crate::caller_conn::Born {
            execution_id,
            sink,
            drive: Box::new(move |exchange| {
                start.exchange = Some(exchange);
                Box::pin(async move {
                    let _token = token;
                    if let Err(e) = worker.drive(start).await {
                        tracing::error!(target: "weft_engine::door", %execution_id, error = %format!("{e:#}"), "a caller's run ended in error");
                    }
                })
            }),
        })
    }
}

/// What an arrival at the door becomes a run with, besides its trigger
/// (`Worker::admit`).
struct Arriving {
    /// The run's place in this worker's cancel registry: the run is this
    /// worker's from the moment it arrives, so a cancel finds it however
    /// early, and it is driven once.
    registered: Registered,
    /// What the trigger wakes with: a caller's opening request, or an
    /// event's payload.
    payload: serde_json::Value,
    /// The credentials its caller sent: the run reads them in memory, and
    /// its record holds none of them.
    redaction: weft_core::caller::Redaction,
    /// Somebody waits to hear the run was born (a fire's firer): a durable
    /// run's birth is on record before that answer leaves.
    acknowledged: bool,
}

/// Why an arrival did not become a run here.
enum Turned {
    /// What its run reads could not be read now.
    Unread(anyhow::Error),
    /// Its plan says it cannot start (`crate::plan::Unready`).
    Unready(Unready),
    /// A fault of this worker's, named.
    Internal(String),
    /// A durable run's birth did not go on record (logged); asking again
    /// later may.
    Unrecorded,
    /// Another worker bore this run already (the same fire handed over
    /// twice).
    BornElsewhere,
    /// The worker is stopping ([`Worker::stopping`]): a run born now would
    /// only have to leave at once. Asking again reaches another copy.
    Stopping,
}

impl Worker {
    /// THE start of a run at this worker's door, for a caller and an event
    /// alike, once its front has let it in: the trigger's plan (`crate::plan`,
    /// made once per trigger and per version of what it reads), its birth
    /// handed to its record through the run's one handle (an unrecorded
    /// run's in this process's memory), and the run's first state, built
    /// from the plan.
    async fn admit(
        &self,
        trigger: &Arc<crate::door::HeldTrigger>,
        entry: &Arc<weft_broker_client::protocol::ArmedEntry>,
        instance: Option<&weft_core::instance::InstanceId>,
        triggers_version: u64,
        arriving: Arriving,
    ) -> Result<RunStart, Turned> {
        let Arriving { registered, payload, redaction, acknowledged } = arriving;
        let execution_id = registered.execution_id;
        if self.stopping.is_cancelled() {
            return Err(Turned::Stopping);
        }
        // Everything the run reads from here on leans on this worker's
        // lease: its plan and image (an image prune keeps what a live
        // worker may be running, a run its door just bore included, before
        // anything of it is on record), then its birth, which makes the run
        // this worker's on record. The door woke its tick when it let the
        // run in (`Limits::admit`), so the lease is being renewed behind
        // this without the run waiting for it.
        let plan = self.plans.plan(&self.clients, self.door.broker().as_ref(), trigger, entry, instance, triggers_version).await.map_err(Turned::Unread)?;
        let startable = plan.start.as_ref().map_err(|unready| Turned::Unready(unready.clone()))?;
        let opening = startable.opening(execution_id, &payload).map_err(|e| Turned::Internal(format!("the run's first state: {e}")))?;
        let settings = entry.spec.settings;
        let journal = self.clients.writer.run(RunSpec { execution_id, settings, keep_for: plan.keep_for, epoch: 1, next_seq: 0, redaction });
        let birth = startable.birth(&plan, execution_id, &payload, crate::now_unix());
        weft_journal::JournalClient::record_events(journal.as_ref(), &birth, Some(&self.replica))
            .await
            .map_err(|e| Turned::Internal(format!("the run's birth: {e:#}")))?;
        if acknowledged && settings.keeping().is_durable() {
            if let Err(e) = journal.flush().await {
                if journal.born_elsewhere() {
                    return Err(Turned::BornElsewhere);
                }
                tracing::error!(target: "weft_engine::door", %execution_id, error = %format!("{e:#}"), "a durable run's birth could not be recorded");
                // The birth may have landed with its answer lost: ended,
                // so it never reads as running.
                self.clients.writer.give_up(execution_id, &format!("its birth could not be recorded: {e:#}")).await;
                return Err(Turned::Unrecorded);
            }
        }
        let born_with = BornWith {
            phase: weft_core::context::Phase::Fire,
            instance: plan.instance.clone(),
            instance_values: startable.ready.instance_values.clone(),
            picks: startable.ready.picks.clone(),
            settings,
        };
        Ok(RunStart {
            execution_id,
            journal,
            program: startable.program.clone(),
            starts_from: StartsFrom::Plan { born_with, opening },
            exchange: None,
            registered,
            slot: None,
        })
    }

    /// Bear the run a caller the door let in starts ([`Self::admit`]). A
    /// refusal is the answer the caller gets.
    async fn admit_caller(&self, admitted: &crate::door::Admitted) -> Result<RunStart, Response> {
        let failed = |status: StatusCode, why: String| (status, why).into_response();
        // The caller's request IS the trigger's wake payload: the trigger
        // node reads it off `ctx.wake` and fans it onto its ports.
        let payload = serde_json::to_value(&admitted.opening)
            .map_err(|e| failed(StatusCode::INTERNAL_SERVER_ERROR, format!("request serialize: {e}")))?;
        // Registered before its caller is handed the exchange: a caller
        // who leaves at once finds the run to cancel. A fresh id is no
        // other run's.
        let registered = self.cancels.claim(weft_core::new_execution_id()).expect("a fresh execution id is no other run's");
        let arriving = Arriving { registered, payload, redaction: admitted.redaction.clone(), acknowledged: false };
        self.admit(&admitted.trigger, &admitted.entry, admitted.instance.as_ref(), admitted.triggers_version, arriving).await.map_err(|turned| match turned {
            Turned::Unread(e) => {
                tracing::error!(target: "weft_engine::door", error = %format!("{e:#}"), "a run's facts could not be read");
                failed(StatusCode::SERVICE_UNAVAILABLE, "this run's settings could not be read; try again in a moment".into())
            }
            // The program cannot run this way (a run it would start needs
            // an instance the call did not name): a refusal of the run, the
            // same status the install's own run refusals answer with.
            Turned::Unready(Unready::Refused(why)) => failed(StatusCode::UNPROCESSABLE_ENTITY, why),
            Turned::Unready(Unready::Waits(RunGate::InfraDown(missing))) => {
                // The fix is the operator's (logged); the caller is told
                // what is true for them.
                tracing::info!(target: "weft_engine::door", "a caller refused: {}", weft_core::infra::run_gate::infra_not_running(&missing));
                crate::door::not_now()
            }
            Turned::Unready(Unready::Waits(RunGate::InstanceValues(refusal) | RunGate::Picks(refusal))) => {
                failed(StatusCode::UNPROCESSABLE_ENTITY, serde_json::to_string(&refusal).expect("a Refusal serializes"))
            }
            Turned::Internal(why) => failed(StatusCode::INTERNAL_SERVER_ERROR, why),
            // Only a birth somebody waits to hear of is flushed at once, and
            // nobody waits on a caller's (`acknowledged` is false above).
            Turned::Unrecorded | Turned::BornElsewhere => unreachable!("a caller's run is not born acknowledged"),
            Turned::Stopping => failed(StatusCode::SERVICE_UNAVAILABLE, "this copy of the program is stopping; try again in a moment".into()),
        })
    }

    /// A trigger's event at this worker's door ([`weft_core::door_fire::DoorFire`]):
    /// its front (the trigger as held, its holder, its program, its
    /// standing, its limits), then [`Self::admit`], and the drive of the run
    /// it started. A fire that cannot become a run now is put in its
    /// trigger's queue for later, which this answers rather than holding
    /// the fire. A refusal read off the held triggers is confirmed against
    /// the broker's answer now: a trigger armed or taken over a moment ago
    /// may not have been heard yet.
    async fn admit_event(
        &self,
        fire: weft_core::door_fire::DoorFire,
    ) -> Result<(weft_core::door_fire::Fired, Option<tokio::task::JoinHandle<RunAnswer>>), Response> {
        use weft_core::door_fire::Fired;
        let unread = |e: anyhow::Error| {
            tracing::error!(target: "weft_engine::door", token = %fire.token, error = %format!("{e:#}"), "a fire's trigger could not be read");
            (StatusCode::SERVICE_UNAVAILABLE, "this worker could not read the trigger; try again in a moment").into_response()
        };
        let broker = self.door.broker().clone();
        let held = broker.triggers().await.map_err(unread)?;
        let served = |triggers: &crate::door::Triggers| {
            triggers.get(&fire.token).filter(|trigger| fire.held_by.is_none() || trigger.held_by == fire.held_by).cloned()
        };
        let (triggers, trigger) = match served(&held) {
            Some(trigger) => (held, trigger),
            None => {
                let fresh = broker.fresh_triggers().await.map_err(unread)?;
                match (fresh.get(&fire.token).cloned(), served(&fresh)) {
                    (None, _) => return Ok((Fired::Dropped { reason: "the trigger is gone".into() }, None)),
                    (Some(_), None) => return Ok((Fired::NotHeld, None)),
                    (Some(_), Some(trigger)) => (fresh, trigger),
                }
            }
        };
        let entry = match &trigger.entry {
            crate::door::HeldEntry::Armed(entry) => entry.clone(),
            crate::door::HeldEntry::Unservable { why, .. } => return Ok((Fired::Dropped { reason: why.clone() }, None)),
        };
        let park = |reason: String, instance_gap: bool| self.park_fire(&fire, reason, instance_gap);
        // Armed on another version of the program: the install's drain
        // hands it to the workers running that one.
        if entry.binary_hash != self.door.binary_hash {
            return park("its trigger is armed on another version of the program".into(), false).await.map(|fired| (fired, None));
        }
        match entry.standing.arrival(crate::now_unix() as i64) {
            weft_core::arrival::Arrival::Live => {}
            weft_core::arrival::Arrival::Wait => {
                return park(format!("its trigger is {}", entry.standing.status.as_str()), false).await.map(|fired| (fired, None));
            }
            weft_core::arrival::Arrival::Refused => {
                return Ok((Fired::Dropped { reason: format!("its trigger takes no work ({})", entry.standing.status.as_str()) }, None));
            }
        }
        let execution_id = weft_core::door_fire::run_of_fire(fire.fire_id);
        // Taken now, in one step with the check: the same fire delivered
        // twice at once starts one run.
        let Some(registered) = self.cancels.claim(execution_id) else {
            return Ok((Fired::AlreadyBorn, None));
        };
        // An event waits for a run permit like a caller does; one that
        // waited as long as a caller may waits in its trigger's queue.
        let Ok(permit) = self.door.take_permit().await else {
            return park("this copy of the program takes all the runs it may".into(), false).await.map(|fired| (fired, None));
        };
        let slot = match self.door.admit_event(permit, &fire.token, fire.caller.as_deref(), &entry.spec.limits.resolve()) {
            Ok(slot) => slot,
            Err(refused) => {
                let reason = format!("{} is reached", refused.reason.describe());
                return match refused.reason {
                    // A caller is told when to come back; an event past the
                    // trigger's own limits waits in its queue for room.
                    weft_core::signal::limits::Limited::PerCaller | weft_core::signal::limits::Limited::InvalidTokens => {
                        Ok((Fired::Refused { reason, retry_after_secs: refused.retry_after_secs }, None))
                    }
                    weft_core::signal::limits::Limited::PerEntry | weft_core::signal::limits::Limited::AtOnce => {
                        park(reason, false).await.map(|fired| (fired, None))
                    }
                };
            }
        };
        let arriving = Arriving {
            registered,
            payload: fire.payload.clone(),
            redaction: weft_core::caller::Redaction::default(),
            acknowledged: true,
        };
        let born = match self.admit(&trigger, &entry, entry.instance.as_ref(), triggers.version, arriving).await {
            Ok(born) => born,
            Err(turned) => {
                // Nothing started: the run is not counted toward its entry.
                slot.uncount();
                return match turned {
                    Turned::Unread(e) => Err(unread(e)),
                    Turned::Unready(Unready::Refused(why)) => Ok((Fired::Dropped { reason: why }, None)),
                    Turned::Unready(Unready::Waits(RunGate::InfraDown(missing))) => {
                        park(weft_core::infra::run_gate::infra_not_running(&missing), false).await.map(|fired| (fired, None))
                    }
                    Turned::Unready(Unready::Waits(RunGate::InstanceValues(refusal))) => park(refusal.to_string(), true).await.map(|fired| (fired, None)),
                    Turned::Unready(Unready::Waits(RunGate::Picks(refusal))) => park(refusal.to_string(), false).await.map(|fired| (fired, None)),
                    Turned::Internal(why) => Err((StatusCode::INTERNAL_SERVER_ERROR, why).into_response()),
                    Turned::BornElsewhere => Ok((Fired::AlreadyBorn, None)),
                    Turned::Unrecorded => park("the run could not be recorded".into(), false).await.map(|fired| (fired, None)),
                    Turned::Stopping => Err((StatusCode::SERVICE_UNAVAILABLE, "this worker is stopping").into_response()),
                };
            }
        };
        // The run goes on its own, counted toward its entry until it ends.
        Ok((Fired::Started { execution_id }, Some(self.drive_born(born, Some(slot)))))
    }

    /// Put `fire` in its trigger's queue, for `reason`: the install's drain
    /// hands it back once it can become a run. One waiting on what its
    /// instance provides (`instance_gap`) is not retried on a timer: the
    /// instance's next change of values routes it again.
    async fn park_fire(&self, fire: &weft_core::door_fire::DoorFire, reason: String, instance_gap: bool) -> Result<weft_core::door_fire::Fired, Response> {
        use weft_core::door_fire::Fired;
        // A fire that already failed to become a run backs off further.
        let attempts = if instance_gap { fire.attempts } else { fire.attempts + 1 };
        let parked = weft_task_store::parked_fires::waiting(
            fire.fire_id,
            fire.payload.clone(),
            fire.caller.clone(),
            attempts,
            instance_gap.then(|| reason.clone()),
        );
        let parked = self.door.broker().park_fire(&fire.token, &parked, fire.held_by.as_deref()).await.map_err(|e| {
            tracing::error!(target: "weft_engine::door", token = %fire.token, error = %format!("{e:#}"), "a fire could not be put in its trigger's queue");
            (StatusCode::SERVICE_UNAVAILABLE, "the fire could not be queued; try again in a moment").into_response()
        })?;
        Ok(match parked {
            weft_broker_client::protocol::DoorParked::Parked => Fired::Parked { reason, instance_gap },
            weft_broker_client::protocol::DoorParked::QueueFull => {
                tracing::warn!(target: "weft_engine::door", token = %fire.token, %reason, "a trigger's queue is full; a fire is dropped");
                Fired::Dropped { reason: format!("{reason}, and the trigger's queue of waiting events is full") }
            }
            weft_broker_client::protocol::DoorParked::Gone => Fired::Dropped { reason: "the trigger is gone".into() },
            weft_broker_client::protocol::DoorParked::TakesNoWork => Fired::Dropped { reason: "the trigger takes no work".into() },
            weft_broker_client::protocol::DoorParked::NotHeld => Fired::NotHeld,
        })
    }

    /// Drive a run born at the door to its end on a task of its own, so the
    /// caller's request can go away without stopping it. `slot` holds its
    /// place at the door until it ends.
    fn drive_born(&self, mut start: RunStart, slot: Option<crate::door::Slot>) -> tokio::task::JoinHandle<RunAnswer> {
        let worker = self.clone();
        let token = self.background.token();
        start.slot = slot;
        tokio::spawn(async move {
            let _token = token;
            answer_of(worker.drive(start).await)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_driver::engine_test_rig::{clients, MemJournal};

    /// A run stopped at the platform's cap is told the way out that fits
    /// it: a pause for one nobody is talking to, and for one with a caller
    /// a reconnect over kept state.
    #[test]
    fn the_cap_names_the_way_out_that_fits_the_run() {
        let cap = std::time::Duration::from_secs(3600);
        let live = run_cap_reached(ExecutionId::nil(), cap, true);
        assert!(live.contains("60 minute limit") && live.contains("reconnect"), "{live}");
        let plain = run_cap_reached(ExecutionId::nil(), cap, false);
        assert!(plain.contains("60 minute limit") && plain.contains("pause"), "{plain}");
    }

    #[test]
    fn a_worker_door_admits_only_its_key() {
        let secret = weft_core::caller_token::ProjectSecret::of(b"install", uuid::Uuid::from_u128(1));
        let key = secret.worker_door_key();
        let door = WeftCredential::of(&secret);
        let mut headers = HeaderMap::new();
        assert!(!door.admits_headers(&headers), "no bearer");
        headers.insert(axum::http::header::AUTHORIZATION, format!("Bearer {key}").parse().unwrap());
        assert!(!door.admits_headers(&headers), "a caller's own Authorization is the program's, never the door's");
        headers.insert(weft_platform_traits::WORKER_AUTH_HEADER, format!("Bearer {key}").parse().unwrap());
        assert!(door.admits_headers(&headers));
        let other = weft_core::caller_token::ProjectSecret::of(b"install", uuid::Uuid::from_u128(2)).worker_door_key();
        headers.insert(weft_platform_traits::WORKER_AUTH_HEADER, format!("Bearer {other}").parse().unwrap());
        assert!(!door.admits_headers(&headers), "another project's key");
    }


    /// The broker's side of the runs a worker drives: the cancels waiting
    /// for them (asking only reads, so every ask finds them), every ask
    /// recorded, and nothing to claim.
    #[derive(Default)]
    struct Runs {
        asked: std::sync::atomic::AtomicUsize,
        cancels: std::sync::Mutex<Vec<ExecutionId>>,
    }

    #[async_trait::async_trait]
    impl crate::context::RunClient for Runs {
        async fn claim(&self, _execution_id: ExecutionId) -> anyhow::Result<Option<weft_journal::record::Claimed>> {
            Ok(None)
        }
        async fn let_go(&self, _execution_id: ExecutionId, _why: weft_broker_client::protocol::LetGo) -> anyhow::Result<()> {
            unreachable!("nothing is driven here")
        }
        async fn answers(&self, _execution_id: ExecutionId, _taken: &[String], _wait: std::time::Duration) -> anyhow::Result<Vec<weft_broker_client::protocol::RunAnswer>> {
            unreachable!("nothing is driven here")
        }
        async fn cancels(&self) -> anyhow::Result<Vec<weft_broker_client::protocol::RunCancel>> {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self
                .cancels
                .lock()
                .unwrap()
                .iter()
                .map(|execution_id| weft_broker_client::protocol::RunCancel { execution_id: *execution_id, cause: weft_core::exec::CancelCause::User })
                .collect())
        }
    }

    struct NoCatalog;
    impl NodeCatalog for NoCatalog {
        fn lookup(&self, _t: &str) -> Option<&'static dyn weft_core::Node> {
            None
        }
        fn all(&self) -> Vec<&'static str> {
            Vec::new()
        }
    }

    fn config() -> WorkerConfig {
        WorkerConfig {
            project_id: uuid::Uuid::from_u128(1),
            tenant_id: "t".into(),
            replica: "worker-1".into(),
            binary_hash: "bin-1".into(),
            edge: crate::door::Edge::default(),
            secret: weft_core::caller_token::ProjectSecret::of(b"secret", uuid::Uuid::from_u128(1)),
            port: 0,
            run_cap: None,
            resume_when_caller_leaves: false,
        }
    }

    fn worker_with(clients: EngineClients) -> Worker {
        let config = config();
        let door = crate::door::Door::new(
            config.project_id,
            config.tenant_id.clone(),
            config.binary_hash.clone(),
            clients.door_broker.clone(),
            crate::door::Edge::default(),
            config.secret.clone(),
            crate::door::RunPermits::new(64, std::time::Duration::from_secs(30)),
        );
        new_worker(Arc::new(NoCatalog), clients, &config, door)
    }

    fn worker_over(runs: Arc<Runs>, line: Arc<crate::context::TestLine>) -> Worker {
        let mut clients = clients(Arc::new(MemJournal::default()));
        clients.runs = runs;
        clients.line = line;
        worker_with(clients)
    }

    /// A worker whose door asks `broker`, with a trigger `t1` armed for its
    /// program, as `arm` leaves it.
    fn worker_firing(arm: impl FnOnce(&mut crate::door::fake::FakeDoorBroker, &mut weft_broker_client::protocol::DoorTrigger)) -> (Worker, Arc<crate::door::fake::FakeDoorBroker>) {
        let mut trigger = crate::door::fake::event_trigger("t1", "timer");
        let mut broker = crate::door::fake::FakeDoorBroker::bare(Vec::new());
        arm(&mut broker, &mut trigger);
        broker.triggers.get_mut().unwrap().push(trigger);
        let broker = Arc::new(broker);
        let mut clients = clients(Arc::new(MemJournal::default()));
        clients.door_broker = broker.clone();
        clients.line = crate::context::TestLine::new();
        (worker_with(clients), broker)
    }

    fn a_fire(token: &str) -> weft_core::door_fire::DoorFire {
        weft_core::door_fire::DoorFire {
            token: token.into(),
            fire_id: uuid::Uuid::from_u128(7),
            payload: serde_json::json!({ "tick": 1 }),
            caller: None,
            held_by: None,
            attempts: 0,
        }
    }

    async fn fired(worker: &Worker, fire: weft_core::door_fire::DoorFire) -> weft_core::door_fire::Fired {
        match worker.admit_event(fire).await {
            Ok((fired, _)) => fired,
            Err(answer) => panic!("the door answered {}", answer.status()),
        }
    }

    fn entry(trigger: &mut weft_broker_client::protocol::DoorTrigger) -> &mut weft_broker_client::protocol::ArmedEntry {
        crate::door::fake::armed(trigger)
    }

    /// While its trigger is parked, a fire waits in the trigger's queue,
    /// under its own id and as the worker read it (the listener already
    /// processed it), to run once the trigger is back.
    #[tokio::test]
    async fn a_fire_for_a_parked_trigger_waits_in_its_queue() {
        let (worker, broker) = worker_firing(|_, trigger| {
            entry(trigger).standing.status = weft_core::projects::ProjectStatus::Inactive;
        });
        let answer = fired(&worker, a_fire("t1")).await;
        assert!(matches!(answer, weft_core::door_fire::Fired::Parked { instance_gap: false, .. }), "{answer:?}");
        let parked = broker.parked.lock().unwrap();
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].0, "t1");
        assert_eq!(parked[0].1.fire_id, uuid::Uuid::from_u128(7), "the fire keeps its id: it starts the same run later");
    }

    #[tokio::test]
    async fn a_fire_for_a_trigger_that_takes_no_work_or_is_gone_is_dropped() {
        let (worker, broker) = worker_firing(|_, trigger| {
            entry(trigger).standing.status = weft_core::projects::ProjectStatus::Inactive;
            entry(trigger).standing.accepting_fires = false;
        });
        assert!(matches!(fired(&worker, a_fire("t1")).await, weft_core::door_fire::Fired::Dropped { .. }));
        assert!(matches!(fired(&worker, a_fire("gone")).await, weft_core::door_fire::Fired::Dropped { .. }));
        assert!(broker.parked.lock().unwrap().is_empty());
    }

    /// A fire for a trigger armed on another version of the program waits
    /// in the queue: the install hands it to the workers of that version.
    #[tokio::test]
    async fn a_fire_for_another_programs_trigger_waits_for_its_workers() {
        let (worker, broker) = worker_firing(|_, trigger| entry(trigger).binary_hash = "bin-2".into());
        assert!(matches!(fired(&worker, a_fire("t1")).await, weft_core::door_fire::Fired::Parked { .. }));
        assert_eq!(broker.parked.lock().unwrap().len(), 1);
    }

    /// An event a holder picked up is taken only while the signal is still
    /// held under that holder: another took it, and serves its own events.
    #[tokio::test]
    async fn a_holders_event_is_taken_only_while_it_holds_the_signal() {
        let (worker, _) = worker_firing(|_, trigger| trigger.held_by = Some("holder-2".into()));
        let mut fire = a_fire("t1");
        fire.held_by = Some("holder-1".into());
        assert_eq!(fired(&worker, fire).await, weft_core::door_fire::Fired::NotHeld);
    }

    /// An event past its trigger's per-minute limit waits in its queue for
    /// the next minute: an event's work is kept, never dropped for being
    /// early. Nobody called, so nobody is told to come back.
    #[tokio::test]
    async fn an_event_past_its_triggers_per_minute_limit_waits_in_its_queue() {
        let limits = weft_core::signal::EntryLimits { per_minute: Some(1), ..Default::default() };
        let (worker, broker) = worker_firing(|_, trigger| entry(trigger).spec.limits = limits);
        // A run of this minute already went.
        let _slot = worker.door.admit_event(worker.door.take_permit().await.unwrap(), "t1", None, &limits.resolve()).unwrap();
        assert!(matches!(fired(&worker, a_fire("t1")).await, weft_core::door_fire::Fired::Parked { .. }));
        assert_eq!(broker.parked.lock().unwrap().len(), 1);
    }

    fn cancel_heard(execution_id: ExecutionId) -> weft_task_store::pg_signal::Heard {
        weft_task_store::pg_signal::Heard::Signal {
            channel: weft_task_store::runs::CANCEL_CHANNEL,
            payload: weft_task_store::runs::cancel_payload(uuid::Uuid::from_u128(1), execution_id).into(),
        }
    }

    async fn cancelled(flag: &CancellationFlag) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !flag.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the cancel reached the flag");
    }

    async fn until(what: &str, holds: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !holds() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    // A cancel the line announces for a run driven here reaches its flag;
    // one for a run driven elsewhere asks nothing; the line is held open
    // exactly while something is driven; and a line that opens again asks
    // for the cancels of every run driven.
    weft_core::stress_test! {
        name: a_heard_cancel_fires_the_flag_of_the_run_driven,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let runs = Arc::new(Runs::default());
            let line = crate::context::TestLine::new();
            let worker = worker_over(runs.clone(), line.clone());
            spawn_cancel_watch(worker.clone());

            let driven = weft_core::new_execution_id();
            let registered = worker.cancels.claim(driven).expect("a fresh run");
            let flag = registered.flag.clone();
            until("driving, the line is held open", || line.held.load(std::sync::atomic::Ordering::SeqCst) > 0).await;
            line.pushed.send(cancel_heard(weft_core::new_execution_id())).unwrap();
            runs.cancels.lock().unwrap().push(driven);
            line.pushed.send(cancel_heard(driven)).unwrap();
            cancelled(&flag).await;
            assert_eq!(runs.asked.load(std::sync::atomic::Ordering::SeqCst), 1, "a cancel of a run driven elsewhere asks nothing");

            let missed = weft_core::new_execution_id();
            let _missed = worker.cancels.claim(missed).expect("a fresh run");
            let flag = _missed.flag.clone();
            runs.cancels.lock().unwrap().push(missed);
            line.pushed.send(weft_task_store::pg_signal::Heard::Recheck).unwrap();
            cancelled(&flag).await;

            drop(registered);
            drop(_missed);
            until("with nothing driven, the line is let go of", || line.held.load(std::sync::atomic::Ordering::SeqCst) == 0).await;
        }
    }

    /// A delivery of a run whose drive here is still leaving claims it
    /// once that drive left.
    #[tokio::test]
    async fn a_claim_waits_for_the_runs_last_drive_here_to_leave() {
        let cancels = Cancels::new();
        let execution_id = weft_core::new_execution_id();
        let leaving = cancels.claim(execution_id).expect("a fresh run");
        let mut claiming = tokio::spawn({
            let cancels = cancels.clone();
            async move { cancels.claim_once_left(execution_id).await }
        });
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), &mut claiming).await.is_err(), "the drive has not left");
        drop(leaving);
        let claimed = claiming.await.unwrap();
        assert!(cancels.contains(execution_id));
        drop(claimed);
        assert!(!cancels.contains(execution_id));
    }

    #[tokio::test]
    async fn a_call_for_a_run_that_is_not_queued_says_so() {
        let worker = worker_over(Arc::new(Runs::default()), crate::context::TestLine::new());
        let execution_id = weft_core::new_execution_id();
        assert_eq!(worker.run_execution_id(execution_id).await.unwrap(), Some(RunAnswer::NothingToRun));
        assert!(!worker.cancels.contains(execution_id), "nothing claimed, nothing registered");
    }
}
