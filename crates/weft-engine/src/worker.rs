//! The worker: the project's compiled program, serving HTTP.
//!
//! Weft calls a worker for each run it hands one; the worker never goes
//! looking for work. Three ways in, one drive:
//!
//! - `POST /_weft/run/<run_id>`: carry on this queued run. The
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
//! no new node and, once the nodes it runs have ended, is handed back
//! whole for another worker to carry on from its record (a node still
//! running `HAND_BACK_GRACE` later is stopped where it is, and the next
//! worker fails it as cut short); one that cannot (unrecorded, tied to
//! the caller on the line, or with a bus open) carries on as if nothing
//! was asked, getting as far as it can before the platform stops the
//! process, which ends it the way a crash would. The worker writes what
//! is left of their records as each one ends. On a
//! platform that stops a worker no call holds open, a run that outlives
//! its caller has to leave the same way once its caller leaves, and
//! carries on under the call weft holds open for its resume.

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
use weft_core::{RunId, NodeCatalog};
use weft_journal::RunEvent;

use crate::context::EngineClients;
use crate::run_driver::{run_one, BornWith, RunOutcome, StartsFrom};
use crate::journal_writer::{DriveJournal, RunSpec};
use crate::line_watch::{spawn_line_watch, Cancels, LineWatch, Registered, REASK_FIRST, REASK_LONGEST};
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
    /// The program this worker runs (its binary hash): its door
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

/// How long a run on a worker told to stop has to end the nodes
/// it is running and hand itself back whole, before those nodes are
/// stopped where they are and the run is handed back without them. Half
/// of the ten seconds Cloud Run waits between its stop and its kill, so
/// the run's last rows and its hand-back still land.
const HAND_BACK_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// How long before the platform's cap a run stops itself, so the ending
/// it writes is its own rather than a cut connection.
pub(crate) const RUN_CAP_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);

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
    /// The runs it holds while they only wait outside (`crate::run_holds`).
    holds: Arc<crate::run_holds::Holds>,
    /// Told whenever a run comes, or one of [`Self::holds`] goes quiet or
    /// moves again: what the worker has under way may have changed.
    under_way_changed: Arc<tokio::sync::Notify>,
    /// What this worker keeps of its programs and of the runs its triggers
    /// start (`crate::plan`).
    plans: Arc<crate::plan::Plans>,
    /// Every drive this worker runs, the gate shutdown waits on.
    background: Arc<weft_core::in_flight::InFlight>,
    /// Fired when the platform tells the worker to stop: every run it
    /// drives has to leave (see the module doc), and no run is born here.
    stopping: tokio_util::sync::CancellationToken,
    /// Fired [`HAND_BACK_GRACE`] after [`Self::stopping`]: the nodes still
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
/// answers a caller (`RunStarted::stand_in`): nobody is on the line,
/// so the stand-in serves the trigger's spec and the opening request.
struct StandIn {
    spec: weft_core::primitive::SignalSpec,
    request: weft_core::caller::LiveRequest,
}

impl StandIn {
    /// The stand-in caller a run started by hand through a trigger that
    /// answers a caller serves (`RunStarted::stand_in`): its request
    /// is the firing kick's payload. `None` for any other run: a caller on
    /// the line was on the worker that bore its run, and none follows it.
    /// `birth` is the run's birth, `firing_payload` what its firing kick
    /// was given (`ClaimedRecord::firing_payload`).
    fn of(birth: &RunEvent, firing_payload: Option<&serde_json::Value>) -> Result<Option<Self>> {
        let RunEvent::RunStarted { stand_in: Some(spec), .. } = birth else {
            return Ok(None);
        };
        let payload = firing_payload
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("a run started by hand through a caller's trigger has no firing kick to serve"))?;
        let request: weft_core::caller::LiveRequest =
            serde_json::from_value(payload).context("the firing kick of a run started by hand through a caller's trigger is not a request")?;
        // The headers a gate reads were refused at the command, which
        // reads the gate's connection; this floor holds the rest.
        if let Some(header) = request.credential_typed_by_hand(&[]) {
            anyhow::bail!("{}", weft_core::caller::fired_credential(header));
        }
        Ok(Some(Self { spec: (**spec).clone(), request }))
    }

    /// The exchange its run serves through the stand-in, recorded on the
    /// run's record like a real caller's. Only an HTTP trigger can be
    /// fired: a socket's shape is a conversation over time, and there is
    /// nothing honest to invent for the caller's next message.
    fn exchange(self, run_id: RunId, journal: Arc<DriveJournal>, replica: &str) -> Result<crate::run_driver::Exchange> {
        let (protocol, cfg) = weft_core::signal::live_connection(&self.spec).map_err(|e| anyhow::anyhow!("the caller of run {run_id}: {e}"))?;
        if protocol != weft_core::signal::Protocol::Http {
            anyhow::bail!(
                "a Socket cannot be fired: its shape is a conversation over time, and there is \
                 nothing honest to invent for the caller's next message. Point a real client at \
                 it (`weft activate` prints the URL)"
            );
        }
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> = ExchangeJournal::new(journal, replica.to_string(), cfg.journal);
        // The stand-in serves the REQUEST and records the answer, and that
        // is all it can honestly do: a fired run's body, when the author
        // typed one, is a field of the trigger's own wake payload, and the
        // node reads it there.
        let fired = crate::fired_caller::FiredCaller::open(
            run_id,
            weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol),
            self.request,
            sink.clone(),
        );
        Ok(crate::run_driver::Exchange { conn: fired, live: None, sink })
    }
}

/// What a drive needs about the run it drives, wherever the run came
/// from: a claim, or this worker's door.
struct RunStart {
    run_id: RunId,
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
    exchange: Option<crate::run_driver::Exchange>,
    /// Its place in the cancel registry, and so its cancel: taken before
    /// anything of the run starts (`Cancels::claim`).
    registered: Registered,
    /// Its place at the door (a run born at this worker's door, or a fire
    /// it started), given up when the run's own work ends.
    slot: Option<crate::door::Slot>,
}

impl Worker {
    /// The watch over this worker's line, for the runs it drives.
    fn line_watch(&self) -> LineWatch {
        LineWatch::new(self.project_id, &self.clients, self.cancels.clone())
    }

    /// What this worker has under way, its lease holding `lease_secs`
    /// without a tick: what its door ticks or sleeps on. Once told to
    /// stop it vouches for nothing and ticks for as long as it lives, so
    /// its lease lapses a lease length after the process goes, whatever its
    /// runs wait on (`Worker::stopping` wakes the door).
    fn under_way(&self, lease_secs: i64) -> crate::door::UnderWay {
        if self.stopping.is_cancelled() {
            return crate::door::UnderWay::Stopping;
        }
        crate::door::UnderWay::of(self.clients.writer.settled(), self.cancels.count(), self.holds.quiet(), lease_secs)
    }

    /// Whether this worker does real work right now, not told to stop: a
    /// run that moves, or a record on its way.
    fn works(&self) -> bool {
        self.under_way(0) == crate::door::UnderWay::Working
    }

    /// Stop for the cut of its line (`cut`), in one step with the look
    /// that decides it, if at this instant it drives runs, every one of
    /// them only waits outside and every record is written (the state its
    /// door sleeps in). With no run able to come, go, go quiet or move
    /// again meanwhile, it takes no new run from here on, which also hands
    /// back each of those runs that can pause, and ends the others, which
    /// hold their waits in this worker, at the cut ([`Limit::LineCut`]).
    /// Otherwise nothing is done, and the answer says what stood in the
    /// way.
    fn stopped_for_cut(&self, cut: weft_broker_client::line::Cut) -> CutLook {
        let quiet = self.holds.held_still();
        let driven = self.cancels.held_still();
        let runs = || driven.iter().flat_map(|shard| shard.iter());
        if runs().next().is_none() || runs().any(|(run_id, _)| !quiet.contains_key(run_id)) {
            return CutLook::Moving;
        }
        if !self.clients.writer.settled() {
            return CutLook::RecordsPending;
        }
        self.stopping.cancel();
        let left = (cut.opened + cut.max_age).saturating_duration_since(tokio::time::Instant::now());
        for (run_id, flag) in runs() {
            if quiet.get(run_id).is_some_and(|how| !how.can_pause) {
                Limit::LineCut { cut: cut.max_age, left }.end(flag, *run_id);
            }
        }
        CutLook::Stopped
    }

    /// Claim queued run `run_id` and carry it on to its end, on a
    /// task of its own so whoever handed it here can go away without
    /// stopping it (a run stops itself before the platform's cap, see
    /// `RUN_CAP_MARGIN`). `None` once this worker is stopping: a run
    /// claimed then would only be handed back, and weft hands it to another
    /// worker instead.
    async fn carry_on_run(&self, run_id: RunId) -> Result<Option<RunAnswer>> {
        // Registered before the claim: a cancel announced once the claim
        // committed finds the run here.
        let registered = tokio::select! {
            biased;
            () = self.stopping.cancelled() => return Ok(None),
            registered = self.cancels.claim_once_left(run_id) => registered,
        };
        // Stopped as it registered (a stop decides with every registration
        // held still, `Worker::stopped_for_cut`): it goes no further.
        if self.stopping.is_cancelled() {
            return Ok(None);
        }
        // The claim makes the run this worker's on record, which its lease
        // keeps: the registration woke the door's tick, and the claim does
        // not wait for it.
        let Some(claimed) = self.clients.runs.claim(run_id).await.context("claim the run")? else {
            return Ok(Some(RunAnswer::NothingToRun));
        };
        if let Some(cause) = claimed.cancel_requested.clone() {
            registered.flag.cancel_because(cause);
        }
        let worker = self.clone();
        let token = self.background.token();
        let drive = tokio::spawn(async move {
            let _token = token;
            answer_of(worker.carry_on(run_id, claimed, registered).await)
        });
        drive.await.map(Some).context("the drive panicked outside its guard")
    }

    /// Carry on a run this worker just claimed, from its record, read a
    /// page at a time.
    async fn carry_on(
        &self,
        run_id: RunId,
        claimed: weft_journal::record::Claimed,
        registered: Registered,
    ) -> Result<()> {
        let record = &**self.clients.writer.record();
        let project = self.plans.project(&self.clients, &claimed.definition_hash).await;
        // A record this worker cannot read, a program it cannot fetch, or a
        // stand-in it cannot serve still ends the run: it is this worker's
        // now.
        let keep_for = project.as_ref().map_or(weft_core::run_settings::KeepFor::WEFT_DEFAULT, |project| project.defaults.keep_for());
        // A program that cannot be fetched is why the record cannot be read.
        let read = match project {
            Ok(project) => {
                let program = self.plans.program(&project, &claimed.definition_hash);
                crate::run_driver::ClaimedRecord::read(|run, after| weft_journal::decoded_page(record, run, after), &*self.clients.project, run_id, program.project.clone())
                    .await
                    .map(|read| (program, read))
            }
            Err(e) => Err(e),
        };
        let (settings, next_seq, read) = match read {
            Ok((program, read)) => (birth_settings(&read.birth), read.last_seq + 1, Ok((program, read))),
            // The record is read again on this rare path only, in one
            // pass: where its rows end (the run's ending goes after them)
            // and the charges it leaves open. When that pass fails too,
            // the broker ends the run after its last row and books what it
            // leaves open, as for any run whose ending cannot be written.
            Err(e) => match Leftovers::of(record, run_id).await {
                Ok(left) => (None, left.last_seq.map_or(0, |last| last + 1), Err((e, left.charges))),
                Err(again) => {
                    let why = format!("{e:#}; and reading its record again to end it failed too: {again:#}");
                    self.clients.writer.give_up(run_id, &why).await;
                    return Err(anyhow::anyhow!(why));
                }
            },
        };
        let settings = settings.unwrap_or_default();
        let journal = self.clients.writer.run(RunSpec {
            run_id,
            settings,
            keep_for: settings.kept_for(keep_for),
            epoch: claimed.epoch,
            next_seq,
            secrets: Vec::new(),
            program: None,
        });
        // What fails here still books the charges the record leaves open:
        // read with it, or in the pass above.
        let opened = match read {
            Ok((program, read)) => {
                let exchange = StandIn::of(&read.birth, read.firing_payload.as_ref()).and_then(|stand_in| {
                    stand_in.map(|stand_in| stand_in.exchange(run_id, journal.clone(), &self.replica)).transpose()
                });
                match exchange {
                    Ok(exchange) => Ok((program, read, exchange)),
                    Err(e) => Err((e, read.charges)),
                }
            }
            Err(failed) => Err(failed),
        };
        let (program, read, exchange) = match opened {
            Ok(opened) => opened,
            Err((e, charges)) => {
                return Err(crate::run_driver::fail_run(&self.clients.writer, &journal, run_id, &self.replica, Some(charges), e).await);
            }
        };
        self.drive(RunStart {
            run_id,
            journal,
            program,
            starts_from: StartsFrom::Record(Box::new(read)),
            exchange,
            registered,
            slot: None,
        })
        .await
    }

    /// Drive one run to its end or until it lets go: one claimed, or one
    /// born at this worker's door.
    async fn drive(&self, start: RunStart) -> Result<()> {
        let RunStart { run_id, journal, program, starts_from, exchange, registered, slot } = start;
        let cancellation = registered.flag.clone();
        // Every run is asked to leave this worker when it is told to stop,
        // or when its caller left: it suspends if it can and is carried on
        // by the next worker, and carries on here if it cannot, until the
        // process goes (the driver decides both). The ask ends with the
        // drive.
        let hand_back = crate::run_driver::HandBack { asked: self.stopping.child_token(), overdue: self.stopping_overdue.clone() };
        let _asked_ends = hand_back.asked.clone().drop_guard();
        // On a platform that stops a worker no call holds open, a run has to
        // leave this worker once its caller is gone (hung up, or answered):
        // it suspends and carries on under the delivery weft holds open for
        // its resume, or stays on as long as the worker does if it cannot
        // (a tied run its caller hung up on is cancelled already, before the
        // caller reads as gone).
        let caller = exchange.as_ref().and_then(|exchange| exchange.live.clone());
        let leaves_with = caller.clone().filter(|_| self.resume_when_caller_leaves);
        let run = run_one(
            self.catalog.clone(),
            &self.clients,
            crate::run_driver::RunDrive {
                run_id,
                journal,
                program,
                starts_from,
                cancellation: cancellation.clone(),
                exchange,
                hand_back: Some(&hand_back),
                // Its own cap ends when `while_driving` ends it.
                hold: Some(self.holds.of(
                    run_id,
                    self.run_cap.map(|cap| (crate::now_unix() + cap.saturating_sub(RUN_CAP_MARGIN).as_secs()) as i64),
                )),
            },
            &self.replica,
            &self.tenant_id,
        );
        let outcome = self.while_driving(run, run_id, &cancellation, caller.is_some(), leaves_with, &hand_back).await;
        // The run's own work is over: its place at the door (its entry's
        // runs at once, its run permit) is free once the slot goes, and a
        // failure is counted for `weft status`.
        if let Some(slot) = slot {
            if matches!(outcome, Err(_) | Ok(RunOutcome::Failed { .. } | RunOutcome::Stuck { .. })) {
                slot.failed(crate::now_unix() as i64);
            }
        }
        // A run parked or handed back with its whole record written is let
        // go of: it loses nothing when this worker goes away later, and
        // whoever carries it on owns it next. One the broker will not let
        // go of is ended by it, naming why: nobody would carry it on.
        let why = match &outcome {
            Ok(RunOutcome::Stalled) => Some(weft_broker_client::protocol::LetGo::Parked),
            Ok(RunOutcome::HandedBack) => Some(weft_broker_client::protocol::LetGo::HandedBack),
            _ => None,
        };
        if let Some(why) = why {
            if let Err(refused) = self.let_go(run_id, why).await {
                self.clients.writer.give_up(run_id, &format!("the run could not be let go of ({why:?}): {refused:#}")).await;
                return Err(refused);
            }
        }
        outcome.map(|_| ())
    }

    /// Let go of a run whose record is written (whole, or for a durable
    /// run handed back because a write got no answer, the part that
    /// landed), for `why`, asking until the broker answers: until then this
    /// worker owns the run on record, and nothing else may pick it up. An
    /// outage is asked about again. The broker saying the run is not this
    /// worker's is the let-go done already: an earlier ask whose answer was
    /// lost, or the run taken from this worker meanwhile (its lease lapsed
    /// while the broker was out of reach and another worker took it on, or
    /// it ended). Any other refusal is the broker's answer, and fails the drive
    /// with it, since asking again would hear the same.
    async fn let_go(&self, run_id: RunId, why: weft_broker_client::protocol::LetGo) -> Result<()> {
        // Whether an earlier ask may have landed with its answer lost.
        let mut maybe_landed = false;
        let mut wait = REASK_FIRST;
        loop {
            match self.clients.runs.let_go(run_id, why).await {
                Ok(()) => return Ok(()),
                Err(e) if broker_conflict(&e) => {
                    if maybe_landed {
                        tracing::info!(target: "weft_engine::worker", %run_id, "the run was let go of by an earlier ask whose answer was lost");
                    } else {
                        tracing::info!(target: "weft_engine::worker", %run_id, ?why, "the run is no longer this worker's to let go of (another worker took it on, or it ended)");
                    }
                    return Ok(());
                }
                Err(e) if broker_outage(&e) => {
                    maybe_landed = true;
                    tracing::warn!(target: "weft_engine::worker", %run_id, ?why, error = %format!("{e:#}"), "could not let go of the run yet; trying again");
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(REASK_LONGEST);
                }
                Err(e) => {
                    let e = e.context(format!("let go of run {run_id} ({why:?})"));
                    tracing::error!(target: "weft_engine::worker", %run_id, error = %format!("{e:#}"), "the broker refused to let go of the run");
                    return Err(e);
                }
            }
        }
    }
}

/// How the run born with `birth` is kept.
fn birth_settings(birth: &RunEvent) -> Option<weft_core::run_settings::RunSettings> {
    match birth {
        RunEvent::RunStarted { settings, .. } => Some(*settings),
        _ => None,
    }
}

/// What a run's record leaves behind for a worker that cannot carry it
/// on: where its rows end, and the charges it leaves open.
struct Leftovers {
    /// The last row; `None` for a record with none.
    last_seq: Option<i32>,
    /// The charges opened in every row that decodes: a row that does not
    /// (often why the record cannot be carried on) costs only the charges
    /// it would have opened, never the ones the others did.
    charges: weft_journal::ChargeBook,
}

impl Leftovers {
    /// Run `run_id`'s, read from `record` a page at a time, in one pass.
    async fn of(record: &dyn weft_journal::RecordClient, run_id: RunId) -> Result<Self> {
        let start = Self { last_seq: None, charges: Default::default() };
        weft_journal::record::fold_pages(|after| record.record_of(run_id, after), None, start, move |left: &mut Self, page| {
            left.last_seq = weft_journal::record::RecordPage::last_seq(&page);
            let (rows, _) = page.decode_lossy(run_id);
            rows.iter().for_each(|row| left.charges.apply(&row.event));
            Ok(())
        })
        .await
    }
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
        run: impl std::future::Future<Output = Result<RunOutcome>>,
        run_id: RunId,
        cancellation: &CancellationFlag,
        live: bool,
        leaves_with: Option<Arc<crate::caller_conn::LiveCallerConnection>>,
        hand_back: &crate::run_driver::HandBack,
    ) -> Result<RunOutcome> {
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
                    Limit::RunCap { cap: cap.expect("armed only with a cap"), live }.end(cancellation, run_id);
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

/// Journal sink that projects caller events to `RunEvent::Caller*` rows,
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
    /// One connection is one run, so the run is the same for
    /// every message; kept from the first one rather than passed to the
    /// flush.
    run_id: Option<RunId>,
    messages: Vec<weft_core::stream_journal::WindowedCallerMessage>,
    /// The program's messages the caller will never receive, each
    /// decided as one of `messages` was sent, so it joins the same row
    /// (`aggregate_caller_window`).
    dropped: Vec<u64>,
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
            // The first tick one window from now, not at once: a plain
            // `interval` completes its first tick immediately, which
            // would write the window's first message in a row of its own.
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + window, window);
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
    /// journal will. `dropped` joins under the same lock as the message,
    /// so a flush between the two can never leave it in a row of its own.
    fn hold(
        &self,
        run_id: RunId,
        offset: u64,
        direction: weft_core::stream_journal::CallerDirection,
        payload: &weft_core::bus::WirePayload,
        terminal: bool,
        dropped: Option<u64>,
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
        pending.run_id.get_or_insert(run_id);
        pending.kept_bytes += weighs;
        pending.dropped.extend(dropped);
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
        let (run_id, messages, dropped) = {
            let mut pending = self.pending.lock().expect("caller journal buffer");
            if pending.messages.is_empty() {
                return;
            }
            pending.kept_bytes = 0;
            (pending.run_id, std::mem::take(&mut pending.messages), std::mem::take(&mut pending.dropped))
        };
        let Some(run_id) = run_id else { return };
        let Some(window) = weft_core::stream_journal::aggregate_caller_window(messages, dropped) else {
            return;
        };
        self.emit(weft_journal::RunEvent::CallerWindow {
            run_id,
            first_offset: window.first_offset,
            last_offset: window.last_offset,
            messages: window.messages,
            totals: window.totals,
            dropped: window.dropped,
            at_unix: window.last_at_unix,
        });
    }

    /// A lifecycle row (connected, errored, disconnected) goes out on
    /// its own, so the open window is written first: the row stream
    /// must never say a thing happened before something it followed.
    /// Both land on the one queue, in this order, and the writer keeps
    /// them in it.
    fn emit_after_flush(&self, event: weft_journal::RunEvent) {
        self.flush();
        self.emit(event);
    }

    /// Hand one row to the lane. A row the lane refuses (an earlier one
    /// failed to be written) degrades the exchange's record.
    fn emit(&self, event: weft_journal::RunEvent) {
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
        // The record keeps the value with its secrets' markers, never what
        // the caller received.
        OutboundChunk::Revealed { kept, .. } => weft_core::bus::WirePayload::Json(kept.clone()),
    }
}

impl crate::caller_conn::CallerJournalSink for ExchangeJournal {
    fn degraded(&self) -> Option<String> {
        self.degraded.lock().expect("caller journal degraded").clone()
    }
    fn connected(&self, run_id: RunId, offset: u64, protocol: weft_core::signal::Protocol) {
        self.emit(weft_journal::RunEvent::CallerConnected {
            run_id,
            offset,
            protocol: protocol.as_wire_str().to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn inbound(&self, run_id: RunId, offset: u64, msg: &weft_core::caller::InboundMessage) {
        self.hold(
            run_id,
            offset,
            weft_core::stream_journal::CallerDirection::Inbound,
            &inbound_payload(msg),
            false,
            None,
        );
    }
    fn outbound(
        &self,
        run_id: RunId,
        offset: u64,
        chunk: &weft_core::caller::OutboundChunk,
        terminal: bool,
        dropped: Option<u64>,
    ) {
        self.hold(
            run_id,
            offset,
            weft_core::stream_journal::CallerDirection::Outbound,
            &outbound_payload(chunk),
            terminal,
            dropped,
        );
    }
    fn errored(&self, run_id: RunId, offset: u64, message: &str) {
        self.emit_after_flush(weft_journal::RunEvent::CallerErrored {
            run_id,
            offset,
            message: message.to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn disconnected(&self, run_id: RunId, offset: u64, reason: &str, dropped: &[u64]) {
        // The exchange is over: write what is held, say so, and let the
        // window clock stop. Nothing said after this can be lost,
        // because nothing is said after this.
        self.emit_after_flush(weft_journal::RunEvent::CallerDisconnected {
            run_id,
            offset,
            reason: reason.to_string(),
            dropped: dropped.to_vec(),
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

impl crate::caller_conn::RunCanceller for RegistryCanceller {
    fn cancel(&self, run_id: RunId, cause: weft_core::run::CancelCause) {
        self.cancels.cancel(run_id, cause);
    }
}

/// A time limit a run reaches, which ends it with a cause that says which
/// limit, how early, and what to do ([`Limit::end`], [`Limit::cause`]): the
/// one way a limit ends a run, whichever limit it is.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Limit {
    /// The cap on one stretch of a run (`WorkerConfig::run_cap`), reached
    /// [`RUN_CAP_MARGIN`] early; `live` when a caller is on the line, whose
    /// connection the cap bounds.
    RunCap { cap: std::time::Duration, live: bool },
    /// The cut of the worker's line to the broker after `cut`, reached by
    /// a run that waits without being able to pause, `left` before it
    /// (see [`keep_ahead_of_line_cut`]).
    LineCut { cut: std::time::Duration, left: std::time::Duration },
    /// The same cap on the one request a live node test runs inside, from
    /// its start to its report (`crate::test_rig`), reached
    /// [`RUN_CAP_MARGIN`] early.
    #[cfg(feature = "node-tests")]
    NodeTestCap { cap: std::time::Duration },
    /// The cap a live trigger's author sets on one caller's session
    /// (`maxSessionSecs`), reached on the dot: the caller is answered 504
    /// and a run tied to that caller ends (`crate::caller_conn`).
    SessionCap { cap: std::time::Duration },
}

impl Limit {
    /// End run `run_id` through its cancel `flag`, unless something
    /// already did.
    pub(crate) fn end(self, flag: &CancellationFlag, run_id: RunId) {
        if !flag.is_cancelled() {
            tracing::info!(target: "weft_engine::worker", %run_id, limit = ?self, "the run reached a platform limit");
            flag.cancel_because(self.cause(run_id));
        }
    }

    /// What run `run_id` is ended with: the limit, how early, and
    /// the way out that fits it.
    pub(crate) fn cause(self, run_id: RunId) -> weft_core::run::CancelCause {
        let detail = match self {
            Limit::RunCap { cap, live: true } => format!(
                "run {run_id} stopped {} before the {} minute limit this platform puts on one \
                 connection. A conversation meant to last longer keeps its state outside the run and \
                 has its client reconnect, each connection its own run",
                seconds(RUN_CAP_MARGIN),
                cap.as_secs() / 60,
            ),
            Limit::RunCap { cap, live: false } => format!(
                "run {run_id} stopped {} before the {minutes} minute limit this platform puts on one \
                 stretch of a run. A run that pauses whole (every branch waiting on a timer or a form) picks \
                 back up with a fresh {minutes} minutes, so split long work with a pause",
                seconds(RUN_CAP_MARGIN),
                minutes = cap.as_secs() / 60,
            ),
            Limit::LineCut { cut, left } => format!(
                "run {run_id} stopped {} before the {} minute limit this platform puts on the line \
                 between its worker and weft, counted from when that line opened, not from the run's start: every \
                 run on the worker only waited, and a worker whose runs only wait opens no new line, which would \
                 wake the database. A run that can pause (recorded, not tied to its caller, no bus open) lets go of \
                 its worker while it waits, and may wait as long as it needs",
                seconds(left),
                cut.as_secs() / 60,
            ),
            #[cfg(feature = "node-tests")]
            Limit::NodeTestCap { cap } => format!(
                "the live test's run {run_id} stopped {} before the {} minute limit this platform puts \
                 on one request, which a live test runs inside from its start to its report. A node test cannot \
                 pause, so any wait it held gave up here; a job that takes longer is tested as part of a run, \
                 which pauses while it waits",
                seconds(RUN_CAP_MARGIN),
                cap.as_secs() / 60,
            ),
            Limit::SessionCap { cap } => format!(
                "run {run_id} stopped when its caller's session reached the {} second limit its \
                 trigger sets (maxSessionSecs), and the caller was told it was a time limit (a 504, or close \
                 code 1011 on a socket). A session meant to last longer \
                 raises maxSessionSecs, or sets it to 0 for no limit",
                cap.as_secs(),
            ),
        };
        weft_core::run::CancelCause::Limit { detail }
    }
}

/// `d` in whole seconds, said the way a person reads it.
fn seconds(d: std::time::Duration) -> String {
    match d.as_secs() {
        1 => "1 second".into(),
        secs => format!("{secs} seconds"),
    }
}

fn new_worker(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: &WorkerConfig, door: Arc<crate::door::Door>) -> Worker {
    // A run coming, going quiet or moving again wakes the door's tick, and
    // the watch on the line's cut (`keep_ahead_of_line_cut`).
    let under_way_changed = Arc::new(tokio::sync::Notify::new());
    let changed = {
        let (door, under_way_changed) = (door.clone(), under_way_changed.clone());
        move || {
            door.wake();
            under_way_changed.notify_one();
        }
    };
    let holds = crate::run_holds::Holds::new(Box::new(changed.clone()));
    let cancels = Cancels::new(Box::new(changed));
    // Told to stop, the door ticks from then on (`Worker::under_way`).
    let stopping = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let (stopping, door) = (stopping.clone(), Arc::downgrade(&door));
        async move {
            stopping.cancelled().await;
            if let Some(door) = door.upgrade() {
                door.wake();
            }
        }
    });
    let left_out = catalog.left_out();
    Worker {
        holds,
        under_way_changed,
        door,
        project_id: config.project_id,
        catalog,
        clients,
        replica: config.replica.clone(),
        tenant_id: config.tenant_id.clone(),
        cancels,
        plans: Arc::new(crate::plan::Plans::new(config.project_id, left_out)),
        background: weft_core::in_flight::InFlight::new("worker run"),
        stopping,
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

async fn run_handler(State(state): State<ServerState>, headers: HeaderMap, Path(run_id): Path<String>) -> Response {
    if !state.weft_credential.admits_headers(&headers) {
        return (StatusCode::UNAUTHORIZED, "this worker answers the install only").into_response();
    }
    let Ok(run_id) = run_id.parse::<RunId>() else {
        return (StatusCode::BAD_REQUEST, "not a run id").into_response();
    };
    match state.worker.carry_on_run(run_id).await {
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
    // The port first: a worker that cannot have it ends here, before it
    // takes anything on.
    let listener = crate::listen::bind(config.port).await?;
    let budget = crate::busy::Budget::of_cgroup();
    tracing::info!(target: "weft_engine::worker", ?budget, "this worker's memory budget");
    let door = crate::door::Door::new(
        config.project_id,
        config.tenant_id.clone(),
        config.binary_hash.clone(),
        clients.door_broker.clone(),
        config.edge,
        config.secret.clone(),
        crate::door::RunPermits::from_env(budget)?,
    );
    // The line is held open while runs are driven (the line watch) and
    // while records wait to be written (the writer's own calls); idle, it
    // closes as any line does, and the door's copy of its triggers reads
    // the broker until it is back (`weft_task_store::held_copy`).
    let worker = new_worker(catalog, clients, &config, door.clone());
    // The door ticks while this worker owns anything on record: a run it
    // drives that does more than wait outside, or a record still on its
    // way. With only waiting runs, it vouches for the worker until the
    // earliest of their waits gives up, and sleeps. It ticks on the io
    // runtime, apart from the threads node bodies run on.
    let lease = weft_core::time_scale::scaled_secs(weft_task_store::worker_door::WORKER_LEASE_SECS);
    let under_way = worker.clone();
    door.start_ticks(&worker.clients.io, Box::new(move || under_way.under_way(lease)));
    // It lives as long as the process.
    spawn_line_watch(worker.line_watch());
    // What the runs share goes once nothing used it for the idle window.
    worker.clients.shared.sweep_every(std::time::Duration::from_secs(30));
    // SYNC: the `/_weft` prefix <-> weft_core::route::RESERVED_SEGMENT
    let own = Router::new()
        .route("/_weft/run/{run_id}", post(run_handler))
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
    let app = app.layer(axum::middleware::from_fn_with_state(crate::busy::MemoryGuard::start(budget), crate::busy::refuse_when_busy));
    // Told to stop, or nearing the cut of its line while its runs only
    // wait, the worker hands its recorded runs back and takes no new call,
    // while the calls it holds go on to their end. While it works, its line
    // is replaced before the cut instead.
    stop_on_signal(&worker);
    keep_ahead_of_line_cut(&worker);
    let stopping = worker.stopping.clone();
    crate::caller_conn::serve(
        app.layer(axum::middleware::map_response(mark_worker_answer)),
        listener,
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
/// end, let go of what they shared, and wait for every record queued to
/// be written. No deadline: what is waited on is the work already under
/// way, and the platform's kill is the bound.
async fn wind_down(worker: &Worker) {
    worker.background.wait_zero().await;
    // No run is left to use them: a pool's connections are given back to
    // the database now, while the records go out, rather than held until
    // the process is gone.
    worker.clients.shared.let_go_all();
    // Every run's rows still queued go out before the process does: a
    // fast run that ended leaves its record trailing behind it.
    worker.clients.writer.written().await;
}

/// What a look at stopping before the line's cut found
/// ([`Worker::stopped_for_cut`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutLook {
    /// Every run only waited, every record was written: it stopped.
    Stopped,
    /// No run, or one that does more than wait.
    Moving,
    /// Every run only waits, but records are still on their way.
    RecordsPending,
}

/// Once [`Worker::stopping`] fired, which takes no new run and hands back
/// every run that can be suspended once its running nodes end: fire
/// [`Worker::stopping_overdue`] [`HAND_BACK_GRACE`] later, which stops
/// the nodes still running.
async fn overdue_after_grace(worker: &Worker) {
    tokio::time::sleep(HAND_BACK_GRACE).await;
    worker.stopping_overdue.cancel();
}

/// Stop the worker once the platform says to. A run that cannot be
/// suspended carries on until the platform ends the process.
fn stop_on_signal(worker: &Worker) {
    let worker = worker.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(target: "weft_engine::worker", "told to stop: every run that can be suspended is handed back once its running nodes end; the others carry on until the process goes");
        worker.stopping.cancel();
        overdue_after_grace(&worker).await;
    });
}

/// Keep the worker ahead of the cut the broker said it makes on its line.
/// From half the line's age on, a worker that works replaces its line
/// ([`crate::context::WorkerLine::replace`]): its work keeps the database
/// awake anyway, and nothing is missed or read again. A worker whose runs
/// only wait (the state its door sleeps in) opens no line: the broker copy
/// a new line lands on may have to read who the worker is, which would wake
/// the database. Once its line is within [`RUN_CAP_MARGIN`] of the cut it
/// stops instead ([`Worker::stopped_for_cut`]), decided in one step with the
/// stop as the margin is reached, then again whenever that state may have
/// been entered (a run going quiet, a run leaving, the records written)
/// until the line goes. A line that closed for want of anything to do and
/// opened again is a new line, counted afresh. Nothing ends the process
/// from outside then: it winds down, and the platform starts a fresh worker
/// on the next request.
fn keep_ahead_of_line_cut(worker: &Worker) {
    let mut cut = worker.clients.line.cut_at();
    let worker = worker.clone();
    tokio::spawn(async move {
        'lines: loop {
            let Some(told) = *cut.borrow_and_update() else {
                if cut.changed().await.is_err() {
                    return;
                }
                continue;
            };
            let within_margin = told.opened + told.max_age.saturating_sub(RUN_CAP_MARGIN);
            tokio::select! {
                changed = cut.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue 'lines;
                }
                () = tokio::time::sleep_until(told.opened + told.max_age / 2) => {}
            }
            // A replacement that failed is tried again once what the worker
            // has under way changed, not in a loop.
            let mut failed = false;
            loop {
                // Armed before the look, so a change between the look and
                // the wait still wakes it.
                let left = worker.cancels.left.notified();
                tokio::pin!(left);
                left.as_mut().enable();
                if worker.stopping.is_cancelled() {
                    return;
                }
                if !failed && worker.works() {
                    match worker.clients.line.replace().await {
                        // The new line's cut comes on `cut`.
                        Ok(()) => continue 'lines,
                        Err(e) => {
                            tracing::warn!(target: "weft_engine::worker", error = %format!("{e:#}"), "the line to the broker could not be replaced; it is tried again once the work changes");
                            failed = true;
                        }
                    }
                }
                let margin_reached = tokio::time::Instant::now() >= within_margin;
                let looked = margin_reached.then(|| worker.stopped_for_cut(told));
                if looked == Some(CutLook::Stopped) {
                    tracing::info!(target: "weft_engine::worker", line_cut_secs = told.max_age.as_secs(), "every run only waits and weft cuts this worker's line soon, on which they hear their answers: the ones that can pause are handed back, the others end, and the next request starts a fresh worker");
                    overdue_after_grace(&worker).await;
                    return;
                }
                // `written` answers at once if the records went out since
                // the look.
                tokio::select! {
                    changed = cut.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue 'lines;
                    }
                    () = worker.under_way_changed.notified() => failed = false,
                    () = &mut left => failed = false,
                    () = tokio::time::sleep_until(within_margin), if !margin_reached => {}
                    () = worker.clients.writer.written(), if looked == Some(CutLook::RecordsPending) => {}
                }
            }
        }
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
            ExchangeJournal::new(start.journal.clone(), self.worker.replica.clone(), admitted.live_config.journal);
        let run_id = start.run_id;
        let worker = self.worker.clone();
        start.slot = Some(admitted.slot);
        let token = self.worker.background.token();
        Ok(crate::caller_conn::Born {
            run_id,
            sink,
            drive: Box::new(move |exchange| {
                start.exchange = Some(exchange);
                Box::pin(async move {
                    let _token = token;
                    if let Err(e) = worker.drive(start).await {
                        tracing::error!(target: "weft_engine::door", %run_id, error = %format!("{e:#}"), "a caller's run ended in error");
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
    /// The secrets it is born holding: its caller's credentials
    /// (`crate::door::Admitted::secrets`).
    secrets: Vec<weft_journal::frame::KeptSecret>,
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
        let Arriving { registered, payload, secrets, acknowledged } = arriving;
        let run_id = registered.run_id;
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
        let opening = startable.opening(&payload);
        let settings = entry.spec.settings;
        let journal = self.clients.writer.run(RunSpec {
            run_id,
            settings,
            keep_for: plan.keep_for,
            epoch: 1,
            next_seq: 0,
            secrets,
            program: startable.runs.kept().cloned(),
        });
        let birth = startable.birth(&plan, run_id, &payload, crate::now_unix());
        weft_journal::JournalClient::record_events(journal.as_ref(), &birth, Some(&self.replica))
            .await
            .map_err(|e| Turned::Internal(format!("the run's birth: {e:#}")))?;
        if acknowledged && settings.keeping().is_durable() {
            if let Err(e) = journal.flush().await {
                if journal.born_elsewhere() {
                    return Err(Turned::BornElsewhere);
                }
                tracing::error!(target: "weft_engine::door", %run_id, error = %format!("{e:#}"), "a durable run's birth could not be recorded");
                // The birth may have landed with its answer lost: ended,
                // so it never reads as running.
                self.clients.writer.give_up(run_id, &format!("its birth could not be recorded: {e:#}")).await;
                return Err(Turned::Unrecorded);
            }
        }
        let born_with = BornWith {
            phase: weft_core::context::Phase::Fire,
            instance: plan.instance.clone(),
            instance_values: startable.ready.instance_values.clone(),
            picks: startable.ready.picks.clone(),
            settings,
            given: startable.given.clone(),
        };
        Ok(RunStart {
            run_id,
            journal,
            program: startable.program.clone(),
            starts_from: StartsFrom::Plan { born_with, kicked: opening },
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
        let registered = self.cancels.claim(weft_core::new_run_id()).expect("a fresh run id is no other run's");
        let arriving = Arriving { registered, payload, secrets: admitted.secrets.clone(), acknowledged: false };
        self.admit(&admitted.trigger, &admitted.entry, admitted.instance.as_ref(), admitted.triggers_version, arriving).await.map_err(|turned| match turned {
            Turned::Unread(e) => {
                tracing::error!(target: "weft_engine::door", error = %format!("{e:#}"), "a run's facts could not be read");
                failed(StatusCode::SERVICE_UNAVAILABLE, "this run's settings could not be read; try again in a moment".into())
            }
            // The program cannot run this way (a run it would start needs
            // an instance the call did not name): a refusal of the run, the
            // same status the install's own run refusals answer with,
            // counted as dropped where `weft status` reads it, like a fire.
            Turned::Unready(Unready::Refused(why)) => {
                self.door.dropped(&admitted.trigger.token, &why);
                failed(StatusCode::UNPROCESSABLE_ENTITY, why)
            }
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
    /// [`Self::take_event`], and a fire it dropped counted at the door
    /// with its reason. A drop is acknowledged (nothing tries a refusal
    /// again), so the count is what `weft status` shows of it.
    async fn admit_event(
        &self,
        fire: weft_core::door_fire::DoorFire,
    ) -> Result<(weft_core::door_fire::Fired, Option<tokio::task::JoinHandle<RunAnswer>>), Response> {
        let token = fire.token.clone();
        let taken = self.take_event(fire).await;
        if let Ok((weft_core::door_fire::Fired::Dropped { reason }, _)) = &taken {
            self.door.dropped(&token, reason);
        }
        taken
    }

    /// A trigger's event: its front (the trigger as held, its holder, its
    /// program, its standing, its limits), then [`Self::admit`], and the
    /// drive of the run it started. A fire that cannot become a run now is
    /// put in its trigger's queue for later, which this answers rather
    /// than holding the fire. A refusal read off the held triggers is
    /// confirmed against the broker's answer now: a trigger armed or taken
    /// over a moment ago may not have been heard yet.
    async fn take_event(
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
        use weft_core::door_fire::WaitsOn;
        let park = |reason: String, on: WaitsOn| self.park_fire(&fire, reason, on);
        // Armed on another version of the program: the install's drain
        // hands it to the workers running that one.
        if entry.binary_hash != self.door.binary_hash {
            return park("its trigger is armed on another version of the program".into(), WaitsOn::Room).await.map(|fired| (fired, None));
        }
        match entry.standing.arrival(crate::now_unix() as i64) {
            weft_core::arrival::Arrival::Live => {}
            weft_core::arrival::Arrival::Wait => {
                return park(format!("its trigger is {}", entry.standing.status.as_str()), WaitsOn::Trigger).await.map(|fired| (fired, None));
            }
            weft_core::arrival::Arrival::Refused => {
                return Ok((Fired::Dropped { reason: format!("its trigger takes no work ({})", entry.standing.status.as_str()) }, None));
            }
        }
        let run_id = weft_core::door_fire::run_of_fire(fire.fire_id);
        // Taken now, in one step with the check: the same fire delivered
        // twice at once starts one run.
        let Some(registered) = self.cancels.claim(run_id) else {
            return Ok((Fired::AlreadyBorn, None));
        };
        // An event waits for a run permit like a caller does; one that
        // waited as long as a caller may waits in its trigger's queue.
        let Ok(permit) = self.door.take_permit().await else {
            return park("this copy of the program takes all the runs it may".into(), WaitsOn::Room).await.map(|fired| (fired, None));
        };
        let limits = match fire.released {
            true => entry.spec.limits.resolve().for_held(),
            false => entry.spec.limits.resolve(),
        };
        let slot = match self.door.admit_event(permit, &fire.token, fire.caller.as_deref(), &limits) {
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
                        park(reason, WaitsOn::Room).await.map(|fired| (fired, None))
                    }
                };
            }
        };
        let arriving = Arriving {
            registered,
            payload: fire.payload.clone(),
            secrets: Vec::new(),
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
                        park(weft_core::infra::run_gate::infra_not_running(&missing), WaitsOn::Infra).await.map(|fired| (fired, None))
                    }
                    Turned::Unready(Unready::Waits(RunGate::InstanceValues(refusal))) => {
                        park(refusal.to_string(), WaitsOn::Instance).await.map(|fired| (fired, None))
                    }
                    Turned::Unready(Unready::Waits(RunGate::Picks(refusal))) => park(refusal.to_string(), WaitsOn::Picks).await.map(|fired| (fired, None)),
                    Turned::Internal(why) => Err((StatusCode::INTERNAL_SERVER_ERROR, why).into_response()),
                    Turned::BornElsewhere => Ok((Fired::AlreadyBorn, None)),
                    Turned::Unrecorded => park("the run could not be recorded".into(), WaitsOn::Failure).await.map(|fired| (fired, None)),
                    Turned::Stopping => Err((StatusCode::SERVICE_UNAVAILABLE, "this worker is stopping").into_response()),
                };
            }
        };
        // The run goes on its own, counted toward its entry until it ends.
        Ok((Fired::Started { run_id }, Some(self.drive_born(born, Some(slot)))))
    }

    /// Put `fire` in its trigger's queue, waiting on `on` for `reason`:
    /// the install's drain hands it back once it can become a run, after
    /// a backoff or on what it waits on
    /// (`weft_task_store::parked_fires::Turn::next`).
    async fn park_fire(
        &self,
        fire: &weft_core::door_fire::DoorFire,
        reason: String,
        on: weft_core::door_fire::WaitsOn,
    ) -> Result<weft_core::door_fire::Fired, Response> {
        use weft_core::door_fire::Fired;
        let parked = weft_task_store::parked_fires::waiting(fire.fire_id, fire.payload.clone(), fire.caller.clone(), fire.attempts, on, &reason);
        let parked = self.door.broker().park_fire(&fire.token, &parked, fire.held_by.as_deref()).await.map_err(|e| {
            tracing::error!(target: "weft_engine::door", token = %fire.token, error = %format!("{e:#}"), "a fire could not be put in its trigger's queue");
            (StatusCode::SERVICE_UNAVAILABLE, "the fire could not be queued; try again in a moment").into_response()
        })?;
        Ok(match parked {
            weft_broker_client::protocol::DoorParked::Parked => Fired::Parked { reason, on },
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
    use crate::run_driver::engine_test_rig::{clients, MemJournal};

    /// Every platform limit ends a run through the same cancel, told the
    /// way out that fits it: a pause for a run nobody is talking to, a
    /// reconnect over kept state for one with a caller, a run that can
    /// pause for one that waited on the worker's line; each says how early
    /// it stopped: the margin for the cap, what was left for the line.
    #[test]
    fn a_limit_ends_a_run_naming_the_way_out_that_fits_it() {
        let hour = std::time::Duration::from_secs(3600);
        let margin = RUN_CAP_MARGIN.as_secs();
        for (limit, early, way_out) in [
            (Limit::RunCap { cap: hour, live: true }, margin, "reconnect"),
            (Limit::RunCap { cap: hour, live: false }, margin, "split long work with a pause"),
            (Limit::LineCut { cut: hour, left: std::time::Duration::from_secs(37) }, 37, "A run that can pause"),
        ] {
            let early = format!("{early} seconds before the 60 minute limit");
            let flag = CancellationFlag::new_arc();
            limit.end(&flag, RunId::nil());
            let detail = match flag.cause() {
                Some(weft_core::run::CancelCause::Limit { detail }) => detail,
                other => panic!("{limit:?} ended the run with {other:?}"),
            };
            assert!(detail.contains(&early) && detail.contains(way_out), "{detail}");
            Limit::RunCap { cap: hour, live: false }.end(&flag, RunId::nil());
            assert!(matches!(flag.cause(), Some(weft_core::run::CancelCause::Limit { detail: again }) if again == detail), "the first ending stands");
        }
    }

    /// A live node test reaching its request's cap is ended as a time
    /// limit that says it cannot pause, and where a longer job is tested.
    #[cfg(feature = "node-tests")]
    #[test]
    fn a_live_test_at_its_cap_ends_saying_why_it_cannot_wait_longer() {
        let flag = CancellationFlag::new_arc();
        Limit::NodeTestCap { cap: std::time::Duration::from_secs(3600) }.end(&flag, RunId::nil());
        let detail = match flag.cause() {
            Some(weft_core::run::CancelCause::Limit { detail }) => detail,
            other => panic!("ended with {other:?}"),
        };
        let early = format!("{} seconds before the 60 minute limit", RUN_CAP_MARGIN.as_secs());
        assert!(detail.contains(&early) && detail.contains("cannot pause") && detail.contains("part of a run"), "{detail}");
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


    /// The broker's side of the runs a worker drives: the cancels and the
    /// answers waiting for them (reading changes nothing, so every read
    /// finds them), every read recorded, and nothing to claim.
    #[derive(Default)]
    struct Runs {
        asked: std::sync::atomic::AtomicUsize,
        cancels: std::sync::Mutex<Vec<RunId>>,
        answers: std::sync::Mutex<Vec<weft_broker_client::protocol::RunAnswerOf>>,
    }

    #[async_trait::async_trait]
    impl crate::context::RunClient for Runs {
        async fn claim(&self, _run_id: RunId) -> anyhow::Result<Option<weft_journal::record::Claimed>> {
            Ok(None)
        }
        async fn let_go(&self, _run_id: RunId, _why: weft_broker_client::protocol::LetGo) -> anyhow::Result<()> {
            unreachable!("nothing is driven here")
        }
        async fn answers(&self, _run_id: RunId, _taken: &[String]) -> anyhow::Result<Vec<weft_broker_client::protocol::RunAnswer>> {
            unreachable!("nothing is driven here")
        }
        async fn rechecked(&self) -> anyhow::Result<weft_broker_client::protocol::RunRecheckedResponse> {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let cancels = self.cancels.lock().unwrap();
            Ok(weft_broker_client::protocol::RunRecheckedResponse {
                cancels: cancels
                    .iter()
                    .map(|run_id| weft_broker_client::protocol::RunCancel { run_id: *run_id, cause: weft_core::run::CancelCause::User })
                    .collect(),
                answers: self.answers.lock().unwrap().clone(),
            })
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
        worker_running(Arc::new(NoCatalog), clients)
    }

    /// A worker running `catalog`'s nodes over `clients`.
    fn worker_running(catalog: Arc<dyn NodeCatalog>, clients: EngineClients) -> Worker {
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
        new_worker(catalog, clients, &config, door)
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
            released: false,
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
        assert!(matches!(answer, weft_core::door_fire::Fired::Parked { on: weft_core::door_fire::WaitsOn::Trigger, .. }), "{answer:?}");
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

    /// A dropped fire is acknowledged, and counted at the door with its
    /// reason: the next tick states it, where `weft status` reads it.
    #[tokio::test]
    async fn a_dropped_fire_is_counted_with_its_reason_on_the_next_tick() {
        let (worker, broker) = worker_firing(|_, trigger| {
            entry(trigger).standing.status = weft_core::projects::ProjectStatus::Inactive;
            entry(trigger).standing.accepting_fires = false;
        });
        let weft_core::door_fire::Fired::Dropped { reason } = fired(&worker, a_fire("t1")).await else { panic!("dropped") };
        worker.door.tick(None, false).await.unwrap();
        let ticks = broker.ticks.lock().unwrap();
        let counts = &ticks.last().expect("a tick").counts;
        let dropped: Vec<_> = counts.iter().filter(|count| count.key == "dropped:t1").collect();
        assert_eq!(dropped.len(), 1, "{counts:?}");
        assert_eq!((dropped[0].hits, dropped[0].reason.as_deref()), (1, Some(reason.as_str())));
        assert!(counts.iter().filter(|count| count.key != "dropped:t1").all(|count| count.reason.is_none()), "{counts:?}");
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

    /// Work released from its trigger's queue takes half of the trigger's
    /// runs at once, and waits in its queue past that, while a new arrival
    /// still gets the other half.
    #[tokio::test]
    async fn released_work_takes_half_of_its_triggers_room() {
        let limits = weft_core::signal::EntryLimits { at_once: Some(4), ..Default::default() };
        let (worker, broker) = worker_firing(|_, trigger| entry(trigger).spec.limits = limits);
        let mut going = Vec::new();
        for _ in 0..2 {
            going.push(worker.door.admit_event(worker.door.take_permit().await.unwrap(), "t1", None, &limits.resolve().for_held()).unwrap());
        }
        let released = weft_core::door_fire::DoorFire { released: true, fire_id: uuid::Uuid::from_u128(8), ..a_fire("t1") };
        assert!(matches!(fired(&worker, released).await, weft_core::door_fire::Fired::Parked { .. }), "past half, released work waits");
        assert_eq!(broker.parked.lock().unwrap().len(), 1);
        assert!(
            worker.door.admit_event(worker.door.take_permit().await.unwrap(), "t1", None, &limits.resolve()).is_ok(),
            "a new arrival still has room"
        );
    }

    fn cancel_heard(run_id: RunId) -> weft_task_store::pg_signal::Heard {
        weft_task_store::pg_signal::Heard::Signal {
            channel: weft_task_store::runs::CANCEL_CHANNEL,
            payload: weft_task_store::runs::run_payload(uuid::Uuid::from_u128(1), run_id).into(),
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
    // one for a run driven elsewhere asks nothing; the broker hearing again
    // after a quiet spell, and a line lost, ask nothing; the line is held
    // open exactly while something is driven; and a line that opens again
    // (or may have missed some) asks once for the cancels of every run
    // driven.
    weft_core::stress_test! {
        name: a_heard_cancel_fires_the_flag_of_the_run_driven,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let runs = Arc::new(Runs::default());
            let line = crate::context::TestLine::new();
            let worker = worker_over(runs.clone(), line.clone());
            spawn_line_watch(worker.line_watch());

            let driven = weft_core::new_run_id();
            let registered = worker.cancels.claim(driven).expect("a fresh run");
            let flag = registered.flag.clone();
            until("driving, the line is held open", || line.held.load(std::sync::atomic::Ordering::SeqCst) > 0).await;
            line.pushed.send(cancel_heard(weft_core::new_run_id())).unwrap();
            runs.cancels.lock().unwrap().push(driven);
            line.pushed.send(cancel_heard(driven)).unwrap();
            cancelled(&flag).await;
            assert_eq!(runs.asked.load(std::sync::atomic::Ordering::SeqCst), 1, "a cancel of a run driven elsewhere asks nothing");
            line.pushed.send(weft_task_store::pg_signal::Heard::Resumed).unwrap();
            line.pushed.send(weft_task_store::pg_signal::Heard::Lost).unwrap();

            let missed = weft_core::new_run_id();
            let _missed = worker.cancels.claim(missed).expect("a fresh run");
            let flag = _missed.flag.clone();
            runs.cancels.lock().unwrap().push(missed);
            line.pushed.send(weft_task_store::pg_signal::Heard::Recheck).unwrap();
            cancelled(&flag).await;
            assert_eq!(runs.asked.load(std::sync::atomic::Ordering::SeqCst), 2, "only the recheck asked again");

            drop(registered);
            drop(_missed);
            until("with nothing driven, the line is let go of", || line.held.load(std::sync::atomic::Ordering::SeqCst) == 0).await;
        }
    }

    fn an_answer(run_id: RunId, token: &str) -> weft_broker_client::protocol::RunAnswerOf {
        weft_broker_client::protocol::RunAnswerOf {
            run_id,
            token: token.into(),
            answer: weft_core::primitive::WaitAnswer::Given { value: serde_json::json!(token) },
        }
    }

    // A line that may have missed some, under three waiting runs, makes one
    // read for all of them: the answers go to the drives in one telling,
    // each taking its own. An answer handed to a run driven here is passed
    // on to its drive, which reads its own; one for a run driven elsewhere
    // is not.
    weft_core::stress_test! {
        name: a_recheck_with_three_waiting_runs_reads_once_for_all,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            use crate::context::RunTold;
            let runs = Arc::new(Runs::default());
            let line = crate::context::TestLine::new();
            let worker = worker_over(runs.clone(), line.clone());
            let mut told = worker.clients.told.subscribe();
            spawn_line_watch(worker.line_watch());
            let waiting: Vec<Registered> = (0..3).map(|_| worker.cancels.claim(weft_core::new_run_id()).expect("a fresh run")).collect();
            let ids: Vec<RunId> = waiting.iter().map(|run| run.run_id).collect();
            *runs.answers.lock().unwrap() = vec![an_answer(ids[0], "a"), an_answer(ids[2], "c")];
            until("driving, the line is held open", || line.held.load(std::sync::atomic::Ordering::SeqCst) > 0).await;

            line.pushed.send(weft_task_store::pg_signal::Heard::Recheck).unwrap();
            let RunTold::Rechecked(answers) = told.recv().await.unwrap() else { panic!("the answers read once") };
            assert_eq!(answers.iter().map(|answer| (answer.run_id, answer.token.as_str())).collect::<Vec<_>>(), vec![(ids[0], "a"), (ids[2], "c")]);
            assert_eq!(runs.asked.load(std::sync::atomic::Ordering::SeqCst), 1, "one read for the three runs");

            let handed = |run| weft_task_store::pg_signal::Heard::Signal {
                channel: weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
                payload: weft_task_store::runs::run_payload(uuid::Uuid::from_u128(1), run).into(),
            };
            line.pushed.send(handed(weft_core::new_run_id())).unwrap();
            line.pushed.send(handed(ids[1])).unwrap();
            assert!(matches!(told.recv().await.unwrap(), RunTold::Answered(run) if run == ids[1]), "only the run driven here");
            assert_eq!(runs.asked.load(std::sync::atomic::Ordering::SeqCst), 1, "an answer handed reads nothing here");
            drop(waiting);
        }
    }

    /// The race a sleeping door must never lose: the door sleeps because
    /// its only run waits outside, having vouched for the worker; a caller
    /// is admitted and its run registered while it sleeps. The registration
    /// itself wakes the tick, which then keeps the lease for the new run.
    #[tokio::test(start_paused = true)]
    async fn a_run_registered_while_the_door_sleeps_on_waiting_runs_wakes_it() {
        let (worker, broker) = worker_firing(|_, _| {});
        let (cancels, holds) = (worker.cancels.clone(), worker.holds.clone());
        worker.door.start_ticks(&tokio::runtime::Handle::current(), Box::new(move || crate::door::UnderWay::of(true, cancels.count(), holds.quiet(), 15)));
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(Some(crate::run_holds::Quiet { until: 5_000, can_pause: false }));
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        let asleep = broker.ticks.lock().unwrap().clone();
        assert_eq!(asleep.last().unwrap().vouch_until, Some(5_015), "vouched until the wait ends, and a lease past it");

        let _caller = worker.cancels.claim(uuid::Uuid::from_u128(2)).expect("a fresh run");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let woken = broker.ticks.lock().unwrap().clone();
        assert_eq!(woken.len(), asleep.len() + 1, "the new run woke the tick at once");
        assert_eq!(woken.last().unwrap().vouch_until, None, "a run that moves: one lease length at a time again");
        assert!(!woken.last().unwrap().shortens, "which rings nothing: only a stop does");
        drop((waiting, quiet));
    }

    /// A worker told to stop while its only run waits vouches for nothing
    /// from then on: its door wakes at once, says the lease got shorter,
    /// and ticks every second for as long as the process lives, so the
    /// lease lapses a lease length after it goes.
    #[tokio::test(start_paused = true)]
    async fn a_stopping_worker_stops_vouching_and_ticks_while_it_lives() {
        let (worker, broker) = worker_firing(|_, _| {});
        let under_way = worker.clone();
        worker.door.start_ticks(&tokio::runtime::Handle::current(), Box::new(move || under_way.under_way(15)));
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(Some(crate::run_holds::Quiet { until: 5_000, can_pause: false }));
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        let asleep = broker.ticks.lock().unwrap().clone();
        assert_eq!(asleep.last().unwrap().vouch_until, Some(5_015), "asleep, vouched for");

        worker.stopping.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(3_500)).await;
        let stopping = broker.ticks.lock().unwrap()[asleep.len()..].to_vec();
        assert!(stopping.len() >= 3, "a tick at once, then one a second: {}", stopping.len());
        assert!(stopping.iter().all(|tick| tick.vouch_until.is_none()), "vouching for nothing");
        assert_eq!(stopping.iter().filter(|tick| tick.shortens).count(), 1, "the first one says the lease got shorter");
        assert!(stopping[0].shortens);
        drop((waiting, quiet));
    }

    const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);

    /// A worker watching its line's cut, the line opened now and cut after
    /// an hour.
    fn watching_its_line() -> (Worker, Arc<crate::context::TestLine>) {
        let line = crate::context::TestLine::new();
        let worker = worker_over(Arc::new(Runs::default()), line.clone());
        keep_ahead_of_line_cut(&worker);
        line.cut.send_replace(Some(weft_broker_client::line::Cut { opened: tokio::time::Instant::now(), max_age: HOUR }));
        (worker, line)
    }

    fn replaced(line: &crate::context::TestLine) -> usize {
        line.replaced.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A run holding its waits in the worker until its hold, far off.
    fn holding() -> Option<crate::run_holds::Quiet> {
        Some(crate::run_holds::Quiet { until: i64::MAX, can_pause: false })
    }

    /// Whether `flag` was ended at the line's cut, `left` before it.
    fn ended_at_the_cut(flag: &CancellationFlag, left: &str) -> bool {
        matches!(
            flag.cause(),
            Some(weft_core::run::CancelCause::Limit { detail })
                if detail.contains("the line between its worker and weft") && detail.contains(&format!("stopped {left} before"))
        )
    }

    /// Let the watch on the line's cut act on what just changed, no time
    /// passing.
    async fn settled() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    /// A worker whose only run waits stops as its line comes within the
    /// margin of its cut, not before, and that run ends naming the time
    /// that was left; the hand-back is overdue after the grace. A line
    /// closed and opened again counts afresh.
    #[tokio::test(start_paused = true)]
    async fn a_worker_whose_runs_only_wait_stops_before_its_line_is_cut() {
        let (worker, line) = watching_its_line();
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(holding());
        tokio::time::sleep(HOUR / 2).await;
        // Closed while idle, opened again: a new line, counted from now.
        line.cut.send_replace(None);
        line.cut.send_replace(Some(weft_broker_client::line::Cut { opened: tokio::time::Instant::now(), max_age: HOUR }));
        tokio::time::sleep(HOUR - RUN_CAP_MARGIN - std::time::Duration::from_secs(1)).await;
        assert!(!worker.stopping.is_cancelled(), "the new line still has time");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(worker.stopping.is_cancelled(), "stopping within the margin of the cut");
        let left = format!("{} seconds", RUN_CAP_MARGIN.as_secs());
        assert!(ended_at_the_cut(&waiting.flag, &left), "the waiting run ends naming the time left: {:?}", waiting.flag.cause());
        tokio::time::sleep(HAND_BACK_GRACE).await;
        assert!(worker.stopping_overdue.is_cancelled(), "nodes still running are stopped after the grace");
        drop((waiting, quiet));
    }

    /// A busy worker replaces its line once the line is half its age, so a
    /// run that holds its wait, arriving on a line 58 minutes old, is not
    /// ended at that line's cut: the line it waits on is the new one, and
    /// the worker stops for that one's cut only once every run waits.
    #[tokio::test(start_paused = true)]
    async fn a_busy_worker_replaces_its_line_and_a_late_held_run_lives_on() {
        let (worker, line) = watching_its_line();
        let moving = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        tokio::time::sleep(HOUR / 2 - std::time::Duration::from_secs(1)).await;
        assert_eq!(replaced(&line), 0, "a line under half its age is kept");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(replaced(&line), 1, "replaced at half its age while the worker works");
        tokio::time::sleep(HOUR / 2 - std::time::Duration::from_secs(120)).await;
        let held = worker.cancels.claim(uuid::Uuid::from_u128(2)).expect("a fresh run");
        let held_quiet = worker.holds.of(uuid::Uuid::from_u128(2), None);
        held_quiet.quiet(holding());
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
        assert!(!worker.stopping.is_cancelled(), "past the first line's cut, nothing stops");
        assert!(!held.flag.is_cancelled(), "the held run lives on");
        assert_eq!(replaced(&line), 2, "the new line is replaced at its own half, as the worker still works");
        let moving_quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        moving_quiet.quiet(holding());
        settled().await;
        assert!(!worker.stopping.is_cancelled(), "every run waits, but the new line is far from its cut");
        drop((moving, moving_quiet, held, held_quiet));
    }

    /// A run that moves again (its answer came) as the margin arrives has
    /// the line replaced rather than the worker stopped; it waits again
    /// on the new line, far from its cut.
    #[tokio::test(start_paused = true)]
    async fn a_run_that_moves_as_the_margin_arrives_has_the_line_replaced() {
        let (worker, line) = watching_its_line();
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(holding());
        tokio::time::sleep(HOUR - RUN_CAP_MARGIN - std::time::Duration::from_millis(1)).await;
        assert_eq!(replaced(&line), 0, "a worker whose runs only wait opens no line");
        // Answered as the margin arrives.
        quiet.quiet(None);
        settled().await;
        assert_eq!(replaced(&line), 1, "it works: the line is replaced");
        quiet.quiet(holding());
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        assert!(!worker.stopping.is_cancelled(), "a new line: no stop");
        assert!(!waiting.flag.is_cancelled());
        drop((waiting, quiet));
    }

    /// A line that could not be replaced is cut as before: a run that moves
    /// keeps the worker going into the margin, and once it leaves and the
    /// others only wait, the worker stops then.
    #[tokio::test(start_paused = true)]
    async fn a_worker_whose_line_would_not_be_replaced_stops_once_its_last_moving_run_leaves() {
        let (worker, line) = watching_its_line();
        line.refuses.store(true, std::sync::atomic::Ordering::SeqCst);
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(holding());
        let moving = worker.cancels.claim(uuid::Uuid::from_u128(2)).expect("a fresh run");
        tokio::time::sleep(HOUR - std::time::Duration::from_secs(30)).await;
        assert!(!worker.stopping.is_cancelled(), "a run moves: no stop");
        drop(moving);
        settled().await;
        assert!(worker.stopping.is_cancelled(), "the last moving run left: it stops then");
        assert!(ended_at_the_cut(&waiting.flag, "30 seconds"), "{:?}", waiting.flag.cause());
        drop((waiting, quiet));
    }

    /// In one stop, a waiting run that can pause is left to the stopping
    /// path, which hands it back, and one that holds its waits in the
    /// worker ends at the cut.
    #[tokio::test(start_paused = true)]
    async fn a_stop_hands_back_what_can_pause_and_ends_what_cannot() {
        let (worker, _line) = watching_its_line();
        let pausable = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let pausable_quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        pausable_quiet.quiet(Some(crate::run_holds::Quiet { until: i64::MAX, can_pause: true }));
        let held = worker.cancels.claim(uuid::Uuid::from_u128(2)).expect("a fresh run");
        let held_quiet = worker.holds.of(uuid::Uuid::from_u128(2), None);
        held_quiet.quiet(holding());
        let asked = worker.stopping.child_token();
        tokio::time::sleep(HOUR - RUN_CAP_MARGIN + std::time::Duration::from_secs(1)).await;
        assert!(asked.is_cancelled(), "asked to leave: the run that can pause is handed back");
        assert!(!pausable.flag.is_cancelled(), "a run that can pause is never ended at the cut");
        let left = format!("{} seconds", RUN_CAP_MARGIN.as_secs());
        assert!(ended_at_the_cut(&held.flag, &left), "{:?}", held.flag.cause());
        drop((pausable, pausable_quiet, held, held_quiet));
    }

    /// A record that writes only when let through.
    struct GatedRecord {
        record: Arc<MemJournal>,
        through: tokio::sync::Semaphore,
    }

    #[async_trait::async_trait]
    impl weft_journal::RecordClient for GatedRecord {
        async fn record_batch(&self, batch: Vec<u8>) -> Result<weft_journal::frame::BatchAnswer, weft_journal::BatchError> {
            self.through.acquire().await.expect("the gate stays").forget();
            self.record.record_batch(batch).await
        }
        async fn record_of(&self, run_id: RunId, after: Option<i32>) -> anyhow::Result<weft_journal::record::RawRecord> {
            self.record.record_of(run_id, after).await
        }
        async fn secret(&self, id: weft_core::secret::SecretId) -> anyhow::Result<Option<serde_json::Value>> {
            self.record.secret(id).await
        }
        async fn give_up(&self, run_id: RunId, why: String) -> Result<(), weft_journal::BatchError> {
            self.record.give_up(run_id, why).await
        }
    }

    /// Every run only waits but its records are still on their way at the
    /// margin: no stop yet; once they are written, the worker stops then,
    /// however soon after the look they landed.
    #[tokio::test(start_paused = true)]
    async fn a_worker_stops_once_the_records_of_its_waiting_runs_are_written() {
        let gate = Arc::new(GatedRecord { record: Arc::new(MemJournal::default()), through: tokio::sync::Semaphore::new(0) });
        let mut clients = clients(Arc::new(MemJournal::default()));
        clients.writer = crate::journal_writer::WorkerJournal::start(gate.clone(), Default::default(), &clients.io);
        let line = crate::context::TestLine::new();
        clients.line = line.clone();
        let worker = worker_with(clients);
        // Records on their way are work: only a line that would not be
        // replaced leaves the worker to stop at its cut.
        line.refuses.store(true, std::sync::atomic::Ordering::SeqCst);
        keep_ahead_of_line_cut(&worker);
        line.cut.send_replace(Some(weft_broker_client::line::Cut { opened: tokio::time::Instant::now(), max_age: HOUR }));

        let run_id = uuid::Uuid::from_u128(1);
        let waiting = worker.cancels.claim(run_id).expect("a fresh run");
        let quiet = worker.holds.of(run_id, None);
        quiet.quiet(holding());
        let journal = worker.clients.writer.run(crate::journal_writer::RunSpec {
            run_id,
            settings: Default::default(),
            keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
            epoch: 1,
            next_seq: 0,
            secrets: Vec::new(),
            program: None,
        });
        journal.hand(&RunEvent::RunFailed { run_id, error: "on its way".into(), at_unix: 0 }, None).unwrap();
        tokio::time::sleep(HOUR - RUN_CAP_MARGIN + std::time::Duration::from_secs(1)).await;
        assert!(!worker.clients.writer.settled(), "the record is held at the gate");
        assert!(!worker.stopping.is_cancelled(), "records on their way: no stop yet");

        gate.through.add_permits(1_000);
        for _ in 0..500 {
            if worker.stopping.is_cancelled() {
                break;
            }
            // The writer runs on its own runtime, in real time.
            tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(10))).await.unwrap();
        }
        assert!(worker.stopping.is_cancelled(), "the records went out: it stops then");
        assert!(waiting.flag.is_cancelled());
        drop((waiting, quiet, journal));
    }

    /// A stopping worker lets go of what its runs shared once the last one
    /// ended, and not before: a pool's connections are given back to the
    /// database then, rather than when the process goes.
    #[tokio::test]
    async fn winding_down_lets_go_of_what_the_runs_shared_once_they_ended() {
        let (worker, _broker) = worker_firing(|_, _| {});
        drop(worker.clients.shared.named::<usize, _, _>("pool", || async { Ok(1) }).await.unwrap());
        let run = worker.background.token();
        let winding = tokio::spawn({
            let worker = worker.clone();
            async move { wind_down(&worker).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(worker.clients.shared.len(), 1, "a run still going keeps it");
        drop(run);
        tokio::time::timeout(std::time::Duration::from_secs(5), winding).await.expect("wound down").unwrap();
        assert!(worker.clients.shared.is_empty(), "let go once the runs ended");
    }

    /// A line the broker keeps for good never stops the worker.
    #[tokio::test(start_paused = true)]
    async fn a_line_kept_for_good_never_stops_the_worker() {
        let (worker, line) = watching_its_line();
        line.cut.send_replace(None);
        let waiting = worker.cancels.claim(uuid::Uuid::from_u128(1)).expect("a fresh run");
        let quiet = worker.holds.of(uuid::Uuid::from_u128(1), None);
        quiet.quiet(holding());
        tokio::time::sleep(HOUR * 24).await;
        assert!(!worker.stopping.is_cancelled());
        drop((waiting, quiet));
    }

    #[tokio::test]
    async fn a_call_for_a_run_that_is_not_queued_says_so() {
        let worker = worker_over(Arc::new(Runs::default()), crate::context::TestLine::new());
        let run_id = weft_core::new_run_id();
        assert_eq!(worker.carry_on_run(run_id).await.unwrap(), Some(RunAnswer::NothingToRun));
        assert!(!worker.cancels.contains(run_id), "nothing claimed, nothing registered");
    }

    /// The metadata of a node written here, which has no metadata.json.
    fn bare_metadata(node_type: &str) -> weft_core::node::NodeMetadata {
        serde_json::from_value(serde_json::json!({ "type": node_type, "label": node_type, "description": "" })).expect("metadata")
    }

    /// Panics as it runs.
    struct Boom;
    impl weft_core::NodeManifest for Boom {
        fn manifest(&self) -> &'static weft_core::node::NodeMetadata {
            static M: std::sync::OnceLock<weft_core::node::NodeMetadata> = std::sync::OnceLock::new();
            M.get_or_init(|| bare_metadata("Boom"))
        }
    }
    #[async_trait::async_trait]
    impl weft_core::Node for Boom {
        async fn run(&self, _ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            panic!("the node broke");
        }
    }

    /// Sends `value` on, writing down that it ran under the name it was
    /// given.
    struct Sends {
        name: &'static str,
        ran: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }
    impl weft_core::NodeManifest for Sends {
        fn manifest(&self) -> &'static weft_core::node::NodeMetadata {
            static M: std::sync::OnceLock<weft_core::node::NodeMetadata> = std::sync::OnceLock::new();
            M.get_or_init(|| bare_metadata("Sends"))
        }
    }
    #[async_trait::async_trait]
    impl weft_core::Node for Sends {
        async fn run(&self, ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            self.ran.lock().unwrap().push(self.name);
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("value", serde_json::json!("ok"))).await
        }
    }

    /// `first -> after`, `first` of `first_type`.
    fn two_steps(first_type: &str) -> weft_core::ProjectDefinition {
        let node = |id: &str, node_type: &str, input: bool| serde_json::json!({
            "id": id, "nodeType": node_type, "position": { "x": 0, "y": 0 },
            "inputs": if input { serde_json::json!([{ "name": "value", "portType": "String", "required": true }]) } else { serde_json::json!([]) },
            "outputs": [{ "name": "value", "portType": "String", "required": false }]
        });
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node("first", first_type, false), node("after", "After", true)],
            "edges": [{ "id": "e", "source": "first", "sourceHandle": "value", "target": "after", "targetHandle": "value" }]
        }))
        .expect("two steps")
    }

    /// Drive one run of `project` on `worker`, born now with `settings`
    /// and holding `slot`, the way the worker drives a run its door bore.
    /// Answers its id once its record is written.
    async fn drive_here(
        worker: &Worker,
        project: weft_core::ProjectDefinition,
        settings: weft_core::run_settings::RunSettings,
        slot: Option<crate::door::Slot>,
    ) -> RunId {
        use crate::run_driver::engine_test_rig::{claimed, handle};
        let run_id = weft_core::new_run_id();
        let birth = vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id, project_id: project.id, entry_node: "first".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
                cut_from: None, binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                seed: None, instance: None, fired_trigger: None, stand_in: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings,
            },
            RunEvent::NodeKicked { run_id, node_id: "first".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ];
        // Born the way the worker's door bears a run: its birth handed
        // under this worker's own name.
        let journal = handle(&worker.clients, run_id, &birth, 0, Vec::new());
        weft_journal::JournalClient::record_events(journal.as_ref(), &birth, Some(&worker.replica)).await.expect("a birth is handed");
        let project = Arc::new(project);
        let read = claimed(&worker.clients, run_id, &project, &birth).await;
        let registered = worker.cancels.claim(run_id).expect("a fresh run");
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            worker.drive(RunStart {
                run_id,
                journal,
                program: Arc::new(ProgramTables::new(project)),
                starts_from: StartsFrom::Record(Box::new(read)),
                exchange: None,
                registered,
                slot,
            }),
        )
        .await
        .expect("the drive ends")
        .expect("the drive runs");
        worker.clients.writer.written().await;
        run_id
    }

    /// A node that panics fails its run and leaves nothing of the run on
    /// its worker: no cancel registered, no hold, no record waiting to
    /// be written, so the worker has nothing under way and may retire.
    /// The same worker then takes the next run and carries it through.
    #[tokio::test]
    async fn a_panicking_node_fails_its_run_and_leaves_the_worker_free() {
        let ran = Arc::new(std::sync::Mutex::new(Vec::new()));
        let catalog = crate::run_driver::engine_test_rig::catalog(vec![
            ("Boom", Box::new(Boom)),
            ("Sends", Box::new(Sends { name: "first", ran: ran.clone() })),
            ("After", Box::new(Sends { name: "after", ran: ran.clone() })),
        ]);
        let journal = Arc::new(MemJournal::default());
        let worker = worker_running(catalog, clients(journal.clone()));

        let broke = drive_here(&worker, two_steps("Boom"), Default::default(), None).await;
        let record = journal.events_of(broke);
        assert!(
            record.iter().any(|e| matches!(e, RunEvent::RunFailed { error, .. } if error.contains("panicked"))),
            "the run fails on the panic: {record:?}"
        );
        assert!(ran.lock().unwrap().is_empty(), "nothing behind the panicking node ran: {:?}", ran.lock().unwrap());
        assert_eq!(worker.cancels.count(), 0, "the panicking run's cancel is gone");
        assert!(worker.holds.held_still().is_empty(), "nothing of it is held");
        assert_eq!(worker.under_way(60), crate::door::UnderWay::Nothing, "the worker has nothing under way, so it may retire");

        let fine = drive_here(&worker, two_steps("Sends"), Default::default(), None).await;
        assert!(journal.events_of(fine).iter().any(|e| matches!(e, RunEvent::RunCompleted { .. })), "{:?}", journal.events_of(fine));
        assert_eq!(*ran.lock().unwrap(), vec!["first", "after"], "the same worker carried the next run through");
        assert_eq!(worker.under_way(60), crate::door::UnderWay::Nothing);
    }

    /// A run its entry lets one of at a time holds the entry's room while
    /// it runs and gives it back as it ends, an unrecorded run included:
    /// a caller arriving meanwhile is turned away, one arriving after it
    /// ended is let in.
    #[tokio::test]
    async fn an_unrecorded_run_gives_its_entrys_room_back_as_it_ends() {
        let mut one_at_a_time = crate::door::fake::route("t1", "route", "status");
        crate::door::fake::armed(&mut one_at_a_time).spec.limits = weft_core::signal::EntryLimits { at_once: Some(1), ..Default::default() };
        let mut clients = clients(Arc::new(MemJournal::default()));
        clients.door_broker = crate::door::fake::FakeDoorBroker::new(vec![one_at_a_time]);
        let ran = Arc::new(std::sync::Mutex::new(Vec::new()));
        let catalog = crate::run_driver::engine_test_rig::catalog(vec![
            ("Sends", Box::new(Sends { name: "first", ran: ran.clone() })),
            ("After", Box::new(Sends { name: "after", ran })),
        ]);
        let worker = worker_running(catalog, clients);
        let headers = {
            let mut headers = HeaderMap::new();
            headers.insert("host", "127.0.0.1:9000".parse().unwrap());
            headers
        };
        let call = || crate::door::Call {
            method: "GET", raw_path: "/status", raw_query: "", headers: &headers,
            peer: "10.0.0.1".parse().unwrap(), relayed: false, route_prefix: "",
        };
        let crate::door::Arrived::Run(admitted, _) = worker.door.arrive(call(), axum::body::Body::empty()).await else { panic!("the first caller is let in") };
        assert!(
            matches!(worker.door.arrive(call(), axum::body::Body::empty()).await, crate::door::Arrived::Answer(_)),
            "while its run goes, the entry has no room"
        );

        let unrecorded = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, false).unwrap();
        drive_here(&worker, two_steps("Sends"), unrecorded, Some(admitted.slot)).await;
        assert!(
            matches!(worker.door.arrive(call(), axum::body::Body::empty()).await, crate::door::Arrived::Run(..)),
            "the run ended, so its room is free again"
        );
    }
}
