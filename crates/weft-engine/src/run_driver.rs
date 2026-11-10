//! Drives one run from boot to a terminal
//! outcome: completion, failure, stall, or stuck. The pulse loop
//! lives here.
//!
//! Shape:
//! - Boot: fold what the run starts from (its record as the worker's
//!   claim read it, or the birth this worker just handed its record) to
//!   recover pulses, firings, kicked roots, and pending deliveries.
//! - Dispatch: ready nodes run as bodies (`Bodies`: the first on the
//!   drive's own task, the ones beside it spawned). Each reports back
//!   through an mpsc channel; the main loop applies results to
//!   `pulses` and `firings` (single-writer invariant).
//! - Pause: a node waiting (`state.wait`) holds the wait in its call
//!   (`crate::held_waits`), and the loop ends it: with its answer, which
//!   carries the body on where it is, or by pausing it. A run pauses in
//!   one way only: every node still running waits (a held wait, a bus
//!   read, a stream pull, a yield), something outside the run moves it
//!   next, and the run stayed that quiet for its `pauseAfterSecs` (at
//!   once for a hand-back, [`pause_decision`]); then the loop tells them
//!   all to pause. Each saves its state and returns a suspension, a held
//!   wait with its token (the loop marks its firing `WaitingForInput` on
//!   it) and any other wait with none. Once nothing runs and at least
//!   one firing is waiting, the loop returns `Stalled`. A worker that
//!   picks the run up opens its buses again, and runs every paused
//!   firing again from the top with what it saved ([`Carried`]): the
//!   ones paused inside the run, and the ones whose wait was answered.
//!   A run that cannot pause right now (a caller on the line,
//!   `recorded: false`) never pauses: its held wait ends with its answer,
//!   given up once nothing moved for the run's `holdSecs`, or paused once
//!   the run can pause again and the same rule pauses it.
//! - Answers: only the worker driving a run writes its record, so an
//!   answer to a wait of a run being driven waits at the broker for its
//!   worker to take it (`RunClient::answers`), which records it as the
//!   run's own `SuspensionResolved`. A run that holds a wait, or keeps
//!   running while a firing an earlier drive paused waits, asks for them
//!   (an answer to that firing resumes it in place); any other stalled
//!   run lets go, and an answer that came meanwhile is handed to it as it
//!   does.
//! - Ending: whatever the run ends with, its buses' last rows, its spend
//!   and its caller's last words go on its record first, then its ending
//!   (`run_one_observed`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use weft_core::run::forget::{Forgetting, Places, Tables};
use weft_core::run::scope::{scope_permission, settle_table, tear_down_loop, ScopePermission, ScopeTable};
use weft_core::run::emission::{body_emission, terminal_sweep_emission};
use weft_core::run::loop_runtime::{
    self as loops, classify_loop_out, close_loop_outward, instantiate, LoopAdvance, LoopRuntime,
    LoopStreamItem,
};
use weft_core::run::ready::owned_bag;
use weft_core::run::{
    check_completion, firing_at, firing_at_mut, set_firing_status,
    postprocess::{close_unmentioned_downstream, postprocess_output, OutputBag},
    NodeFiring, NodeFiringStatus, NodeFiringTable,
};
use weft_core::generator::{StreamEnd, DEFAULT_MAX_BUFFERED_ITEMS};
use weft_core::liveness::FiringLocation;
use weft_core::node::NodeOutput;
use weft_core::primitive::{KickedNode, RecordedWait, RunSnapshot};
use weft_core::run::saved::SavedState;
use weft_core::project::ProgramIndex;
use weft_core::pulse::{PulseStatus, PulseTable};
use weft_core::cancellation::CancellationFlag;
use weft_core::{RunId, RunContext, NodeCatalog, ProjectDefinition};

use weft_journal::{RunEvent, JournalClient};

use crate::context::{
    ship_node_completed, ship_node_failed, ship_node_lifecycle, ship_node_skipped,
    ship_node_suspended, ship_port_closed, ship_port_emissions, EngineClients, NodeTaskOutcome,
    RunRecord, RunnerHandle, TaskMsg,
};
use crate::held_waits::unsuspendable;
use crate::journal_writer::{DriveJournal, Leaving};
use crate::now_unix;
use crate::stream_runtime::{AbsorbKind, StreamRuntime};
use crate::wait_tracker::DeliveryGate;

/// Asking a run to let go of its worker (`RunDrive::hand_back`), so the
/// next worker carries it on from its record.
#[derive(Debug, Clone, Default)]
pub struct HandBack {
    /// Start no node from now on, and hand the run back once none is
    /// running.
    pub asked: tokio_util::sync::CancellationToken,
    /// Stop the nodes still running where they are and hand the run back
    /// now: the next worker runs each of them again from its saved state,
    /// or fails it as cut short when it keeps none.
    pub overdue: tokio_util::sync::CancellationToken,
}

/// Outcome the loop reports back to the binary wrapper.
#[derive(Debug, Clone)]
pub enum RunOutcome {
    /// Every firing terminal, nothing in flight, no failure. The run's
    /// outputs are its nodes' emissions in the journal.
    Completed,
    /// A node failed and nothing caught it. `held_too_long`: the node that
    /// failed the run failed on a wait it gave up
    /// (`WeftError::WaitGaveUp`, `crate::held_waits`), so a time limit
    /// ended it, which its caller hears as one
    /// (`crate::caller_conn::Ending::TimeLimit`).
    Failed { error: String, held_too_long: bool },
    /// The run was cancelled (the cancellation flag tripped).
    /// A distinct variant rather than `Failed { error: "cancelled" }`:
    /// the "this exact string means cancelled" contract was invisible
    /// to the type system and decoded by string equality at the
    /// terminal-match and journal sites, so a reworded sentinel at one
    /// producer would silently flip a cancel into a generic failure.
    /// `cause` is what the canceller recorded on the flag, read at the
    /// moment the driver gives up (`recorded_cancel_cause`), so every
    /// terminal row written for the run names the same thing.
    Cancelled { cause: weft_core::run::CancelCause },
    /// Worker stalled: at least one firing is waiting for a signal.
    /// The worker lets go of the run, and the answer that resumes it has
    /// the install queue it for a worker to carry on.
    Stalled,
    /// The run was asked to let go of this worker ([`HandBack`]: the
    /// worker is stopping, or the run's caller left on a platform that
    /// stops a worker no call holds open): it started no node since, and
    /// every node that was running ended or, once the hand-back was
    /// overdue, was stopped where it was. Its record holds all of it, so
    /// the next worker carries it on from there, running a node that was
    /// stopped again from its saved state, or failing it as cut short
    /// when it keeps none. No ending is written; the worker asks for its
    /// resume.
    HandedBack,
    /// Scheduler ran to quiescence but pulses remain pending and
    /// nothing is waiting. Treat as a graph-shape bug. The report
    /// names every firing left holding pulses and the wired ports it
    /// never received, which is what the terminal row prints.
    Stuck { report: weft_core::run::StuckReport },
}

impl RunOutcome {
    /// The run pauses: it writes no ending, and whoever picks it up again
    /// rebuilds it from its record.
    fn pauses(&self) -> bool {
        matches!(self, Self::Stalled | Self::HandedBack)
    }
}

/// The exchange of a run that serves a caller: the real one on its socket,
/// or the stand-in a fired run serves. It ends before the run's own
/// ending, so its last words are on the run's record before it.
pub(crate) struct Exchange {
    /// What the run's nodes reach the caller through (`ctx.caller()`).
    pub(crate) conn: Arc<dyn weft_core::caller::CallerConnection>,
    /// The real socket, which the run's end answers and hangs up. `None`
    /// for a fired run's stand-in, whose exchange is over once the program
    /// answers.
    pub(crate) live: Option<Arc<crate::caller_conn::LiveCallerConnection>>,
    /// Where the exchange is recorded: the run's record.
    pub(crate) sink: Arc<dyn crate::caller_conn::CallerJournalSink>,
}

/// How a run runs, as its birth says: what it is for, whose it is, what
/// its instance provides and the install picked, how it is kept.
#[derive(Debug, Clone)]
pub(crate) struct BornWith {
    pub(crate) phase: weft_core::context::Phase,
    pub(crate) instance: Option<weft_core::instance::InstanceId>,
    pub(crate) instance_values: weft_core::instance::InstanceValues,
    pub(crate) picks: weft_core::picks::Picks,
    pub(crate) settings: weft_core::run_settings::RunSettings,
    /// What the run is given at the places its program says (handed
    /// values, stand-ins' outputs): its own, never its program's.
    pub(crate) given: std::sync::Arc<weft_core::project::Given>,
}

impl BornWith {
    /// What the run's birth `birth` says. An event that is no birth is a
    /// broken record.
    pub(crate) fn of_birth(run_id: RunId, birth: &RunEvent) -> anyhow::Result<Self> {
        match birth {
            RunEvent::RunStarted { phase, instance, instance_values, picks, settings, given, .. } => Ok(Self {
                phase: *phase,
                instance: instance.clone(),
                instance_values: (**instance_values).clone(),
                picks: (**picks).clone(),
                settings: *settings,
                given: std::sync::Arc::new((**given).clone()),
            }),
            _ => anyhow::bail!("run {run_id} has a record but no RunStarted; its record is broken"),
        }
    }
}

/// What a run starts from.
pub(crate) enum StartsFrom {
    /// Born here from a plan (`crate::plan`): its roots kicked as the plan
    /// makes them (`crate::plan::Startable::opening`), its birth already
    /// handed to its record. Nothing is folded.
    Plan { born_with: BornWith, kicked: weft_core::primitive::KickTable },
    /// Carried on from its record, as the worker read it after its claim:
    /// folded.
    Record(Box<ClaimedRecord>),
}

/// A claimed run's record as its worker reads it to carry the run on, a
/// page at a time: its fold fed as the pages come, beside what else the
/// drive reads off it (its birth, the charges it leaves open, the request
/// a stand-in serves), never the record whole.
pub(crate) struct ClaimedRecord {
    /// Its first event, the run's birth.
    pub(crate) birth: RunEvent,
    /// What its firing kick was given, when it has one: the request a run
    /// started by hand through a caller's trigger serves (`StandIn`).
    pub(crate) firing_payload: Option<serde_json::Value>,
    /// The charges it leaves open.
    pub(crate) charges: weft_journal::ChargeBook,
    /// The calls its firings owe when they end.
    pub(crate) owed: weft_journal::OwedCalls,
    /// What the run inherits (`weft run --seed`).
    pub(crate) chain: weft_journal::SeedChain,
    /// Its fold, the whole record taken.
    pub(crate) fold: weft_journal::LiveFold,
    /// Its last row: a write of this worker goes after it.
    pub(crate) last_seq: i32,
    /// The memory note of the worker it was lost by, when that worker went
    /// away full, as the claim read it (`weft_journal::record::Claimed`).
    pub(crate) lost_full: Option<weft_broker_client::protocol::MemoryNote>,
}

impl ClaimedRecord {
    /// Read run `run_id`'s record through `page` (one page of a run's
    /// record after a row, `weft_journal::decoded_page`), folding it over
    /// `project`, its seed's ancestors read the same way and their
    /// programs from `projects`. `lost_full` is the claim's memory note.
    pub(crate) async fn read<P, Fut>(
        page: P,
        projects: &dyn crate::context::ProjectClient,
        run_id: RunId,
        project: Arc<ProjectDefinition>,
        lost_full: Option<weft_broker_client::protocol::MemoryNote>,
    ) -> anyhow::Result<Self>
    where
        P: Fn(RunId, Option<i32>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<weft_journal::JournalRow>>>,
    {
        let first = page(run_id, None).await?;
        let birth = match first.first() {
            Some(row) if matches!(row.event, RunEvent::RunStarted { .. }) => row.event.clone(),
            _ => anyhow::bail!("run {run_id} has a record that does not start with its RunStarted; its record is broken"),
        };
        // One chain for the whole drive: the ancestors are terminal and
        // their rows never change.
        let chain = weft_journal::seed_chain(
            Some(&birth),
            &page,
            |id, hash| async move {
                projects.fetch_definition(id, &hash).await?.map(Arc::new).ok_or_else(|| anyhow::anyhow!("seed definition {hash} is missing from project {id}"))
            },
        )
        .await?;
        let fold = weft_journal::LiveFold::new(weft_journal::Fold::new(run_id, project).driving(), &chain, &page).await?;
        // The first page is taken here, the rest walked after it.
        let last = weft_journal::record::RecordPage::last_seq(&first).expect("the first page holds the birth");
        let mut read = Reading { fold, charges: weft_journal::ChargeBook::default(), owed: weft_journal::OwedCalls::default(), firing_payload: None, last_seq: last };
        read = weft_journal::on_blocking_pool(move || read.take(first).map(|()| read)).await?;
        let read = weft_journal::record::fold_pages(|after| page(run_id, after), Some(last), read, |read: &mut Reading, rows| read.take(rows)).await?;
        let Reading { fold, charges, owed, firing_payload, last_seq } = read;
        Ok(Self { birth, firing_payload, charges, owed, chain, fold, last_seq, lost_full })
    }
}

/// [`ClaimedRecord::read`] taking a page.
struct Reading {
    fold: weft_journal::LiveFold,
    charges: weft_journal::ChargeBook,
    owed: weft_journal::OwedCalls,
    firing_payload: Option<serde_json::Value>,
    /// The last row taken.
    last_seq: i32,
}

impl Reading {
    fn take(&mut self, page: Vec<weft_journal::JournalRow>) -> anyhow::Result<()> {
        if let Some(last) = weft_journal::record::RecordPage::last_seq(&page) {
            self.last_seq = last;
        }
        for row in &page {
            self.charges.apply(&row.event);
            self.owed.apply(&row.event);
            if let (None, RunEvent::NodeKicked { firing: true, payload, .. }) = (&self.firing_payload, &row.event) {
                self.firing_payload = Some(payload.clone().unwrap_or_default());
            }
        }
        self.fold.apply(&page)
    }
}

/// One run, as its drive is handed it.
pub(crate) struct RunDrive<'a> {
    pub(crate) run_id: RunId,
    /// The run's one handle on its record: everything the run writes, its
    /// nodes', its buses' and its caller's exchange, goes through it.
    pub(crate) journal: Arc<DriveJournal>,
    /// The tables of the program (and the part of it) the run runs.
    pub(crate) program: Arc<crate::plan::ProgramTables>,
    pub(crate) starts_from: StartsFrom,
    pub(crate) cancellation: Arc<CancellationFlag>,
    pub(crate) exchange: Option<Exchange>,
    /// How the run is asked to let go of this worker. `None` for a run
    /// nobody asks that of.
    pub(crate) hand_back: Option<&'a HandBack>,
    /// How the run says it only waits on something outside it, so its
    /// worker may stop ticking (`crate::run_holds`). `None` for a run no
    /// worker's tick looks at.
    pub(crate) hold: Option<crate::run_holds::RunHold>,
}

/// Run one run to a terminal state or a pause. `replica` stamps
/// every write with this worker's replica id; the broker takes a write
/// only from the run's owner.
///
/// An error out of the drive (the record will not fold, an engine
/// invariant broke, a write of the run failed) ends the run Failed, naming
/// it, so it never reads as running forever (`end_run`).
pub(crate) async fn run_one(
    catalog: Arc<dyn NodeCatalog>,
    clients: &EngineClients,
    run: RunDrive<'_>,
    replica: &str,
    tenant_id: &str,
) -> anyhow::Result<RunOutcome> {
    run_one_observed(catalog, clients, run, replica, tenant_id).await.map(|drove| drove.outcome)
}

/// What a drive ended with: the outcome, plus the tables the worker
/// held at the end. The tables are what a refold of the journal has
/// to rebuild, so the engine's own tests fold the rows the run wrote
/// and compare; production reads the outcome alone (hence the tables
/// are unread outside test builds).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct Drove {
    pub(crate) outcome: RunOutcome,
    pub(crate) pulses: PulseTable,
    pub(crate) firings: NodeFiringTable,
    pub(crate) loop_runtime: LoopRuntime,
    pub(crate) kicked: weft_core::primitive::KickTable,
    pub(crate) scopes: ScopeTable,
}

/// `run_one`, keeping the worker's tables.
pub(crate) async fn run_one_observed(
    catalog: Arc<dyn NodeCatalog>,
    clients: &EngineClients,
    run: RunDrive<'_>,
    replica: &str,
    tenant_id: &str,
) -> anyhow::Result<Drove> {
    let RunDrive { run_id, journal, program, starts_from, cancellation, exchange, hand_back, hold } = run;
    // The run's spend still being worked out, which goes on its record
    // before its ending (`crate::metering`).
    let record = RunRecord::new(journal.clone());
    // The run owns its shared wait tracker; the bus coordinator,
    // the stream runtime and every firing's handle are its clients.
    //
    // Per-run bus coordinator: shared with every RunnerHandle so
    // `create_bus` can register its bus, and read by the loop's idle-
    // wait stuck-check (loop stuck + any bus live -> close every bus;
    // every cursor wakes with None, every wait wakes with Closed).
    //
    // The run's first bus starts the one-task bus-journal pump: every bus
    // append pings the coordinator's `journal_pump_notify`; the pump walks
    // every live bus, drains its unjournaled tail, and hands the entries to
    // the run's record so the inspector can replay the conversation. A bus
    // row that is refused degrades that bus (`SendError::JournalDegraded`)
    // rather than failing the run.
    let waits = crate::wait_tracker::WaitTracker::new();
    let bus_coordinator = crate::context::BusCoordinator::recorded(waits.clone(), run_id, journal.clone(), replica.to_string());
    // A panic inside the drive (an engine invariant checked with a
    // panic, a bug) is an error out of the drive like any other: caught
    // here so the run still gets its Failed ending below, instead of
    // unwinding past it and reading as running forever.
    let caller = exchange.as_ref().map(|exchange| exchange.conn.clone());
    let answering = exchange.as_ref().and_then(|exchange| exchange.live.as_ref());
    let drove = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(drive_run_id(
        program,
        catalog,
        run_id,
        clients,
        &record,
        &journal,
        replica,
        tenant_id,
        &cancellation,
        caller,
        answering,
        starts_from,
        waits,
        bus_coordinator.clone(),
        hand_back,
        hold.as_ref(),
    )))
    .await
    {
        Ok(drove) => drove,
        Err(panic) => Err(anyhow::anyhow!(
            "the drive of run {run_id} panicked: {}",
            panic_message(panic.as_ref())
        )),
    };
    // Everything the run says goes on its record before its ending, in
    // this order. Its buses' last rows: close every live bus and wait for
    // the pump to hand them over.
    bus_coordinator.shutdown(drove.as_ref().is_ok_and(|drove| matches!(drove.outcome, RunOutcome::Stalled | RunOutcome::HandedBack))).await;
    drop(bus_coordinator);
    // A durable run whose record stopped being written (a write got no
    // answer, `DriveJournal::must_hand_back`) after the drive's last look
    // can end no way but handed back: nothing after that write lands, its
    // ending included. Decided here, before its spend and its caller's
    // last words, so both go the way a hand-back's do. A write of the
    // ending itself that goes unanswered is only known once it was tried
    // (`end_run`): the caller has heard the ending by then, and a held
    // answer is refused for want of its record.
    let drove = match drove {
        Ok(drove) if journal.must_hand_back() && !drove.outcome.pauses() => {
            tracing::info!(
                target: "weft_engine::run_driver",
                %run_id, outcome = ?drove.outcome,
                "the run's record could not be written; handing the run back"
            );
            Ok(Drove { outcome: RunOutcome::HandedBack, ..drove })
        }
        drove => drove,
    };
    // Its spend: every figure still being worked out lands before the
    // ending. A charge still open is a call that spent and whose amount no
    // response stated yet. A run that pauses lets go of it, its opening on
    // record for the drive that picks the run up (`reopen_charges`); a run
    // that ended books it as the unknown it is, those this drive reopened
    // from its record included. A durable run handed back because its
    // record could not be written lets go of them the same way: what its
    // record holds is what the next worker reopens. Any other run whose
    // record lane is poisoned never carries on (the broker ends it,
    // `end_run`), so it books them unknown too: those writes fail loudly
    // on the poisoned lane, and the broker's give-up books whatever its
    // record leaves open (`weft_journal::unpriced_charges_closed`). A call
    // whose answer is
    // still being read holds this wait, and the metering tap bounds it: it
    // cuts an answer silent for its limit, or any answer once the run is
    // cancelled (`crate::metering::ANSWER_SILENCE_LIMIT`).
    let lets_go = drove.as_ref().is_ok_and(|drove| drove.outcome.pauses());
    let closing = if lets_go && (!journal.is_poisoned() || journal.must_hand_back()) {
        crate::metering::Closing::Release
    } else {
        crate::metering::Closing::Unknown("the run ended before the job was read back".into())
    };
    clients.open_charges.close_run_id(run_id, &record.costs, closing).await;
    // Its caller's last words. A durable run's whole answer that waits
    // for its record leaves once the ending is on it too: one write holds
    // both.
    let held = match &exchange {
        Some(exchange) => finish_exchange(exchange, drove.as_ref().map(|drove| &drove.outcome), hand_back).await,
        None => None,
    };
    let ended = end_run(&clients.writer, &journal, run_id, replica, drove).await;
    if let Some(conn) = held {
        conn.release_answer().await;
        hang_up(conn, ended.as_ref().is_ok_and(|drove| drove.outcome.pauses()), hand_back).await;
    }
    ended
}

/// A durable run's whole answer waits in its connection for the run's
/// record (`LiveCallerConnection::release_answer`). When the answer is the
/// run's last act, the run ends first and its ending rides the same write
/// (`run_one`); once there is more to do (a node to start, or
/// the drive about to wait on one still running), the answer goes as soon
/// as what came before it is on record.
fn let_answer_go(answering: Option<&Arc<crate::caller_conn::LiveCallerConnection>>) {
    if let Some(conn) = answering.filter(|conn| conn.holds_answer()) {
        let conn = conn.clone();
        tokio::spawn(async move { conn.release_answer().await });
    }
}

/// Resolves once the run's caller has an answer held ([`let_answer_go`]);
/// never for a run without one.
async fn answer_held(answering: Option<&Arc<crate::caller_conn::LiveCallerConnection>>) {
    match answering {
        Some(conn) => conn.answer_held().await,
        None => std::future::pending().await,
    }
}

/// End the exchange the way the run left it: a run that did not complete
/// tells its caller why (per the error mode) instead of leaving a silently
/// dropped socket; a run that completed ends it as the program left it (a
/// finished stream, a closed socket, or a loud "never answered" on a
/// silent route); a run that pauses says it carries on. The outbound queue
/// drops a push once the caller was answered or closed, so a run that
/// already spoke its last word is not double-messaged. A caller slow to
/// read its last words holds the hang-up, except for a run that has to let
/// go of a stopping worker before the platform's kill. Then everything the
/// exchange said is handed to the run's record.
///
/// A last word still waiting for the run's record (a durable run's whole
/// answer, `LiveCallerConnection::release_answer`) is handed back: it goes
/// once the run's ending is on record too, and the hang-up waits for it.
async fn finish_exchange<'e>(
    exchange: &'e Exchange,
    outcome: Result<&RunOutcome, &anyhow::Error>,
    hand_back: Option<&HandBack>,
) -> Option<&'e Arc<crate::caller_conn::LiveCallerConnection>> {
    let mut held = None;
    if let Some(conn) = &exchange.live {
        use crate::caller_conn::Ending;
        match outcome {
            // Weft could not keep the run (a write failed, the broker went).
            Err(e) => conn.surface_error(&format!("run failed: {e}"), Ending::Stopped).await,
            Ok(RunOutcome::Failed { error, held_too_long }) => {
                let ending = if *held_too_long { Ending::TimeLimit } else { Ending::ProgramFailed };
                conn.surface_error(&format!("run failed: {error}"), ending).await
            }
            Ok(RunOutcome::Cancelled { cause }) => conn.surface_error(&format!("run cancelled: {cause}"), Ending::of_cancel(cause)).await,
            Ok(RunOutcome::Stuck { report }) => conn.surface_error(&report.to_string(), Ending::ProgramFailed).await,
            Ok(RunOutcome::Completed) => conn.run_ended().await,
            // A run pauses with its caller still on the line only when the
            // caller is being let go at the platform's limit on one
            // connection: it hears why. Otherwise the caller is gone (a
            // connected caller keeps the run on its worker), and nobody is
            // left to tell.
            Ok(RunOutcome::Stalled) => conn.let_go().await,
            Ok(RunOutcome::HandedBack) => conn.run_carries_on(crate::caller_conn::HANDED_BACK).await,
        }
        if conn.holds_answer() {
            held = Some(conn);
        } else {
            hang_up(conn, outcome.is_ok_and(RunOutcome::pauses), hand_back).await;
        }
    }
    exchange.sink.close();
    held
}

/// Wait for the caller to have read the exchange's last words, except for
/// a run that `lets_go` of a stopping worker, which waits no longer than
/// its hand-back may.
async fn hang_up(conn: &crate::caller_conn::LiveCallerConnection, lets_go: bool, hand_back: Option<&HandBack>) {
    tokio::select! {
        () = conn.hang_up() => {}
        () = hand_back_overdue(hand_back), if lets_go => {}
    }
}

/// Hand the run's ending to its record and let go of it: a run that
/// pauses waits for its whole record and writes no ending; a run that
/// ended (or whose drive failed, ended Failed naming why) hands its ending
/// over, a durable one waiting for it. When a write of the run failed,
/// its record is behind and this worker can write nothing more of it: a
/// durable run whose write got no answer is handed back from the prefix
/// that landed (`DriveJournal::must_hand_back`), its ending unwritten,
/// for the worker that picks it up; any other run, and a drive that
/// failed (a panic), is ended Failed by the broker after the last row
/// that landed (`WorkerJournal::give_up`), unless another worker bore it
/// (whose run it is).
async fn end_run(
    writer: &crate::journal_writer::WorkerJournal,
    journal: &DriveJournal,
    run_id: RunId,
    replica: &str,
    drove: anyhow::Result<Drove>,
) -> anyhow::Result<Drove> {
    let written = match &drove {
        Ok(drove) if drove.outcome.pauses() => journal.leave(Leaving::CarriesOn).await,
        Ok(drove) => hand_ending(journal, &terminal_event(run_id, &drove.outcome), replica).await,
        Err(e) => hand_ending(journal, &RunEvent::RunFailed { run_id, error: format!("{e:#}"), at_unix: now_unix() }, replica).await,
    };
    let Err(unwritten) = written else { return drove };
    let drove = match drove {
        Ok(drove) if journal.must_hand_back() => {
            tracing::warn!(
                target: "weft_engine::run_driver",
                %run_id, error = %format!("{unwritten:#}"),
                "the run's record could not be written; it is handed back to be carried on from what landed"
            );
            return Ok(Drove { outcome: RunOutcome::HandedBack, ..drove });
        }
        drove => drove,
    };
    let why = match &drove {
        Ok(drove) => format!("the run ended ({:?}) but its record could not be written: {unwritten:#}", drove.outcome),
        Err(e) => format!("{e:#}; and its record could not be written: {unwritten:#}"),
    };
    if !journal.born_elsewhere() {
        writer.give_up(run_id, &why).await;
    }
    Err(anyhow::anyhow!(why))
}

/// End a run that could not start driving (its program would not load,
/// its record would not read, its caller's start would not parse) Failed
/// with `error`, the way any drive that fails ends (`end_run`). `charges`
/// are what the run's record leaves open, when it could be read: no drive
/// reopened them, so they are booked unknown before the ending
/// (`weft_journal::unpriced_charges_closed`). Answers the error, naming
/// whatever kept its ending from being written too.
pub(crate) async fn fail_run(
    writer: &crate::journal_writer::WorkerJournal,
    journal: &DriveJournal,
    run_id: RunId,
    replica: &str,
    charges: Option<weft_journal::ChargeBook>,
    error: anyhow::Error,
) -> anyhow::Error {
    let closing = charges.map_or_else(Vec::new, |charges| {
        weft_journal::unpriced_charges_closed(run_id, charges, "the run could not start again, so it ended before the job was read back", now_unix())
    });
    // Refused only once a write of the run failed: the ending's write then
    // fails too and the broker's give-up books them from the record.
    let error = match JournalClient::record_events(journal, &closing, Some(replica)).await {
        Ok(()) => error,
        Err(unbooked) => anyhow::anyhow!("{error:#}; and its open charges could not be booked: {unbooked:#}"),
    };
    match end_run(writer, journal, run_id, replica, Err(error)).await {
        Err(e) => e,
        Ok(_) => unreachable!("a run ended for an error answers an error"),
    }
}

/// Hand `ending` to the run's record after everything else and let go: a
/// durable run waits for it, a fast one does not.
async fn hand_ending(journal: &DriveJournal, ending: &RunEvent, replica: &str) -> anyhow::Result<()> {
    JournalClient::record_event(journal, ending, Some(replica)).await?;
    journal.leave(Leaving::Ended).await
}

/// The text of a caught panic: what `panic!` was given, or a marker
/// for a payload that is not text.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "a panic with a non-text payload".to_string()
    }
}

/// The run's fold of its own record: what an answer to one of its waits
/// is read through, made the first time one is (a run born from a plan
/// starts with none) and fed only the rows that landed since. Folding a
/// long run's record, and copying its state out, is real work: both run
/// on the blocking pool (`weft_journal::on_blocking_pool`), never on the
/// threads that serve the worker's door and keep its lease.
pub(crate) struct RunFold {
    run_id: RunId,
    project: Arc<ProjectDefinition>,
    /// What the run inherits (`weft run --seed`); empty for every other.
    chain: weft_journal::SeedChain,
    live: Option<weft_journal::LiveFold>,
}

impl RunFold {
    /// The fold of a run carried on from its record (`ClaimedRecord`),
    /// which folded it, and the state it holds (see [`checked_snapshot`]).
    async fn of_live(
        run_id: RunId,
        project: Arc<ProjectDefinition>,
        chain: weft_journal::SeedChain,
        live: weft_journal::LiveFold,
    ) -> anyhow::Result<(Self, RunSnapshot)> {
        weft_journal::on_blocking_pool(move || {
            let snap = checked_snapshot(run_id, &live)?;
            Ok((Self { run_id, project, chain, live: Some(live) }, snap))
        })
        .await
    }

    /// No fold yet: a run born from a plan, which inherits nothing.
    fn unborn(run_id: RunId, project: Arc<ProjectDefinition>) -> Self {
        Self { run_id, project, chain: Default::default(), live: None }
    }

    /// Catch the fold up with the run's record through `journal`,
    /// reading only the rows past the last it took, and answer the state
    /// it then holds (see [`checked_snapshot`]).
    async fn caught_up(&mut self, journal: &dyn JournalClient) -> anyhow::Result<RunSnapshot> {
        let run_id = self.run_id;
        let after = self.live.as_ref().and_then(weft_journal::LiveFold::folded);
        let rows = weft_journal::rows_after(|after| journal.record_page(run_id, after), after).await?;
        let live = match self.live.take() {
            Some(live) => live,
            None => {
                let fold = weft_journal::Fold::new(run_id, self.project.clone()).driving();
                weft_journal::LiveFold::new(fold, &self.chain, &|run, after| journal.record_page(run, after)).await?
            }
        };
        // A fold that refused rows keeps them, so every later snapshot
        // refuses the same way (`checked_snapshot`).
        let (live, snap) = weft_journal::on_blocking_pool(move || {
            let mut live = live;
            let snap = live.apply(&rows).and_then(|()| checked_snapshot(run_id, &live));
            (live, snap)
        })
        .await;
        self.live = Some(live);
        snap
    }

    /// What the firing of `record`, being resumed, sent before it
    /// paused: every port its record mentions, with the value the fold
    /// holds for it, `None` for a port it closed. The fold took the
    /// whole record before any resume (the answer it resumes on, or the
    /// boot), so a port the record says it emitted with no value here
    /// is a fold behind its record: refused.
    fn sent_before_pause(&self, record: &weft_core::run::NodeFiring) -> anyhow::Result<crate::context::SentBeforePause> {
        let output = self.live()?.firing_output(&record.node_id, &record.frames);
        record
            .mentioned_ports
            .iter()
            .map(|port| {
                if record.closed_output_ports.contains(port) {
                    return Ok((port.clone(), None));
                }
                let value = output.and_then(|bag| bag.get(port)).ok_or_else(|| {
                    anyhow::anyhow!(
                        "run {}: '{}' at frames {:?} emitted on '{port}' before it paused, \
                         but the run's fold holds no value for it",
                        self.run_id, record.node_id, record.frames,
                    )
                })?;
                Ok((port.clone(), Some(value.clone())))
            })
            .collect()
    }

    fn live(&self) -> anyhow::Result<&weft_journal::LiveFold> {
        self.live.as_ref().ok_or_else(|| anyhow::anyhow!("run {} was folded before anything of it was read", self.run_id))
    }
}

/// Record the answers waiting for the run's waits as its own
/// `SuspensionResolved`s, and catch the run's fold up with its record,
/// which then holds everything the run did since its fold last looked (the
/// waits it registered among it) and the answers last. Answers the state
/// the fold then holds. The broker hands an answer until the batch that
/// records it commits (which takes it off its queue), so they are on
/// record before anything asks again.
async fn take_answers(
    journal: &DriveJournal,
    fold: &mut RunFold,
    run_id: RunId,
    replica: &str,
    answers: Vec<weft_broker_client::protocol::RunAnswer>,
) -> anyhow::Result<RunSnapshot> {
    let at_unix = now_unix();
    let resolved: Vec<RunEvent> = answers
        .into_iter()
        .map(|answer| RunEvent::wait_answered(run_id, answer.token, answer.answer, at_unix))
        .collect();
    JournalClient::record_events(journal, &resolved, Some(replica)).await?;
    journal.flush().await?;
    fold.caught_up(journal).await
}

/// Put the answers to suspended nodes on the run's record and resume, in
/// place, the nodes whose current wait they answer. Surgical: a full
/// re-fold (`apply_snapshot`) would read the nodes still running (live bus
/// tasks, AHEAD of the record mid-flight) as crashed ones.
#[allow(clippy::too_many_arguments)]
async fn resume_answered(
    journal: &DriveJournal,
    fold: &mut RunFold,
    run_id: RunId,
    replica: &str,
    answers: Vec<weft_broker_client::protocol::RunAnswer>,
    firings: &NodeFiringTable,
    pulses: &mut PulseTable,
    kicked: &mut weft_core::primitive::KickTable,
    carried: &mut Carried,
) -> anyhow::Result<()> {
    if answers.is_empty() {
        return Ok(());
    }
    let snap = take_answers(journal, fold, run_id, replica, answers).await?;
    let resumed = resume_resolved_suspensions_in_place(snap, firings, pulses, kicked, carried)?;
    if resumed > 0 {
        tracing::info!(
            target: "weft_engine::resume",
            run_id = %run_id,
            resumed,
            "resumed suspension(s) in process while other nodes run"
        );
    }
    Ok(())
}

/// `run_one` without its ending and its bus pump: every `?` in
/// here ends the run, and the wrapper writes that end and closes the
/// buses.
#[allow(clippy::too_many_arguments)]
async fn drive_run_id(
    program: Arc<crate::plan::ProgramTables>,
    catalog: Arc<dyn NodeCatalog>,
    run_id: RunId,
    clients: &EngineClients,
    record: &RunRecord,
    drive_journal: &Arc<DriveJournal>,
    replica: &str,
    tenant_id: &str,
    cancellation: &Arc<CancellationFlag>,
    // The caller of this run, if any (the real one, or the stand-in a
    // fired run serves); threaded into every firing's `RunnerHandle` (so
    // `ctx.caller()` resolves) and into whether the run can pause (a
    // caller on the line it does not outlive keeps it on this worker).
    caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    // The run's live caller, whose durable answer may wait in the
    // connection for the record (`LiveCallerConnection::release_answer`).
    answering: Option<&Arc<crate::caller_conn::LiveCallerConnection>>,
    starts_from: StartsFrom,
    waits: Arc<crate::wait_tracker::WaitTracker>,
    bus_coordinator: Arc<crate::context::BusCoordinator>,
    hand_back: Option<&HandBack>,
    hold: Option<&crate::run_holds::RunHold>,
) -> anyhow::Result<Drove> {
    let project_arc = program.project.clone();
    let project = &*project_arc;
    let program_idx = &program.index;
    let mut pulses: PulseTable = Default::default();
    let mut firings: NodeFiringTable = Default::default();
    // Kicked nodes (the run's entry points: the firing trigger,
    // manual-run roots, infra-setup roots, at frames `[]`; and every
    // scope's own roots, at the copy's frames). Folded from
    // `RunEvent::NodeKicked`, a scope's opening and
    // `LoopIterationLaunched`. The scheduler dispatches each
    // not-yet-dispatched kick once at its own frames; the payload for
    // the firing trigger threads through to the `ctx.wake` bag.
    let mut kicked = weft_core::primitive::KickTable::new();
    // What the run decided about its scopes (`weft_core::run::scope`):
    // no row of its own, the fold decides the same from the same rows.
    let mut scopes = ScopeTable::default();
    // What each firing carries on from, out of the journal fold: the
    // waits it made and the state it saved last.
    let mut carried = Carried::default();
    let mut loop_runtime = LoopRuntime::new();
    // What the run's tables are done with goes as the drive goes
    // (`weft_core::run::forget`): a run carried on from its record takes
    // over what its fold forgot.
    let mut forgetting = Forgetting::new().keeping_first_failure();
    // It learns which iterations to look at from the loops' own news.
    loop_runtime.report();

    let journal: &dyn JournalClient = drive_journal.as_ref();
    let (born_with, mut fold) = match starts_from {
        StartsFrom::Plan { born_with, kicked: opening } => {
            // The run's values go where its program says it is given
            // something, and nowhere else. A run carried on from its record
            // is held to the same by the fold of that record (its snapshot
            // refuses a birth that breaks it).
            born_with.given.check_against(project).map_err(|why| anyhow::anyhow!("run {run_id} cannot run: {why}"))?;
            kicked = opening;
            (born_with, RunFold::unborn(run_id, project_arc.clone()))
        }
        StartsFrom::Record(claimed) => {
            let ClaimedRecord { birth, charges, owed, chain, fold, lost_full, .. } = *claimed;
            reopen_charges(clients, record, replica, run_id, cancellation, charges);
            // What its firings owed when the drive before went away, still
            // owed here.
            *record.owed.lock().expect("owed calls poisoned") = owed;
            let born_with = BornWith::of_birth(run_id, &birth)?;
            let (fold, mut snap) = RunFold::of_live(run_id, project_arc.clone(), chain, fold).await?;
            let buses = std::mem::take(&mut snap.buses);
            let crashed = apply_snapshot(
                &program, snap, &mut pulses, &mut firings, &mut kicked, &mut scopes, &mut carried,
                &mut loop_runtime, &mut forgetting,
            )?;
            // A node left running (whether it runs again from its saved
            // state or is failed) means the worker before went away
            // mid-drive, as a crash or an overdue hand-back does.
            let left_running = firings
                .open()
                .any(|e| e.status() == NodeFiringStatus::Running && !program.boundaries.contains(&e.node_id));
            reopen_buses(&bus_coordinator, &buses, &firings, &carried, left_running)
                .map_err(|why| anyhow::anyhow!("run {run_id} cannot carry on: {why}"))?;
            // A run claimed with its cancel waiting ends every open firing
            // Cancelled below, a body a dead drive left behind included.
            if !cancellation.is_cancelled() {
                fail_crashed_firings(
                    crashed, lost_full.as_ref(), run_id, project, program_idx, &mut pulses, &mut firings,
                    journal, replica,
                )
                .await;
            }
            (born_with, fold)
        }
    };
    let BornWith { phase, instance, instance_values, picks, settings, given } = born_with;
    if cancellation.is_cancelled() {
        // Nothing runs, but the run still gets its end: a cancel from
        // inside this worker (its caller refused or gone before
        // attaching), or one waiting for it when it was claimed. A run
        // carried on from its record ends what its record leaves open
        // (a parked wait, a body a dead drive left) Cancelled with it,
        // the same rows a cancel of a run nobody drives writes.
        let cause = recorded_cancel_cause(cancellation);
        cancel_open_firings(
            &mut firings, &mut pulses, &mut kicked, &mut scopes, &mut loop_runtime, run_id, project, program_idx,
            journal, replica, &cause.to_string(), phase, &given,
        )
        .await;
        forget_what_ended(
            &program, run_id, &mut forgetting, &mut pulses, &mut firings, &mut kicked, &mut scopes, &mut carried,
            &mut loop_runtime, |_| false,
        );
        crate::end_calls::settle_run(&end_call_sender(clients, record, replica, run_id, instance.as_ref()), record, project, true).await;
        return Ok(Drove { outcome: RunOutcome::Cancelled { cause }, pulses, firings, loop_runtime, kicked, scopes });
    }

    let outcome = drive(
        &program,
        catalog.as_ref(),
        run_id,
        clients,
        record,
        drive_journal,
        replica,
        tenant_id,
        cancellation,
        &waits,
        &bus_coordinator,
        caller.as_ref(),
        answering,
        &mut pulses,
        &mut firings,
        &mut kicked,
        &mut scopes,
        carried,
        &mut loop_runtime,
        phase,
        instance.as_ref(),
        &instance_values,
        &picks,
        &given,
        settings,
        &mut fold,
        &mut forgetting,
        hand_back,
        hold,
    )
    .await;
    // A durable run whose record write got no answer is handed back, also
    // when the failed write surfaced as an error from inside a turn (a
    // write the turn waited on) rather than at its top: its record is a
    // prefix of what it did either way (`DriveJournal::must_hand_back`).
    let sender = end_call_sender(clients, record, replica, run_id, instance.as_ref());
    let mut outcome = match outcome {
        Err(e) if drive_journal.must_hand_back() => {
            tracing::info!(
                target: "weft_engine::run_driver",
                %run_id, error = %format!("{e:#}"),
                "the run's record could not be written; handing the run back"
            );
            RunOutcome::HandedBack
        }
        Err(e) => {
            // The run fails on this: what its firings still owe goes out
            // as at any end that is not a cancel.
            crate::end_calls::settle_run(&sender, record, project, false).await;
            return Err(e);
        }
        Ok(outcome) => outcome,
    };

    // A route's caller is still waiting and nothing was ever sent: the
    // graph closed every path to its answer (a skipped Reply). That is a
    // failure, recorded with its reason (an unrecorded route's run leaves
    // that failure on record), and the caller is told the same.
    if matches!(outcome, RunOutcome::Completed) && caller.as_ref().is_some_and(|c| c.owes_answer()) {
        outcome = RunOutcome::Failed { error: weft_core::caller::NO_ANSWER.to_string(), held_too_long: false };
    }
    // The drive is over and every body with it: what its last turn ended
    // goes too.
    forget_what_ended(
        &program, run_id, &mut forgetting, &mut pulses, &mut firings, &mut kicked, &mut scopes, &mut Carried::default(),
        &mut loop_runtime, |_| false,
    );
    // A run that ends for good sends what its firings still owe; one that
    // pauses keeps it on its record for the drive that picks it up.
    if !outcome.pauses() {
        crate::end_calls::settle_run(&sender, record, project, matches!(outcome, RunOutcome::Cancelled { .. })).await;
    }
    Ok(Drove { outcome, pulses, firings, loop_runtime, kicked, scopes })
}

/// What sends the calls run `run_id`'s firings owe as its drive ends
/// (`crate::end_calls`).
fn end_call_sender<'a>(
    clients: &'a EngineClients,
    record: &'a RunRecord,
    replica: &'a str,
    run_id: RunId,
    instance: Option<&'a weft_core::instance::InstanceId>,
) -> crate::end_calls::Sender<'a> {
    crate::end_calls::Sender { clients, journal: &record.journal, replica, run_id, instance }
}

/// Hold again, before anything of the run runs, every charge its record
/// leaves open (`book`): opened by a call of an earlier drive that paused
/// or died before a response stated the amount. Each gets the sink the
/// call that opened it had (its node, iteration, service and whose key),
/// so the read that states the amount books it there. A drive that ends
/// the run before such a read books it as unknown, as it does every
/// charge it holds; this is what makes that hold for charges an earlier
/// drive opened too, a run a worker claims with its cancel already waiting
/// included. A run ended with no drive (cancelled by the dispatcher while
/// parked or queued, given up by the broker) is booked by whoever writes
/// that ending (`weft_journal::unpriced_charges_closed`).
fn reopen_charges(
    clients: &EngineClients,
    record: &RunRecord,
    replica: &str,
    run_id: RunId,
    cancellation: &Arc<CancellationFlag>,
    book: weft_journal::ChargeBook,
) {
    for charge in book.into_open() {
        let sink = Arc::new(crate::metering::CostSink {
            journal: record.journal.clone(),
            writer: clients.writer.clone(),
            replica: replica.to_string(),
            pending: record.costs.clone(),
            open_charges: clients.open_charges.clone(),
            run_id,
            node_id: charge.node_id,
            frames: charge.frames,
            service: charge.service,
            origin: charge.origin,
            cancellation: cancellation.clone(),
        });
        clients.open_charges.reopen(charge.charge, charge.scratch, sink);
    }
}

/// Stop the drive once one of its journal writes failed: the journal is
/// then behind what the worker did (see `DriveJournal`), and nothing new
/// starts on it.
fn stop_if_poisoned(drive_journal: &DriveJournal, run_id: RunId) -> anyhow::Result<()> {
    anyhow::ensure!(
        !drive_journal.is_poisoned(),
        "a journal write failed mid-drive for run {run_id}; the journal no longer holds what \
         the worker did, so the run cannot go on"
    );
    Ok(())
}

/// The run's state as its fold says, refusing to resume over a row the
/// fold could not apply: a state rebuilt from a partial log is a state
/// that never existed (skips un-happen, closures never cascade), so the
/// run fails loudly here and `weft clean` removes it, instead of
/// resuming wrong. A rejected row stays on the fold, so every later
/// snapshot of the same run refuses the same way.
///
/// The fold (`LiveFold`) is built once per drive, on top of the run's
/// seed chain: a seeded run (`RunStarted.seed`) first inherits
/// from its ancestors (terminal runs whose rows never change, read once),
/// so the inherited nodes sit Completed in the table and only the stale
/// ones dispatch. After that, each wake folds only the rows that landed
/// since the last one.
fn checked_snapshot(run_id: RunId, live: &weft_journal::LiveFold) -> anyhow::Result<RunSnapshot> {
    let snap = live.snapshot();
    if snap.corruptions.is_empty() {
        return Ok(snap);
    }
    let reasons: Vec<String> = snap
        .corruptions
        .iter()
        .map(|c| format!("{:?}: {}", c.site, c.reason))
        .collect();
    anyhow::bail!(
        "the record of run {run_id} does not fit the program it runs now ({} row(s) rejected: \
         {}), so the run cannot resume: a resync moved it, paused, onto a build it no longer \
         fits, or its record is damaged. Its record is kept: `weft run --seed={run_id}` runs the \
         program again from what it did, and `weft clean {run_id}` removes it.",
        reasons.len(),
        reasons.join("; "),
    )
}

/// Resolves once the run is asked to let go of this worker
/// ([`HandBack::asked`]). Pending forever for a run nobody asks that of.
async fn handed_back(hand_back: Option<&HandBack>) {
    match hand_back {
        Some(hand_back) => hand_back.asked.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Resolves once the run's hand-back is overdue ([`HandBack::overdue`]).
async fn hand_back_overdue(hand_back: Option<&HandBack>) {
    match hand_back {
        Some(hand_back) => hand_back.overdue.cancelled().await,
        None => std::future::pending().await,
    }
}

/// The run is handed back now (its hand-back is overdue, or its record
/// could not be written, `why`): the nodes still running stop where they
/// are, what they said before that is applied (and goes on record when it
/// still can), and the run is handed back. Their records stay open, so
/// the next worker runs each one again from its saved state, or fails it
/// as cut short when it keeps none.
#[allow(clippy::too_many_arguments)]
async fn cut_short(
    bodies: &mut Bodies,
    task_tx: &mpsc::UnboundedSender<TaskMsg>,
    task_rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    why: &str,
) -> RunOutcome {
    tracing::info!(
        target: "weft_engine::run_driver",
        run_id = %run_id,
        in_flight = bodies.len(),
        "{why}; handing the run back and stopping the nodes still running"
    );
    // A body that already ended is reaped first, so one that panicked
    // still gets its failure, naming the panic, before the rest stop.
    while let Some(ended) = bodies.try_next() {
        body_ended(ended, task_tx, run_id);
    }
    bodies.shutdown().await;
    apply_task_msgs(task_rx, run_id, project, program_idx, pulses, firings, forgetting, journal, replica, stream_rt, false).await;
    // Every delivery wait died with its task: no producer reads a success
    // that never happened.
    stream_rt.fail_all_gates("the run was handed back to another worker");
    RunOutcome::HandedBack
}

/// The node bodies a drive runs. The first body runs in place, polled by
/// the drive's own task; one that becomes ready while another runs is
/// spawned, so parallel branches run in parallel while a straight chain
/// never leaves its task. A body that panics ends like one that failed,
/// wherever it ran ([`body_ended`]).
struct Bodies {
    /// The body polled by the drive's own task, at most one.
    here: futures::stream::FuturesUnordered<futures::future::BoxFuture<'static, Ended>>,
    here_at: Option<FiringLocation>,
    /// Every other body, each on a task of its own.
    spawned: JoinSet<Ended>,
    spawned_at: HashMap<tokio::task::Id, FiringLocation>,
}

/// A body that ended: its firing, the firing's handle, and the panic it
/// ended with, if it did. `loc` is `None` for a spawned task the drive
/// holds no firing of (it cannot be); `runner` is `None` only for a body
/// the drive aborted.
struct Ended {
    loc: Option<FiringLocation>,
    runner: Option<Arc<crate::context::RunnerHandle>>,
    panicked: Option<String>,
}

impl Bodies {
    fn new() -> Self {
        Self { here: Default::default(), here_at: None, spawned: JoinSet::new(), spawned_at: HashMap::new() }
    }

    /// Start the body of the firing at `loc`, polling it once where it
    /// starts: a body that ends in that poll (most nodes only turn their
    /// inputs into outputs) is handed back at once and costs no task. One
    /// still going keeps polling in place when nothing else does, else it
    /// moves to a task of its own, so bodies that wait run side by side.
    /// A body that never yields runs to its end in that first poll, the
    /// way the one polled in place always has.
    fn start(
        &mut self,
        loc: FiringLocation,
        runner: Arc<crate::context::RunnerHandle>,
        body: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Option<Ended> {
        let at = loc.clone();
        let mut body: futures::future::BoxFuture<'static, Ended> = Box::pin(async move {
            let panicked = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body)).await.err();
            Ended { loc: Some(at), runner: Some(runner), panicked: panicked.map(|panic| panic_message(panic.as_ref())) }
        });
        // Polled with no waker of the drive's: one still going is polled
        // again right away where it goes next (the set in place polls a
        // future pushed to it on its next poll, a task on its spawn),
        // which hands it the waker it waits with.
        if let Some(ended) = futures::FutureExt::now_or_never(&mut body) {
            return Some(ended);
        }
        if self.is_empty() {
            self.here_at = Some(loc);
            self.here.push(body);
        } else {
            let task = self.spawned.spawn(body);
            self.spawned_at.insert(task.id(), loc);
        }
        None
    }

    fn len(&self) -> usize {
        self.here.len() + self.spawned.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The firings whose bodies run.
    fn firings(&self) -> impl Iterator<Item = &FiringLocation> {
        self.here_at.iter().chain(self.spawned_at.values())
    }

    /// Whether the body of the firing at `loc` runs.
    fn runs(&self, loc: &FiringLocation) -> bool {
        self.firings().any(|at| at == loc)
    }

    /// A body that already ended, without waiting. The body in place is
    /// polled once here, so it moves on every turn of the drive, however
    /// busy the other bodies keep it; between turns the drive's idle wait
    /// polls it ([`Self::next`]).
    fn try_next(&mut self) -> Option<Ended> {
        if !self.here.is_empty() {
            if let Some(Some(ended)) = futures::FutureExt::now_or_never(futures::StreamExt::next(&mut self.here)) {
                self.here_at = None;
                return Some(ended);
            }
        }
        let joined = self.spawned.try_join_next_with_id()?;
        Some(self.reaped(joined))
    }

    /// The next body to end. `None` at once when none runs.
    async fn next(&mut self) -> Option<Ended> {
        tokio::select! {
            Some(ended) = futures::StreamExt::next(&mut self.here), if !self.here.is_empty() => {
                self.here_at = None;
                Some(ended)
            }
            Some(joined) = self.spawned.join_next_with_id(), if !self.spawned.is_empty() => Some(self.reaped(joined)),
            else => None,
        }
    }

    fn reaped(&mut self, joined: Result<(tokio::task::Id, Ended), tokio::task::JoinError>) -> Ended {
        match joined {
            Ok((id, ended)) => {
                self.spawned_at.remove(&id);
                ended
            }
            Err(join_err) => {
                let loc = self.spawned_at.remove(&join_err.id());
                Ended { loc, runner: None, panicked: Some(join_err.to_string()) }
            }
        }
    }

    /// Stop every body where it is: the one in place is dropped, the
    /// spawned ones are driven to their abort point.
    async fn shutdown(&mut self) {
        self.here.clear();
        self.here_at = None;
        self.spawned.shutdown().await;
        self.spawned_at.clear();
    }
}

/// End every bus nobody is on that no node of the run can still join
/// ([`weft_core::run::bus_reach`]), as its last member left: the buses
/// that said they are unattended since the last turn join `unattended`,
/// and each stays there until it ends, or somebody joins it. Answers
/// whether one ended: its readers wake to its end, which moves the run.
fn end_unattended_buses(
    unattended: &mut HashSet<uuid::Uuid>,
    waits: &crate::wait_tracker::WaitTracker,
    bus_coordinator: &crate::context::BusCoordinator,
    pulses: &PulseTable,
    firings: &NodeFiringTable,
    loop_runtime: &LoopRuntime,
) -> bool {
    unattended.extend(waits.take_unattended());
    unattended.retain(|id| bus_coordinator.bus(*id).is_some_and(|bus| !bus.is_closed() && !bus.attended()));
    if unattended.is_empty() {
        return false;
    }
    // A running body's use is its bus's; a parked firing's body runs again
    // and uses the bus again if it does.
    let joinable = weft_core::run::bus_reach::joinable_buses(pulses, firings, loop_runtime, |firing, id| {
        firing.status() == NodeFiringStatus::Running
            && uuid::Uuid::parse_str(id).ok().and_then(|id| bus_coordinator.bus(id)).is_some_and(|bus| {
                bus.used_by(&FiringLocation::new(firing.node_id.clone(), firing.frames.clone()))
            })
    });
    let mut ended = false;
    unattended.retain(|id| {
        let Some(bus) = bus_coordinator.bus(*id) else { return false };
        let made_not_sent = bus_coordinator
            .maker_of(*id)
            .is_some_and(|(at, port)| weft_core::run::bus_reach::not_sent_yet(firings, &at, &port));
        if made_not_sent || joinable.contains(&id.to_string()) {
            return true;
        }
        ended |= bus.end_unattended();
        false
    });
    ended
}

/// The run cannot move any other way (every running node waits, and
/// nothing outside the run can answer): no node a marker is on its way to
/// can start before something ends, so every bus nobody is on ends now, as
/// its last member left, instead of the run being found stuck. Answers
/// whether one ended.
fn end_every_unattended_bus(unattended: &mut HashSet<uuid::Uuid>, bus_coordinator: &crate::context::BusCoordinator) -> bool {
    let mut ended = false;
    for id in unattended.drain() {
        if let Some(bus) = bus_coordinator.bus(id) {
            ended |= bus.end_unattended();
        }
    }
    ended
}

/// A body ended: its firing is forgotten, so the location is free to fire
/// again. Returns whether a location was freed: the caller treats that as
/// a turn with work (the scan that ran before the reap held the location,
/// and nothing else wakes the loop to rescan it). A body that panicked
/// never sent its terminal, so the panic is turned into a Failed terminal
/// for its firing, catchable on its `_error` output like a failure the body
/// returned (a panic is the node failing, not the program's shape);
/// otherwise its record stays Running and the run never learns the node
/// ended. A body that returned already reported through the task channel.
fn body_ended(ended: Ended, task_tx: &mpsc::UnboundedSender<TaskMsg>, run_id: RunId) -> bool {
    match (ended.loc, ended.panicked) {
        (Some(_), None) => true,
        (Some(loc), Some(panic)) => {
            let err = format!("node task panicked: {panic}");
            tracing::error!(
                target: "weft_engine::run_driver",
                run_id = %run_id,
                node = %loc.node_id,
                frames = ?loc.frames,
                error = %err,
                "in-flight node task panicked; failing the node"
            );
            // Route the panic through the SAME failure path a body-returned
            // error takes: a synthetic Failed terminal on the task channel,
            // which drains in FIFO order, so any pulses the node emitted
            // before panicking are applied first and this terminal fails the
            // node with the correct mentioned set off its record (keeping
            // already-emitted ports' values, closing only the rest).
            let _ = task_tx.send(TaskMsg::Terminal {
                loc,
                run_id,
                outcome: NodeTaskOutcome::Failed { message: err.clone(), catchable: Some(err), gave_up_wait: false },
                runner: ended.runner,
            });
            true
        }
        (None, panicked) => {
            // A task the drive holds no firing of: it starts none such.
            // Fail loud rather than silently drop it.
            tracing::error!(
                target: "weft_engine::run_driver",
                run_id = %run_id,
                panicked = ?panicked,
                "a node task ended with no recorded firing; engine invariant violated"
            );
            false
        }
    }
}

// A firing's identity `(node_id, frames)` is `weft_core::liveness::
// FiringLocation`, the same key the wait tracker and the stream runtime
// use, so the driver never grows a second name for one concept.

/// The set of parked nodes whose CURRENT suspension is now resolved. A
/// `WaitingForInput` firing resumes ONLY if the wait it is parked on (its
/// `callback_id`) has an ending among its recorded waits; one paused
/// inside the run (no wait) carries on whenever the run does.
///
/// Checking "ANY ended wait" instead of the current token would livelock
/// a body that waits twice: after the first wait ends, the firing
/// permanently holds an ended wait, so every re-fold would re-dispatch
/// the node, the body would run again and pause on its next wait,
/// shipping fresh NodeResumed + NodeSuspended rows that the refetch loop
/// treats as progress, spinning until its deadline. Scoping
/// to the current token is what both resume paths (boot-time
/// `apply_snapshot` and mid-drive `resume_resolved_suspensions_in_place`)
/// depend on, so it lives here once.
fn resolved_waiting_locations(firings: &NodeFiringTable, waits: &HashMap<FiringLocation, Vec<RecordedWait>>) -> std::collections::HashSet<FiringLocation> {
    firings
        .waiting()
        .filter(|e| match e.callback_id.as_deref() {
            Some(token) => waits
                .get(&FiringLocation::new(e.node_id.clone(), e.frames.clone()))
                .is_some_and(|recorded| recorded.iter().any(|wait| wait.token == token && wait.ended.is_some())),
            // Paused inside the run: it carries on whenever the run does.
            None => true,
        })
        .map(|e| FiringLocation::new(e.node_id.clone(), e.frames.clone()))
        .collect()
}

/// What a run's firings carry on from, by firing, out of its fold: the
/// waits each made (`RunSnapshot::recorded_waits`) and the state each saved
/// last (`RunSnapshot::saved_states`). A firing dispatched again takes its
/// own out ([`Carried::resume_of`]).
#[derive(Default)]
struct Carried {
    waits: HashMap<FiringLocation, Vec<RecordedWait>>,
    saved: HashMap<FiringLocation, SavedState>,
}

impl Carried {
    /// What the firing at `at` carries on from: its saved state and its
    /// waits.
    fn resume_of(&mut self, at: &FiringLocation, record: Option<&NodeFiring>) -> crate::context::FiringResume {
        crate::context::FiringResume {
            saved: self.saved.remove(at),
            waits: self.waits.remove(at).unwrap_or_default(),
            items_on_record: record.map_or(0, |record| record.stream_items),
        }
    }
}

/// Open again, before any body runs, every bus a run that paused created
/// (`buses`, from its record: open ones as they stood, ended ones ended),
/// with who was on each (its record's members), each name given back to
/// the paused firing whose saved position holds it. A bus whose members
/// and those names disagree had a join or a leave lost from its record:
/// a name nobody holds would never leave, so the bus could never end by
/// its last member leaving, and it is ended failed instead. A drive that ended with nodes still
/// running (`left_running`: its worker died, or a hand-back went overdue)
/// may have left a bus's last messages off the record, so each such bus
/// is opened only to end it failed, whether those nodes run again from
/// their saved states or are failed: a reader carrying on from it would
/// otherwise read on past messages it never got. So is a bus a saved place
/// is ahead of (the record lost its last entries before the pause).
fn reopen_buses(
    coordinator: &Arc<crate::context::BusCoordinator>,
    buses: &BTreeMap<uuid::Uuid, weft_core::primitive::RecordedBus>,
    firings: &NodeFiringTable,
    carried: &Carried,
    left_running: bool,
) -> Result<(), String> {
    let mut held: HashMap<uuid::Uuid, Vec<(String, FiringLocation, u64)>> = HashMap::new();
    for (at, saved) in &carried.saved {
        if firings.at(&at.node_id, &at.frames).is_none_or(|record| record.status().is_terminal()) {
            continue;
        }
        for (bus, position) in &saved.positions.buses {
            let (Ok(id), Some(registered)) = (uuid::Uuid::parse_str(bus), &position.registered) else { continue };
            held.entry(id).or_default().push((registered.name.clone(), at.clone(), registered.joined_offset));
        }
    }
    // A saved place ahead of what the record holds of a bus (its last
    // rows never landed: a write that failed before the pause, a worker
    // gone before its buses were written) would have the reopened bus hand
    // out those offsets again under the readers' feet.
    let behind_the_record: Vec<uuid::Uuid> = buses
        .iter()
        .filter(|(id, bus)| {
            carried.saved.values().filter_map(|saved| saved.positions.buses.get(&id.to_string())).any(|position| {
                position.cursor.is_some_and(|cursor| cursor > bus.next_offset)
                    || position.registered.as_ref().is_some_and(|registered| registered.joined_offset >= bus.next_offset)
            })
        })
        .map(|(id, _)| *id)
        .collect();
    let unaccounted: Vec<(uuid::Uuid, String)> = buses
        .iter()
        .filter(|(_, bus)| bus.end.is_none())
        .filter_map(|(id, bus)| {
            let holding: std::collections::BTreeSet<String> = held.get(id).into_iter().flatten().map(|(name, _, _)| name.clone()).collect();
            let nobody_holds: Vec<&String> = bus.members.difference(&holding).collect();
            let not_on_it: Vec<&String> = holding.difference(&bus.members).collect();
            (!nobody_holds.is_empty() || !not_on_it.is_empty()).then(|| {
                (*id, format!(
                    "the run's record of who was on this bus disagrees with the nodes that paused on it (on it, held by no paused node: {nobody_holds:?}; held, not on it: {not_on_it:?}), so it lost a join or a leave and its members can no longer end it"
                ))
            })
        })
        .collect();
    coordinator.reopen(buses, held)?;
    if left_running {
        coordinator.close_all(StreamEnd::Failed {
            error: "the worker holding this bus went away, so messages it carried may be lost".into(),
        });
    }
    for (id, why) in unaccounted {
        coordinator.close_bus(id, StreamEnd::Failed { error: why });
    }
    for id in behind_the_record {
        coordinator.close_bus(id, StreamEnd::Failed {
            error: "the run's record lost this bus's last entries before the pause, so messages it carried may be lost".into(),
        });
    }
    Ok(())
}

/// Mark a set of firing locations for re-dispatch as resumes. For each
/// location: a kicked entry node (no inbound pulses) gets `dispatched`
/// reset so the kick-synthesis path re-fires it; every other location
/// gets the exact pulse IDs its dispatch consumed (`pulses_absorbed`)
/// put back on their wires, pending (`PulseTable::unabsorb`), so the
/// dispatch loop re-fires it. In both
/// cases the non-terminal firing record stays, so the dispatch loop ships
/// `NodeResumed` (not a duplicate `NodeStarted`). One un-absorb mechanic,
/// shared by both resume paths.
///
/// Pulse IDs are looked up directly (not by count): a count-based
/// un-absorb would restore the wrong firing's pulses if two firings at
/// different frame stacks shared a node and one needs re-dispatch.
///
/// Delivery waits are DELIBERATELY untouched by the un-absorb: a
/// producer's `yield_downstream` resolves on the FIRST
/// absorption of its pulses (the producer already resumed and moved
/// on), and its gate resolves exactly once. Do not "fix" this by
/// re-arming gates here; a re-armed gate has no waiter.
///
/// Refused when a firing's record names a pulse the table does not hold
/// as taken there: the run's state does not hold together, and running
/// the node on part of its inputs would be wrong.
fn redispatch_locations(
    to_un_absorb: &std::collections::HashSet<FiringLocation>,
    pulses: &mut PulseTable,
    firings: &NodeFiringTable,
    kicked: &mut weft_core::primitive::KickTable,
) -> anyhow::Result<()> {
    for loc in to_un_absorb {
        if kicked.get(loc).is_some_and(|kick| kick.dispatched()) {
            kicked.set_dispatched(loc, false);
        }
    }

    // In order, so the pulses go back on their wires the same way on
    // every resume.
    let mut locations: Vec<&FiringLocation> = to_un_absorb.iter().collect();
    locations.sort();
    for loc in locations {
        let Some(record) = firings.get(&loc.node_id).and_then(|records| records.get(&loc.frames)) else { continue };
        if !record.status().is_terminal() {
            pulses
                .unabsorb(&loc.node_id, &record.pulses_absorbed)
                .map_err(|why| anyhow::anyhow!("the run cannot resume at frames {:?}: {why}", loc.frames))?;
        }
    }
    Ok(())
}

/// Returns the firings a dead worker left running, which the caller
/// fails (`fail_crashed_firings`): a node that may have acted outside its
/// run never runs twice by itself. Refused when the run's state does not
/// hold together (`redispatch_locations`).
fn apply_snapshot(
    program: &crate::plan::ProgramTables,
    snap: RunSnapshot,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    carried: &mut Carried,
    loop_runtime: &mut LoopRuntime,
    forgetting: &mut Forgetting,
) -> anyhow::Result<Vec<CrashedFiring>> {
    // A worker drives a run on a fold that forgets as the drive does
    // (`Fold::driving`); one that kept everything holds tables whose ended
    // places were never marked.
    *forgetting = snap.forgetting.ok_or_else(|| anyhow::anyhow!(
        "run {}: the fold handed to the drive keeps everything it folded, and a drive takes over only a fold that forgets (`Fold::driving`); this is a bug in the worker",
        snap.run_id,
    ))?;
    *pulses = snap.pulses;
    *firings = snap.firings;
    *kicked = snap.kicked;
    *scopes = snap.scopes;
    carried.waits = snap.recorded_waits;
    carried.saved = snap.saved_states;
    // The fold drove every loop instance from the loop rows; the
    // resumed worker takes them over as its own runtime. Buffered
    // stream items are not in it: their pulses refolded Pending and
    // the routing pass re-ingests them.
    *loop_runtime = snap.loop_runtime;
    // `snap.corruptions` is empty here: `checked_snapshot` refused the
    // snapshot otherwise.

    // A WaitingForInput firing re-dispatches when the wait it is
    // CURRENTLY parked on has an ending, or when it paused inside the run
    // (no wait: it carries on with the run), and is still non-terminal
    // (`resolved_waiting_locations`). Without this scoping, every fresh
    // worker spawn would re-dispatch every still-suspended sibling, run
    // its body, reach its wait, find no answer yet, re-suspend, churning
    // the journal with spurious NodeStarted/Suspended cycles per fresh
    // worker.
    let resume_locations = resolved_waiting_locations(firings, &carried.waits);

    // A worker that died mid-firing: a `Running` firing with no terminal
    // in the journal belonged to a worker that went away while the
    // node's body ran. Whatever the body had done outside (an email
    // sent, a row written, a paid call) may have partly happened, and
    // nothing in the journal says how far it got, so the node is never
    // run again by itself: the caller fails it (`fail_crashed_firings`),
    // catchable on its `_error` output like any failure. A retry is
    // only ever something the program asks for.
    //
    // Except a node that saved a state (`ctx.keep`): it said where it is,
    // so it runs again from there, as a resumed firing does (a `once`
    // whose result never got saved runs again, which is the gap its docs
    // name; a stream it read hands it again what it took since that save,
    // since a firing's takes go on record with its saves). And except a
    // pure node (`features.pure`) that had not emitted yet: it did nothing
    // outside its run and nothing inside it either, so it runs again,
    // exactly as if its start had never been written. One that emitted
    // already, or read a stream, with no state saved, is failed like any
    // other node.
    //
    // A firing being Running here means its worker is gone because at
    // most ONE worker drives a run at a time: its execute or
    // resume task is claimed by one replica under a lease, the claim
    // makes that replica the run's owner
    // (`run.owner_replica`), and the broker refuses journal
    // writes from anyone else. So a prior worker whose claim lapsed
    // cannot ship its own NodeCompleted after this one failed the firing.
    // This path runs only on a fold of a drive with nothing in flight
    // (a fresh worker's boot, or a re-drive after a stall): mid-drive
    // (`resume_resolved_suspensions_in_place`) a Running firing is a live
    // task, not a dead one, so that path omits it.
    //
    // A loop's boundary (its LoopIn or LoopOut) is re-dispatched too:
    // runtime machinery, not user code, whose whole state (launched,
    // out_fired, stream_end) is journal-backed so its re-fire is
    // idempotent.
    let mut run_again: std::collections::HashSet<FiringLocation> =
        std::collections::HashSet::new();
    let mut crashed: Vec<CrashedFiring> = Vec::new();
    for e in firings.open() {
        if e.status() != NodeFiringStatus::Running {
            continue;
        }
        let loc = FiringLocation::new(e.node_id.clone(), e.frames.clone());
        let consumed_a_stream = !program.index.stream_inputs(&e.node_id).is_empty();
        let pure_and_untouched = program.node(&e.node_id).is_some_and(|node| node.features.pure)
            && e.mentioned_ports.is_empty()
            && !consumed_a_stream;
        if program.boundaries.contains(&e.node_id) || pure_and_untouched || carried.saved.contains_key(&loc) {
            run_again.insert(loc);
        } else {
            crashed.push(CrashedFiring { consumed_a_stream, loc });
        }
    }

    let to_un_absorb: std::collections::HashSet<FiringLocation> =
        resume_locations.union(&run_again).cloned().collect();

    redispatch_locations(&to_un_absorb, pulses, firings, kicked)?;
    Ok(crashed)
}

/// A firing a dead worker left running (see `apply_snapshot`).
struct CrashedFiring {
    loc: FiringLocation,
    /// The node was reading a stream: having saved no state, it put none
    /// of its takes on record, so the items it pulled are back on the
    /// stream and go with its failure, which its message says.
    consumed_a_stream: bool,
}

/// The message a firing a dead worker left running fails with. `lost_full`
/// is that worker's memory note, when it went away with its memory nearly
/// full: the message then says it ran out of memory and which lever
/// raises it.
fn crashed_firing_message(firing: &CrashedFiring, lost_full: Option<&weft_broker_client::protocol::MemoryNote>) -> String {
    let node = &firing.loc.node_id;
    let doing = if firing.consumed_a_stream { "reading a stream" } else { "running" };
    let cause = match lost_full {
        Some(note) => format!(
            "the worker running '{node}' ran out of memory while it was {doing} (it was at {}% of its {}); \
             raise it with `weft workers set --memory`",
            note.used_bytes.saturating_mul(100) / note.limit_bytes.max(1),
            memory_size(note.limit_bytes),
        ),
        None => format!("the worker running '{node}' went away while it was {doing}"),
    };
    if firing.consumed_a_stream {
        format!(
            "{cause}; it was not run again, because it saved no state to carry on from and may \
             have partly acted on the items it had read. The rest of its stream goes with it. \
             Re-run the run to start the stream over once you have checked what it did."
        )
    } else {
        format!(
            "{cause}; it was not run again, because it may have partly happened. Re-run from here \
             once you have checked what it did."
        )
    }
}

/// `bytes` the way `weft workers set --memory` takes a size: whole `Gi`
/// when it is some, else `Mi`.
fn memory_size(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{}Gi", bytes / GIB)
    } else {
        format!("{}Mi", bytes / MIB)
    }
}

// Layer 1: what a firing a dead worker left running fails with, with and
// without that worker's memory note.
#[cfg(test)]
mod crashed_firing_message_tests {
    use super::*;
    use weft_broker_client::protocol::MemoryNote;

    fn crashed(consumed_a_stream: bool) -> CrashedFiring {
        CrashedFiring { loc: FiringLocation::new("load", vec![]), consumed_a_stream }
    }

    /// A worker that went away full names the memory and the lever, at
    /// what share of which size; one that left no note went away, and
    /// that is all the message says.
    #[test]
    fn a_worker_that_went_away_full_ran_out_of_memory() {
        let note = MemoryNote { used_bytes: 486 << 20, limit_bytes: 512 << 20, at_unix: 100 };
        let running = crashed(false);
        assert_eq!(
            crashed_firing_message(&running, Some(&note)),
            "the worker running 'load' ran out of memory while it was running (it was at 94% of its 512Mi); \
             raise it with `weft workers set --memory`; it was not run again, because it may have partly \
             happened. Re-run from here once you have checked what it did."
        );
        let went_away = "the worker running 'load' went away while it was running; it was not run again";
        assert!(crashed_firing_message(&running, None).starts_with(went_away));

        let streaming = crashed(true);
        let message = crashed_firing_message(&streaming, Some(&note));
        assert!(message.starts_with("the worker running 'load' ran out of memory while it was reading a stream (it was at 94% of its 512Mi)"), "{message}");
        assert!(message.contains("Re-run the run to start the stream over"), "{message}");
        assert!(crashed_firing_message(&streaming, None).starts_with("the worker running 'load' went away while it was reading a stream; it was not"));
    }

    #[test]
    fn a_limit_reads_the_way_the_lever_takes_it() {
        assert_eq!(memory_size(1 << 30), "1Gi");
        assert_eq!(memory_size(4 << 30), "4Gi");
        assert_eq!(memory_size(512 << 20), "512Mi");
        assert_eq!(memory_size(1536 << 20), "1536Mi");
    }
}

/// Fail each firing `apply_snapshot` found a dead worker left running:
/// a loud `NodeFailed` with the cause, downstream closures included,
/// through the one failure door, so with `_error` wired the message
/// goes there and the branch carries on. `lost_full` is that worker's
/// memory note, read with the claim, when it went away full.
#[allow(clippy::too_many_arguments)]
async fn fail_crashed_firings(
    crashed: Vec<CrashedFiring>,
    lost_full: Option<&weft_broker_client::protocol::MemoryNote>,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    for firing in crashed {
        let err = crashed_firing_message(&firing, lost_full);
        let loc = &firing.loc;
        // The record came out of the refold with the ports the firing
        // had already emitted on; those keep their values, as on the
        // fold's reading of the NodeFailed row.
        let mentioned = mentioned_ports(firings, &loc.node_id, run_id, &loc.frames);
        handle_node_failure(
            &loc.node_id, &mentioned, run_id, &loc.frames, &err, Some(err.clone()), false, project,
            program_idx, pulses, firings, journal, replica,
        )
        .await;
    }
}

/// Surgically resume the parked nodes whose CURRENT suspension just
/// resolved, IN PLACE, without re-folding the whole run. Used by
/// the mid-drive resume poll: nodes still running keep unrelated nodes
/// genuinely Running in-flight, so a full `apply_snapshot` would read
/// them as firings a dead worker left running and fail them (a
/// `NodeFailed` journaled over a live task). This touches ONLY the resolved
/// waiters: it reads the run's fold solely to recover their recorded waits
/// and saved states, then for each WaitingForInput firing whose
/// `callback_id` token is now resolved it (a) installs what that firing
/// carries on from into the live map and (b) un-absorbs the pulses the
/// original dispatch consumed, so the next drain re-fires it as a resume
/// (NodeResumed). Returns how many nodes it resumed.
///
/// Mirrors the `resume_locations` half of `apply_snapshot` (the half
/// about firings a dead worker left Running is deliberately omitted:
/// mid-flight a Running firing is a live task, not a dead one).
fn resume_resolved_suspensions_in_place(
    snap: RunSnapshot,
    firings: &NodeFiringTable,
    pulses: &mut PulseTable,
    kicked: &mut weft_core::primitive::KickTable,
    carried: &mut Carried,
) -> anyhow::Result<usize> {
    // Which parked nodes have their CURRENT suspension resolved now?
    // Computed against the FRESHLY-FOLDED waits (the live map is stale
    // mid-drive, which is exactly why this path exists), NOT the firings
    // a dead worker left Running that apply_snapshot also handles:
    // mid-flight a Running firing is a live in-flight task, not a dead
    // one.
    let resume_locations = resolved_waiting_locations(firings, &snap.recorded_waits);

    if resume_locations.is_empty() {
        return Ok(0);
    }

    // Install what exactly those firings carry on from, so the body gets
    // its saved state back and its wait the answer.
    for key in &resume_locations {
        if let Some(waits) = snap.recorded_waits.get(key) {
            carried.waits.insert(key.clone(), waits.clone());
        }
        if let Some(saved) = snap.saved_states.get(key) {
            carried.saved.insert(key.clone(), saved.clone());
        }
    }

    // Re-dispatch them as resumes (kicked-reset + pulse un-absorb), the
    // same mechanic the boot-time path uses.
    redispatch_locations(&resume_locations, pulses, firings, kicked)?;

    Ok(resume_locations.len())
}

// ---------- Main drive loop ----------

/// Internal loop body called once per run by `run_one`.
#[allow(clippy::too_many_arguments)]
async fn drive(
    program: &Arc<crate::plan::ProgramTables>,
    catalog: &dyn NodeCatalog,
    run_id: RunId,
    clients: &EngineClients,
    record: &RunRecord,
    drive_journal: &Arc<DriveJournal>,
    replica: &str,
    tenant_id: &str,
    cancellation: &Arc<CancellationFlag>,
    waits: &Arc<crate::wait_tracker::WaitTracker>,
    bus_coordinator: &Arc<crate::context::BusCoordinator>,
    caller: Option<&Arc<dyn weft_core::caller::CallerConnection>>,
    answering: Option<&Arc<crate::caller_conn::LiveCallerConnection>>,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    mut carried: Carried,
    loop_runtime: &mut LoopRuntime,
    phase: weft_core::context::Phase,
    // Which instance the run is for, from its `RunStarted`; every
    // firing's ctx carries it. (`replica` above is this worker.)
    instance: Option<&weft_core::instance::InstanceId>,
    // What that instance provides, as the run was born with it (its
    // `RunStarted`): every `@instance_filled` field reads these.
    instance_values: &weft_core::instance::InstanceValues,
    // The install's picks, as the run was born with them (its
    // `RunStarted`): every connection picked on the install reads
    // these.
    picks: &weft_core::picks::Picks,
    // What the run is given, as its `RunStarted` says: handed values and
    // stand-ins' outputs at the places its program says.
    given: &weft_core::project::Given,
    // How the run is kept, from its `RunStarted`: whether it can
    // pause, and how long a wait it cannot pause on holds.
    settings: weft_core::run_settings::RunSettings,
    // The run's fold. The in-place resume folds the answers it takes.
    fold: &mut RunFold,
    // What the run's tables forgot, and still have to forget.
    forgetting: &mut Forgetting,
    // See `run_one`.
    hand_back: Option<&HandBack>,
    // See `RunDrive::hold`.
    hold: Option<&crate::run_holds::RunHold>,
) -> anyhow::Result<RunOutcome> {
    let project: &ProjectDefinition = &program.project;
    let program_idx = &program.index;
    // The driver's rows and its nodes' (through their `RunRecord`) go to
    // the run's record in the background, in one order (`DriveJournal`).
    let journal: &dyn JournalClient = drive_journal.as_ref();
    // What infra the program declares: every firing's handle checks a
    // shared `Infra` handle against it.
    let declared_infra = program.declared_infra.clone();
    // ONE ordered channel from node tasks to the loop. A node sends
    // `TaskMsg::Emission` zero or more times while it runs (each
    // `pulse_downstream` / `close_port`, applied without closing the
    // emitting node), then exactly one `TaskMsg::Terminal` when its body
    // returns. FIFO ordering on this single channel is load-bearing: the
    // loop always observes a node's emissions BEFORE its terminal, so the
    // close-unmentioned-ports sweep at the terminal sees the complete set
    // of emitted ports. (Two separate channels left a window where a
    // terminal could be read before a just-sent emission, closing a port
    // that was actually emitted and skipping its consumer until a
    // re-dispatch, an emit-then-immediately-return race.)
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<crate::context::TaskMsg>();
    // This drive's stream bookkeeping (live feeds + pending delivery
    // gates), reporting to the run's shared wait tracker.
    let mut stream_rt = StreamRuntime::new(waits.clone());
    // The node bodies running (`Bodies`), each named by its firing, so a
    // body that PANICS (and never sends its terminal) still becomes a
    // terminal `NodeFailed` for the right (node, frames).
    let mut bodies = Bodies::new();

    // Rehydrate sweep: a resumed stream-driven loop whose durable
    // `stream_end` is already recorded has no close pulse left (it was
    // consumed the moment it routed), so nothing in the live flow
    // would ever re-evaluate that end. An idle such loop would wedge
    // forever (or, once every record is terminal, read as a false
    // clean completion with its downstream never fired). Settle those
    // ends NOW; loops with items still pending re-delivery settle
    // later through the normal stream_push / LoopOut chain.
    settle_rehydrated_stream_ends(
        project, program_idx, pulses, firings, journal, replica, run_id,
        loop_runtime, &mut stream_rt,
    )
    .await;

    // Two-pass stuck detector: a single no-progress drain can mean the
    // runtime just hasn't polled spawned tasks yet. Only after the loop
    // idle-waits AND wakes AND re-drains AND still finds nothing to do
    // do we declare stuck. The flag flips true when the select returns,
    // false on real progress and after a stuck-close.
    let mut idled_since_progress = false;

    // The answers the run waits for while this worker keeps it: a wait
    // held in a node's call (`crate::held_waits`), and a suspended node
    // while a bus keeps the worker (drive() never returns while a bus is
    // open, so without this its answer would never reach it): asked for
    // while some wait for one (`crate::outside_waits::AnswerAsk`).
    let mut ask = crate::outside_waits::AnswerAsk::new(clients, run_id);
    // How long the run has been quiet: every running node waiting, and one
    // of those waits ended only from outside (see the stuck-check below).
    let mut hold_clock = crate::outside_waits::HoldClock::new(clients.clock.as_ref(), run_id, settings.hold_secs());
    // When the run went quiet (every running node waiting): one that can
    // pause stays on its worker its `pauseAfterSecs` from then before it
    // pauses ([`pause_decision`]), and anything moving in it starts the
    // count over.
    let mut quiet_since: Option<std::time::Instant> = None;
    // Set once the drive told every waiting body to pause
    // ([`pause_decision`]): from then on nothing starts and no answer is
    // taken, and once the last body is gone the run lets go, `Stalled`
    // (an answer from outside picks it up again) or, paused for a
    // hand-back, `HandedBack` (the next worker does).
    let mut paused: Option<RunOutcome> = None;
    // The buses nobody is on that a node of the run may still join: each
    // turn ends the ones none can join any more ([`end_unattended_buses`]).
    let mut unattended: HashSet<uuid::Uuid> = HashSet::new();
    'turn: loop {
        // Poison checkpoint: a journal write failed somewhere since
        // the last iteration. The journal is now a strict prefix of
        // the live state; driving further would compound the
        // divergence (every later refold rebuilds a different world).
        // A durable run whose write got no answer at all is handed back
        // from that prefix, the way a crash leaves it, and the next
        // worker carries it on (`DriveJournal::must_hand_back`). Any other
        // failed write stops the drive: the error becomes the run's Failed
        // terminal (`run_one`), which is the only end this run gets,
        // since nothing respawns a run whose task failed.
        if drive_journal.must_hand_back() {
            return Ok(cut_short(
                &mut bodies, &task_tx, &mut task_rx, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                &mut stream_rt, "its record could not be written",
            )
            .await);
        }
        stop_if_poisoned(drive_journal, run_id)?;

        // Cancellation checkpoint. Checked at the TOP of every
        // iteration regardless of whether the previous iteration
        // made progress. The flag is persistent (AtomicBool), so
        // there's no race between cancel() landing and the next
        // check.
        //
        // Before returning Cancelled, `shutdown().await`
        // the in-flight JoinSet: simply dropping the JoinSet aborts
        // its tasks at their next yield point, but a task that's
        // mid-journal-write may finish writing (e.g. NodeCompleted)
        // AFTER the cancel path wrote NodeCancelled for the same
        // (node, frames). The fold is last-write-wins, so the final
        // state would flip to Completed and downstream nodes would
        // receive a fake output. Awaiting shutdown drives every
        // task to its abort point deterministically before we
        // declare the run cancelled.
        if cancellation.is_cancelled() {
            tracing::info!(
                target: "weft_engine::run_driver",
                run_id = %run_id,
                in_flight = bodies.len(),
                "cancellation observed at loop top; draining in-flight tasks"
            );
            cancel_cleanup(
                &mut bodies,
                &mut task_rx,
                firings,
                forgetting,
                run_id,
                project,
                program_idx,
                pulses,
                journal,
                replica,
                loop_runtime,
                kicked,
                scopes,
                &mut stream_rt,
                &recorded_cancel_cause(cancellation).to_string(),
                phase,
                given,
            )
            .await;
            return Ok(RunOutcome::Cancelled { cause: recorded_cancel_cause(cancellation) });
        }

        // The run's scopes are decided here, synchronously and without a
        // journal row: a decision is a function of the program and of the
        // pulses, so the fold decides the same from the same rows
        // (`settle_table`, which it runs after every row). What opened
        // sends what starts at its ports and kicks its members no wire
        // feeds; what did not run closes what leaves it and is told,
        // however deep. What the run will never take settles with no row
        // (a pulse inside a copy that did not run, or on a gate already
        // decided, is absorbed; a pulse into a trigger at Fire is
        // dropped). A producer that yield_downstream'ed into any of those
        // is parked on the pulses; its wait fails loudly instead of
        // hanging.
        let pass = settle_table(project, program_idx, given, phase, run_id, now_unix(), pulses, scopes, kicked);
        let scopes_moved = !pass.decided.is_empty() || !pass.emissions.is_empty();
        if scopes_moved {
            idled_since_progress = false;
        }
        stream_rt.on_pulses_absorbed(
            &pass.absorbed,
            AbsorbKind::Skipped {
                reason: "the consumer will never take it (the scope around it did not run, or the gate it \
                         feeds was decided already), so the awaited delivery can never happen",
            },
        );
        stream_rt.on_pulses_absorbed(
            &pass.dropped,
            AbsorbKind::Skipped {
                reason: "the target is a trigger; wires into a trigger are inert at Fire (its \
                         ports replay the setup-time snapshot), so the awaited delivery can \
                         never happen",
            },
        );

        // Gates settle before either kind of stream
        // consumer sees an end. Routing still precedes readiness so a running
        // consumer cannot dispatch a second time over its queued items.
        let acted = route_stream_pulses(
            run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
            &mut stream_rt, loop_runtime, kicked, scopes,
        ).await;
        if acted > 0 { idled_since_progress = false; }

        // Asked to let go of this worker. A run that can be suspended
        // starts no node from here on, the ones running go on to their
        // end or pause where they wait (`pause_decision`), and the run is
        // handed back once none is left (below). What waits to run stays
        // in the table and on record, for the worker that carries the run
        // on. A run that cannot be suspended carries on here as if nothing
        // was asked, getting as far as it can: the process going away ends
        // it the way a crash would. It drains from the turn it can be
        // suspended (its caller gone).
        let (asked, overdue) = hand_back.map_or((false, false), |hand_back| {
            let overdue = hand_back.overdue.is_cancelled();
            (overdue || hand_back.asked.is_cancelled(), overdue)
        });
        let can_pause = unsuspendable(caller, settings).is_none();
        let draining = asked && can_pause;
        // Past the hand-back's deadline every node still running is
        // stopped where it is, a wait held in a node's call included, also
        // in a run that could only be suspended once the deadline was gone
        // (its last bus closed late): the deadline was spent either way.
        if draining && overdue {
            return Ok(cut_short(
                &mut bodies, &task_tx, &mut task_rx, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                &mut stream_rt, "the hand-back is overdue",
            )
            .await);
        }

        let mut ready = program.ready(pulses, given);
        // What tells a resume from a firing that already took its inputs
        // (see the dispatch below): the parked firings whose current wait
        // is answered, or that paused inside the run.
        let resolved_waiters = resolved_waiting_locations(firings, &carried.waits);
        ready.retain_mut(|(id, group)| {
            let node = program.node(id).expect("ready node belongs to project");
            // Items keep coming down a stream while the firing reading it
            // is parked on a wait: they wait on their wire for its resumed
            // body, and dispatch nothing.
            let at = FiringLocation::new(id.clone(), group.frames.clone());
            if firing_at(firings, id, group.run_id, &group.frames).is_some_and(|record| record.status() == NodeFiringStatus::WaitingForInput)
                && !resolved_waiters.contains(&at)
                && only_stream_items(pulses, program_idx, id, &group.pulse_ids)
            {
                return false;
            }
            match scope_permission(program_idx, node, &group.frames, scopes) {
                ScopePermission::Pending => false,
                ScopePermission::Allowed => true,
                ScopePermission::Skipped(scope) => {
                    group.skip = Some(weft_core::run::skip::SkipReason::ScopeSkipped { scope });
                    true
                }
            }
        });
        // Work this worker leaves for the next one: what is ready, and a
        // kick not dispatched yet.
        let held_back = draining && (!ready.is_empty() || kicked.any_undispatched());
        if draining || paused.is_some() {
            ready.clear();
        }
        // A kick not dispatched yet that has no pulse-driven ReadyGroup at
        // its location gets a synthesized one so the scheduler picks it
        // up. After the synthesis (or after we observe a pulse-driven
        // group at the same key), it is marked dispatched so the next tick
        // doesn't double-fire. Only the kicks not dispatched yet are
        // looked at: a long loop leaves one dispatched kick per iteration
        // of each body root behind it. (What a kicked node's body reads
        // as its wake is read off its kick at the dispatch below, a
        // resume's included.)
        let to_dispatch: Vec<FiringLocation> =
            if draining || paused.is_some() { Vec::new() } else { kicked.undispatched().cloned().collect() };
        for loc in to_dispatch {
            let node = program.node(&loc.node_id)
                .ok_or_else(|| anyhow::anyhow!("kick names unknown node '{}'", loc.node_id))?;
            let info = kicked.get_mut(&loc).expect("an undispatched kick is in the table");
            // A kick a scope's teardown left (`scope_skipped`) is decided
            // already: it dispatches into its skip whatever the copies
            // around it say (a loop torn down before any iteration tells
            // its body at the loop's own frames, where no copy is ever
            // decided). Every other kick obeys the copies around it.
            if info.scope_skipped.is_none() {
                match scope_permission(program_idx, node, &loc.frames, scopes) {
                    ScopePermission::Pending => continue,
                    ScopePermission::Allowed => {}
                    ScopePermission::Skipped(scope) => info.scope_skipped = Some(scope),
                }
            }
            // Not yet dispatched. If a pulse-driven ReadyGroup at this
            // location already covers the node (the entry node also
            // received a regular pulse, and every wired port of it was
            // satisfied), let the pulse-driven dispatch run; we just
            // mark it dispatched. Either way the dispatch reads the
            // kick's payload.
            let already_in_ready = ready
                .iter()
                .any(|(rid, g)| *rid == loc.node_id && g.frames == loc.frames);
            if !already_in_ready {
                // Synthesize a ReadyGroup at the kick's frames from the
                // node's written constants (and a firing trigger's port
                // snapshot): `kicked_group` is the one derivation, shared
                // with the fold.
                // A kick reads its own writing and, for a fired
                // trigger, its baked ports.
                // Nothing waiting on a wire feeds it: a kick absorbs no
                // pulse, so folding one in would leave it pending for
                // ever and the run could never complete. A
                // trigger's pulses are dropped outright in the firing
                // phase (`settle_table`).
                ready.push((
                    loc.node_id.clone(),
                    weft_core::run::ready::kicked_group(node, info, &loc.frames, run_id, project, program_idx, given),
                ));
            }
            kicked.set_dispatched(&loc, true);
        }
        if !ready.is_empty() {
            // Once per batch of ready nodes in every run: a line a person reads only while
            // following one run closely, so its list is built only then.
            if tracing::enabled!(target: "weft_engine::run_driver", tracing::Level::DEBUG) {
                let ids: Vec<&str> = ready.iter().map(|(id, _)| id.as_str()).collect();
                tracing::debug!(
                    target: "weft_engine::run_driver",
                    run_id = %run_id,
                    ready_ids = ?ids,
                    "ready batch"
                );
            }
            // Dispatching new work counts as progress: the just-spawned
            // tasks haven't been polled by the runtime yet, so the next
            // no-progress drain must NOT immediately declare stuck.
            idled_since_progress = false;
        }

        let dispatched_this_turn = scopes_moved || !ready.is_empty();
        // A body that ended in the poll that started it frees its
        // location like one reaped below.
        let mut ended_at_start = false;

        // Dispatch every ready group. Each dispatch either short-circuits
        // (skip/failure) or spawns a task; every path absorbs the group's
        // pulses, so a non-empty batch is progress. More work starts: a
        // held answer does not wait for it.
        if !ready.is_empty() {
            let_answer_go(answering);
        }
        for (node_id, mut group) in ready {
            tracing::debug!(
                target: "weft_engine::run_driver",
                node = %node_id,
                run_id = %group.run_id,
                frames = ?group.frames,
                "dispatching ready group"
            );
            let (Some(node_def), Some(node_ports)) = (program.node(&node_id), program.ports(&node_id, &group.frames)) else {
                // Unreachable by construction: pulse-driven groups come
                // from `project.nodes` itself and kicks are synthesized
                // from the same definition set. If it ever fires, a
                // silent skip would park this group's pending pulses
                // forever; fail the drive loudly instead.
                return Err(anyhow::anyhow!(
                    "dispatch: ready group references node '{node_id}' that is not in the \
                     project definition; corrupt compiled project shape"
                ));
            };
            // A record already at this location makes the dispatch a
            // resume, and only two kinds are: a firing parked (on a wait
            // whose answer is in, or paused inside the run), and one left
            // Running with no body (a loop boundary, a pure node that sent
            // nothing, or a node that saved a state, which a dead worker
            // left, run again after a refold).
            // One run fires a node once per frames, so a group over
            // any other record (a terminal one, a body still running, a
            // wait still unanswered) is a pulse that reached a firing
            // after it took its inputs: fail loud BEFORE anything is
            // absorbed or journaled, never run the node twice.
            let loc = FiringLocation::new(node_id.clone(), group.frames.clone());
            let resuming = match firing_at(firings, &node_id, group.run_id, &group.frames) {
                // A firing that ended here and was forgotten is ended all
                // the same.
                None if forgetting.ended(&Places { project, index: program_idx, run_id }, &node_id, &group.frames) => {
                    return Err(anyhow::anyhow!(
                        "dispatch: '{node_id}' at frames {:?} became ready again after its firing ended; one run \
                         fires a node once per frames (engine invariant violated)",
                        group.frames,
                    ));
                }
                None => None,
                Some(record) => {
                    let resumes = match record.status() {
                        NodeFiringStatus::WaitingForInput => resolved_waiters.contains(&loc),
                        NodeFiringStatus::Running => !bodies.runs(&loc),
                        _ => false,
                    };
                    if !resumes {
                        return Err(anyhow::anyhow!(
                            "dispatch: '{node_id}' at frames {:?} became ready again while its firing \
                             is {} with its {}; one run fires a node once per frames (engine \
                             invariant violated)",
                            group.frames,
                            record.status().as_str(),
                            if bodies.runs(&loc) { "body running" } else { "inputs taken" },
                        ));
                    }
                    Some(record)
                }
            };
            // For the NodeResumed row: the token of the wait this
            // dispatch is actually resuming on, the record's CURRENT
            // parked token (`callback_id`); `None` for a firing paused
            // inside the run, and for one a dead worker left running (run
            // again from its saved state after a refold). What
            // the firing carries on from (its saved state, its waits, the
            // wait it paused in), read while its record still says where
            // it was parked. And what the firing already sent, with its
            // values: the body run again sends those ports as no-ops.
            let parked_token: Option<String> = resuming.and_then(|record| record.callback_id.clone());
            let resume = carried.resume_of(&loc, resuming);
            // A firing parked on a wait carries on from the state it saved
            // before it waited. One with none paused before weft kept node
            // states (it resumed by running its body over its record), and
            // its node's code, moved to a saved state since, cannot carry on
            // from that record.
            let paused_before_states = (parked_token.is_some() && resume.saved.is_none()).then(|| {
                format!(
                    "this run paused before this version of weft, which picks a paused node up from the state it \
                     saved (`ctx.keep`), and '{}' saved none: it cannot carry on. Start the run again \
                     (`weft run --seed={run_id}` reuses what it already did).",
                    place_of(project, &loc),
                )
            });
            let sent_before_pause = match resuming {
                Some(record) => fold.sent_before_pause(record)?,
                None => Default::default(),
            };
            let is_resume = resuming.is_some();
            // Where this firing runs, spelled: the node under the call
            // sites on its frames (`one.db` inside the file the site
            // `one` includes). What a person is told, what an infra
            // node's row is keyed by, and where the run's picks and its
            // instance's values for this node are kept.
            let place = {
                let call_path: Vec<String> =
                    weft_core::frames::call_path(&group.frames).into_iter().map(str::to_string).collect();
                weft_core::project::address_of(project, &node_id, &call_path)
            };
            let loop_boundary = matches!(
                node_def.node_type.as_str(),
                weft_core::project::boundary_types::LOOP_IN | weft_core::project::boundary_types::LOOP_OUT
            );
            // The input bag of a dispatch that will run a body: what the
            // ready paths delivered (wired pulses + body literals), a
            // `@instance_filled` field taking what the run's instance
            // provides at this place, and a connection picked on the
            // install taking the install's pick, both as the run was born
            // with them, in the place a value written in the source would
            // sit. Filled BEFORE anything is absorbed, because what it
            // holds can skip the node: a field the program lets an
            // instance leave empty, empty here (`unfilled`).
            let mut filled = (group.skip.is_none() && group.error.is_none() && !loop_boundary).then(|| {
                let mut bag = owned_bag(&group.received.input);
                weft_core::picks::fill_picks(node_def, &mut bag, picks.get(&place));
                with_instance_values(node_def, &place, instance, instance_values, bag)
            });
            if let Some(Ok(bag)) = &filled {
                if !is_resume {
                    group.skip = weft_core::run::skip::unfilled(node_def, bag);
                }
            }
            // Absorb input pulses for this dispatch, EXCEPT pulses on
            // generator-typed input ports of a dispatch that will RUN:
            // those are the stream's items (and its end), delivered to
            // the running consumer through its feed by the routing pass
            // instead of being consumed by the dispatch. A SKIP dispatch
            // absorbs everything (skip is the whole group's consumption).
            let generator_ports = program_idx.stream_inputs(&node_id);
            let mut deferred: std::collections::HashSet<uuid::Uuid> =
                std::collections::HashSet::new();
            if group.skip.is_none() && !generator_ports.is_empty() {
                if let Some(bucket) = pulses.get(&node_id) {
                    for p in bucket.iter() {
                        if group.pulse_ids.contains(&p.id)
                            && generator_ports.contains(&p.target_port)
                        {
                            deferred.insert(p.id);
                        }
                    }
                }
            }
            let absorbed_now: Vec<uuid::Uuid> = group
                .pulse_ids
                .iter()
                .copied()
                .filter(|id| !deferred.contains(id))
                .collect();
            pulses.absorb(&node_id, |p| absorbed_now.contains(&p.id));
            // A run dispatch DELIVERS the absorbed values (any waiting
            // producer resumes); a skip dispatch consumes them with no
            // delivery ever possible, so its waiting producers fail
            // loudly instead of hanging.
            if let Some(reason) = &group.skip {
                let msg = format!(
                    "the consumer '{node_id}' skipped ({reason}), so the awaited \
                     delivery can never happen"
                );
                stream_rt.on_pulses_absorbed(&absorbed_now, AbsorbKind::Skipped { reason: &msg });
            } else if let Some(err) = &group.error {
                // A pre-dispatch failure (a port refused its value)
                // never runs the body, so nothing was delivered.
                let msg = format!(
                    "the consumer '{node_id}' failed before running ({err}), so the awaited \
                     delivery can never happen"
                );
                stream_rt.on_pulses_absorbed(&absorbed_now, AbsorbKind::Skipped { reason: &msg });
            } else {
                stream_rt.on_pulses_absorbed(&absorbed_now, AbsorbKind::Taken);
            }

            // A resume continues the record (WaitingForInput back to
            // Running); a first dispatch opens it.
            if is_resume {
                let record = set_firing_status(firings, &node_id, group.run_id, &group.frames, NodeFiringStatus::Running)
                    .expect("a resumed firing has its record (read above)");
                // Extend `pulses_absorbed` with this resume dispatch's
                // newly-absorbed pulses, exactly as the fold does on
                // `NodeResumed` (it absorbs every pulse pending at the
                // location when the row lands, which is this same set).
                // This keeps the live record equal to what a refold
                // produces (RAM == refold), the invariant the in-place
                // resume path (`resume_resolved_suspensions_in_place`)
                // relies on when it reads `pulses_absorbed` straight from
                // the live record. The matching fold-side flip (the pulses
                // absorbed in the table) is what makes a
                // refold-after-stall NOT re-fire this node. Both sides
                // must stay in lockstep.
                for id in &absorbed_now {
                    if !record.pulses_absorbed.contains(id) {
                        record.pulses_absorbed.push(*id);
                    }
                }
                // Clear the suspension token, exactly as the fold does on
                // `NodeResumed` (events.rs). The record is now Running,
                // not parked, so it no longer holds a pending callback;
                // leaving the old token here is a RAM-vs-refold divergence
                // (a refold would show None) and a trap for any future
                // reader of `callback_id` on a Running record.
                record.callback_id = None;
            } else {
                let mut record = NodeFiring::started(
                    &node_id, group.frames.clone(), group.run_id, group.received.clone(), absorbed_now.clone(), now_unix(),
                );
                record.skip_reason = group.skip.clone();
                record.error = group.error.clone();
                firings.insert(record);
            }

            // Ship the lifecycle event NOW, before any branch. Every
            // dispatch that reaches here has already absorbed
            // `group.pulse_ids` and created (or resumed) a firing record;
            // the journal MUST carry a matching NodeStarted/NodeResumed so
            // a refold reconstructs that record and re-absorbs those
            // pulses. Shipping it after the skip/error/passthrough/loop
            // branches (the old placement) meant a skipped or failed
            // firing absorbed its pulses in RAM but journaled no absorbing
            // event: on refold the pulses came back Pending with no record,
            // and `find_ready_nodes` (which keys purely on Pending pulses)
            // re-fired the node, replaying the whole skip/failure cascade
            // on every respawn. A skipped/failed firing is still a firing
            // the fold must reconstruct.
            ship_node_lifecycle(
                journal, replica, run_id, &node_id, &group.frames,
                parked_token.as_deref(), is_resume,
            ).await;

            if let Some(reason) = &group.skip {
                handle_node_skip(
                    &node_id, group.run_id, &group.frames, reason,
                    project, program_idx, pulses, firings, kicked, scopes, journal, replica,
                )
                .await;
                continue;
            }

            if let Some(err) = &group.error {
                // Pre-dispatch failure: the body never ran, so no port
                // was ever emitted. Empty mentioned set → close every
                // declared output port.
                let mentioned = std::collections::HashSet::new();
                handle_node_failure(
                    &node_id, &mentioned, group.run_id, &group.frames, err, None, false,
                    project, program_idx, pulses, firings, journal, replica,
                )
                .await;
                continue;
            }

            // Loop boundary nodes (LoopIn/LoopOut) are NOT in the
            // catalog: they're built-in firings handled inline by the
            // engine + `LoopRuntime`. Intercept BEFORE the catalog
            // lookup so the dispatch produces per-iteration body
            // pulses (LoopIn) or records LoopOut state and emits
            // outward at termination (LoopOut). NodeStarted was shipped
            // by the common path above; these ship NodeCompleted here
            // after the inline handler returns, and run synchronously
            // (there is no async body to spawn).
            if loop_boundary {
                let outcome = handle_loop_boundary_firing(
                    node_def,
                    &group,
                    project,
                    program_idx,
                    pulses,
                    journal,
                    replica,
                    loop_runtime,
                    &mut stream_rt,
                    kicked,
                    scopes,
                )
                .await;
                match outcome {
                    Ok(()) => {
                        mark_completed(firings, &node_id, run_id, &group.frames);
                        // Loop boundary: closures are the loop machinery's
                        // job, not the generic sweep.
                        ship_node_completed(journal, replica, run_id, &node_id, &group.frames).await;
                    }
                    Err(err) => {
                        handle_loop_boundary_failure(
                            node_def, run_id, &group.frames, &err,
                            project, program_idx, pulses, firings, journal, replica,
                            loop_runtime,
                        )
                        .await;
                    }
                }
                continue;
            }

            let node_impl = match catalog.lookup(&node_def.node_type) {
                Some(n) => n,
                None => {
                    // A type whose package the build left out says so, and
                    // where its errors are; any other is unknown.
                    let err = match catalog.left_out().packages.iter().find(|p| p.node_types.contains(&node_def.node_type)) {
                        Some(package) => package.reason(None),
                        None => format!("unknown node type: {}", node_def.node_type),
                    };
                    let mentioned = std::collections::HashSet::new();
                    handle_node_failure(
                        &node_id, &mentioned, group.run_id, &group.frames, &err, None, false,
                        project, program_idx, pulses, firings, journal, replica,
                    )
                    .await;
                    continue;
                }
            };

            // The lifecycle event (NodeStarted / NodeResumed) was already
            // shipped above, right after the record was created, so every
            // dispatch path (skip, fail, passthrough, loop, body) carries
            // it. Don't ship a second one here.
            //
            // Generator inputs: this firing is about to RUN, so every
            // wired generator port gets a live feed, and the bag's value
            // for the port becomes the feed's handle marker (the item
            // that dispatched us stays Pending and is routed into the
            // feed by the next routing pass; the node's typed read of
            // the port resolves the marker to the feed).
            if !generator_ports.is_empty() {
                let loc = FiringLocation::new(node_id.clone(), group.frames.clone());
                let wired = weft_core::run::ready::wired_inputs(project, program_idx, &node_id, &group.frames);
                let mut feed_error: Option<String> = None;
                // In declared port order: the feeds are created one per
                // port, in the order the node declares them.
                for port in generator_ports.iter().map(String::as_str) {
                    if !wired.contains(port) {
                        // An unwired optional generator input stays
                        // absent; a read answers None honestly.
                        continue;
                    }
                    // A resumed firing's feed starts past what it took,
                    // with what a drain of it held, ended when its end
                    // had reached it.
                    let positions = resume.saved.as_ref().map(|saved| &saved.positions);
                    let taken = positions.and_then(|at| at.taken.get(port).copied()).unwrap_or(0);
                    let drained = positions.and_then(|at| at.drained.get(port).cloned()).unwrap_or_default();
                    let ended = positions.and_then(|at| at.ended.get(port).cloned());
                    let starts_ended = ended.is_some();
                    match stream_rt.create_feed(&loc, port, task_tx.clone(), taken, drained, ended) {
                        Ok(marker) => {
                            if starts_ended {
                                hand_over_untaken(stream_rt.feed_for(&loc, port), &loc, port, group.run_id, pulses);
                            }
                            // An ENDED generator port (an empty
                            // stream, or a closure racing dispatch)
                            // is a live feed, never a closed port;
                            // `firing_input` (run/ready.rs) is the
                            // one owner of that rule and never lists
                            // a generator port as closed.
                            debug_assert!(
                                !group.received.closed_ports.iter().any(|p| p == port),
                                "a generator port must never list as closed"
                            );
                            if let Some(Ok(bag)) = &mut filled {
                                bag.insert(port.to_string(), marker);
                            }
                        }
                        Err(e) => {
                            feed_error = Some(e);
                            break;
                        }
                    }
                }
                // A refused feed is an engine dispatch bug (a second
                // dispatch at a live `(firing, port)`): fail this
                // firing loudly and tear down whatever feeds this
                // dispatch DID create, so the router never feeds an
                // orphan.
                if let Some(err) = feed_error {
                    retire_consumer_streams(
                        project, &loc, group.run_id, pulses, journal, replica, &mut stream_rt,
                    )
                    .await;
                    handle_node_failure(
                        &node_id, &std::collections::HashSet::new(), group.run_id,
                        &group.frames, &err, None, false, project, program_idx, pulses, firings,
                        journal, replica,
                    )
                    .await;
                    continue;
                }
            }
            // Whether the node exists once per instance HERE: a node inside
            // a file included twice can be at one site and not the other.
            let per_instance = node_def.per_instance_at(&weft_core::frames::call_path(&group.frames));
            //
            // The node's ONE input bag: the one filled above, the
            // remaining braces config values, and declared defaults for
            // whatever is still absent (a closed wire is never defaulted).
            let delivered = filled.expect("a dispatch that runs its body filled its bag before absorbing");
            let inputs = match delivered.and_then(|delivered| {
                weft_core::context::node_input_bag(node_def, &group.frames, delivered, &group.received.closed_ports)
            }).and_then(|bag| {
                // The node's rules the compile could not decide, asked on
                // what arrived; a node with none pays nothing.
                weft_core::rules::recheck(node_def, project, &bag).map(|()| bag).map_err(|e| e.to_string())
            }) {
                Ok(bag) => bag,
                // A broken spec or malformed widget handle: the node
                // fails loud instead of running on a bag it can never
                // read correctly; a rule that fires on what arrived fails
                // it with the rule's message. Feeds created for this dispatch are
                // retired FIRST, so its terminal record (checked ahead
                // of any feed by the routing scan) is never shadowed by
                // an orphan feed that would swallow the stream.
                Err(err) => {
                    if !generator_ports.is_empty() {
                        let loc = FiringLocation::new(node_id.clone(), group.frames.clone());
                        retire_consumer_streams(
                            project, &loc, group.run_id, pulses, journal, replica, &mut stream_rt,
                        )
                        .await;
                    }
                    let mentioned = std::collections::HashSet::new();
                    handle_node_failure(
                        &node_id, &mentioned, group.run_id, &group.frames, &err, None, false,
                        project, program_idx, pulses, firings, journal, replica,
                    )
                    .await;
                    continue;
                }
            };
            let declared_outputs = node_ports.outputs.clone();
            let declared_inputs = node_ports.inputs.clone();
            // What reads each output in THIS run: the wires out of the
            // node at its frames (a run of part of the program keeps
            // only the wires inside it).
            let wired_outputs: std::collections::HashSet<String> = program_idx
                .get_outgoing(project, &node_id, &group.frames)
                .into_iter()
                .map(|edge| edge.source_handle.as_deref().unwrap_or("default").to_string())
                .collect();
            // Every dispatch of a kicked node sees the wake it was kicked
            // with, a resume's too: a kicked node that parked on a wait
            // and runs again once it is answered runs its body from the
            // top, and the body's `ctx.wake` must still hold what the
            // listener delivered.
            let wake_payload = kicked.get(&FiringLocation::new(node_id.clone(), group.frames.clone())).and_then(KickedNode::wake_payload);
            let mut runner = RunnerHandle::new(
                project.id,
                group.run_id,
                node_id.clone(),
                place.clone(),
                node_def.node_type.clone(),
                group.frames.clone(),
                clients.clone(),
                record.clone(),
                node_def.published_service.clone(),
                replica.to_string(),
                tenant_id.to_string(),
                cancellation.clone(),
                waits.clone(),
                bus_coordinator.clone(),
                declared_outputs,
                declared_inputs,
                wired_outputs,
            )
            .with_resume(resume)
            .with_feeds(stream_rt.feeds_of(&loc))
            .with_sent_before_pause(sent_before_pause)
            .with_emit_channel(task_tx.clone())
            .with_caller_connection(caller.cloned())
            .with_per_instance(per_instance)
            .with_run_instance(instance.cloned())
            .with_declared_infra(declared_infra.clone())
            .with_baked_outputs(node_def.baked_outputs.clone())
            .with_settings(settings);
            // What a trigger wakes with is a declared contract
            // (`firesWith` in its metadata), so a payload that does not
            // match fails the firing HERE, naming the field, instead of
            // somewhere inside the body where the author cannot see what
            // was expected. Both ways a payload arrives are covered by
            // the one check: a listener whose fields moved, and a
            // hand-typed `weft run --fire`. Read before the payload is
            // handed to the runner; acted on below, next to the other
            // reason a firing cannot start, so there is one failure path.
            let fire_payload_refusal = match (&wake_payload, node_def.features.is_trigger) {
                (Some(payload), true)
                    if matches!(phase, weft_core::context::Phase::Fire) =>
                {
                    match program_idx.fire_payload(&node_id) {
                        Ok(None) => None,
                        Ok(Some(shape)) => weft_core::node::check_fire_payload_against(shape, Some(payload)).err(),
                        Err(why) => Some(why.to_string()),
                    }
                }
                _ => None,
            };
            let has_wake = wake_payload.is_some();
            if let Some(payload) = wake_payload {
                runner = runner.with_wake_payload(payload);
            }
            // A concrete clone survives next to the ctx so the spawn can
            // give back the firing's provider accesses after the body ends.
            let runner = Arc::new(runner);
            let runner_for_close = runner.clone();
            let runner_for_bake = runner.clone();
            // Kept by the drive beside the body, so a firing that panics
            // still has its open waits withdrawn (`withdrawn_at_end`).
            let runner_for_end = runner.clone();
            let handle = runner as Arc<dyn weft_core::context::ContextHandle>;

            // Input values for provisioning: the same one bag the
            // run-time dispatch built above, so provision bodies read
            // the same view a `run` body would. Copied only for an infra
            // node's provisioning, which reads it beside its `run`: any
            // other firing hands its one bag to its body.
            let is_infra_setup_provision =
                matches!(phase, weft_core::context::Phase::InfraSetup) && node_def.requires_infra;
            let provision_input = is_infra_setup_provision.then(|| inputs.clone());
            let ctx = RunContext::new(
                project.id,
                node_id.clone(),
                node_def.node_type.clone(),
                node_def.label.clone(),
                group.run_id,
                group.frames.clone(),
                instance.cloned(),
                inputs,
                phase,
                handle,
            )
            .marked_pure(node_def.features.pure);
            // A firing cannot start when the payload that woke it is not
            // what the trigger declared, nor carry on from a pause it has
            // no state for.
            if let Some(err) = fire_payload_refusal.or(paused_before_states) {
                if !generator_ports.is_empty() {
                    let loc = FiringLocation::new(node_id.clone(), group.frames.clone());
                    retire_consumer_streams(
                        project, &loc, group.run_id, pulses, journal, replica, &mut stream_rt,
                    )
                    .await;
                }
                let mentioned = std::collections::HashSet::new();
                handle_node_failure(
                    &node_id, &mentioned, group.run_id, &group.frames, &err, None, false,
                    project, program_idx, pulses, firings, journal, replica,
                )
                .await;
                continue;
            }

            // The lifecycle event (NodeStarted or NodeResumed) was
            // already shipped earlier in this loop body, before
            // the spawn. Don't ship a second one here.

            // Spawn the node's body as a task. For infra nodes in
            // `Phase::InfraSetup` the body runs in two stages:
            //   1. `node_impl.provision_infra(infra_ctx, input)` returns
            //      an InfraSpec. Failure here = node fails with stage
            //      "provision"; downstream cascade-skips.
            //   2. The engine enqueues an Apply lifecycle command
            //      carrying the spec, via the broker, and waits on it.
            //      The supervisor that owns the project claims it,
            //      compiles and hashes the spec, picks skip / fresh /
            //      replace against the prior applied state, and applies
            //      through the platform's infra host. Failure here = node
            //      fails with stage "apply".
            //   3. `node_impl.run(ctx)` runs as usual, with
            //      `ctx.endpoint(name)` now resolving against the
            //      freshly-applied infra_node row. Failure here = node
            //      fails with stage "run"; the infra stays up
            //      (provisioned-but-run-failed sub-state).
            //
            // Otherwise, the engine picks the node body from the phase
            // plus the manifest (`node_body_for`): a trigger's
            // `setup_trigger` at TriggerSetup; at Fire, `run` for THE
            // firing trigger (its dispatch carries the wake payload)
            // while every other trigger terminates as Completed so its
            // ports close; nothing at InfraSetup either. A body sees the
            // phase only as `ctx.is_setup()` (set on the ctx above), for
            // a node whose `run` does less in a setup than in a real
            // run. The task sends its terminal back on the
            // shared `task_tx` (after any emissions it sent on the same
            // channel); the main loop applies the effect on
            // `pulses`/`firings`.
            let body = node_body_for(phase, node_def.features.is_trigger, has_wake);
            let tx = task_tx.clone();
            let loc_task = FiringLocation::new(node_id.clone(), group.frames.clone());
            let run_id_task = group.run_id;
            // node_impl is &'static dyn Node (see NodeCatalog::lookup
            // contract). No allocation or unsafe needed.
            let provision_project_id = project.id;
            let provision_project = project.id;
            let provision_place = place;
            let provision_tenant_id = tenant_id.to_string();
            let provision_clients = clients.clone();
            let provision_copy =
                weft_core::instance::copy_owner(per_instance, instance).cloned();
            let run_instance = instance.cloned();
            let baked_outputs = node_def.baked_outputs.clone();
            let failing_type = node_def.node_type.clone();
            // The firing's start goes to the journal in the background
            // (`DriveJournal`): a fast run's body starts now, whatever is
            // still being sent, unless an earlier write already failed. A
            // durable run's body starts once its start, and everything
            // before it, is on record: the nodes starting together wait on
            // one write. A pure body starts at once in a durable run too:
            // it does nothing outside the run, so a crash before its start
            // is on record runs it again with nothing to undo.
            if drive_journal.must_hand_back() {
                continue 'turn;
            }
            stop_if_poisoned(drive_journal, run_id)?;
            let on_record = (drive_journal.keeping().is_durable() && !node_def.features.pure).then(|| drive_journal.clone());
            let body = async move {
                if let Some(journal) = on_record {
                    // A write that failed stopped the drive (it hears the
                    // poison): the body never runs.
                    if journal.flush().await.is_err() {
                        return;
                    }
                }
                if let Some(provision_input) = provision_input {
                    // 1. Call the node's provision body.
                    let infra_ctx = weft_core::infra::InfraProvisionContext::new(
                        provision_project_id,
                        provision_place.clone(),
                        provision_tenant_id.clone(),
                    );
                    let spec = match node_impl.provision_infra(infra_ctx, provision_input).await {
                        Ok(s) => s,
                        Err(e) => {
                            let (message, catchable) = weft_core::context::node_failure(
                                &e,
                                &failing_type,
                                &loc_task.node_id,
                                provision_clients.needs_from,
                            );
                            let _ = tx.send(TaskMsg::Terminal {
                                loc: loc_task,
                                run_id: run_id_task,
                                outcome: NodeTaskOutcome::Failed {
                                    message: format!("provision: {message}"),
                                    catchable: catchable.map(|m| format!("provision: {m}")),
                                    gave_up_wait: matches!(e, weft_core::error::WeftError::WaitGaveUp(_)),
                                },
                                runner: Some(runner_for_bake),
                            });
                            return;
                        }
                    };

                    // 2. Enqueue an Apply lifecycle command and wait.
                    //
                    // The worker does NOT compile or hash the spec.
                    // Compile requires the per-(project, node) image
                    // tag map; only the supervisor has the role +
                    // RBAC to read it. More importantly, the
                    // supervisor derives the real `copy_id` at
                    // apply time, so any worker-side hash would be
                    // computed against a placeholder and would never
                    // match the supervisor's hash anyway.
                    //
                    // Single source of compile + hash: the
                    // supervisor reads the prior `infra_node` row,
                    // compiles the new spec with the real copy
                    // id + image tags, hashes, decides skip / fresh
                    // / replace, and executes. The worker just polls
                    // the command row for terminal state.
                    //
                    // After this returns Ok the supervisor has
                    // written `infra_node` to Running (or short-
                    // circuited on Skip), so the subsequent execute
                    // can call `ctx.endpoint_url` and get a live URL.
                    if let Err(e) = crate::context::apply_via_supervisor(
                        provision_clients.infra_state.as_ref(),
                        provision_clients.clock.as_ref(),
                        provision_project,
                        &provision_place,
                        provision_copy.as_ref(),
                        &spec,
                        run_id_task,
                    )
                    .await
                    {
                        let message = format!("apply: {e:#}");
                        let _ = tx.send(TaskMsg::Terminal {
                            loc: loc_task,
                            run_id: run_id_task,
                            outcome: NodeTaskOutcome::Failed { catchable: Some(message.clone()), message, gave_up_wait: false },
                            runner: Some(runner_for_bake),
                        });
                        return;
                    }
                    // 3. Fall through to run.
                }

                // The node body returns `()`: it fires downstream only via
                // `ctx.pulse_downstream` (emissions ride the SAME task
                // channel, applied by the loop while the task runs, and
                // always BEFORE this terminal by FIFO ordering). The
                // return just signals terminal outcome.
                //
                // The guard gives back every runtime-granted provider
                // access the body opened, on EVERY exit: awaited inline on
                // normal completion, spawned detached when the task is
                // ABORTED mid-body (a cancel), where no code after the
                // body ever runs. Runtime plumbing, not the node's job.
                let access_guard = AccessCloseGuard(Some(runner_for_close));
                let body_end = BodyEnd(Some(runner_for_bake.clone()));
                let runs_body = matches!(body, NodeBody::Run);
                let result = match body {
                    NodeBody::Run => node_impl.run(ctx).await,
                    NodeBody::SetupTrigger => node_impl.setup_trigger(ctx).await,
                    // A trigger has no infra-phase work: terminate as
                    // Completed without invoking the node, so its ports
                    // close and downstream learns nothing is coming.
                    NodeBody::SkipTrigger => Ok(()),
                };
                // The firing ends here (or pauses): anything a task the
                // body spawned does through its ctx from now on is
                // refused, so what goes out ahead of the terminal below
                // is all the firing's own, and its buses end the way it
                // returned.
                body_end.returned(&result);
                access_guard.close_now().await;
                // A body that returned leaves its baked outputs to what its
                // copy saved, wherever it emitted nothing; an infra setup
                // then saves what went out on them, which the node waits
                // for, so a setup that ended has saved its bake.
                let result = match result {
                    Ok(()) if runs_body && !baked_outputs.is_empty() => {
                        let saved = provision_clients
                            .infra
                            .baked_outputs(run_id_task, run_instance.as_ref(), &provision_place, provision_copy.as_ref())
                            .await
                            .map_err(|e| weft_core::error::WeftError::NodeFailed(format!("read what this node's infra saved for its baked outputs: {e:#}")));
                        match saved {
                            Ok(saved) if is_infra_setup_provision => {
                                // Only what went out: the broker merges it over
                                // what the copy saved, so a value the infra
                                // pushed meanwhile is never written over with
                                // the one read before it.
                                let mut baked = runner_for_bake.baked_emitted();
                                baked.retain(|port, _| baked_outputs.contains(port));
                                provision_clients
                                    .infra_state
                                    .save_bake(run_id_task, &provision_place, provision_copy.as_ref(), baked)
                                    .await
                                    .map(|()| saved)
                                    .map_err(|e| weft_core::error::WeftError::NodeFailed(format!("save what this node baked: {e:#}")))
                            }
                            other => other,
                        }
                    }
                    Ok(()) => Ok(Default::default()),
                    Err(e) => Err(e),
                };
                let outcome = match result {
                    Ok(saved) => NodeTaskOutcome::Completed { saved },
                    Err(weft_core::error::WeftError::Suspended { token }) => {
                        NodeTaskOutcome::Waiting(token)
                    }
                    Err(weft_core::error::WeftError::WaitSkipped(_)) => {
                        NodeTaskOutcome::Skipped(weft_core::run::skip::SkipReason::WaitSkipped)
                    }
                    Err(e) => {
                        let (message, catchable) =
                            weft_core::context::node_failure(&e, &failing_type, &loc_task.node_id, provision_clients.needs_from);
                        NodeTaskOutcome::Failed {
                            message: runner_for_bake.with_infra_hint(message),
                            catchable,
                            gave_up_wait: matches!(e, weft_core::error::WeftError::WaitGaveUp(_)),
                        }
                    }
                };
                let _ = tx.send(TaskMsg::Terminal {
                    loc: loc_task,
                    run_id: run_id_task,
                    outcome,
                    runner: Some(runner_for_bake),
                });
            };
            if let Some(ended) = bodies.start(FiringLocation::new(node_id.clone(), group.frames.clone()), runner_for_end, body) {
                ended_at_start |= body_ended(ended, &task_tx, run_id);
            }
        }

        // Reap every body that ended, without blocking: its firing
        // location is free to fire again on the next readiness scan
        // (the hold above reads `bodies.firings()`), and a body that
        // PANICKED gets its synthetic Failed terminal queued here, so
        // the drain right below applies it before that location can
        // read free with a record still Running.
        let mut freed_a_location = ended_at_start;
        while let Some(ended) = bodies.try_next() {
            freed_a_location |= body_ended(ended, &task_tx, run_id);
        }

        // Drain the task channel in FIFO order: each `Emission`
        // (a still-running node's `pulse_downstream` / `close_port`)
        // postprocesses into downstream pulses and records the mentioned
        // port, keeping the emitting node Running; each `Terminal` closes
        // the firing's record and closes every UNmentioned output port.
        // Because emissions and the terminal share this one ordered
        // channel, a node's emissions are always drained before its
        // terminal, so the close-unmentioned sweep sees the complete
        // mentioned set (no emit-then-return race). Non-blocking so we
        // keep dispatching newly-ready nodes next iteration.
        let progress = apply_task_msgs(
            &mut task_rx,
            run_id,
            project,
            program_idx,
            pulses,
            firings,
            forgetting,
            journal,
            replica,
            &mut stream_rt,
            /* is_cancel = */ false,
        )
        .await;

        // What the run is done with goes: a firing that ended (its body
        // reaped, and every message it sent drained just above), and an
        // iteration nothing inside is alive in any more. A body still
        // running keeps its firing's record: its late messages read it.
        forget_what_ended(program, run_id, forgetting, pulses, firings, kicked, scopes, &mut carried, loop_runtime, |at| bodies.runs(at));

        // A bus nobody is on ends once no node can still join it: read
        // after every message of the turn is applied, so a pulse a body
        // sent before it returned is on its wire here.
        let ended_a_bus = end_unattended_buses(&mut unattended, waits, bus_coordinator, pulses, firings, loop_runtime);

        if progress || ended_a_bus {
            idled_since_progress = false;
            continue;
        }

        // A turn that dispatched anything is progress even with no
        // task message behind it: a boundary, a loop boundary or a
        // skip fires synchronously and leaves its pulses and kicks for
        // the NEXT turn's readiness scan. Ending the run here would
        // judge a table that still holds that work (a loop body's
        // roots kicked by the LoopIn this turn are invisible to
        // `check_completion`, and the run would read Completed).
        //
        // A turn that freed a location is the same: this turn's scan
        // ran BEFORE the reap and held the location, and a task whose
        // terminal was drained on an earlier turn (it sent, then was
        // reaped a turn later) wakes nothing else. Without the rescan
        // the pulses pending at that location wait for an unrelated
        // event, and the stuck-check below could read the remaining
        // tasks (parked on what this location would produce) as a
        // deadlock and close their buses.
        if dispatched_this_turn || freed_a_location {
            idled_since_progress = false;
            continue;
        }

        // No progress from draining. Check: is anything still in flight?
        if bodies.is_empty() {
            // A run handed back with work left reads as unfinished here,
            // which it is not: the next worker runs that work. A run that
            // paused for a hand-back is the same.
            if held_back || matches!(paused, Some(RunOutcome::HandedBack)) {
                return Ok(RunOutcome::HandedBack);
            }
            return terminate(project, program_idx, pulses, firings, kicked, scopes).await;
        }

        // Stuck-check: we drained twice without progress, with at least
        // one idle-wait in between. The first no-progress pass might just
        // mean "tasks were scheduled but not yet polled by the runtime";
        // the second no-progress pass AFTER the select woke us means the
        // wake came from somewhere (a task finishing, an emission landing,
        // a cancellation), we re-drained, and there is STILL nothing for
        // the loop to do but the in-flight tasks are alive. That is the
        // definition of stuck: the only way out is to close every live
        // bus (every waiting `wait_for` / cursor wakes with `Closed` /
        // `None`, bodies fail or recover, tasks complete, the next
        // iteration drains them and either dispatches more work or
        // terminates).
        //
        // Persisted external suspensions (firings-table
        // `WaitingForInput` records, via `waiting_count`) keep us out of
        // stuck: those waits fire from outside the worker.
        // Stuck only when EVERY in-flight task is a node run
        // PARKED and CAUGHT UP on a bus, AND a bus exists to be the wait.
        // `deadlock_provable(bodies.firings())` evaluates this under ONE
        // liveness-map-lock snapshot: liveness is keyed by node run
        // `(node_id, frames)`, so a node holding several bus
        // registrations is ONE entry and counts ONCE. The close fires
        // only when every running body is a parked-and-caught-up node:
        // every live task accounted for as a parked node, none off
        // computing.
        //
        // Why per-node liveness is sound where a per-waiter count was
        // not: the old shape counted loose cursor waits and required
        // "waiters >= in_flight", which assumed one waiter per task and
        // broke the moment a node held two concurrent waits (e.g. a
        // `select!` over two cursors): a live node could be closed under.
        // Keying on the node run removes that assumption: a node's
        // several concurrent waits collapse to one entry that counts as
        // parked only when EVERY wait is parked.
        //
        // ASSUMPTION (holds for every catalog node today): all of a node
        // run's bus waits run on that node's single dispatched
        // task. A node body that `tokio::spawn`ed a detached task holding
        // a bus cursor would attribute that helper's wait to the node
        // while the helper is invisible to `in_flight`, breaking the
        // count. No node does this; if a future pattern needs detached
        // bus work, enforce "bus waits run on the node's own task" loudly
        // (compare `tokio::task::id()` in `enter_wait`) rather than
        // letting the count silently skew.
        //
        // GROUND-TRUTH, not a scheduler race. A node parked on a bus is
        // counted only when it has observed that bus's CURRENT settled
        // append generation (read under the bus's log lock inside
        // `deadlock_provable`). A receiver woken by a send but still
        // unpolled in another worker thread's queue has NOT re-evaluated
        // since the send bumped the generation, so it reads as behind
        // and is excluded; a receiver mid-evaluation (observed recorded,
        // search not yet returned) reads as not-parked and is excluded;
        // a receiver that RESOLVED but has not yet acted has left its
        // wait, dropping the parked count below `in_flight`. Every case
        // keeps the count short of `in_flight`, suppressing the close BY
        // CONSTRUCTION (the generation recorded BEFORE the evaluation,
        // the parked flag set only AFTER every pre-park re-check failed),
        // not by betting the runtime polled the woken peer in time. So
        // the per-node generation check alone is exact at the check
        // instant: no across-park generation comparison and no grace
        // timer are needed. Every liveness transition (`enter_wait` /
        // `exit_wait` / `observed` / `parked` / `on_append`) re-wakes
        // this loop, so once a lagging node re-checks (consuming the
        // message or re-parking caught-up) we re-evaluate promptly.
        //
        // A genuine deadlock (every in-flight task a node parked having
        // observed its bus's final generation) still closes; a live
        // send-then-park fails the count or the generation, so a
        // conversation is never torn down under a peer not yet scheduled.
        // The driver's identity set of in-flight firings (not a count:
        // a stray wait entry from a task a body detached must be able
        // to neither block nor fake the proof).
        //
        // The `task_rx.is_empty()` guard is load-bearing and its ORDER
        // matters: the tracker proves every in-flight task is parked;
        // the driver must separately prove its own queue is drained,
        // because a task can enqueue a message (a take, an emission)
        // and re-park between this loop's drain and this check. Once
        // `deadlock_provable` holds, no task can enqueue anything NEW,
        // and anything enqueued before a task parked is already
        // visible to `is_empty()`, so "provable AND empty" is sound;
        // checking emptiness first would not be.
        // The idle pass is reached by YIELDING, never by waiting for a
        // notification: the deadlock's last liveness wake can be
        // consumed by an iteration that also drained a message (which
        // resets `idled_since_progress`), and after that nothing ever
        // notifies again, since every park already happened. The extra
        // pass exists only so a freshly-dispatched batch is not judged
        // before the runtime has touched it at all; CORRECTNESS rests
        // on `deadlock_provable` being exact at the check instant (see
        // its comment), not on any scheduling the yield provides.
        if !idled_since_progress {
            idled_since_progress = true;
            tokio::task::yield_now().await;
            continue;
        }
        // Every running node waits, and nothing new can come from inside
        // the run (a run already pausing only waits for its bodies to
        // go). What happens then is `pause_decision`'s: pause them all, hold
        // quiet for something outside, or unwind a deadlock.
        let all_waiting = paused.is_none() && waits.deadlock_provable(bodies.firings()) && task_rx.is_empty();
        let on_caller = all_waiting && answering.is_some_and(|conn| conn.waited_on(waits));
        // The clock is read only once the run is quiet, so a run that never
        // goes quiet pays nothing for its stay before a pause.
        let decision = all_waiting.then(|| {
            let now = clients.clock.now();
            let since = *quiet_since.get_or_insert(now);
            pause_decision(AllWaiting {
                pausable: waits.all_pausable(bodies.firings()),
                can_pause,
                suspended: waiting_count(firings) > 0,
                held: waits.held().pending(),
                on_caller,
                draining,
                quiet_for: now.saturating_duration_since(since),
                pause_after_secs: settings.pause_after_secs(),
                cap_left: hold.and_then(|hold| hold.cap_left(now_unix())),
            })
        });
        hold_clock.quiet(decision == Some(WhenAllWait::Hold));
        // How long the run stays quiet on its worker before it pauses.
        let mut pause_in = None;
        match decision {
            Some(WhenAllWait::StayQuiet(left)) => pause_in = Some(left),
            Some(WhenAllWait::Pause) => {
                tracing::info!(
                    target: "weft_engine::run_driver",
                    %run_id, waiting = bodies.len(), draining,
                    "every running node waits, and what moves the run next is outside it: pausing them all"
                );
                paused = Some(if draining { RunOutcome::HandedBack } else { RunOutcome::Stalled });
                waits.pause();
            }
            Some(WhenAllWait::Unwind) => {
                // A bus nobody is on, held open only by a node that waits on
                // its end itself, ends first; only a run still stuck after
                // that is unwound.
                if !end_every_unattended_bus(&mut unattended, bus_coordinator) {
                    unwind(run_id, bus_coordinator, &mut stream_rt, waits, bodies.len());
                }
                // Don't `continue`: the waiting tasks wake in other tokio
                // tasks; their results arrive on `result_rx` shortly.
                // Falling through to the idle-wait yields to the runtime
                // and wakes on the first real event. The select below
                // re-sets `idled_since_progress = true` on return; the
                // next iteration drains the unwound state.
            }
            Some(WhenAllWait::Hold) | None => {}
        }
        if decision.is_none() {
            quiet_since = None;
        }

        // At least one in-flight task. Block until something happens:
        // a task terminates, a still-running node emits (a live bus
        // node sits in its loop and only emits; its task never ends
        // until the bus closes, so we MUST also wake on an emission
        // or we'd hang), a task entered a bus wait (so the next
        // stuck-check can fire if everything else is silent), or
        // cancellation.
        //
        // Arm `on_wait_change` BEFORE entering the select. `Notify::notified`
        // only registers a waiter the first time the future is polled,
        // and `notify_waiters` stores no permit; a wait that fires
        // between future creation and first poll would be lost without
        // pin + `enable()`.
        //
        // DO NOT poll `result_rx` here: `recv().await` would consume
        // the message and drop it. Same reason we don't drain emit_rx
        // here; we just need the wakeup.
        // Ask for the run's answers ONLY while a wait is held, or a firing
        // an earlier pause parked waits while others run, and never once
        // the run is pausing. Not armed otherwise, so the common path never
        // asks.
        ask.while_needed(paused.is_none() && (waits.held().pending() || waiting_count(firings) > 0));
        // The hold runs out `holdSecs` after the run went quiet, and never
        // runs while its caller is on the line: a run keeps its caller
        // for as long as it stays, so neither the caller is cut nor the
        // run unwound.
        let caller_connected = caller.is_some_and(|conn| conn.is_connected());
        let quiet_for = hold_clock.quiet_for(!caller_connected);
        // While quiet, its worker may stop ticking, vouching for itself
        // until the run's hold gives up, or, with its caller on the line,
        // until the caller's caps end.
        if let Some(hold) = hold {
            match (caller_connected, answering) {
                (true, Some(conn)) => hold.quiet_on_caller(conn.session_ends(), can_pause),
                _ => hold.quiet(hold_clock.until().map(|until| crate::run_holds::Quiet { until, can_pause })),
            }
        }
        let held_out = hold_clock.held_out(quiet_for);
        tokio::pin!(held_out);
        let pause_due = async {
            match pause_in {
                Some(left) => clients.clock.sleep(left).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(pause_due);
        // A caller hanging up: the next turn looks again at whether the
        // run can pause (a connected caller is what kept it from it).
        let watch_caller = caller_connected;
        let caller_left = async {
            match caller {
                Some(conn) => conn.disconnected().await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(caller_left);
        // Whether anything moved in the run while the loop waited, which
        // starts the hold clock over.
        let mut moved = true;
        let resume_poll = ask.next();
        tokio::pin!(resume_poll);

        // The drive waits for news, not for its own rows, which keep
        // going out in the background; a failed one stops it. A held
        // answer does not wait for the news either.
        if drive_journal.must_hand_back() {
            continue 'turn;
        }
        stop_if_poisoned(drive_journal, run_id)?;
        let_answer_go(answering);
        let on_wait_change = waits.wait_notified();
        tokio::pin!(on_wait_change);
        on_wait_change.as_mut().enable();
        tokio::select! {
            // A journal write failed while the drive waited: the next turn
            // stops it, rather than waiting on a long node first.
            () = drive_journal.poisoned() => {}
            // A node still running answered the caller: the next turn lets
            // the answer go.
            () = answer_held(answering) => {}
            answers = resume_poll.as_mut() => {
                // The broker answered. An answer to a suspended node (its
                // record waits on the token: after what the nodes already
                // sent is applied, a node that just suspended is one)
                // resumes it in place (`resume_answered`). Any other answer
                // is a held wait's: it goes to the node's call holding it,
                // which records it, or is kept for the node that holds or
                // suspends on it next (`HeldWaits::answer`). The ask answers
                // only once it found some, so something moved.
                let answers = answers?;
                moved = !answers.is_empty();
                apply_task_msgs(
                    &mut task_rx, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                    &mut stream_rt, false,
                )
                .await;
                let suspended_on = suspended_tokens(firings);
                let (suspended, held): (Vec<_>, Vec<_>) = answers.into_iter().partition(|answer| suspended_on.contains(answer.token.as_str()));
                for answer in held {
                    waits.held().answer(&answer.token, answer.answer);
                }
                resume_answered(
                    drive_journal, fold, run_id, replica, suspended, firings, pulses, kicked, &mut carried,
                )
                .await?;
            }
            ended = bodies.next() => {
                // A body ended while the loop was idle (the body in place
                // runs here, while the loop waits for news). Bodies a
                // cancel stops are dropped in `cancel_cleanup` and never
                // surface in this idle-wait arm.
                if let Some(ended) = ended {
                    // The next turn rescans regardless, so the freed
                    // flag is not needed here.
                    body_ended(ended, &task_tx, run_id);
                }
            }
            task_msg = task_rx.recv() => {
                // Apply this message immediately (we consumed it from the
                // channel, so we can't let it drop). It may be an Emission
                // OR a Terminal; both must be handled here, not just
                // emissions. Then loop. Live path, not cancel: a bad-shape
                // emission journals NodeFailed.
                if let Some(msg) = task_msg {
                    apply_one_task_msg(
                        msg, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                                &mut stream_rt,
                        /* is_cancel = */ false,
                    )
                    .await;
                }
            }
            _ = on_wait_change.as_mut() => {
                // A task's wait state changed (bus, stream pull, or
                // delivery). The next drain will re-check stuck.
            }
            // Nothing moved in the run for its `holdSecs` while only the
            // outside could end its waits: the waits it holds are given
            // up, each failing its node's call. With none held, what keeps
            // the worker is nodes that cannot pause waiting on each other
            // beside a suspended one, and the run unwinds the way a
            // deadlock does (the buses close, else the stream waits fail).
            () = held_out.as_mut(), if quiet_for.is_some() => {
                moved = false;
                if hold_clock.ran_out(waits.held(), unsuspendable(caller, settings)) == 0 {
                    unwind(run_id, bus_coordinator, &mut stream_rt, waits, bodies.len());
                }
            }
            // The run stayed quiet for its `pauseAfterSecs` with nothing
            // left but to pause: the next turn pauses it.
            () = pause_due.as_mut(), if pause_in.is_some() => {
                moved = false;
            }
            () = caller_left.as_mut(), if watch_caller => {}
            // Asked to let go: the next turn starts nothing new, if the
            // run can be suspended.
            () = handed_back(hand_back), if !asked => {}
            // Overdue: the next turn stops what still runs, if the run can
            // be suspended.
            () = hand_back_overdue(hand_back), if !overdue => {}
            _ = cancellation.cancelled() => {
                tracing::info!(
                    target: "weft_engine::run_driver",
                    run_id = %run_id,
                    "cancellation observed at idle wait; exiting Cancelled"
                );
                cancel_cleanup(
                    &mut bodies,
                    &mut task_rx,
                        firings,
                    forgetting,
                        run_id,
                    project,
                    program_idx,
                    pulses,
                    journal,
                    replica,
                    loop_runtime,
                    kicked,
                    scopes,
                    &mut stream_rt,
                    &recorded_cancel_cause(cancellation).to_string(),
                    phase,
                    given,
                )
                .await;
                return Ok(RunOutcome::Cancelled { cause: recorded_cancel_cause(cancellation) });
            }
        }
        // We just unblocked from the idle-wait. The next no-progress
        // drain is allowed to declare stuck.
        idled_since_progress = true;
        // Moved: the stay before a pause starts over the next time the run
        // goes quiet. Moved, or its hold gave up: quiet no longer, before it
        // goes on (telling a run that is not quiet changes nothing).
        if moved {
            quiet_since = None;
        }
        if hold_clock.turned(moved) {
            if let Some(hold) = hold {
                hold.quiet(None);
            }
        }
    }
}

/// What the drive knows once every running node waits and nothing inside
/// the run can move it ([`pause_decision`]).
#[derive(Debug, Clone, Copy)]
struct AllWaiting {
    /// Every one of them waits in a call that holds its firing's state
    /// (`weft_core::liveness::wait_or_pause`), so each can pause.
    pausable: bool,
    /// The run can pause (`crate::held_waits::unsuspendable`).
    can_pause: bool,
    /// A firing is parked on a wait, whose answer comes from outside: one
    /// an earlier drive paused, its wait not answered yet.
    suspended: bool,
    /// A wait is held in a node's call, whose answer comes from outside.
    held: bool,
    /// A node waits on its caller's next message.
    on_caller: bool,
    /// The run was asked to let go of its worker.
    draining: bool,
    /// How long the run has been this quiet.
    quiet_for: std::time::Duration,
    /// How long a run that can pause stays quiet before it pauses
    /// (`weft_core::run_settings::RunSettings::pause_after_secs`).
    pause_after_secs: u32,
    /// What is left of the run's own cap on this worker, which ends the
    /// run rather than pausing it (`crate::run_holds::RunHold::cap_left`).
    cap_left: Option<std::time::Duration>,
}

/// What the drive does with a run whose every running node waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhenAllWait {
    /// Tell every waiting call to pause: each saves its state and
    /// suspends, and the run lets go of its worker.
    Pause,
    /// Stay on the worker, quiet, this much longer before pausing:
    /// anything that moves the run meanwhile (an answer reaching a held
    /// wait, a message) carries it on with no pause.
    StayQuiet(std::time::Duration),
    /// Wait, quiet, for something outside to end a wait (an answer, the
    /// caller's next message), the hold clock running.
    Hold,
    /// Nothing outside can end any of the waits: a deadlock, unwound.
    Unwind,
}

/// A run pauses in one way: every waiting call can pause, the run can,
/// and what moves it next comes from outside it (a held wait's answer, a
/// parked firing's) once it stayed quiet for its `pauseAfterSecs`
/// (`StayQuiet` until then), so a run fed by frequent messages takes each
/// one where it is instead of pausing and being picked up again for every
/// one. A hand-back pauses it now, for the next worker to wait, and the
/// stay never runs past `cap_left`. A run that cannot pause holds for what
/// outside can still end a wait (a held wait, a parked firing's answer,
/// the caller); with nothing outside, it is a deadlock.
fn pause_decision(at: AllWaiting) -> WhenAllWait {
    if at.pausable && at.can_pause && (at.held || at.suspended || at.draining) {
        if at.draining {
            return WhenAllWait::Pause;
        }
        let left = std::time::Duration::from_secs(at.pause_after_secs.into()).saturating_sub(at.quiet_for);
        let left = at.cap_left.map_or(left, |cap_left| left.min(cap_left));
        if left.is_zero() {
            WhenAllWait::Pause
        } else {
            WhenAllWait::StayQuiet(left)
        }
    } else if at.held || at.suspended || at.on_caller {
        WhenAllWait::Hold
    } else {
        WhenAllWait::Unwind
    }
}

/// Whether every pulse of `ids` pending at `node` lands on one of its
/// stream inputs: items of a stream, which never dispatch a firing that
/// already took its inputs.
fn only_stream_items(pulses: &PulseTable, program_idx: &ProgramIndex, node: &str, ids: &[uuid::Uuid]) -> bool {
    let streams = program_idx.stream_inputs(node);
    !streams.is_empty()
        && pulses.get(node).is_some_and(|bucket| {
            bucket.iter().filter(|pulse| ids.contains(&pulse.id)).all(|pulse| streams.contains(&pulse.target_port))
        })
}

// Layer 1: what the drive does once every running node waits.
#[cfg(test)]
mod pause_decision_tests {
    use super::*;

    fn secs(n: u64) -> std::time::Duration {
        std::time::Duration::from_secs(n)
    }

    /// A pausable run whose every node waits, one of them on an answer
    /// from outside held in its call, quiet for `quiet_for` with a window
    /// of ten seconds.
    fn waiting(quiet_for: u64) -> AllWaiting {
        AllWaiting {
            pausable: true,
            can_pause: true,
            suspended: false,
            held: true,
            on_caller: false,
            draining: false,
            quiet_for: secs(quiet_for),
            pause_after_secs: 10,
            cap_left: None,
        }
    }

    /// A run stays quiet for its window, then pauses; `0` pauses at once.
    #[test]
    fn a_run_stays_quiet_for_its_window_then_pauses() {
        assert_eq!(pause_decision(waiting(0)), WhenAllWait::StayQuiet(secs(10)));
        assert_eq!(pause_decision(waiting(4)), WhenAllWait::StayQuiet(secs(6)));
        assert_eq!(pause_decision(waiting(10)), WhenAllWait::Pause);
        assert_eq!(pause_decision(waiting(11)), WhenAllWait::Pause);
        assert_eq!(pause_decision(AllWaiting { pause_after_secs: 0, ..waiting(0) }), WhenAllWait::Pause, "0 pauses at once");
        let parked = AllWaiting { held: false, suspended: true, ..waiting(3) };
        assert_eq!(pause_decision(parked), WhenAllWait::StayQuiet(secs(7)), "a firing an earlier drive parked waits on the outside too");
    }

    /// A hand-back pauses the run now, whatever its window says, and the
    /// window never outlasts the run's cap on its worker.
    #[test]
    fn a_hand_back_or_the_cap_cuts_the_window() {
        assert_eq!(pause_decision(AllWaiting { draining: true, ..waiting(0) }), WhenAllWait::Pause);
        assert_eq!(pause_decision(AllWaiting { draining: true, held: false, ..waiting(0) }), WhenAllWait::Pause, "a hand-back pauses it for the next worker");
        assert_eq!(pause_decision(AllWaiting { cap_left: Some(secs(3)), ..waiting(2) }), WhenAllWait::StayQuiet(secs(3)));
        assert_eq!(pause_decision(AllWaiting { cap_left: Some(secs(30)), ..waiting(2) }), WhenAllWait::StayQuiet(secs(8)));
        assert_eq!(pause_decision(AllWaiting { cap_left: Some(secs(0)), ..waiting(0) }), WhenAllWait::Pause, "the cap is here");
    }

    #[test]
    fn a_run_that_cannot_pause_holds_for_the_outside() {
        assert_eq!(pause_decision(AllWaiting { can_pause: false, ..waiting(60) }), WhenAllWait::Hold);
        let caller = AllWaiting { can_pause: false, held: false, on_caller: true, ..waiting(0) };
        assert_eq!(pause_decision(caller), WhenAllWait::Hold, "a connected caller keeps the run on its worker");
        let unpausable_wait = AllWaiting { pausable: false, ..waiting(60) };
        assert_eq!(pause_decision(unpausable_wait), WhenAllWait::Hold, "a wait that cannot pause keeps it too");
    }

    #[test]
    fn nodes_waiting_only_on_each_other_are_a_deadlock() {
        assert_eq!(pause_decision(AllWaiting { held: false, ..waiting(0) }), WhenAllWait::Unwind);
        assert_eq!(pause_decision(AllWaiting { held: false, can_pause: false, ..waiting(0) }), WhenAllWait::Unwind);
    }
}

/// Every running node waits on another and nothing outside will end it:
/// unwind, gentlest first. Closing the buses may unwind everything
/// (bodies return, producers terminate and close their streams
/// naturally), so the harder resolution only fires when a re-proven
/// deadlock remains with no live bus: every pending delivery wait errors
/// its producer, every live feed is poisoned so its consumer's next pull
/// errors. Each stage re-proves on a later turn before escalating.
fn unwind(
    run_id: RunId,
    bus_coordinator: &crate::context::BusCoordinator,
    stream_rt: &mut StreamRuntime,
    waits: &crate::wait_tracker::WaitTracker,
    in_flight: usize,
) {
    if bus_coordinator.has_live_buses() {
        tracing::warn!(
            target: "weft_engine::run_driver",
            run_id = %run_id,
            in_flight,
            parked_nodes = waits.parked_nodes_count(),
            "every in-flight task is parked with no unconsumed activity; closing all buses to unwind"
        );
        bus_coordinator.close_all(weft_core::bus::StreamEnd::Failed {
            error: "the run was stuck: every node on this bus waited on another, and nothing could \
                    end the wait"
                .into(),
        });
    } else {
        tracing::warn!(
            target: "weft_engine::run_driver",
            run_id = %run_id,
            in_flight,
            parked_nodes = waits.parked_nodes_count(),
            "every in-flight task is parked on a stream pull or an emission delivery that can never resolve; failing them loudly"
        );
        stream_rt.resolve_deadlock(
            "run deadlocked: every running node is waiting on a stream item or an emission delivery that no \
             remaining node can ever produce",
        );
    }
}

/// The tokens the run's suspended nodes wait on, by their records.
fn suspended_tokens(firings: &NodeFiringTable) -> std::collections::HashSet<&str> {
    firings.waiting().filter_map(|e| e.callback_id.as_deref()).collect()
}

/// `delivered` with the run instance's values in `node`'s `@instance_filled`
/// fields at `place` (see `weft_core::instance::fill_literals`), or why the
/// node cannot run: it takes values an instance provides, and the run is for
/// no instance. The dispatcher refuses such a run before it is born; this is
/// the floor under that.
fn with_instance_values(
    node: &weft_core::project::NodeDefinition,
    place: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    values: &weft_core::instance::InstanceValues,
    mut delivered: serde_json::Map<String, Value>,
) -> Result<serde_json::Map<String, Value>, String> {
    if weft_core::instance::instance_filled_fields(node).next().is_none() {
        return Ok(delivered);
    }
    if instance.is_none() {
        return Err(format!(
            "node '{place}' takes values its instance provides, and this run is for no instance; \
             start it for one (`--instance <id>`)"
        ));
    }
    weft_core::instance::fill_literals(node, &mut delivered, values.get(place));
    Ok(delivered)
}

/// A closure is structural: it tells the consumer "this port is dead
/// at this frame stack". A user-emitted null is data; these are
/// different signals and the consumer-side `skip` distinguishes them.
///
/// Build (and apply to `pulses`) the CLOSURE pulses on every output port
/// the firing did NOT mention. Nothing is journaled for them: the
/// firing's terminal row is the fact, and the fold sweeps the same
/// ports from the program with the same emission id
/// (`terminal_sweep_emission`, derived from the firing's location), so
/// the closures it puts on the wires are these.
#[allow(clippy::too_many_arguments)]
fn build_unmentioned_closures(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &NodeFiringTable,
    // Why the firing produced nothing on these ports, when that was a
    // failure: the node's own, or the one a skip inherited from the
    // input that closed on it (still naming the node that broke).
    // Rides every closure (a generator port's as a failed stream end,
    // so the consumer's pull gets the error). None when the node
    // completed or declined.
    failure: Option<&weft_core::pulse::Failure>,
) -> Vec<weft_core::run::PulseEmission> {
    // Loop boundary nodes (LoopIn, LoopOut) fire many times during a
    // loop's lifetime and their outward output ports must NOT
    // auto-close on per-iteration firings. The engine closes them at
    // the loop's outward emit moment. This is the single chokepoint
    // for the language layer's "close unmentioned" semantics, so the
    // skip lives here, NOT in `run::postprocess` (which stays
    // generic over node types).
    if is_loop_boundary_node(project, node_id) {
        return Vec::new();
    }
    let emission_id = terminal_sweep_emission(node_id, frames);
    let mut emissions = Vec::new();
    // Teardown cannot propagate (this is the shared tail of failure /
    // skip / completion paths), so a sweep error (node or port missing
    // from the project: corrupt compiled shape) is logged at error
    // level here, the single chokepoint. Partial emissions still land
    // so whatever closed before the error reaches downstream.
    if let Err(e) = close_unmentioned_downstream(
        node_id, mentioned, emission_id, run_id, frames, project, pulses, program_idx, &mut emissions,
        failure, &firing_at(firings, node_id, run_id, frames).map(|record| record.closed_output_ports.clone()).unwrap_or_default(),
    ) {
        tracing::error!(
            target: "weft_engine::run_driver",
            node = node_id,
            error = %e,
            "closure sweep failed; downstream consumers of this node's unclosed ports \
             will neither fire nor skip"
        );
    }
    emissions
}

/// Whether `node_id` is a `LoopIn` or `LoopOut` boundary node. Lives
/// in the engine, not in `weft-core`, so the language layer stays
/// generic over node types.
fn is_loop_boundary_node(project: &ProjectDefinition, node_id: &str) -> bool {
    project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.node_type == weft_core::project::boundary_types::LOOP_IN || n.node_type == weft_core::project::boundary_types::LOOP_OUT)
        .unwrap_or(false)
}

/// LoopIn / LoopOut firing handler. The engine treats these two
/// boundary node types as built-in: no catalog impl, no spawned task.
/// LoopIn instantiates / looks up the `LoopInstance` (the pure
/// derivation in `weft_core::run::loop_runtime::instantiate`) and
/// launches the iterations due now. LoopOut records per-iteration
/// writes via `LoopRuntime` and, on termination, emits assembled
/// gather lists + final carry values on its outer outputs at the
/// parent frame stack. Every state change the fold needs is journaled
/// as a loop row; the values are never on the rows, the fold reads
/// them off the boundary firings' pulses.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_loop_boundary_firing(
    node_def: &weft_core::project::NodeDefinition,
    group: &weft_core::run::ready::ReadyGroup,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
) -> Result<(), String> {
    use weft_core::primitive::LoopTerminationReason;

    let key = loops::instance_key(node_def, &group.frames, group.run_id)?;
    let group_id = key.group_id.clone();

    // A boundary firing over an instance that already terminated FAILED
    // must route to the FAILURE path, not the idle-complete path:
    // completing it would journal NodeCompleted and silently lose the
    // loop's failure status (the run would fold with zero Failed
    // nodes). Two reachable cases: a crash-resume where LoopTerminated
    // {Failed} landed but the boundary's NodeFailed did not, AND a live
    // same-worker-life straggler (a sibling iteration's LoopOut still
    // queued after another iteration's failure marked the instance
    // Failed). Both route to `handle_loop_boundary_failure`, whose
    // already-terminated branch ships NodeFailed and no second
    // LoopTerminated. Only FAILED is gated: idle replays after
    // OverExhausted / DoneVoted / MaxItersReached / Cancelled are
    // legitimate and stay Ok.
    if matches!(
        loop_runtime.get(&key).and_then(|inst| inst.terminated),
        Some(LoopTerminationReason::Failed)
    ) {
        return Err(format!(
            "loop '{group_id}' already terminated Failed; boundary {} firing routed to the \
             failure path",
            node_def.id
        ));
    }

    if node_def.node_type == weft_core::project::boundary_types::LOOP_IN {
        let firing = instantiate(loop_runtime, node_def, project, &group.received, &group.frames, group.run_id)?;
        if firing.first_instantiation {
            crate::context::record_from_replica(
                journal,
                weft_journal::RunEvent::LoopInstantiated {
                    run_id: group.run_id,
                    group_id: group_id.clone(),
                    parent_frames: group.frames.clone(),
                    at_unix: now_unix(),
                },
                replica,
            )
            .await;
        }
        // Decide which iterations to launch now. Launches derive from
        // the instance's (journal-backed) `launched` set, NEVER from
        // `first_instantiation`: on crash-resume the LoopIn re-fires
        // with first_instantiation=false while the rehydrated instance
        // may have launched only a subset (or none) of its iterations,
        // and deciding from first_instantiation would silently skip
        // the rest (the loop "completes" without running). The inverse
        // hazard is covered too: an index in `launched` has its
        // `LoopIterationLaunched` row in the journal, so the fold put
        // its body pulses on the wires and re-launching it would
        // duplicate them.
        let (already_launched, inst_terminated) = loop_runtime
            .get(&key)
            .map(|inst| (inst.launched.clone(), inst.terminated.is_some()))
            .unwrap_or_default();
        let to_launch: Vec<u32> = if inst_terminated {
            Vec::new()
        } else if firing.is_stream {
            // Stream-driven: iterations launch when items arrive (the
            // engine's routing pass feeds `stream_push`), never here.
            Vec::new()
        } else if firing.config.parallel {
            // Parallel is list-driven by construction here: the
            // compiler rejects `parallel` without `over`, and a stream
            // returned above, so a resolved cap always exists.
            let cap = firing.iter_cap.ok_or_else(|| {
                "parallel loop without a resolved iteration cap (compiled shape drifted)"
                    .to_string()
            })?;
            (0..cap).filter(|i| !already_launched.contains(i)).collect()
        } else if already_launched.is_empty() && firing.iter_cap != Some(0) {
            vec![0]
        } else {
            // Sequential with a launch already recorded: subsequent
            // launches are handled by LoopOut's
            // `LoopAdvance::LaunchNext` path, not LoopIn.
            Vec::new()
        };
        if firing.iter_cap == Some(0) {
            // Zero-iteration loop: terminate immediately. The runtime
            // assembles the outward payload (a length-0 list for EVERY
            // declared gather port, initial carry values) and marks
            // the instance terminated so a later cancel walk doesn't
            // double-emit closures on the same parent_frames.
            //
            // Reason mirrors the live-path logic in `record_loop_out`:
            // when iter_cap was capped at max_iters (including the
            // user writing `maxIters: 0`), the binding constraint is
            // MaxItersReached. Otherwise OverExhausted (empty over).
            let reason = if firing.config.max_iters == firing.iter_cap {
                LoopTerminationReason::MaxItersReached
            } else {
                LoopTerminationReason::OverExhausted
            };
            match loop_runtime.emit_outward(&key, reason)? {
                LoopAdvance::EmitOutward { reason, gather, carry, .. } => {
                    emit_loop_outward(
                        project, program_idx, pulses, journal, replica, &key, gather, carry, reason,
                        loop_runtime,
                    )
                    .await?;
                }
                // Idle = the instance was already terminated: a
                // crash-resume replay of this LoopIn after the
                // LoopTerminated row landed. The outward pulses are
                // already journaled; emitting again would duplicate
                // them downstream.
                LoopAdvance::Idle => {}
                LoopAdvance::LaunchNext { .. } => {
                    return Err(format!(
                        "LoopIn '{}': emit_outward returned LaunchNext for a \
                         zero-iteration loop; LoopRuntime invariant violated",
                        node_def.id,
                    ));
                }
            }
        } else {
            for index in to_launch {
                launch_iteration(
                    project, program_idx, pulses, journal, replica, &key, index, None, loop_runtime,
                    kicked, scopes,
                )
                .await?;
            }
        }
        Ok(())
    } else {
        // LoopOut firing for one iteration.
        let index = group
            .frames
            .last()
            .and_then(weft_core::frames::Frame::loop_index)
            .ok_or_else(|| format!("LoopOut '{}' fired with no iteration frame on top of its stack", node_def.id))?;
        let inst_config = loop_runtime
            .get(&key)
            .map(|inst| inst.config.clone())
            .ok_or_else(|| {
                format!(
                    "LoopOut '{}' fired at parent_frames={:?} index={index} but no LoopInstance exists; \
                     LoopIn must fire before LoopOut",
                    node_def.id, key.parent_frames,
                )
            })?;
        // Read the writes off the firing BEFORE the journal write, so
        // a refused firing (a drifted config) leaves no row behind.
        let writes = classify_loop_out(node_def, &inst_config, &group.received)?;

        // Journal the firing ONLY when the runtime will record it as
        // new state (instance live, index not already fired). The fold
        // applies `LoopOutFired` unconditionally, so a row for a
        // firing the live runtime refused (post-termination) or
        // already holds (crash-resume replay) would diverge the
        // rehydrated instance from the live one.
        if loop_runtime.loop_out_is_new(&key, index)? {
            crate::context::record_from_replica(
                journal,
                weft_journal::RunEvent::LoopOutFired {
                    run_id: group.run_id,
                    group_id: group_id.clone(),
                    parent_frames: key.parent_frames.clone(),
                    index,
                    at_unix: now_unix(),
                },
                replica,
            )
            .await;
        }

        let advance = loop_runtime.record_loop_out(
            project, &key, index, writes.gather_writes, writes.carry_writes, writes.done_vote,
        )?;
        match advance {
            LoopAdvance::Idle => Ok(()),
            LoopAdvance::LaunchNext { index: next, stream_item: Some(item) } => {
                launch_stream_iteration(
                    project, program_idx, pulses, journal, replica, &key, next, item, loop_runtime,
                    stream_rt, kicked, scopes,
                )
                .await
            }
            LoopAdvance::LaunchNext { index: next, stream_item: None } => {
                launch_iteration(
                    project, program_idx, pulses, journal, replica, &key, next, None, loop_runtime,
                    kicked, scopes,
                )
                .await
            }
            LoopAdvance::EmitOutward { reason, gather, carry, .. } => {
                emit_loop_outward(
                    project, program_idx, pulses, journal, replica, &key, gather, carry, reason,
                    loop_runtime,
                )
                .await?;
                // A stream-driven loop that terminated (a done vote,
                // the max cap) may leave buffered items behind; they
                // can never launch, so drop them durably.
                let loop_in_id = weft_core::project::loop_in_id(&group_id);
                drop_loop_stream_leftovers(
                    &key, &loop_in_id, group.run_id, pulses, journal, replica,
                    loop_runtime, stream_rt,
                )
                .await;
                Ok(())
            }
        }
    }
}

/// Launch one iteration: put the body's pulses on the wires and open the
/// iteration's copy of the loop, kicking the body's roots (the pure
/// derivation in `weft_core::run::loop_runtime::launch_iteration`), and
/// journal the launch row. The row is the
/// ONE fact of the launch; on crash-resume `launched.contains(index)`
/// is true iff the fold put the body pulses on the wires from that
/// row, so `record_loop_out`'s already-launched guard correctly
/// returns Idle instead of double-launching.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn launch_iteration(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    key: &weft_core::primitive::LoopInstanceKey,
    index: u32,
    stream_item: Option<LoopStreamItem>,
    loop_runtime: &mut LoopRuntime,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
) -> Result<(), String> {
    let stream_pulse = stream_item.as_ref().map(|item| item.pulse.to_string());
    loops::launch_iteration(
        loop_runtime, key, index, stream_item, project, program_idx, pulses, scopes, kicked, now_unix(),
    )?;
    crate::context::record_from_replica(
        journal,
        weft_journal::RunEvent::LoopIterationLaunched {
            run_id: key.run_id,
            group_id: key.group_id.clone(),
            parent_frames: key.parent_frames.clone(),
            index,
            stream_pulse,
            at_unix: now_unix(),
        },
        replica,
    )
    .await;
    Ok(())
}

/// Ship a terminated loop's outward payload: the assembled gather
/// lists + final carry values onto LoopOut's outer outputs at the
/// parent frame stack (the pure derivation in
/// `weft_core::run::loop_runtime::emit_loop_outward`), then journal
/// `LoopTerminated`; the fold puts the same pulses on the wires from
/// that row.
///
/// On postprocess failure nothing partial ships (postprocess
/// pre-validates before touching state): the outward ports are CLOSED
/// instead so downstream skips cascade rather than deadlock, the
/// termination is journaled as `Failed` (and the RAM instance
/// re-marked to match, keeping live and rehydrated state identical),
/// and the error propagates so the boundary firing lands in the
/// standard failure path.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_loop_outward(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    key: &weft_core::primitive::LoopInstanceKey,
    gather: HashMap<String, Vec<Option<Arc<serde_json::Value>>>>,
    carry: HashMap<String, Arc<serde_json::Value>>,
    reason: weft_core::primitive::LoopTerminationReason,
    loop_runtime: &mut LoopRuntime,
) -> Result<(), String> {
    match loops::emit_loop_outward(key, gather, carry, &BTreeMap::new(), project, program_idx, pulses) {
        Ok((_output, _emissions)) => {
            journal_loop_terminated(journal, replica, key, reason).await;
            Ok(())
        }
        Err(e) => {
            let failed = weft_core::primitive::LoopTerminationReason::Failed;
            // The instance was terminated by the advance that got us
            // here; a failed outward emit re-marks it Failed (the
            // documented override), in the one place `terminated` is
            // ever rewritten.
            if let Some(inst) = loop_runtime.get_mut(key) {
                inst.terminated = Some(failed);
            }
            close_loop_outward(key, project, program_idx, pulses, failed);
            journal_loop_terminated(journal, replica, key, failed).await;
            Err(e)
        }
    }
}

async fn journal_loop_terminated(
    journal: &dyn JournalClient,
    replica: &str,
    key: &weft_core::primitive::LoopInstanceKey,
    reason: weft_core::primitive::LoopTerminationReason,
) {
    crate::context::record_from_replica(
        journal,
        weft_journal::RunEvent::LoopTerminated {
            run_id: key.run_id,
            group_id: key.group_id.clone(),
            parent_frames: key.parent_frames.clone(),
            reason,
            at_unix: now_unix(),
        },
        replica,
    )
    .await;
}

/// Failure tail for an inline boundary firing (LoopIn / LoopOut). On
/// top of the standard failure bookkeeping (Failed status + NodeFailed
/// + the generic sweep, which deliberately no-ops for loop boundary
/// nodes), a failed boundary firing kills its WHOLE loop: close the
/// loop's outward surface so downstream skips cascade instead of
/// deadlocking, and when an instance was already journaled, terminate
/// it (RAM + a `LoopTerminated{Failed}` row) so cancel walks and
/// resumed workers both see a dead loop rather than a live instance
/// nothing will ever drive again.
///
/// Which row the fold derives the outward closures from depends on
/// the three cases: a LIVE instance is terminated FIRST
/// (`LoopTerminated{Failed}` carries the teardown; a crash between it
/// and the NodeFailed leaves the instance dead and the closures
/// delivered, self-consistent), then NodeFailed; a NO-instance failure
/// (config parse, missing carry seed, iter-count errors) has only the
/// NodeFailed, and the fold closes the loop's outward surface from
/// that row when it finds no instance; an ALREADY terminated instance
/// means the teardown was journaled by `emit_loop_outward`'s failure
/// path, so the NodeFailed adds nothing.
#[allow(clippy::too_many_arguments)]
async fn handle_loop_boundary_failure(
    node_def: &weft_core::project::NodeDefinition,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
    loop_runtime: &mut LoopRuntime,
) {
    use weft_core::primitive::LoopTerminationReason;

    let key = match loops::instance_key(node_def, frames, run_id) {
        Ok(key) => key,
        Err(e) => {
            // Unreachable for LoopIn/LoopOut (the compiler always sets
            // the boundary). Still fail the node loudly with no loop
            // teardown.
            tracing::error!(
                target: "weft_engine::loop_runtime",
                node = %node_def.id,
                error = %e,
                "boundary failure: cannot tear down its loop"
            );
            let mentioned = std::collections::HashSet::new();
            handle_node_failure(
                &node_def.id, &mentioned, run_id, frames, err, None, false,
                project, program_idx, pulses, firings, journal, replica,
            )
            .await;
            return;
        }
    };
    match loop_runtime.get(&key).map(|inst| inst.terminated.is_some()) {
        // LIVE instance: dead marker and closures first, then the
        // boundary's own failure.
        Some(false) => {
            loop_runtime
                .terminate(&key, LoopTerminationReason::Failed)
                .expect("the instance was found live just above");
            close_loop_outward(&key, project, program_idx, pulses, LoopTerminationReason::Failed);
            journal_loop_terminated(journal, replica, &key, LoopTerminationReason::Failed).await;
        }
        // ALREADY terminated: the prior LoopTerminated closed the
        // outward surface.
        Some(true) => {}
        // NO instance: the outward surface closes now, in RAM; the
        // fold does the same from the NodeFailed row below.
        None => {
            close_loop_outward(&key, project, program_idx, pulses, LoopTerminationReason::Failed);
        }
    }

    // Boundary firings never emit through the task channel, so the
    // generic mentioned set is empty (and the sweep no-ops for a loop
    // boundary anyway).
    let mentioned = std::collections::HashSet::new();
    handle_node_failure(
        &node_def.id, &mentioned, run_id, frames, err, None, false, project, program_idx, pulses, firings,
        journal, replica,
    )
    .await;
}

/// Fail a firing: mark Failed, ship `NodeFailed`, close every
/// output port the firing did NOT already emit on. The single failure
/// path (real `execute` error, dispatch-time error, unknown node type,
/// output type-check failure, the worker going away mid-firing). Ports
/// already emitted KEEP their values: a node that fired A then crashed
/// before firing B still has A's value live downstream, only B gets
/// closed. "Stuff already sent stays sent" (the principle that drives
/// the closure semantics).
///
/// `catchable` is what the node's `_error` output carries if the
/// failure is caught (`None` for a failure that never is). Caught
/// (`weft_core::context::caught_failure`: `_error` is wired and the
/// firing has no stream open), the node instead puts the message on `_error`
/// and completes, its other unmentioned ports closing as at any
/// completion. `gave_up_wait`: the firing failed on a
/// wait it gave up at a time limit, which an uncaught failure's record
/// and its `NodeFailed` row carry (`NodeFiring::gave_up_wait`).
#[allow(clippy::too_many_arguments)]
async fn handle_node_failure(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    catchable: Option<String>,
    gave_up_wait: bool,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    let error_wired = program_idx
        .get_outgoing(project, node_id, frames)
        .iter()
        .any(|edge| edge.source_handle.as_deref() == Some(weft_core::context::ERROR_PORT));
    let opened_a_stream = firing_at(firings, node_id, run_id, frames).is_some_and(|record| {
        let node = project.nodes.iter().find(|n| n.id == node_id);
        record.opened_a_stream(
            node.into_iter()
                .flat_map(|n| n.outputs.iter())
                .filter(|p| p.port_type.opens_a_stream())
                .map(|p| p.name.as_str()),
        )
    });
    if let Some(message) = weft_core::context::caught_failure(catchable, opened_a_stream, error_wired) {
        if catch_into_error_port(
            node_id, mentioned, run_id, frames, err, message, project, program_idx, pulses,
            firings, journal, replica,
        )
        .await
        {
            return;
        }
    }
    // One time for the record and its row, so a fold of the row keeps
    // the same first failure as this drive (`first_failed`).
    let at_unix = now_unix();
    mark_failed(firings, node_id, run_id, frames, err, gave_up_wait, at_unix);
    // The closures on every unmentioned port go on the wires in RAM;
    // the NodeFailed row is the fact the fold sweeps the same ports
    // from.
    let failure = weft_core::pulse::Failure::at(project, node_id, frames, err);
    build_unmentioned_closures(
        node_id, mentioned, run_id, frames, project, program_idx, pulses, firings, Some(&failure),
    );
    ship_node_failed(journal, replica, run_id, node_id, frames, err, gave_up_wait, at_unix).await;
}

/// The caught half of [`handle_node_failure`]: put `message` on the
/// node's `_error` output, then complete its firing exactly as a body
/// that returned would (the `PortEmitted` row, then `NodeCompleted`,
/// every other unmentioned port closing). Answers false, having changed
/// nothing, when the value cannot go out (the record is gone, or the
/// emission is refused), so the caller fails the firing as it would have.
///
/// `_error` already mentioned means this catch already ran and its
/// worker died between the `PortEmitted` row and `NodeCompleted` (the
/// body never touches `_error`: the ctx refuses it).
/// The refold rebuilt the emission, so the firing only completes, with
/// no second value on the port.
#[allow(clippy::too_many_arguments)]
async fn catch_into_error_port(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    message: String,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) -> bool {
    let port = weft_core::context::ERROR_PORT;
    if firing_at(firings, node_id, run_id, frames).is_none() {
        return false;
    }
    if mentioned.contains(port) {
        complete_caught(node_id, mentioned, run_id, frames, project, program_idx, pulses, firings, journal, replica)
            .await;
        return true;
    }
    let bag: OutputBag = std::iter::once((port.to_string(), Arc::new(serde_json::Value::String(message)))).collect();
    let emission_id = body_emission(node_id, frames);
    let mut emissions = Vec::new();
    if let Err(e) = postprocess_output(
        node_id, &bag, emission_id, run_id, frames, project, pulses, program_idx, &mut emissions,
    ) {
        tracing::error!(
            target: "weft_engine::run_driver",
            %run_id, node = %node_id, error = %e,
            "the caught failure could not go out on '{port}'; failing the node instead"
        );
        return false;
    }
    tracing::warn!(
        target: "weft_engine::run_driver",
        %run_id, node = %node_id, frames = ?frames, error = %err,
        "the node failed; its failure went to the wired '{port}' output"
    );
    // The node's log says so too, where `weft logs` and the inspector
    // read a failure: a caught one is otherwise only a value on a port.
    crate::context::record_from_replica(
        journal,
        weft_journal::RunEvent::LogLine {
            run_id,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            level: "warn".to_string(),
            message: format!("failed, handed to the '{port}' output: {err}"),
            at_unix_ms: Some(crate::now_unix_ms()),
            seq: None,
            at_unix: crate::now_unix(),
        },
        replica,
    )
    .await;
    ship_port_emissions(journal, replica, run_id, emission_id, node_id, frames, &bag).await;
    let mut mentioned = mentioned.clone();
    mentioned.insert(port.to_string());
    if let Some(record) = firing_at_mut(firings, node_id, run_id, frames) {
        record.mentioned_ports.insert(port.to_string());
    }
    complete_caught(node_id, &mentioned, run_id, frames, project, program_idx, pulses, firings, journal, replica)
        .await;
    true
}

/// What a completed firing's copy saved for its baked outputs
/// (`weft_core::infra::bake`), put on each baked output the body emitted
/// nothing on, as the body's own emission would have been: a value
/// downstream reads like any other, and the record says it came out of
/// this firing. One the body emitted on keeps the body's value; one with
/// nothing saved closes with the rest. A saved value the program no
/// longer takes (its output's type changed since) fails the firing,
/// naming the fix.
#[allow(clippy::too_many_arguments)]
async fn emit_saved_values(
    node_id: &str,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    saved: std::collections::BTreeMap<String, serde_json::Value>,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) -> Result<(), String> {
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else { return Ok(()) };
    let mentioned = mentioned_ports(firings, node_id, run_id, frames);
    let bag: OutputBag = saved
        .into_iter()
        .filter(|(port, _)| node.baked_outputs.contains(port) && !mentioned.contains(port))
        .map(|(port, value)| (port, Arc::new(value)))
        .collect();
    if bag.is_empty() {
        return Ok(());
    }
    let emission_id = body_emission(node_id, frames);
    let mut emissions = Vec::new();
    postprocess_output(node_id, &bag, emission_id, run_id, frames, project, pulses, program_idx, &mut emissions).map_err(|e| {
        format!(
            "what this node's infra saved for its baked outputs no longer fits them ({e}); \
             `weft infra upgrade` makes them again"
        )
    })?;
    ship_port_emissions(journal, replica, run_id, emission_id, node_id, frames, &bag).await;
    if let Some(record) = firing_at_mut(firings, node_id, run_id, frames) {
        record.mentioned_ports.extend(bag.keys().cloned());
    }
    Ok(())
}

/// The end of a caught failure, once `error` carries the message: the
/// firing completes, every other unmentioned port closing.
#[allow(clippy::too_many_arguments)]
async fn complete_caught(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    mark_completed(firings, node_id, run_id, frames);
    build_unmentioned_closures(
        node_id, mentioned, run_id, frames, project, program_idx, pulses, firings, None,
    );
    ship_node_completed(journal, replica, run_id, node_id, frames).await;
}

/// The output ports a firing has put on a wire or closed, off its
/// record: what its termination sweep leaves alone.
fn mentioned_ports(
    firings: &NodeFiringTable,
    node_id: &str,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
) -> std::collections::HashSet<String> {
    firing_at(firings, node_id, run_id, frames).map(|e| e.mentioned_ports.clone()).unwrap_or_default()
}

/// An emission the engine refused (over the stream cap, a bad-shape
/// value, a close on an undeclared port): nothing was committed, and a
/// producer parked on its delivery gets the error. Live, the firing
/// fails now. In the cancel drain the firing is about to be ended
/// Cancelled by the cancel walk, which owns its status and its
/// closures, so the error is only logged: a NodeFailed there would
/// show a shape error on a run the user cancelled.
#[allow(clippy::too_many_arguments)]
async fn refuse_emission(
    loc: &FiringLocation,
    err: String,
    delivery: Option<&DeliveryGate>,
    held: &crate::held_waits::HeldWaits,
    is_cancel: bool,
    run_id: weft_core::RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    if let Some(gate) = delivery {
        gate.fail(err.clone());
    }
    if is_cancel {
        tracing::warn!(
            target: "weft_engine::run_driver",
            %run_id, node = %loc.node_id, frames = ?loc.frames, error = %err,
            "an emission was refused while the run was being cancelled; the firing ends Cancelled"
        );
        return;
    }
    // The body still runs: what it waits on, or waits on next, ends with
    // this failure now.
    held.fail_firing(loc, &err);
    let mentioned = mentioned_ports(firings, &loc.node_id, run_id, &loc.frames);
    handle_node_failure(
        &loc.node_id, &mentioned, run_id, &loc.frames, &err, None, false, project, program_idx, pulses,
        firings, journal, replica,
    )
    .await;
}

/// Skip a firing (a scope/condition decided it shouldn't run):
/// mark Skipped, ship `NodeSkipped`, close downstream. Same
/// downstream teardown as a failure, just a
/// different lifecycle event.
#[allow(clippy::too_many_arguments)]
async fn handle_node_skip(
    node_id: &str,
    run_id: weft_core::RunId,
    frames: &weft_core::frames::LoopFrames,
    reason: &weft_core::run::skip::SkipReason,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    mark_skipped(firings, node_id, run_id, frames, reason);
    // A node inside a scope that did not run carries NO closures: the
    // scope's teardown closed every wire leaving it and kicked every
    // node inside into its own `ScopeSkipped` skip (nested scopes
    // included). A cascade here would only manufacture a second firing
    // for nodes the teardown already settled.
    if matches!(reason, weft_core::run::skip::SkipReason::ScopeSkipped { .. }) {
        ship_node_skipped(journal, replica, run_id, node_id, frames, reason).await;
        return;
    }
    // A LoopIn that skipped (its loop gated off) takes its loop down: the
    // generic sweep below deliberately no-ops for loop boundaries
    // (per-iteration firings must not auto-close the outward ports). The
    // loop's skip surface is its outward ports at the loop's own frames;
    // the NodeSkipped row is the fact the fold closes them from. Then
    // everything inside is kicked into a skip that says its loop did not
    // run.
    let skipped_loop = program_idx
        .node(project, node_id)
        .filter(|n| n.node_type == weft_core::project::boundary_types::LOOP_IN)
        .and_then(|n| n.group_boundary.as_ref())
        .map(|gb| gb.group_id.clone());
    if let Some(loop_id) = skipped_loop {
        tear_down_loop(project, program_idx, &loop_id, frames, reason, run_id, now_unix(), pulses, scopes, kicked);
        ship_node_skipped(journal, replica, run_id, node_id, frames, reason).await;
        return;
    }
    // A skipped node's body never runs, so it never emitted on ANY
    // output port: close every port. Same shape as a pre-dispatch
    // failure: empty mentioned set means "close everything". A skip
    // that was itself the consequence of a failure closes with that
    // failure, so the next node down still reads a failure.
    let mentioned = std::collections::HashSet::new();
    build_unmentioned_closures(
        node_id, &mentioned, run_id, frames, project, program_idx, pulses, firings,
        reason.inherited_failure(),
    );
    ship_node_skipped(journal, replica, run_id, node_id, frames, reason).await;
}

/// Gives a firing's runtime-granted provider accesses back on every exit
/// of the node task. Normal completion calls [`Self::close_now`] (awaited
/// inline, before the terminal ships); an ABORT (cancel) drops the guard
/// mid-body, and the drop spawns the close detached, because nothing after
/// the abort point ever runs. A close that cannot even be spawned (no
/// runtime) is fine to skip silently here: the credential's own window is
/// the documented backstop for exactly this case.
struct AccessCloseGuard(Option<Arc<crate::context::RunnerHandle>>);

impl AccessCloseGuard {
    async fn close_now(mut self) {
        if let Some(runner) = self.0.take() {
            runner.close_opened_accesses().await;
        }
    }
}

impl Drop for AccessCloseGuard {
    fn drop(&mut self) {
        if let Some(runner) = self.0.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move { runner.close_opened_accesses().await });
            }
        }
    }
}

/// Ends a firing's body on every exit of the node task
/// (`RunnerHandle::end_body`): with the body's own result when it returns
/// ([`Self::returned`]); failed when the guard drops mid-body instead (a
/// panic, or an abort on a cancel), so a bus the body held never reads
/// as a stream that finished.
struct BodyEnd(Option<Arc<crate::context::RunnerHandle>>);

impl BodyEnd {
    fn returned(mut self, result: &weft_core::error::WeftResult<()>) {
        if let Some(runner) = self.0.take() {
            runner.end_body(weft_core::bus::body_end(result));
        }
    }
}

impl Drop for BodyEnd {
    fn drop(&mut self) {
        if let Some(runner) = self.0.take() {
            runner.end_body(Some(weft_core::bus::StreamEnd::Failed {
                error: "the node stopped before its body returned (it panicked, or its run was cancelled)".into(),
            }));
        }
    }
}

/// Single cancellation cleanup path. Called from BOTH cancellation
/// entry points (loop-top check and idle-wait branch) so they have
/// identical drain semantics. The order matters:
///
/// 1. `bodies.shutdown().await` stops every body: the one in place is
///    dropped where it is, and every spawned task is driven to its abort
///    point. A task mid-`record_event` finishes its write; a task
///    waiting on `cursor.next()` wakes via the abort and unwinds.
///    Without this, a journal write racing with the outer cancel
///    path could flip the final state to Completed AFTER we wrote
///    NodeCancelled (last-write-wins fold).
///
/// 2. Drain the task channel (one FIFO pass). In-channel
///    `pulse_downstream` emissions land on their records' mentioned
///    sets (and their downstream pulses) BEFORE the cancel walk
///    computes the "unmentioned" closure set, so a port the body just
///    emitted on isn't wrongly closed as unmentioned ("stuff already
///    sent stays sent"). In-channel terminals are NOT applied: the
///    user cancelled, so every firing still open when the cancel was
///    observed ends Cancelled, including one whose body happened to
///    return during the abort window (a Completed or Failed there
///    would show a firing outliving its cancelled run).
///
/// 3. `cancel_open_firings`: every open firing ends Cancelled, in RAM
///    and in the journal, its unmentioned ports closed with the
///    reason, then every live loop instance is cancelled the same
///    way.
#[allow(clippy::too_many_arguments)]
async fn cancel_cleanup(
    bodies: &mut Bodies,
    task_rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    loop_runtime: &mut LoopRuntime,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    stream_rt: &mut StreamRuntime,
    // The cause in words, what every cancelled record carries as its
    // error (the same text the `NodeCancelled` rows will carry).
    reason: &str,
    phase: weft_core::context::Phase,
    given: &weft_core::project::Given,
) {
    // An in-flight node's future is aborted immediately; it needs no window
    // to wrap up first. A paid call's answer may be read on a task of a
    // client library's own, which this abort does not reach: the metering
    // tap watches the run's cancel flag, already tripped, and cuts such a
    // call itself, booking it and releasing the run's ending
    // (`crate::metering`). A call whose answer lives in the node's future
    // is booked by the tap's drop.
    // 1. Stop every body.
    bodies.shutdown().await;
    // 2. Drain the task channel in one FIFO pass (see the doc above).
    drain_task_msgs_for_cancel(
        task_rx, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
        stream_rt,
    )
    .await;
    // Every delivery wait dies with its task (the abort above): fail
    // the gates so no producer future that survives to be polled once
    // more reads a phantom success. The feeds die with the
    // StreamRuntime.
    stream_rt.fail_all_gates("the run was cancelled");
    // What the stopped bodies took off their streams goes on record
    // before their cancel, as a body that ends puts it there.
    for at in firings.open().map(|e| FiringLocation::new(e.node_id.clone(), e.frames.clone())).collect::<Vec<_>>() {
        if let Some(taken) = crate::context::takes_to_record(&stream_rt.feeds_of(&at), run_id, &at) {
            if let Err(err) = journal.record_event(&taken, Some(replica)).await {
                tracing::warn!(target: "weft_engine", error = %err, node = %at.node_id, "failed to journal what a cancelled firing took off its streams");
            }
        }
    }
    // 3. The cancel walk.
    cancel_open_firings(
        firings, pulses, kicked, scopes, loop_runtime, run_id, project, program_idx, journal, replica,
        reason, phase, given,
    )
    .await;
}

/// The cancel walk: every firing of `run_id` still open ends Cancelled,
/// in RAM (as the fold ends it from the row) and in the journal (one
/// `NodeCancelled` per firing, carrying `reason`), and every output
/// port it never mentioned closes with the reason as the stream end a
/// consumer's pull reads. Then every live loop instance is cancelled
/// and its outward surface closed (`cancel_loop_instances`). Idempotent:
/// a walk over a table with nothing open writes nothing, so the
/// terminal path runs it unconditionally.
#[allow(clippy::too_many_arguments)]
async fn cancel_open_firings(
    firings: &mut NodeFiringTable,
    pulses: &mut PulseTable,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    loop_runtime: &mut LoopRuntime,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    journal: &dyn JournalClient,
    replica: &str,
    reason: &str,
    phase: weft_core::context::Phase,
    given: &weft_core::project::Given,
) {
    let open: Vec<FiringLocation> = firings.open().map(|e| FiringLocation::new(e.node_id.clone(), e.frames.clone())).collect();
    for loc in open {
        let mentioned = mentioned_ports(firings, &loc.node_id, run_id, &loc.frames);
        mark_cancelled(firings, &loc.node_id, run_id, &loc.frames, reason);
        let failure = weft_core::pulse::Failure::at(project, &loc.node_id, &loc.frames, reason);
        build_unmentioned_closures(
            &loc.node_id, &mentioned, run_id, &loc.frames, project, program_idx, pulses, firings,
            Some(&failure),
        );
        let event = weft_journal::RunEvent::NodeCancelled {
            run_id,
            node_id: loc.node_id.clone(),
            frames: loc.frames.clone(),
            reason: reason.to_string(),
            at_unix: now_unix(),
        };
        if let Err(err) = journal.record_event(&event, Some(replica)).await {
            tracing::warn!(
                target: "weft_engine",
                error = %err,
                node = %loc.node_id,
                "failed to journal NodeCancelled"
            );
        }
    }
    // No partial outward emit for a cancelled loop (a real outward
    // emit would be a lie about how many iterations completed): its
    // LoopOut's outward ports close, at the instance's own
    // parent_frames, so inner instances inside the cancelled scope
    // close at the right level too.
    cancel_loop_instances(loop_runtime, run_id, project, program_idx, pulses, journal, replica).await;
    // The closures just put on the wires get the pass any turn's would
    // (the fold runs it after every row it closes from): a scope they
    // decide is decided, and what the run will never take settles. No
    // waiter is parked on any of it: the gates all failed above, or never
    // existed on the refetch-kill path.
    settle_table(project, program_idx, given, phase, run_id, now_unix(), pulses, scopes, kicked);
}

/// On cancellation, walk every non-terminated `LoopInstance` for this
/// run, mark it cancelled, and emit closures on every outward output
/// port of its LoopOut at `parent_frames`. Idempotent: an instance
/// already terminated is skipped and gets no second row.
pub(crate) async fn cancel_loop_instances(
    loop_runtime: &mut LoopRuntime,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
) {
    use weft_core::primitive::LoopTerminationReason;
    // Only what this call cancelled: an instance cancelled by an earlier
    // walk already has its row, and a second one would be noise.
    for key in loop_runtime.cancel_inside(&Vec::new(), run_id) {
        // Journaling the cancellation is what makes it durable across
        // resume: a refold without it rebuilds the instance as live
        // (terminated=None) and the engine drives it again. The fold
        // closes the outward surface from the row, as this does in RAM.
        close_loop_outward(&key, project, program_idx, pulses, LoopTerminationReason::Cancelled);
        journal_loop_terminated(journal, replica, &key, LoopTerminationReason::Cancelled).await;
    }
}

/// A `Pulse` closure's end-of-stream reading: clean unless it carries a
/// producer failure, whose text names the node that broke.
fn stream_end_of_closure(p: &weft_core::pulse::Pulse) -> StreamEnd {
    match &p.failure {
        Some(failure) => StreamEnd::Failed { error: failure.to_string() },
        None => StreamEnd::Finished,
    }
}

/// One action the stream-routing scan decided for a pulse. Collected
/// first (immutable scan of the pulse table), applied after, so the
/// mutation phase never fights the scan's borrows.
enum RouteAction {
    /// An item for a running consumer's feed.
    FeedItem { loc: FiringLocation, port: String, pulse: uuid::Uuid, value: Arc<Value> },
    /// The stream's end for a running consumer's feed.
    FeedClose { loc: FiringLocation, port: String, pulse: uuid::Uuid, end: StreamEnd },
    /// An item for a live stream-driven loop.
    LoopItem { key: weft_core::primitive::LoopInstanceKey, loop_in: String, pulse: uuid::Uuid, value: Arc<Value> },
    /// The stream's end for a live stream-driven loop.
    LoopClose { key: weft_core::primitive::LoopInstanceKey, loop_in: String, pulse: uuid::Uuid, end: StreamEnd },
    /// The consumer is gone (terminal record / terminated loop): the
    /// pulse can never be taken. Absorb it; a delivery gate on a data
    /// item fails loudly.
    Drop { loc: FiringLocation, pulse: uuid::Uuid, closed: bool },
}

/// Route pending generator pulses whose consumer is already in flight
/// (or already gone). Runs every loop iteration BEFORE readiness, so a
/// routed pulse never re-dispatches a running consumer. Pulses whose
/// consumer has not been dispatched yet are left Pending on purpose:
/// the first of them IS what dispatches the consumer. Returns how many
/// pulses it ACTED ON (routed, absorbed, or failed on); the caller
/// counts any of that as drive progress.
#[allow(clippy::too_many_arguments)]
async fn route_stream_pulses(
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    loop_runtime: &mut LoopRuntime,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
) -> usize {
    // Scan phase: decide an action per routable pulse.
    let mut actions: Vec<RouteAction> = Vec::new();
    for (node, generator_ports) in program_idx.stream_consumers(project) {
        let Some(bucket) = pulses.get(&node.id) else { continue };
        let is_loop_in = node.node_type == weft_core::project::boundary_types::LOOP_IN;
        for p in bucket.iter() {
            if p.run_id != run_id
                || p.status != PulseStatus::Pending
                || !generator_ports.contains(&p.target_port)
            {
                continue;
            }
            let loc = FiringLocation::new(node.id.clone(), p.frames.clone());
            if is_loop_in {
                let Some(gb) = node.group_boundary.as_ref() else { continue };
                let key = weft_core::primitive::LoopInstanceKey {
                    group_id: gb.group_id.clone(),
                    parent_frames: p.frames.clone(),
                    run_id,
                };
                match loop_runtime.get(&key) {
                    None => continue, // LoopIn not fired yet: the pulse dispatches it.
                    Some(inst) if inst.terminated.is_some() => {
                        actions.push(RouteAction::Drop { loc, pulse: p.id, closed: p.closed });
                    }
                    Some(_) => {
                        if p.closed {
                            actions.push(RouteAction::LoopClose {
                                key,
                                loop_in: node.id.clone(),
                                pulse: p.id,
                                end: stream_end_of_closure(p),
                            });
                        } else {
                            actions.push(RouteAction::LoopItem {
                                key,
                                loop_in: node.id.clone(),
                                pulse: p.id,
                                value: p.value.clone(),
                            });
                        }
                    }
                }
            } else {
                // The firing record's terminal state is checked FIRST: a
                // record can go terminal while its feed is still in the
                // runtime (a dispatch that failed after creating its
                // feeds), and a feed must never outrank the record's
                // truth. Terminal -> drop; live feed -> route; neither
                // -> the pulse is what dispatches the consumer, leave
                // it Pending.
                if firing_already_terminal(firings, forgetting, project, program_idx, &node.id, run_id, &p.frames) {
                    actions.push(RouteAction::Drop { loc, pulse: p.id, closed: p.closed });
                } else if let Some(feed) = stream_rt.feed_for(&loc, &p.target_port) {
                    // A pausing feed takes no end: the close waits on its
                    // wire for the feed of the resumed body.
                    if p.closed && feed.is_paused() {
                        continue;
                    }
                    if p.closed {
                        actions.push(RouteAction::FeedClose {
                            loc,
                            port: p.target_port.clone(),
                            pulse: p.id,
                            end: stream_end_of_closure(p),
                        });
                    } else {
                        actions.push(RouteAction::FeedItem {
                            loc,
                            port: p.target_port.clone(),
                            pulse: p.id,
                            value: p.value.clone(),
                        });
                    }
                }
            }
        }
    }

    // Apply phase. Each arm re-checks the state it depends on at apply
    // time: an earlier action in the same batch can terminate a loop
    // or retire a feed, and acting on the scan's stale view would
    // strand pulses `Routed` forever (nothing re-scans a non-Pending
    // pulse).
    let acted_on = actions.len();
    for action in actions {
        match action {
            RouteAction::FeedItem { loc, port, pulse, value } => {
                let Some(feed) = stream_rt.feed_for(&loc, &port).cloned() else {
                    // The feed retired since the scan (its consumer
                    // terminated earlier in this batch): the item can
                    // never be taken.
                    let reason = consumer_gone_reason(project, &loc);
                    consume_stream_pulses(
                        &[pulse], &loc, run_id, pulses, journal, replica, stream_rt,
                        AbsorbKind::Skipped { reason: &reason },
                    )
                    .await;
                    continue;
                };
                match feed.push(pulse, value) {
                    Ok(()) => set_pulse_status(pulses, &loc.node_id, pulse, PulseStatus::Routed),
                    Err(e) => {
                        // Item after the stream's end: an ordering bug
                        // this router's arrival-order contract should
                        // make impossible. The stream is corrupt, so
                        // the CONSUMING FIRING FAILS (a run that
                        // silently lost an item must never complete);
                        // the pulse itself is absorbed (it can never be
                        // delivered).
                        let err = format!(
                            "stream '{port}' into '{}' is corrupt: {e}; failing the \
                             consumer (engine ordering bug)",
                            loc.node_id
                        );
                        consume_stream_pulses(
                            &[pulse], &loc, run_id, pulses, journal, replica,
                            stream_rt, AbsorbKind::Skipped { reason: &err },
                        )
                        .await;
                        // Retire the feeds BEFORE failing the record,
                        // like every other terminal path: abandoning
                        // them errors a mid-pull body loudly right now
                        // (instead of parking it until the deadlock
                        // resolver fires with a generic message) and
                        // unregisters the markers.
                        retire_consumer_streams(
                            project, &loc, run_id, pulses, journal, replica, stream_rt,
                        )
                        .await;
                        // The body still runs: what it waits on, or waits
                        // on next, ends with this failure now.
                        stream_rt.held_waits().fail_firing(&loc, &err);
                        let mentioned = mentioned_ports(firings, &loc.node_id, run_id, &loc.frames);
                        handle_node_failure(
                            &loc.node_id, &mentioned, run_id, &loc.frames, &err, None, false,
                            project, program_idx, pulses, firings, journal, replica,
                        )
                        .await;
                    }
                }
            }
            RouteAction::FeedClose { loc, port, pulse, end } => {
                match stream_rt.feed_for(&loc, &port).cloned() {
                    // The firing takes the end as it is routed: off its
                    // wire now, on record with the firing's next save or
                    // its end, like the items it took.
                    // A pausing feed refuses it: the close stays on its wire
                    // for the feed the resumed body gets.
                    Some(feed) => {
                        if feed.close_taken(pulse, end) {
                            apply_stream_item_taken(&loc, pulse, pulses, stream_rt);
                        }
                    }
                    // Its consumer is gone since the scan: absorbed
                    // durably now.
                    None => {
                        consume_stream_pulses(
                            &[pulse], &loc, run_id, pulses, journal, replica, stream_rt,
                            AbsorbKind::Taken,
                        )
                        .await;
                    }
                }
            }
            RouteAction::LoopItem { key, loop_in, pulse, value } => {
                // Staleness guard: the instance can have terminated
                // since the scan (an earlier item's launch failure in
                // this same batch). Its leftover sweep already ran, so
                // pushing would buffer an item nobody will ever drain.
                // A MISSING instance is a different fact: the scan
                // proved it existed and nothing removes instances, so
                // that is an engine bug and fails the loop loudly
                // instead of masquerading as ordinary termination.
                match loop_runtime.get(&key) {
                    None => {
                        fail_loop_from_stream(
                            project, program_idx, pulses, firings, journal, replica,
                            run_id, &key, &loop_in,
                            &format!(
                                "stream item routed to loop '{}' with no LoopInstance; \
                                 engine bug",
                                key.group_id
                            ),
                            loop_runtime, stream_rt,
                        )
                        .await;
                        continue;
                    }
                    Some(inst) if inst.terminated.is_some() => {
                        let loc = loc_of(&loop_in, &key.parent_frames);
                        let reason = format!(
                            "loop '{}' terminated before taking this stream item",
                            key.group_id
                        );
                        consume_stream_pulses(
                            &[pulse], &loc, run_id, pulses, journal, replica, stream_rt,
                            AbsorbKind::Skipped { reason: &reason },
                        )
                        .await;
                        continue;
                    }
                    Some(_) => {}
                }
                match loop_runtime.stream_push(&key, LoopStreamItem { pulse, value, origin: None }) {
                    Ok(LoopAdvance::LaunchNext { index, stream_item: Some(item) }) => {
                        // A launch failure (a corrupt compiled shape at
                        // postprocess time) fails the LOOP loudly, the
                        // same routing every other boundary failure
                        // takes; it must not kill the whole drive.
                        if let Err(e) = launch_stream_iteration(
                            project, program_idx, pulses, journal, replica,
                            &key, index, item, loop_runtime, stream_rt, kicked, scopes,
                        )
                        .await
                        {
                            fail_loop_from_stream(
                                project, program_idx, pulses, firings, journal, replica,
                                run_id, &key, &loop_in, &e, loop_runtime,
                                stream_rt,
                            )
                            .await;
                        }
                    }
                    Ok(LoopAdvance::Idle) => {
                        set_pulse_status(pulses, &loop_in, pulse, PulseStatus::Routed);
                    }
                    Ok(other) => {
                        // An impossible advance is an engine bug in the
                        // loop runtime; it fails THIS loop loudly, never
                        // the whole drive (no stream-routing condition
                        // takes the run down).
                        fail_loop_from_stream(
                            project, program_idx, pulses, firings, journal, replica,
                            run_id, &key, &loop_in,
                            &format!(
                                "stream_push returned an impossible advance {other:?} for \
                                 loop '{}'",
                                key.group_id
                            ),
                            loop_runtime, stream_rt,
                        )
                        .await;
                    }
                    Err(e) => {
                        fail_loop_from_stream(
                            project, program_idx, pulses, firings, journal, replica,
                            run_id, &key, &loop_in, &e, loop_runtime, stream_rt,
                        )
                        .await;
                    }
                }
            }
            RouteAction::LoopClose { key, loop_in, pulse, end } => {
                // Journal the end FIRST, then consume the close pulse:
                // the close pulse is the only other durable trace of
                // the end, so a crash between the two must land on the
                // side that still knows the stream ended (a resumed
                // loop would otherwise wait forever for a close that
                // can never arrive again).
                crate::context::record_from_replica(
                    journal,
                    weft_journal::RunEvent::LoopStreamEnded {
                        run_id,
                        group_id: key.group_id.clone(),
                        parent_frames: key.parent_frames.clone(),
                        end: end.clone(),
                        at_unix: now_unix(),
                    },
                    replica,
                )
                .await;
                // The close is consumed by the routing (loops never
                // pull an end); absorb it durably, then let the
                // instance decide whether it just terminated. A stale
                // close (instance already terminated in this batch)
                // still absorbs.
                consume_stream_pulses(
                    &[pulse], &loc_of(&loop_in, &key.parent_frames), run_id, pulses,
                    journal, replica, stream_rt, AbsorbKind::Taken,
                )
                .await;
                if loop_runtime.get(&key).is_some_and(|inst| inst.terminated.is_none()) {
                    apply_loop_stream_close(
                        project, program_idx, pulses, firings, journal, replica, run_id,
                        &key, &loop_in, end, loop_runtime, stream_rt,
                    )
                    .await;
                }
            }
            RouteAction::Drop { loc, pulse, closed } => {
                let reason = consumer_gone_reason(project, &loc);
                // A late END for a gone consumer carries nothing to
                // deliver, so it absorbs as taken (no gate ever waits
                // on a closure); a dropped ITEM fails its waiting
                // producer. An item nobody waited on is dropped like
                // any value a finished consumer never read; the
                // breadcrumb keeps that legible.
                let kind = if closed {
                    AbsorbKind::Taken
                } else {
                    tracing::debug!(
                        target: "weft_engine::stream",
                        node = %loc.node_id,
                        pulse = %pulse,
                        "dropping a stream item its consumer never took (consumer finished)"
                    );
                    AbsorbKind::Skipped { reason: &reason }
                };
                consume_stream_pulses(
                    &[pulse], &loc, run_id, pulses, journal, replica, stream_rt, kind,
                )
                .await;
            }
        }
    }
    acted_on
}

fn loc_of(node_id: &str, frames: &weft_core::frames::LoopFrames) -> FiringLocation {
    FiringLocation::new(node_id, frames.clone())
}


/// The one user-facing sentence for "this consumer ended and the item
/// can never be taken", shared by every path that says it. The consumer
/// is named by its place (`one.hold` for the `hold` of the file the site
/// `one` includes), which is how a person reads every other message.
fn consumer_gone_reason(project: &ProjectDefinition, loc: &FiringLocation) -> String {
    format!("the consumer '{}' finished without taking this stream item", place_of(project, loc))
}

/// A firing's place, spelled the way a person writes the node: the
/// node under the call sites on its frames.
fn place_of(project: &ProjectDefinition, loc: &FiringLocation) -> String {
    let call_path: Vec<String> =
        weft_core::frames::call_path(&loc.frames).into_iter().map(str::to_string).collect();
    weft_core::project::address_of(project, &loc.node_id, &call_path)
}

/// A feed that starts ended gets the items its firing had not taken
/// before its body starts: its body reads them before that end, never the
/// end first because the routing pass had not come round yet.
fn hand_over_untaken(
    feed: Option<&Arc<weft_core::generator::GeneratorFeed>>,
    loc: &FiringLocation,
    port: &str,
    run_id: RunId,
    pulses: &mut PulseTable,
) {
    let (Some(feed), Some(bucket)) = (feed, pulses.get_mut(&loc.node_id)) else { return };
    for p in bucket.iter_mut() {
        if p.run_id == run_id && p.frames == loc.frames && p.target_port == port && p.status == PulseStatus::Pending && !p.closed {
            feed.push(p.id, p.value.clone()).expect("a feed that starts ended takes every item it is handed");
            p.status = PulseStatus::Routed;
        }
    }
}

fn set_pulse_status(
    pulses: &mut PulseTable,
    node_id: &str,
    pulse: uuid::Uuid,
    status: PulseStatus,
) {
    if let Some(bucket) = pulses.get_mut(node_id) {
        for p in bucket.iter_mut() {
            if p.id == pulse {
                p.status = status;
            }
        }
    }
}

/// Stream pulses were CONSUMED FOR GOOD outside a dispatch (the name
/// says the caller's contract: these pulses are done, durably, and
/// their producers are settled). Journals the consumption
/// (`PulsesConsumed`, the durability a refold needs so a consumed item
/// never re-dispatches its consumer), REMOVES the pulses from the live
/// table, and settles any delivery gates waiting on them per `kind` (a
/// `Taken` pull delivers; a `Skipped` drop, a consumer that finished
/// or a loop that terminated with items still buffered, fails the
/// waiting producer loudly, while an UN-waited producer's dropped
/// items vanish exactly like any value a skipped consumer absorbed).
///
/// Removal (not `Pulse::absorb`'s status flip) is deliberate: a long
/// stream flows thousands to millions of items through one bucket, and
/// every scheduler pass and routing pass walks that bucket. Keeping a
/// spent item as a tombstone would grow the bucket without bound and
/// turn the per-iteration scans quadratic. Consumed items are also
/// never un-absorb targets: a consumer run again from its saved state
/// gets back only the takes its record does not hold, which were never
/// removed from the table. The journal fold removes them identically, so
/// live and refolded tables agree byte for byte.
#[allow(clippy::too_many_arguments)]
async fn consume_stream_pulses(
    ids: &[uuid::Uuid],
    loc: &FiringLocation,
    run_id: RunId,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    kind: AbsorbKind<'_>,
) {
    if ids.is_empty() {
        return;
    }
    drop_consumed_pulses(pulses, &loc.node_id, ids);
    crate::context::record_from_replica(
        journal,
        weft_journal::RunEvent::PulsesConsumed {
            run_id,
            node_id: loc.node_id.clone(),
            frames: loc.frames.clone(),
            pulse_ids: ids.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        },
        replica,
    )
    .await;
    stream_rt.on_pulses_absorbed(ids, kind);
}

/// Drop consumed stream pulses out of the live bucket entirely; see
/// `consume_stream_pulses` for why removal beats a tombstone.
fn drop_consumed_pulses(pulses: &mut PulseTable, node_id: &str, ids: &[uuid::Uuid]) {
    match pulses.get_mut(node_id) {
        Some(_) => pulses.remove_consumed(node_id, ids),
        None => {
            // The consumed pulses were scanned out of this bucket
            // moments ago, so a missing bucket is an engine bug. The
            // journal fold's `PulsesConsumed` arm reports the same
            // condition as corruption; the live side is equally loud.
            tracing::error!(
                target: "weft_engine::stream",
                node = %node_id,
                "consuming pulses from a node with no pulse bucket; engine bug"
            );
        }
    }
}

/// A running consumer's pull took one item (`TaskMsg::StreamItemTaken`):
/// off its wire, and its producer's delivery settled. The take goes on
/// record with the consuming firing's next save, or its end
/// (`RunnerHandle::record_takes`), so a firing run again from its last
/// save after a crash is handed again what it took since.
fn apply_stream_item_taken(loc: &FiringLocation, pulse_id: uuid::Uuid, pulses: &mut PulseTable, stream_rt: &mut StreamRuntime) {
    drop_consumed_pulses(pulses, &loc.node_id, &[pulse_id]);
    stream_rt.on_pulses_absorbed(&[pulse_id], AbsorbKind::Taken);
}

/// A stream consumer's firing ended (any outcome): tear its feeds
/// down. Items it never pulled are dropped durably; their waiting
/// producers fail loudly.
#[allow(clippy::too_many_arguments)]
async fn retire_consumer_streams(
    project: &ProjectDefinition,
    loc: &FiringLocation,
    run_id: RunId,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
) {
    let leftover = stream_rt.retire(loc);
    if leftover.is_empty() {
        return;
    }
    let reason = consumer_gone_reason(project, loc);
    consume_stream_pulses(
        &leftover, loc, run_id, pulses, journal, replica, stream_rt,
        AbsorbKind::Skipped { reason: &reason },
    )
    .await;
}

/// Launch one iteration of a stream-driven loop with `item`, absorbing
/// the item's pulse atomically with the launch row.
#[allow(clippy::too_many_arguments)]
async fn launch_stream_iteration(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    key: &weft_core::primitive::LoopInstanceKey,
    index: u32,
    item: LoopStreamItem,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
) -> Result<(), String> {
    let pulse = item.pulse;
    launch_iteration(
        project, program_idx, pulses, journal, replica, key, index, Some(item), loop_runtime, kicked, scopes,
    )
    .await?;
    // The launch row (with `stream_pulse`) is the take's durability;
    // the RAM removal + gate resolution mirror it here. No separate
    // PulsesConsumed row: the fold reads it off the launch row.
    let loop_in_id = weft_core::project::loop_in_id(&key.group_id);
    drop_consumed_pulses(pulses, &loop_in_id, &[pulse]);
    stream_rt.on_pulses_absorbed(&[pulse], AbsorbKind::Taken);
    Ok(())
}

/// Act on a stream-driven loop's stream ending: terminate it when it
/// is idle (cleanly, or as a loud loop failure on a failed stream),
/// and drop any still-buffered items a termination leaves behind.
#[allow(clippy::too_many_arguments)]
async fn apply_loop_stream_close(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
    run_id: RunId,
    key: &weft_core::primitive::LoopInstanceKey,
    loop_in_id: &str,
    end: StreamEnd,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
) {
    let advance = loop_runtime.stream_close(key, end);
    apply_loop_stream_advance(
        advance, project, program_idx, pulses, firings, journal, replica, run_id, key,
        loop_in_id, loop_runtime, stream_rt,
    )
    .await;
}

/// Re-evaluate every rehydrated stream-driven loop whose durable
/// `stream_end` is already recorded (see the call site in `drive` for
/// why the live flow cannot do this). A loop whose end FINISHED
/// cleanly defers while item pulses are still in flight for its
/// stream port: those items re-deliver through the routing pass and
/// the normal `stream_push` / LoopOut chain settles the end
/// afterwards (settling early would terminate the loop under items it
/// has not run). A FAILED end settles unconditionally, matching the
/// live close ("fail the loop NOW, whatever is in flight"): running
/// body iterations over a stream the engine already knows failed
/// would spend real side effects the live path never spends; the
/// pending items then drop through the terminated-instance guard.
#[allow(clippy::too_many_arguments)]
async fn settle_rehydrated_stream_ends(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
    run_id: RunId,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
) {
    for (key, port, end) in loop_runtime.stream_instances_with_recorded_end() {
        // The one derivation the whole driver uses; the rehydrate
        // already proved this node exists for every instance in the
        // map, so no re-validation here.
        let loop_in_id = weft_core::project::loop_in_id(&key.group_id);
        if matches!(end, StreamEnd::Finished) {
            let items_pending = pulses.get(&loop_in_id).is_some_and(|bucket| {
                bucket.iter().any(|p| {
                    p.run_id == run_id && p.frames == key.parent_frames && p.target_port == port
                })
            });
            if items_pending {
                continue;
            }
        }
        let advance = loop_runtime.settle_stream_end(&key);
        apply_loop_stream_advance(
            advance, project, program_idx, pulses, firings, journal, replica, run_id, &key,
            &loop_in_id, loop_runtime, stream_rt,
        )
        .await;
    }
}

/// Route one stream-end advance (from the live close OR the rehydrate
/// sweep) into the loop's outward emit / loud failure paths.
#[allow(clippy::too_many_arguments)]
async fn apply_loop_stream_advance(
    advance: Result<LoopAdvance, String>,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
    run_id: RunId,
    key: &weft_core::primitive::LoopInstanceKey,
    loop_in_id: &str,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
) {
    match advance {
        Ok(LoopAdvance::EmitOutward { reason, gather, carry, .. }) => {
            if let Err(e) = emit_loop_outward(
                project, program_idx, pulses, journal, replica, key, gather, carry, reason,
                loop_runtime,
            )
            .await
            {
                fail_loop_from_stream(
                    project, program_idx, pulses, firings, journal, replica, run_id, key,
                    loop_in_id, &e, loop_runtime, stream_rt,
                )
                .await;
                return;
            }
            drop_loop_stream_leftovers(
                key, loop_in_id, run_id, pulses, journal, replica, loop_runtime, stream_rt,
            )
            .await;
        }
        Ok(_) => {}
        Err(e) => {
            fail_loop_from_stream(
                project, program_idx, pulses, firings, journal, replica, run_id, key,
                loop_in_id, &e, loop_runtime, stream_rt,
            )
            .await;
        }
    }
}

/// Route a stream-caused loop failure through the standard boundary
/// failure path (LoopTerminated{Failed} + outward closures + a
/// NodeFailed on the LoopIn), then drop the buffered leftovers.
#[allow(clippy::too_many_arguments)]
async fn fail_loop_from_stream(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    journal: &dyn JournalClient,
    replica: &str,
    run_id: RunId,
    key: &weft_core::primitive::LoopInstanceKey,
    loop_in_id: &str,
    err: &str,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
) {
    let Some(node_def) = project.nodes.iter().find(|n| n.id == loop_in_id) else {
        tracing::error!(
            target: "weft_engine::stream",
            loop_in = %loop_in_id,
            "stream failure for a LoopIn missing from the project; corrupt compiled shape"
        );
        return;
    };
    handle_loop_boundary_failure(
        node_def, run_id, &key.parent_frames, err, project, program_idx, pulses, firings,
        journal, replica, loop_runtime,
    )
    .await;
    drop_loop_stream_leftovers(
        key, loop_in_id, run_id, pulses, journal, replica, loop_runtime, stream_rt,
    )
    .await;
}

/// A terminated stream-driven loop's still-buffered items can never
/// launch: drop them durably (their waiting producers fail loudly).
#[allow(clippy::too_many_arguments)]
async fn drop_loop_stream_leftovers(
    key: &weft_core::primitive::LoopInstanceKey,
    loop_in_id: &str,
    run_id: RunId,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    replica: &str,
    loop_runtime: &mut LoopRuntime,
    stream_rt: &mut StreamRuntime,
) {
    let leftover = match loop_runtime.drain_stream_leftovers(key) {
        Ok(ids) => ids,
        Err(e) => {
            // A missing instance here is an engine bug on a
            // termination path that cannot fail further; surface it
            // loudly rather than silently claiming nothing buffered.
            tracing::error!(
                target: "weft_engine::stream",
                loop_id = %key.group_id,
                error = %e,
                "drain_stream_leftovers failed; buffered stream items may stay routed"
            );
            return;
        }
    };
    let loc = loc_of(loop_in_id, &key.parent_frames);
    let reason = format!(
        "loop '{}' terminated before taking this stream item",
        key.group_id
    );
    consume_stream_pulses(
        &leftover, &loc, run_id, pulses, journal, replica, stream_rt,
        AbsorbKind::Skipped { reason: &reason },
    )
    .await;
}

/// Guard one emission's generator targets against unbounded buffering:
/// a producer running ahead of its consumer WITHOUT waiting for
/// delivery may buffer at most its declared per-port cap
/// (`ctx.set_max_buffered_items`, carried on the emission as
/// `stream_caps`; [`DEFAULT_MAX_BUFFERED_ITEMS`] when undeclared)
/// un-taken items per edge; the next emission past the cap fails the
/// producer loudly.
fn check_generator_buffer_cap(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    node_id: &str,
    output: &NodeOutput,
    stream_caps: &HashMap<String, usize>,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
    pulses: &PulseTable,
) -> Result<(), String> {
    let outgoing = program_idx.get_outgoing(project, node_id, frames);
    for port in output.outputs.keys() {
        for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(port.as_str())) {
            if !weft_core::run::ready::edge_targets_generator(program_idx, edge) {
                continue;
            }
            let handle = edge.target_handle.as_deref().unwrap_or("default");
            let cap = stream_caps.get(port.as_str()).copied().unwrap_or(DEFAULT_MAX_BUFFERED_ITEMS);
            let buffered = pulses
                .get(&edge.target)
                .map(|b| {
                    b.iter()
                        .filter(|p| {
                            p.run_id == run_id
                                && &p.frames == frames
                                && p.target_port == handle
                                && !p.closed
                        })
                        .count()
                })
                .unwrap_or(0);
            if buffered >= cap {
                return Err(format!(
                    "stream '{node_id}.{port}' has {buffered} un-taken items buffered (the \
                     cap is {cap}); the consumer is not keeping up. Yield with \
                     yield_downstream so each item waits for its pull, make the consumer \
                     faster, or declare a higher cap with \
                     ctx.set_max_buffered_items(\"{port}\", n) before emitting.",
                ));
            }
        }
    }
    Ok(())
}

/// Apply one `pulse_downstream` (or `close_port`) emission: postprocess
/// it into downstream pulses at the firing's frame stack, ship the mutations,
/// and union the mentioned port name into the firing's mentioned-set.
/// The emitting node stays Running.
#[allow(clippy::too_many_arguments)]
async fn apply_one_emission(
    msg: crate::context::EmitMsg,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    is_cancel: bool,
) {
    let delivery = msg.delivery;
    // Emissions from a firing the ENGINE already terminated mid-flight
    // (a bad-shape / over-cap emission failed it while the body sailed
    // on) are stale: the failure path already closed the firing's
    // unmentioned ports, so applying later values would put pulses
    // downstream of a NodeFailed record, and re-running the failure
    // per queued emission would spam one NodeFailed row per item.
    // Drop them; a delivery wait on one fails loudly.
    if firing_already_terminal(firings, forgetting, project, program_idx, &msg.loc.node_id, run_id, &msg.loc.frames) {
        if let Some(gate) = &delivery {
            gate.fail(format!("the firing of '{}' already failed", place_of(project, &msg.loc)));
        }
        return;
    }
    let mut emissions = Vec::new();
    // Every act that puts pulses on wires is one emission: the pulse
    // ids derive from its id (derived from the firing's place by the
    // handle) and the journal rows carry it, so the fold puts the same
    // pulses on the same wires.
    let emission_id = msg.id;
    let just_mentioned: std::collections::HashSet<String> = match msg.kind {
        // A stream item the firing yielded before its pause, yielded
        // again: nothing goes on a wire, and the gate waits on that
        // item's pulses still waiting to be taken (none: taken already,
        // delivered).
        crate::context::EmitKind::Again { ports } => {
            if let Some(gate) = &delivery {
                let waiting = pulses_of_emission(project, program_idx, pulses, &msg.loc, &ports, emission_id);
                stream_rt.register_gate(gate, &waiting);
            }
            return;
        }
        crate::context::EmitKind::Values(output) => {
            // Generator buffer bound: an emission that would push a
            // generator edge past its un-taken cap fails the producer
            // loudly BEFORE any pulse is committed.
            if let Err(err) = check_generator_buffer_cap(
                project, program_idx, &msg.loc.node_id, &output, &msg.stream_caps, run_id,
                &msg.loc.frames, pulses,
            ) {
                refuse_emission(
                    &msg.loc, err, delivery.as_deref(), stream_rt.held_waits(), is_cancel, run_id, project, program_idx,
                    pulses, firings, journal, replica,
                )
                .await;
                return;
            }

            // The node's owned values become the shared values every
            // wire, the journal row, and the fold point at: one
            // allocation per port from here on.
            let bag: OutputBag = output
                .outputs
                .into_iter()
                .map(|(port, value)| (port, Arc::new(value)))
                .collect();
            // Run postprocess FIRST. It pre-validates the bag (e.g. a
            // wire's key path must read) before touching `pulses`, so a
            // bad-shape emission produces zero live pulses and zero
            // journal rows: the firing fails atomically with no partial
            // state.
            match postprocess_output(
                &msg.loc.node_id,
                &bag,
                emission_id,
                run_id,
                &msg.loc.frames,
                project,
                pulses,
                program_idx,
                &mut emissions,
            ) {
                Ok(set) => {
                    ship_port_emissions(
                        journal, replica, run_id, emission_id, &msg.loc.node_id, &msg.loc.frames,
                        &bag,
                    )
                    .await;
                    set
                }
                Err(err) => {
                    // The node handed the engine a bad-shape value on
                    // an Expand port (or similar): fail the firing
                    // loud. The pre-validation in postprocess_output
                    // means no pulses/mutations were committed, so the
                    // firing fails atomically. Already-mentioned ports
                    // from PRIOR emissions keep their pulses;
                    // unmentioned ports get closed by the failure path.
                    // A delivery-waiting producer parked on this
                    // emission gets the same error back instead of
                    // hanging on pulses that were never created.
                    refuse_emission(
                        &msg.loc, err.to_string(), delivery.as_deref(), stream_rt.held_waits(), is_cancel, run_id, project,
                        program_idx, pulses, firings, journal, replica,
                    )
                    .await;
                    return;
                }
            }
        }
        crate::context::EmitKind::Close { port: port_name } => {
            // Same failure routing as the Values arm above: a
            // `close_port` on an undeclared port is a wiring bug and
            // fails the firing loud (nothing was committed).
            if let Err(err) = weft_core::run::postprocess::emit_port_closure(
                &msg.loc.node_id,
                &port_name,
                emission_id,
                run_id,
                &msg.loc.frames,
                project,
                pulses,
                program_idx,
                &mut emissions,
                None,
            ) {
                refuse_emission(
                    &msg.loc, err.to_string(), delivery.as_deref(), stream_rt.held_waits(), is_cancel, run_id, project,
                    program_idx, pulses, firings, journal, replica,
                )
                .await;
                return;
            }
            ship_port_closed(
                journal, replica, run_id, emission_id, &msg.loc.node_id, &msg.loc.frames,
                &port_name,
            )
            .await;
            firing_at_mut(firings, &msg.loc.node_id, run_id, &msg.loc.frames)
                .expect("a closing emission has a firing record")
                .closed_output_ports.insert(port_name.clone());
            std::iter::once(port_name).collect()
        }
    };

    // Union into the firing's mentioned set (on its record, as the
    // fold keeps it) so termination knows which ports were already
    // touched, whether by value or by closure, and count a stream item
    // the way the fold counts it.
    let record = firing_at_mut(firings, &msg.loc.node_id, run_id, &msg.loc.frames)
        .expect("an emission comes from a firing with a record (checked non-terminal above)");
    record.mentioned_ports.extend(just_mentioned);
    record.count_stream_item(emission_id);

    // A delivery-waiting emission: arm its gate with exactly the
    // pulses this apply created. They resolve at the absorb sites; an
    // emission whose ports created no pulses (unwired, or every pulse
    // deduped against an identical pending value) is trivially
    // delivered.
    if let Some(gate) = &delivery {
        let ids: Vec<uuid::Uuid> = emissions.iter().map(|e| e.pulse.id).collect();
        stream_rt.register_gate(gate, &ids);
    }
    // A pulse toward a node outside the scope stays in the table: the
    // readiness pass dispatches that node as a skip (reason: outside
    // this run), which absorbs the pulse and settles any delivery gate
    // tracking it through the ordinary skip-absorb path.
}

/// The pulses of the emission `emission` the firing at `loc` put on the
/// wires out of its `ports` that are still waiting to be taken: the ids its
/// place derives (`weft_core::run::emission::pulse_id`, a value or a
/// projected closure on each wire), the ones the table holds pending.
fn pulses_of_emission(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &PulseTable,
    loc: &FiringLocation,
    ports: &[String],
    emission: uuid::Uuid,
) -> Vec<uuid::Uuid> {
    program_idx
        .get_outgoing(project, &loc.node_id, &loc.frames)
        .into_iter()
        .filter(|edge| ports.iter().any(|port| edge.source_handle.as_deref().unwrap_or("default") == port))
        .flat_map(|edge| {
            [true, false].map(|closed| (edge.target.clone(), weft_core::run::emission::pulse_id(emission, &edge.id, closed)))
        })
        .filter(|(target, id)| pulses.get(target).is_some_and(|bucket| bucket.iter().any(|pulse| pulse.id == *id)))
        .map(|(_, id)| id)
        .collect()
}

/// Drain the task channel in FIFO order, applying each `Emission`
/// (downstream pulse, node stays Running) and each `Terminal` (close
/// the firing + close unmentioned ports). Because emissions and the
/// terminal ride this ONE ordered channel, a node's emissions are
/// always applied before its terminal, so the close-unmentioned sweep
/// sees the complete mentioned set. Returns true if any were applied.
/// `is_cancel` propagates to a bad-shape emission so it doesn't journal
/// `NodeFailed` and race the dispatcher's `NodeCancelled` write.
#[allow(clippy::too_many_arguments)]
async fn apply_task_msgs(
    rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    is_cancel: bool,
) -> bool {
    let mut any = false;
    while let Ok(msg) = rx.try_recv() {
        any = true;
        apply_one_task_msg(
            msg, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
            stream_rt, is_cancel,
        )
        .await;
    }
    any
}

/// A firing that ended for good, however it ended (its body returned, or
/// failed, or panicked), leaves no wait behind: every wait its calls left
/// open is withdrawn ([`crate::context::RunnerHandle::withdraw_abandoned_waits`]).
/// A firing that pauses keeps its waits: it carries on from them. A
/// completed firing that could not withdraw one fails saying so, since its
/// signal would otherwise stay up until the run ends; one that failed or
/// was skipped keeps that ending, the withdrawal's failure logged.
async fn withdrawn_at_end(
    outcome: NodeTaskOutcome,
    runner: Option<&crate::context::RunnerHandle>,
    loc: &FiringLocation,
    run_id: RunId,
) -> NodeTaskOutcome {
    let Some(runner) = runner.filter(|_| !matches!(outcome, NodeTaskOutcome::Waiting(_))) else { return outcome };
    let Err(e) = runner.withdraw_abandoned_waits().await else { return outcome };
    match outcome {
        NodeTaskOutcome::Completed { .. } => {
            let message = format!("node '{}' left a wait it could not withdraw: {e}", loc.node_id);
            NodeTaskOutcome::Failed { catchable: Some(message.clone()), message, gave_up_wait: false }
        }
        ended => {
            tracing::error!(target: "weft_engine::suspend", %run_id, node = %loc.node_id, error = %e, "a firing that ended left a wait it could not withdraw");
            ended
        }
    }
}

/// Apply ONE task message. The single message the idle-wait `select!`
/// consumes goes through here too (it can't be re-queued), so emissions
/// and terminals are both handled in one place.
#[allow(clippy::too_many_arguments)]
async fn apply_one_task_msg(
    msg: TaskMsg,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
    is_cancel: bool,
) {
    match msg {
        TaskMsg::Emission(emit) => {
            apply_one_emission(
                emit, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                stream_rt, is_cancel,
            )
            .await;
        }
        TaskMsg::StreamItemTaken { loc, pulse_id } => {
            apply_stream_item_taken(&loc, pulse_id, pulses, stream_rt);
        }
        TaskMsg::Terminal { loc, run_id: task_run_id, outcome, runner } => {
            // What the firing took off its streams goes on record before
            // its ending (a pause already put it there with its save).
            record_takes_at_end(runner.as_deref(), task_run_id, &loc).await;
            let outcome = withdrawn_at_end(outcome, runner.as_deref(), &loc, task_run_id).await;
            // A firing that ended (in any way but a pause) sends the calls
            // it owed before its ending is written.
            if let (Some(runner), false) = (runner.as_deref(), matches!(outcome, NodeTaskOutcome::Waiting(_))) {
                runner.settle_end_calls().await;
            }
            match outcome {
            NodeTaskOutcome::Completed { saved } => {
                retire_consumer_streams(project, &loc, task_run_id, pulses, journal, replica, stream_rt)
                    .await;
                // The engine may have already TERMINATED this firing
                // mid-flight (a bad-shape or buffer-overrun emission
                // fails it while the body keeps running); the body's
                // own later terminal is then stale. Marking Completed
                // over the Failed record would flip the fold's
                // last-write-wins and hide the failure entirely.
                if firing_already_terminal(firings, forgetting, project, program_idx, &loc.node_id, task_run_id, &loc.frames) {
                    return;
                }
                // Emissions already happened via `pulse_downstream` and
                // were applied before this terminal (FIFO on the shared
                // channel). Close the record and ship the recorded
                // output, then emit CLOSURE markers on every output port
                // the firing never mentioned, so downstream consumers
                // learn nothing's coming for those ports. Already-
                // mentioned ports keep their emitted values; a node that
                // emits A then B has both A and B as real values
                // downstream.
                if let Err(why) = emit_saved_values(
                    &loc.node_id, task_run_id, &loc.frames, saved, project, program_idx, pulses, firings, journal,
                    replica,
                )
                .await
                {
                    let mentioned = mentioned_ports(firings, &loc.node_id, task_run_id, &loc.frames);
                    handle_node_failure(
                        &loc.node_id, &mentioned, task_run_id, &loc.frames, &why, None, false, project, program_idx,
                        pulses, firings, journal, replica,
                    )
                    .await;
                    return;
                }
                mark_completed(firings, &loc.node_id, task_run_id, &loc.frames);
                let mentioned = mentioned_ports(firings, &loc.node_id, task_run_id, &loc.frames);
                // The unmentioned-port closures go on the wires in RAM;
                // the NodeCompleted row is the fact the fold sweeps the
                // same ports from.
                build_unmentioned_closures(
                    &loc.node_id, &mentioned, task_run_id, &loc.frames,
                    project, program_idx, pulses, firings, None,
                );
                ship_node_completed(journal, replica, task_run_id, &loc.node_id, &loc.frames).await;
            }
            NodeTaskOutcome::Failed { message, catchable, gave_up_wait } => {
                retire_consumer_streams(project, &loc, task_run_id, pulses, journal, replica, stream_rt)
                    .await;
                // Same stale-terminal guard as the Completed arm: the
                // engine's mid-flight failure already terminated the
                // record and swept its ports.
                if firing_already_terminal(firings, forgetting, project, program_idx, &loc.node_id, task_run_id, &loc.frames) {
                    return;
                }
                let mentioned = mentioned_ports(firings, &loc.node_id, task_run_id, &loc.frames);
                handle_node_failure(
                    &loc.node_id, &mentioned, task_run_id, &loc.frames, &message, catchable, gave_up_wait,
                    project, program_idx, pulses, firings, journal, replica,
                )
                .await;
            }
            NodeTaskOutcome::Skipped(reason) => {
                retire_consumer_streams(project, &loc, task_run_id, pulses, journal, replica, stream_rt)
                    .await;
                // Same stale-terminal guard as the Completed arm.
                if firing_already_terminal(firings, forgetting, project, program_idx, &loc.node_id, task_run_id, &loc.frames) {
                    return;
                }
                // A body that ended skipped closes what it never emitted
                // on, as a completed one does; the NodeSkipped row is the
                // fact the fold sweeps the same ports from.
                mark_skipped(firings, &loc.node_id, task_run_id, &loc.frames, &reason);
                let mentioned = mentioned_ports(firings, &loc.node_id, task_run_id, &loc.frames);
                build_unmentioned_closures(
                    &loc.node_id, &mentioned, task_run_id, &loc.frames,
                    project, program_idx, pulses, firings, reason.inherited_failure(),
                );
                ship_node_skipped(journal, replica, task_run_id, &loc.node_id, &loc.frames, &reason).await;
            }
            NodeTaskOutcome::Waiting(token) => {
                // Same stale-terminal guard as the Completed arm: a body
                // the engine failed mid-flight that then paused on a wait
                // stays failed. Back to waiting, its answer would run the
                // body again onto ports the failure already closed, and
                // their consumers would fire a second time. The wait it
                // registered answers nothing and goes with the run. Its
                // body paused (so ended none of its buses), but the firing
                // is over: its buses end failed and the names it held over
                // an earlier pause leave with it, as for any failed body.
                if firing_already_terminal(firings, forgetting, project, program_idx, &loc.node_id, task_run_id, &loc.frames) {
                    tracing::warn!(
                        target: "weft_engine::run_driver",
                        run_id = %task_run_id, node = %loc.node_id, frames = ?loc.frames, ?token,
                        "a firing the engine already failed paused; it stays failed"
                    );
                    if let Some(runner) = runner.as_deref() {
                        runner.end_body(Some(weft_core::bus::StreamEnd::Failed {
                            error: format!("node '{}' had already failed when its body paused", loc.node_id),
                        }));
                        runner.settle_end_calls().await;
                    }
                    retire_consumer_streams(project, &loc, task_run_id, pulses, journal, replica, stream_rt).await;
                    return;
                }
                // Its feeds go, and what was routed into them untaken goes
                // back on its wire for the feed its resumed body gets.
                for pulse in stream_rt.set_aside(&loc) {
                    set_pulse_status(pulses, &loc.node_id, pulse, PulseStatus::Pending);
                }
                mark_waiting(firings, &loc.node_id, task_run_id, &loc.frames, token.as_deref());
                ship_node_suspended(journal, replica, task_run_id, &loc.node_id, &loc.frames, token.as_deref())
                    .await;
            }
            }
        }
    }
}

/// Put on record what a firing that ended took off its streams and did
/// not record yet (`RunnerHandle::record_takes`). A write that fails
/// poisons the drive, which stops on it; the line says which firing.
async fn record_takes_at_end(runner: Option<&RunnerHandle>, run_id: RunId, loc: &FiringLocation) {
    let Some(runner) = runner else { return };
    if let Err(e) = runner.record_takes().await {
        tracing::error!(target: "weft_engine::stream", %run_id, node = %loc.node_id, frames = ?loc.frames, error = %e, "recording a firing's stream takes failed");
    }
}

/// Drain queued task results into in-memory state ONLY (no journal
/// writes, no downstream closure emission). Used by `cancel_cleanup`:
/// a firing whose body returned during the abort window still ends
/// Cancelled (the cancel walk owns its status and its closures), so
/// its terminal is dropped here rather than applied. A Waiting
/// outcome is applied: a firing that just parked is still open, and
/// the walk cancels it like any other.
#[allow(clippy::too_many_arguments)]
async fn drain_task_msgs_for_cancel(
    rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    forgetting: &Forgetting,
    journal: &dyn JournalClient,
    replica: &str,
    stream_rt: &mut StreamRuntime,
) {
    while let Ok(msg) = rx.try_recv() {
        match msg {
            // Emissions: apply with downstream closures (is_cancel=true),
            // recording the mentioned port. FIFO ordering means a node's
            // emissions are applied before its terminal below.
            TaskMsg::Emission(emit) => {
                apply_one_emission(
                    emit, run_id, project, program_idx, pulses, firings, forgetting, journal, replica,
                    stream_rt, /* is_cancel = */ true,
                )
                .await;
            }
            // A take that raced the cancel: apply it normally (a
            // pulse-level write, not a node-status write, so it cannot
            // race the dispatcher's NodeCancelled).
            TaskMsg::StreamItemTaken { loc, pulse_id } => {
                apply_stream_item_taken(&loc, pulse_id, pulses, stream_rt);
            }
            TaskMsg::Terminal { loc, run_id: task_run_id, outcome, .. } => match outcome {
                NodeTaskOutcome::Completed { .. } | NodeTaskOutcome::Failed { .. } | NodeTaskOutcome::Skipped(_) => {
                    tracing::debug!(
                        target: "weft_engine::run_driver",
                        run_id = %task_run_id, node = %loc.node_id, frames = ?loc.frames,
                        "a firing ended during the cancel window; it ends Cancelled with the run"
                    );
                }
                // A firing the engine already failed stays failed, as in
                // `apply_one_task_msg`.
                NodeTaskOutcome::Waiting(token)
                    if !firing_already_terminal(firings, forgetting, project, program_idx, &loc.node_id, task_run_id, &loc.frames) =>
                {
                    mark_waiting(firings, &loc.node_id, task_run_id, &loc.frames, token.as_deref());
                }
                NodeTaskOutcome::Waiting(_) => {}
            },
        }
    }
}

/// Whether the latest record at `(node, run_id, frames)` is already
/// terminal. The stale-terminal guard in `apply_one_task_msg` reads
/// this: an engine-side mid-flight failure (bad-shape emission, buffer
/// overrun) terminates the record while the body still runs, and the
/// body's own later terminal must not resurrect it.
#[allow(clippy::too_many_arguments)]
fn firing_already_terminal(
    firings: &NodeFiringTable,
    forgetting: &Forgetting,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
) -> bool {
    match firing_at(firings, node_id, run_id, frames) {
        Some(record) => record.status().is_terminal(),
        // One the run forgot once it ended is ended all the same.
        None => forgetting.ended(&Places { project, index: program_idx, run_id }, node_id, frames),
    }
}

/// The firing paused: on the wait `token`, or inside the run (`None`).
fn mark_waiting(
    firings: &mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
    token: Option<&str>,
) {
    if let Some(e) = set_firing_status(firings, node_id, run_id, frames, NodeFiringStatus::WaitingForInput) {
        e.callback_id = token.map(str::to_string);
    }
}

/// The run's outcome once nothing is left to run. A failure whose record
/// says it gave up a wait (`NodeFiring::gave_up_wait`, from whichever
/// drive failed it) ended the run at a time limit (see
/// [`RunOutcome::Failed`]).
async fn terminate(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &PulseTable,
    firings: &mut NodeFiringTable,
    kicked: &weft_core::primitive::KickTable,
    scopes: &ScopeTable,
) -> anyhow::Result<RunOutcome> {
    // The parked firings are on the table, whichever drive parked them.
    let has_waiting = waiting_count(firings) > 0;

    let report = || weft_core::run::stuck_report(project, program_idx, pulses, scopes, kicked);
    let completion = check_completion(pulses, firings);
    // Everything that ran ended, and a node the run kicked never started
    // (its scope never decided): the run did not do what it was asked.
    if completion.is_some() && kicked.any_undispatched() {
        let report = report();
        tracing::warn!(target: "weft_engine", %report, "a kicked node never started");
        return Ok(RunOutcome::Stuck { report });
    }
    match completion {
        Some(false) => Ok(RunOutcome::Completed),
        Some(true) => {
            let failed = first_failed(firings);
            Ok(RunOutcome::Failed {
                error: failed.map_or_else(|| "run failed".into(), |record| failure_of(project, record)),
                held_too_long: failed.is_some_and(|record| record.gave_up_wait),
            })
        }
        None => {
            if has_waiting {
                tracing::info!(
                    target: "weft_engine",
                    waiting = waiting_count(firings),
                    "nothing active; all remaining work is waiting on signals: stalling"
                );
                return Ok(RunOutcome::Stalled);
            }
            let report = report();
            tracing::warn!(target: "weft_engine", %report, "no ready nodes and no suspensions");
            Ok(RunOutcome::Stuck { report })
        }
    }
}

fn waiting_count(firings: &NodeFiringTable) -> usize {
    firings.waiting().count()
}

// ---------- Mutation helpers ----------

/// The earliest failure in the table (`NodeFiring::failure_order`): the
/// run's failure. A plain "first hit wins" scan would not pick the root
/// cause the user should see.
fn first_failed(firings: &NodeFiringTable) -> Option<&NodeFiring> {
    firings.failed().min_by_key(|e| e.failure_order())
}

/// Forget what the run is done with (`weft_core::run::forget`): every
/// ended firing `busy` does not hold, then every iteration nothing inside
/// is alive in any more.
#[allow(clippy::too_many_arguments)]
fn forget_what_ended(
    program: &crate::plan::ProgramTables,
    run_id: RunId,
    forgetting: &mut Forgetting,
    pulses: &mut PulseTable,
    firings: &mut NodeFiringTable,
    kicked: &mut weft_core::primitive::KickTable,
    scopes: &mut ScopeTable,
    carried: &mut Carried,
    loop_runtime: &mut LoopRuntime,
    busy: impl Fn(&FiringLocation) -> bool,
) {
    let places = Places { project: &program.project, index: &program.index, run_id };
    let mut tables = Tables {
        pulses,
        firings,
        kicked,
        scopes,
        recorded_waits: &mut carried.waits,
        saved_states: &mut carried.saved,
        loop_runtime,
    };
    forgetting.forget_ended(&places, &mut tables, &busy);
    forgetting.forget_finished_iterations(&places, &mut tables, &busy);
}

/// The run's failure reason from its failed `record`: the node, named the
/// way the source spells it (`cards.answer` for the `answer` inside the
/// file the site `cards` includes, never its compiled id), and why.
fn failure_of(project: &ProjectDefinition, record: &NodeFiring) -> String {
    let call_path: Vec<String> = weft_core::frames::call_path(&record.frames).into_iter().map(str::to_string).collect();
    format!(
        "{}: {}",
        weft_core::project::address_of(project, &record.node_id, &call_path),
        record.error.clone().unwrap_or_else(|| "failed".into())
    )
}

fn mark_completed(
    firings: &mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
) {
    if let Some(e) = set_firing_status(firings, node_id, run_id, frames, NodeFiringStatus::Completed) {
        e.completed_at = Some(now_unix());
    }
}

fn mark_failed(
    firings: &mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    gave_up_wait: bool,
    at_unix: u64,
) {
    if let Some(e) = set_firing_status(firings, node_id, run_id, frames, NodeFiringStatus::Failed) {
        e.completed_at = Some(at_unix);
        e.error = Some(err.to_string());
        e.gave_up_wait = gave_up_wait;
    }
}

fn mark_skipped(
    firings: &mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
    reason: &weft_core::run::skip::SkipReason,
) {
    if let Some(e) = set_firing_status(firings, node_id, run_id, frames, NodeFiringStatus::Skipped) {
        e.completed_at = Some(now_unix());
        // A skip is the firing's whole outcome (decided before the body
        // ran, or the body's wait skipped by a person), so a port's
        // refusal found at readiness is not a fact about it. The
        // `NodeSkipped` row carries no error and the reason, and the fold
        // reads them so; the live record says the same.
        e.error = None;
        e.skip_reason = Some(reason.clone());
        e.callback_id = None;
    }
}

/// The cancel walk's end of an open firing: what the fold does with
/// the firing's `NodeCancelled` row.
fn mark_cancelled(
    firings: &mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &weft_core::frames::LoopFrames,
    reason: &str,
) {
    if let Some(e) = set_firing_status(firings, node_id, run_id, frames, NodeFiringStatus::Cancelled) {
        e.completed_at = Some(now_unix());
        e.error = Some(reason.to_string());
        e.callback_id = None;
    }
}


/// Which `Node` trait method a dispatch invokes. The engine picks it
/// from the phase plus the manifest, so a node never picks its own
/// body; inside `run` it can still ask `ctx.is_setup()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeBody {
    /// The node's normal body (`Node::run`).
    Run,
    /// A trigger's registration body (`Node::setup_trigger`).
    SetupTrigger,
    /// A trigger with nothing to do this run (infra setup, or a Fire
    /// where another trigger fired): the dispatch terminates as
    /// Completed without invoking the node, so every output port
    /// closes and the skip cascade prunes its exclusive branches.
    SkipTrigger,
}

/// The one phase-routing rule: a plain node runs its normal body in
/// EVERY phase (a value feeding a trigger's config must be produced at
/// setup time too); a trigger registers at TriggerSetup, runs at Fire
/// only as THE firing trigger (`has_wake`: this dispatch carries the
/// wake payload), and otherwise just closes its ports (infra setup,
/// or a Fire of a different trigger).
fn node_body_for(
    phase: weft_core::context::Phase,
    is_trigger: bool,
    has_wake: bool,
) -> NodeBody {
    use weft_core::context::Phase;
    if !is_trigger {
        return NodeBody::Run;
    }
    match phase {
        Phase::TriggerSetup => NodeBody::SetupTrigger,
        Phase::Fire if has_wake => NodeBody::Run,
        Phase::Fire => NodeBody::SkipTrigger,
        Phase::InfraSetup => NodeBody::SkipTrigger,
    }
}

/// The cause the canceller recorded on the flag. Read only once the
/// flag is tripped, and `cancel_because` is the flag's only door (it
/// stores the cause before it trips), so a tripped flag always carries
/// one.
fn recorded_cancel_cause(cancellation: &CancellationFlag) -> weft_core::run::CancelCause {
    debug_assert!(cancellation.is_cancelled(), "read the cause only off a tripped flag");
    cancellation
        .cause()
        .expect("a tripped flag carries its cause: cancel_because is the only door")
}

/// The ending an outcome writes: `Completed`, `Failed` and `Stuck` map,
/// `Cancelled` carries its cause. A run that pauses writes none.
fn terminal_event(run_id: RunId, outcome: &RunOutcome) -> RunEvent {
    let at_unix = now_unix();
    match outcome {
        RunOutcome::Completed => RunEvent::RunCompleted { run_id, at_unix },
        // A cancel maps to the proper RunCancelled terminal so the
        // UI renders the cancel affordance instead of a generic failure.
        RunOutcome::Cancelled { cause } => RunEvent::RunCancelled {
            run_id,
            reason: cause.to_string(),
            cause: Some(cause.clone()),
            at_unix,
        },
        RunOutcome::Failed { error, .. } => RunEvent::RunFailed { run_id, error: error.clone(), at_unix },
        RunOutcome::Stuck { report } => RunEvent::RunFailed { run_id, error: report.to_string(), at_unix },
        RunOutcome::Stalled | RunOutcome::HandedBack => unreachable!("a run that pauses writes no ending"),
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// The engine's test modules live in `run_driver_tests/` as child
// modules (`#[path]`), so they keep private-item access (`use super::*`)
// without bloating this file.

#[cfg(test)]
#[path = "run_driver_tests/resume.rs"]
mod resume_tests;

/// Shared layer-3 rig for tests that drive `run_one` with real
/// inline nodes: an in-memory recording journal plus Noop fakes for every
/// I/O client the engine composes. Test modules bring their own nodes,
/// projects, and assertions; the rig is only the dumb plumbing.
#[cfg(test)]
#[path = "run_driver_tests/rig.rs"]
pub(crate) mod engine_test_rig;

#[cfg(test)]
#[path = "run_driver_tests/phase_routing.rs"]
mod phase_routing_tests;

#[cfg(test)]
#[path = "run_driver_tests/bus_comm.rs"]
mod bus_comm_tests;

// Layer 3: branching through the real loop. `_should_flow` decides
// whether a node runs, the skip carries WHY, and the cascade reaches
// everything behind it.
#[cfg(test)]
#[path = "run_driver_tests/branching.rs"]
mod branching_tests;

// Layer 3: a connection the program lets stay empty that has none skips
// its node before the body runs, and the nodes behind it decide on the
// closure.
#[cfg(test)]
#[path = "run_driver_tests/unfilled.rs"]
mod unfilled_tests;

// Layer 3: a node's failure goes to its wired `_error` until it opened a
// bus, a bus ends the way its body returned, and a firing a dead worker
// left running is failed, never run again.
#[cfg(test)]
#[path = "run_driver_tests/error_port.rs"]
mod error_port_tests;

// Layer 3: a call a node registers with `ctx.after_node` is sent once
// when it ends, whether its body returned or failed.
#[cfg(test)]
#[path = "run_driver_tests/end_calls.rs"]
mod end_calls_tests;

// Layer 3: a baked output falls back on what its copy saved.
#[cfg(test)]
#[path = "run_driver_tests/baked.rs"]
mod baked_tests;

// Layer 3: a wire carries values, never bytes; an oversize emission
// fails its node.
#[cfg(test)]
#[path = "run_driver_tests/wire_values.rs"]
mod wire_values_tests;

// Layer 3: a run born at the worker's door writes its birth first.
#[cfg(test)]
#[path = "run_driver_tests/born.rs"]
mod born_tests;

// Layer 3: a node that emits, pauses on a wait and emits more; a port
// sent before the pause sends nothing when the body runs again.
#[cfg(test)]
#[path = "run_driver_tests/pause_emissions.rs"]
mod pause_emissions_tests;

// Layer 3: a run asked to let go of its worker ends its running firings,
// starts no other, and is carried on from its record.
#[cfg(test)]
#[path = "run_driver_tests/hand_back.rs"]
mod hand_back_tests;

// Layer 3: a run that would pause stays quiet on its worker for its
// `pauseAfterSecs` first; an answer inside that time carries it on there.
#[cfg(test)]
#[path = "run_driver_tests/pause_window.rs"]
mod pause_window_tests;

// Layer 3: a run's secrets: only their markers on record, their values
// riding its batches, revealed from memory, from the record, and never
// written for a run that is not recorded.
#[cfg(test)]
#[path = "run_driver_tests/secrets.rs"]
mod secrets_tests;

// Layer 3: an unrecorded run keeps no history, leaves a note of itself
// only when it fails, and holds its waits in its worker.
#[cfg(test)]
#[path = "run_driver_tests/unrecorded.rs"]
mod unrecorded_tests;

// Layer 3: a charge an earlier drive opened is held again by the next
// one, and booked as unknown when the run ends without its read.
#[cfg(test)]
#[path = "run_driver_tests/charges.rs"]
mod charges_tests;

// Layer 3: a node body that holds its thread for long delays none of the
// door's ticks, which keep the worker's lease from the io runtime.
#[cfg(test)]
#[path = "run_driver_tests/lease.rs"]
mod lease_tests;

// Layer 1: a run's failure reason spells the failed node's address.
#[cfg(test)]
#[path = "run_driver_tests/failure_naming.rs"]
mod failure_naming_tests;

// Layer 3: a run's record read a page at a time, and only what landed
// since once its fold took answers.
#[cfg(test)]
#[path = "run_driver_tests/paging.rs"]
mod paging_tests;

// Layer 3: a seeded run inherits its seed's rows through the real
// loop, re-runs only what is stale, and a scoped run fires from a
// value a person provided.
#[cfg(test)]
#[path = "run_driver_tests/seeded.rs"]
mod seeded_tests;

// Layer 3: a file included at several sites runs once per call, each
// call under its own frame, with its own picks and its own waits.
#[cfg(test)]
#[path = "run_driver_tests/include_calls.rs"]
mod include_calls_tests;

// Layer 3: Generator streams + delivery-waiting emissions through the
// real loop. See the module doc for the contracts pinned there.
#[cfg(test)]
#[path = "run_driver_tests/stream.rs"]
mod stream_tests;

// Layer 3: a firing that keeps its state, across drives: waits around
// work done once, a wait in a loop, a stream paused from either end, a
// consumer crashing between saves, a bus conversation paused and picked
// up again, and a run paused before saved states existed.
#[cfg(test)]
#[path = "run_driver_tests/saved_state.rs"]
mod saved_state_tests;

// Layer 3: LoopRuntime integration rig tests. These exercise the engine's
// loop boundary handlers (`handle_loop_boundary_firing`, `launch_iteration`,
// `emit_loop_outward`, `cancel_loop_instances`) against synthetic
// ProjectDefinitions. They confirm the integration points the unit tests on
// `LoopRuntime` alone can't reach: per-iteration pulse emission shapes,
// gather/carry assembly at outward emit, frame-stack keying, and
// cancellation closure emission.
#[cfg(test)]
#[path = "run_driver_tests/loop_rig.rs"]
mod loop_rig_tests;
