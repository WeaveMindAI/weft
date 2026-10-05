//! The worker: the project's compiled program, serving HTTP.
//!
//! Weft calls a worker for each execution; the worker never goes looking
//! for work. Three ways in, one drive:
//!
//! - `POST /_weft/run/<execution_id>`: run this execution (a short run). The
//!   worker claims that execution's task, drives it, and answers when it
//!   ends or waits on something outside it. A duplicate call claims
//!   nothing and says so.
//! - any other path: a live caller, forwarded by the install with its
//!   signed routing ticket (`caller_conn`). The execution is born pinned
//!   to this worker and driven with the caller's connection attached.
//! - `--run <execution_id>` on the command line: the same drive as a job of its
//!   own (a long run), exiting when it ends.
//!
//! While it drives anything, the worker keeps one wait open on the broker
//! for cancels of the executions it drives, so a `weft stop` reaches it at
//! once. On shutdown (the platform stopping the replica) it cancels what
//! it drives, waits for those executions to write their endings, and
//! settles the money.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use weft_core::cancellation::CancellationFlag;
use weft_core::caller::{InboundMessage, OutboundChunk};
use weft_core::{ExecutionId, NodeCatalog, ProjectDefinition};
use weft_task_store::tasks::{ClaimedExecution, Task};
use weft_task_store::{ExecutionPayload, TaskEnd};

use crate::context::EngineClients;
use crate::execution_driver::{run_one_execution, ExecutionOutcome};

/// Who may call this worker's own endpoints (`/_weft/...`).
#[derive(Clone)]
pub enum WorkerDoor {
    /// The platform guards the worker itself (Cloud Run lets only the
    /// install's own identities invoke it), so a call that arrived is
    /// weft's.
    Platform,
    /// A key the install handed this worker when it started it; a call
    /// must present it as its bearer in
    /// [`weft_platform_traits::WORKER_AUTH_HEADER`].
    Key(Arc<Vec<u8>>),
}

impl WorkerDoor {
    /// Read the door from `WEFT_WORKER_DOOR`: `platform`, or `key:<hex>`.
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("WEFT_WORKER_DOOR").context("WEFT_WORKER_DOOR is required: `platform` or `key:<hex>`")?;
        Self::parse(&raw)
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "platform" => Ok(Self::Platform),
            other => {
                let hex_key = other
                    .strip_prefix("key:")
                    .context("WEFT_WORKER_DOOR is `platform` or `key:<hex>`")?;
                let key = hex::decode(hex_key).context("WEFT_WORKER_DOOR's key is not hex")?;
                anyhow::ensure!(key.len() >= 16, "WEFT_WORKER_DOOR's key is too short to guard anything");
                Ok(Self::Key(Arc::new(key)))
            }
        }
    }

    /// Whether a call with these headers may use the worker's own
    /// endpoints.
    pub fn admits_headers(&self, headers: &HeaderMap) -> bool {
        match self {
            Self::Platform => true,
            Self::Key(key) => {
                let presented = headers
                    .get(weft_platform_traits::WORKER_AUTH_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .and_then(|v| hex::decode(v).ok());
                presented.is_some_and(|p| weft_core::signed_token::constant_time_eq(&p, key))
            }
        }
    }
}



/// This worker's identity for its calls to the broker, from
/// `WEFT_WORKER_IDENTITY`: `token:<token>` (a token the install signed and
/// handed the worker when it started it, the local platform) or
/// `gcp-metadata` (the worker's own service account, asked of the metadata
/// server, on Google Cloud).
pub fn identity_from_env() -> Result<Arc<dyn weft_platform_traits::IdentityTokens>> {
    let raw = std::env::var("WEFT_WORKER_IDENTITY")
        .context("WEFT_WORKER_IDENTITY is required: `token:<token>` or `gcp-metadata`")?;
    match raw.trim() {
        "gcp-metadata" => Ok(Arc::new(weft_platform_gcp::MetadataTokens::new())),
        other => {
            let token = other
                .strip_prefix("token:")
                .filter(|t| !t.is_empty())
                .context("WEFT_WORKER_IDENTITY is `token:<token>` or `gcp-metadata`")?;
            Ok(Arc::new(weft_platform_traits::FixedToken(token.to_string())))
        }
    }
}

/// What starts a worker, besides its catalog and its broker clients.
pub struct WorkerConfig {
    pub project_id: uuid::Uuid,
    pub tenant_id: String,
    /// This process's replica id: names its claims, the executions it
    /// drives, and the journal rows it writes.
    pub replica: String,
    pub door: WorkerDoor,
    /// The secret live-caller routing tickets are signed with; `None`
    /// when the install provisioned none, and then this worker takes no
    /// live callers.
    pub caller_token_secret: Option<Vec<u8>>,
    pub port: u16,
    /// The platform's hard cap on one short run, when it has one: a short
    /// run reaching it is stopped, naming the setting that lifts it.
    pub short_run_cap: Option<std::time::Duration>,
}

/// How long before the platform's cap a short run stops itself, so the
/// ending it writes is its own rather than a cut connection.
const SHORT_RUN_CAP_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-execution cancellation flag of every execution this worker drives: a
/// cancel looks up the execution and fires the flag.
type CancelRegistry = Arc<Mutex<HashMap<ExecutionId, Arc<CancellationFlag>>>>;

/// Everything one execution registered on the worker, released when the
/// execution ends however it ends.
///
/// These used to be plain statements after the run returned, which an
/// unwind skips, and an unwind is a DESIGNED path here: the bus
/// shutdown panics when the pump has not drained inside its deadline,
/// after the `catch_unwind` around the drive. The process then kept a
/// cancel flag and a live config for a dead execution for the rest of its
/// life, so a later cancel of that execution reported success while
/// firing a flag nobody reads, and the connection server still handed
/// out a config and accepted a socket for a run that no longer exists.
/// A guard cannot be skipped.
struct ExecutionResidue {
    execution_id: ExecutionId,
    cancel_registry: CancelRegistry,
    /// THIS execution's flag, so the deferred removal can tell it from
    /// a later claim's. The registry is keyed by execution, and a resume of
    /// the same execution can be waiting on the execution gate and register its
    /// own flag the instant this one's gate is released. Removing by
    /// key alone deleted the NEW execution's flag, so its cancel found
    /// nothing and reported a no-op, and worker shutdown never cancelled
    /// it: the exact failure this guard exists to prevent, moved into
    /// the gap between the guard and the task.
    flag: Arc<CancellationFlag>,
    /// Poked once the flag is gone, so the cancel wait stops asking
    /// for this execution.
    driving_changed: Arc<tokio::sync::Notify>,
    caller_registry: crate::caller_conn::CallerRegistry,
    live_configs: LiveConfigMap,
    open_charges: Arc<crate::metering::OpenCharges>,
}

impl Drop for ExecutionResidue {
    fn drop(&mut self) {
        let execution_id = self.execution_id;
        // Money first: a charge belongs to this execution, so a job it
        // submitted and never read back is written down as spend with
        // no figure, here, rather than waiting for the process to die.
        //
        // Nothing in this destructor may panic: a panic in a Drop that
        // is itself running during an unwind aborts the process, and
        // the unwind path is a designed one here (the bus shutdown
        // panics on a pump that will not drain). So every lock is
        // taken defensively and a poisoned one is reported, never
        // unwrapped.
        self.open_charges.flush_execution_id(execution_id, "the execution ended before the job was read back");
        match self.live_configs.lock() {
            Ok(mut configs) => {
                configs.remove(&execution_id);
            }
            Err(_) => tracing::error!(
                target: "weft_engine::worker",
                %execution_id,
                "the live-config map is poisoned, so this execution's config was not dropped; \
                 the connection server may still hand out a config for it until the replica exits"
            ),
        }
        self.caller_registry.detach(execution_id);
        // The cancel registry is an async lock, so its removal is a
        // task; `Handle::try_current` because a destructor can run
        // while the runtime is shutting down, where `tokio::spawn`
        // panics. Removing only if the flag is still OURS.
        let registry = self.cancel_registry.clone();
        let mine = self.flag.clone();
        let changed = self.driving_changed.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let mut reg = registry.lock().await;
                    if reg.get(&execution_id).is_some_and(|f| Arc::ptr_eq(f, &mine)) {
                        reg.remove(&execution_id);
                        changed.notify_one();
                    }
                });
            }
            Err(_) => tracing::error!(
                target: "weft_engine::worker",
                %execution_id,
                "no runtime to drop this execution's cancel flag on (the replica is tearing down); \
                 the entry goes with the process"
            ),
        }
    }
}

/// One worker process: what every drive on it shares.
///
/// The `ProjectDefinition` is not held here: each execution names its
/// own definition hash, fetched from the broker and cached by hash.
#[derive(Clone)]
struct Worker {
    project_id: uuid::Uuid,
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    replica: String,
    tenant_id: String,
    cancel_registry: CancelRegistry,
    /// Poked whenever the set of executions this worker drives changes, so
    /// the cancel wait re-asks for exactly those.
    driving_changed: Arc<tokio::sync::Notify>,
    project_cache: ProjectCache,
    /// The live caller registry: the connection server attaches an
    /// accepted socket here keyed by execution; the execute path awaits it.
    caller_registry: crate::caller_conn::CallerRegistry,
    /// Per-execution live-connection runtime config, set by the execute path
    /// before the caller attaches, read by the connection server.
    live_configs: LiveConfigMap,
    /// Runs a live caller just claimed, each told the moment its drive has
    /// registered the connection's settings (`attach_live_caller`), so the
    /// connection server waits for exactly that, or for the drive's error.
    live_ready: LiveReadyMap,
    /// Every drive this worker runs, the gate shutdown waits on.
    background: Arc<weft_core::in_flight::InFlight>,
    short_run_cap: Option<std::time::Duration>,
}

type ProjectCache = Arc<Mutex<BoundedProjectCache>>;

/// How a call to run one execution ended, as the caller of
/// `/_weft/run/<execution_id>` reads it.
// SYNC: RunAnswer <-> crates/weft-dispatcher/src/delivery.rs (read)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ended", rename_all = "snake_case")]
pub enum RunAnswer {
    /// The execution's task was driven to its end (the execution ended or
    /// waits on something outside it).
    Completed,
    /// The drive failed; its task was failed with this.
    Failed { error: String },
    /// This worker lost the task's claim mid-drive; whoever claims it
    /// next drives it.
    LeaseLost,
    /// Nothing to run: the execution's task was already claimed by
    /// another worker, already done, or pinned elsewhere.
    NothingToRun,
}

impl From<TaskEnd> for RunAnswer {
    fn from(end: TaskEnd) -> Self {
        match end {
            TaskEnd::Completed => Self::Completed,
            TaskEnd::Failed(error) => Self::Failed { error },
            TaskEnd::LeaseLost => Self::LeaseLost,
        }
    }
}

impl Worker {
    /// Claim `execution_id`'s task and drive it to its end. Detached from the
    /// caller: a platform that cuts the request does not drop the drive
    /// halfway; the drive ends on its own terms (and a short run stops
    /// itself before the cap, see `SHORT_RUN_CAP_MARGIN`).
    async fn run_execution_id(&self, execution_id: ExecutionId) -> Result<RunAnswer> {
        let Some(claimed) = self.claim_execution_id(execution_id).await? else {
            return Ok(RunAnswer::NothingToRun);
        };
        self.drive_detached(claimed).await.context("the drive panicked outside its guard")
    }

    /// `execution_id`'s execute or resume task, claimed by this worker
    /// with the execution's journal, or `None` when there is nothing here
    /// to claim.
    async fn claim_execution_id(&self, execution_id: ExecutionId) -> Result<Option<ClaimedExecution>> {
        self.clients
            .tasks
            .claim_execution(&self.replica, self.project_id, &execution_id.to_string())
            .await
            .context("claim the execution's task")
    }

    /// Drive a claimed task to its end on a task of its own, so whoever
    /// handed it here can go away without stopping it.
    fn drive_detached(&self, claimed: ClaimedExecution) -> tokio::task::JoinHandle<RunAnswer> {
        let worker = self.clone();
        let token = self.background.token();
        tokio::spawn(async move {
            let _token = token;
            let store = worker.clients.tasks.clone();
            let replica = worker.replica.clone();
            let ClaimedExecution { task, journal } = claimed;
            let end = weft_task_store::run_claimed_worker_task(store, &replica, &task, worker.drive(&task, journal)).await;
            RunAnswer::from(end)
        })
    }

    /// Drive one claimed execute or resume task: fold the journal the
    /// claim read and run the loop driver. The two are identical here (the
    /// journal carries the lifecycle truth); the dispatcher distinguishes
    /// them so the editor can label the event.
    async fn drive(&self, task: &Task, journal: Vec<weft_journal::RawJournalRow>) -> Result<()> {
        let ctx = self;
        let payload: ExecutionPayload = serde_json::from_value(task.payload.clone())?;
        let execution_id: ExecutionId = payload
            .execution_id
            .parse()
            .map_err(|e| anyhow::anyhow!("bad execution: {e}"))?;

        // Per-execution definition fetch with a worker-local hash cache.
        // First claim of a given (project_id, definition_hash) pays
        // one broker round trip; consecutive claims on the same hash
        // hand back the cached `Arc<ProjectDefinition>` via the
        // cache's `get`. A 404 from the broker (no history row for
        // this hash) is a hard error: the dispatcher should never
        // enqueue a task for a hash whose history row doesn't exist
        // (the set_running_definition_hash precondition refuses that),
        // so a miss here is a real upstream bug.
        let project = fetch_or_cached_project(ctx, &payload.definition_hash).await?;

        // An unrecorded run's journal is its own memory, seeded with the
        // birth its task carried. It is never run twice: a second claim
        // means the first one died somewhere in the middle, and running
        // it again would repeat whatever it had already done.
        // Refused that way, it is still written down: its birth and the
        // failure become its record, as any failed unrecorded run's do,
        // so it lists, and its ending reaches whoever waits on it.
        let unrecorded = match &payload.unrecorded_birth {
            None => None,
            Some(birth) => {
                let birth = birth
                    .iter()
                    .map(|row| weft_journal::decode_event(execution_id, &row.to_string()).map_err(anyhow::Error::msg))
                    .collect::<Result<Vec<_>>>()?;
                if task.attempts > 1 {
                    let error = format!(
                        "unrecorded run {execution_id} was already started once and its worker went away; \
                         it is not run again, because a second run would repeat what the first did"
                    );
                    let mut record = weft_journal::unrecorded::as_recorded(birth);
                    record.push(weft_journal::ExecEvent::ExecutionFailed { execution_id, error: error.clone(), at_unix: crate::now_unix() });
                    ctx.clients
                        .journal
                        .record_retroactively(&record, Some(ctx.replica.as_str()))
                        .await
                        .map_err(|e| e.context(format!("record refused unrecorded run {execution_id}")))?;
                    anyhow::bail!(error);
                }
                Some(weft_journal::UnrecordedJournal::seeded(execution_id, birth, ctx.clients.journal.clone())?)
            }
        };
        // A recorded run starts from the rows its claim read; an unrecorded
        // one's rows are its own memory, which holds its birth.
        let (clients, first_rows) = match &unrecorded {
            Some(memory) => (
                EngineClients { journal: memory.clone(), ..ctx.clients.clone() },
                weft_journal::JournalClient::raw_rows_after(memory.as_ref(), execution_id, 0, std::time::Duration::ZERO).await?,
            ),
            None => (ctx.clients.clone(), journal),
        };

        let flag = CancellationFlag::new_arc();
        ctx.cancel_registry.lock().await.insert(execution_id, flag.clone());
        // The cancel wait now asks for this execution too.
        ctx.driving_changed.notify_one();
        // From here on every exit path, including a panic, releases what
        // this execution registered on the worker.
        let _residue = ExecutionResidue {
            execution_id,
            flag: flag.clone(),
            cancel_registry: ctx.cancel_registry.clone(),
            driving_changed: ctx.driving_changed.clone(),
            caller_registry: ctx.caller_registry.clone(),
            live_configs: ctx.live_configs.clone(),
            open_charges: ctx.clients.open_charges.clone(),
        };
        guard_short_run(ctx, execution_id, flag.clone(), payload.run_class, payload.live_connection.is_some());

        // Live-connection executions carry their trigger's `live_connection`
        // start record. Register the runtime config + the caller's request
        // so the connection server can build the connection when the
        // caller's socket attaches, then wait (bounded by the connect
        // timeout) for the attach so `ctx.caller()` resolves. A no-show
        // leaves `caller = None`; the run proceeds and any node that needs
        // the caller fails loud via the handle's `ensure_connected()`. A
        // malformed record is a dispatcher/worker mismatch: the execution
        // fails rather than running as if nobody were on the line.
        let caller = match &payload.live_connection {
            Some(start) => attach_live_caller(ctx, execution_id, start, clients.journal.clone(), &flag).await?,
            None => None,
        };
        // Keep the connection so we can end the exchange with the caller
        // after the execution returns (the run takes its own reference).
        // Only a REAL caller needs this: a fired run's exchange is over
        // the moment the program answers, with no socket left to tell.
        let caller_after_run = caller.as_ref().and_then(RunCaller::live);
        let caller_journal = caller.as_ref().map(RunCaller::journal);
        let caller = caller.map(|c| c.as_connection());

        let outcome = run_one_execution(
            project,
            ctx.catalog.clone(),
            execution_id,
            clients,
            ctx.replica.clone(),
            ctx.tenant_id.clone(),
            flag,
            caller,
            first_rows,
        )
        .await;

        // A caller is attached and the run is over: the exchange ends
        // now, never when the worker exits. A run that did not complete
        // tells the caller why (per the error mode) instead of leaving a
        // silently dropped socket: a driver error, a failed node, a
        // cancel, a stuck graph, and an execution that was already settled
        // before this task claimed it all end the same way for the
        // caller, no answer is coming. A run that completed (or stalled
        // into a background job, which resumes without a caller) ends
        // the exchange the way the program left it: a finished stream, a
        // closed socket, or a loud "never answered" on a silent route.
        // Safe after the run returned: the outbound queue drops a push
        // once the caller was answered or closed, so a run that already
        // spoke its last word is not double-messaged.
        if let Some(conn) = &caller_after_run {
            match &outcome {
                Err(e) => conn.surface_error(&format!("execution failed: {e}")).await,
                Ok(ExecutionOutcome::Failed { error }) => {
                    conn.surface_error(&format!("execution failed: {error}")).await
                }
                Ok(ExecutionOutcome::Cancelled { cause }) => {
                    conn.surface_error(&format!("execution cancelled: {cause}")).await
                }
                Ok(ExecutionOutcome::Stuck { report }) => conn.surface_error(&report.to_string()).await,
                Ok(ExecutionOutcome::AlreadySettled) => {
                    conn.surface_error("execution already ended before this worker claimed it").await
                }
                Ok(ExecutionOutcome::Completed) => conn.run_ended().await,
                Ok(ExecutionOutcome::Stalled) => conn.run_parked().await,
            }
            conn.hang_up().await;
        }
        // Everything the exchange said (the last window, the error row
        // above, the disconnect the hang-up recorded) is in the run's
        // journal before that journal is read to be settled: an
        // unrecorded run's record is taken once, and a row still queued
        // behind it would be lost.
        if let Some(sink) = &caller_journal {
            sink.close().await;
        }

        // An unrecorded run is over, and so is everything it will write:
        // a failure is recorded whole, anything else is forgotten.
        if let Some(journal) = &unrecorded {
            let settled = journal.settle(Some(ctx.replica.as_str())).await;
            match (&outcome, settled) {
                (_, Ok(_)) => {}
                // The run's own error says more than the settle's.
                (Err(_), Err(e)) => tracing::error!(
                    target: "weft_engine::worker",
                    execution_id = %execution_id,
                    error = %format!("{e:#}"),
                    "an unrecorded run failed and its record could not be written"
                ),
                (Ok(_), Err(e)) => return Err(e.context(format!("settle unrecorded run {execution_id}"))),
            }
        }

        // The cancel flag, the live config and any attached connection
        // are released by `ExecutionResidue` when this returns or
        // unwinds; nothing to do here.
        outcome.map(|_| ())
    }
}


/// The caller for this run, whoever it is: the real one waiting on a
/// socket, or the stand-in a FIRED run serves its own body to.
///
/// Both are `CallerConnection`s, so the trigger and every node behind
/// it run the same code either way. This is the one place that knows
/// the difference, and it knows it from one field on the start record.
enum RunCaller {
    Live(Arc<crate::caller_conn::LiveCallerConnection>),
    Fired(Arc<crate::fired_caller::FiredCaller>),
}

impl RunCaller {
    fn as_connection(&self) -> Arc<dyn weft_core::caller::CallerConnection> {
        match self {
            Self::Live(c) => c.clone(),
            Self::Fired(c) => c.clone(),
        }
    }

    /// The real connection, for the end-of-run tidy-up that only a
    /// socket needs. A fired run's exchange ends when the program
    /// answers and there is nothing to close afterwards.
    fn live(&self) -> Option<Arc<crate::caller_conn::LiveCallerConnection>> {
        match self {
            Self::Live(c) => Some(c.clone()),
            Self::Fired(_) => None,
        }
    }

    /// The sink the exchange is recorded through, live or fired.
    fn journal(&self) -> Arc<dyn crate::caller_conn::CallerJournalSink> {
        match self {
            Self::Live(c) => c.journal(),
            Self::Fired(c) => c.journal(),
        }
    }
}

/// Register a live-connection execution's runtime config + opening request
/// and wait for the caller's socket to attach. Returns the attached
/// connection, or `None` if the caller never arrives within the connect
/// timeout. A start record the worker cannot read (a kind that is not a
/// live caller, a config that does not parse) is an error: the dispatcher
/// wrote it, so it is a version mismatch, not a run without a caller.
async fn attach_live_caller(
    ctx: &Worker,
    execution_id: ExecutionId,
    start: &weft_task_store::kinds::LiveConnectionStart,
    journal: Arc<dyn weft_journal::JournalClient>,
    cancelled: &CancellationFlag,
) -> Result<Option<RunCaller>> {
    // The record carries the full signal spec; its kind says the
    // protocol and the connection's settings (`Signal::CALLER`).
    let (protocol, cfg) = weft_core::signal::live_connection(&start.spec)
        .map_err(|e| anyhow::anyhow!("the live caller on the execute task for {execution_id}: {e}"))?;
    let runtime = weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol);
    let connect_timeout = std::time::Duration::from_secs(runtime.connect_timeout_secs);
    ctx.live_configs.lock().expect("live_configs poisoned").insert(
        execution_id,
        Arc::new(LiveStart {
            runtime,
            heartbeat_secs: cfg.heartbeat_interval_secs,
            request: Arc::new(start.request.clone()),
            journal: journal.clone(),
        }),
    );
    // The caller who claimed this run is waiting for exactly this.
    if let Some(ready) = ctx.live_ready.lock().expect("live_ready poisoned").remove(&execution_id) {
        let _ = ready.send(());
    }
    // A FIRED run has no socket coming, so waiting for one would burn
    // the whole connect timeout and then run with nobody there. Serve
    // the body the author typed instead, and record the exchange the
    // same way a real one is recorded.
    if start.fired.is_some() {
        if protocol != weft_core::signal::Protocol::Http {
            anyhow::bail!(
                "a Socket cannot be fired: its shape is a conversation over time, and there is \
                 nothing honest to invent for the caller's next message. Point a real client at \
                 it (`weft activate` prints the URL)"
            );
        }
        let journal: Arc<dyn crate::caller_conn::CallerJournalSink> = BrokerCallerJournal::start(
            journal,
            ctx.replica.clone(),
            cfg.journal_policy(),
        );
        // The stand-in serves the REQUEST and records the answer, and
        // that is all it can honestly do: a fired run's body, when the
        // author typed one, is a field of the trigger's own wake
        // payload, and the node reads it there. Serving it through here
        // would mean impersonating a caller who never sent it, and the
        // field name to do that would have to live in the language.
        return Ok(Some(RunCaller::Fired(crate::fired_caller::FiredCaller::open(
            execution_id,
            weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol),
            start.request.clone(),
            journal,
        ))));
    }
    // Wait for the connection server to attach the socket for this
    // execution, or for the run to be cancelled while it waits (its caller
    // was refused before attaching): the run then goes straight on to its
    // cancel instead of waiting out the connect timeout.
    tokio::select! {
        attached = ctx.caller_registry.wait_for_attach(execution_id, connect_timeout) => Ok(attached.map(RunCaller::Live)),
        _ = cancelled.cancelled() => Ok(None),
    }
}

/// process-local definition fetch: try the cache first; on miss, call
/// the broker; on success, populate the cache so the next execution
/// of the same shape skips the round trip.
///
/// The broker reads from the append-only `project_definition`
/// history table keyed by `(project_id, definition_hash)`, so a
/// resume task whose `definition_hash` was snapshotted on
/// `ExecutionStarted` (potentially under a now-old shape) still
/// gets the EXACT shape it was started on, even after the user has
/// edited and re-registered the project.
///
/// `Ok(None)` would mean the broker doesn't know about this hash
/// at all (no row was ever registered under it); that's a bug in
/// either the dispatcher's task production or the register flow,
/// so we surface it as a hard error.
async fn fetch_or_cached_project(
    ctx: &Worker,
    definition_hash: &str,
) -> Result<Arc<ProjectDefinition>> {
    {
        let cache = ctx.project_cache.lock().await;
        if let Some(p) = cache.get(definition_hash) {
            return Ok(p.clone());
        }
    }
    match ctx
        .clients
        .project
        .fetch_definition(ctx.project_id, definition_hash)
        .await?
    {
        Some(def) => {
            let arc = Arc::new(def);
            ctx.project_cache
                .lock()
                .await
                .insert(definition_hash.to_string(), arc.clone());
            Ok(arc)
        }
        None => Err(anyhow::anyhow!(
            "no row in project_definition for project {} hash {}; the \
             dispatcher produced a task for a hash that was never recorded \
             (upstream bug in the task-producer)",
            ctx.project_id,
            definition_hash,
        )),
    }
}

/// Per-execution live-connection runtime config + heartbeat interval, set by
/// the execute path and read by the connection server's resolver. Uses a
/// std (sync) mutex: the resolver trait is sync and the critical section
/// is a map lookup, never held across an await.
/// What a live execution registers for the connection server before its
/// caller attaches: the runtime config, the heartbeat interval, and the
/// caller's opening request from the start record.
struct LiveStart {
    runtime: weft_core::caller::CallerRuntimeConfig,
    heartbeat_secs: u64,
    request: Arc<weft_core::caller::LiveRequest>,
    /// The run's journal: the process's, or an unrecorded run's own memory,
    /// so the exchange is kept wherever the rest of the run is.
    journal: Arc<dyn weft_journal::JournalClient>,
}

type LiveConfigMap = Arc<std::sync::Mutex<HashMap<ExecutionId, Arc<LiveStart>>>>;

/// See [`Worker::live_ready`].
type LiveReadyMap = Arc<std::sync::Mutex<HashMap<ExecutionId, tokio::sync::oneshot::Sender<()>>>>;

/// Insertion-ordered cache bounded to `CAP` entries. On overflow it
/// evicts the oldest-inserted hash. Not a true LRU (no per-get reorder):
/// the access pattern is "claim a hash, reuse it for the burst of
/// executions on that shape, move to the next shape," so insertion order
/// already tracks recency closely enough, and the dumb shape keeps the
/// hot `get` path a plain map lookup with no bookkeeping.
struct BoundedProjectCache {
    map: HashMap<String, Arc<ProjectDefinition>>,
    order: std::collections::VecDeque<String>,
}

impl BoundedProjectCache {
    const CAP: usize = 8;

    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    fn get(&self, hash: &str) -> Option<Arc<ProjectDefinition>> {
        self.map.get(hash).cloned()
    }

    fn insert(&mut self, hash: String, def: Arc<ProjectDefinition>) {
        if self.map.insert(hash.clone(), def).is_none() {
            self.order.push_back(hash);
            while self.order.len() > Self::CAP {
                if let Some(evicted) = self.order.pop_front() {
                    self.map.remove(&evicted);
                }
            }
        }
    }
}

/// Stop one execution running on this worker: the single place a cancel
/// is applied, whether it came from a person (`weft stop`, the graph's
/// Cancel), from a deactivate, or from the worker shutting down.
///
/// Firing the flag is the whole of it. The driver owns what happens
/// next: it notices at the top of its next iteration, lets the node
/// tasks it already started finish, closes the charges they opened, and
/// writes the execution's terminal row.
///
/// An unknown execution is not an error. The execution finished on its own
/// before the cancel landed, and the run already has its natural ending.
async fn cancel_execution_id(
    registry: &CancelRegistry,
    execution_id: ExecutionId,
    cause: weft_core::exec::CancelCause,
) {
    let flag = registry.lock().await.get(&execution_id).cloned();
    match flag {
        Some(f) => {
            tracing::info!(
                target: "weft_engine::worker",
                execution_id = %execution_id,
                cause = %cause,
                "firing per-execution cancel flag"
            );
            f.cancel_because(cause);
        }
        None => tracing::debug!(
            target: "weft_engine::worker",
            execution_id = %execution_id,
            "cancel for unknown execution (already terminal); no-op"
        ),
    }
}

// ----- Live caller connection wiring ---------------------------------

/// Journal sink that projects caller events to `ExecEvent::Caller*` rows
/// via the broker. Each event is recorded on a spawned task (the
/// connection hot path stays sync + non-blocking). Live connections are
/// non-durable, so a best-effort spawn matches the design: the exchange
/// is observable/replayable, not a resume-critical durability story.
struct BrokerCallerJournal {
    /// Rows go out through ONE writer task, in the order they were
    /// handed over. Two spawned writes would race, and the pair that
    /// races is exactly the pair whose order carries meaning: the window
    /// holding a conversation's last messages, and the row saying the
    /// caller hung up. A reader would see the goodbye before the words.
    rows: tokio::sync::mpsc::UnboundedSender<CallerRow>,
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

/// What the writer task is handed, in order: a row to write, or a
/// closing sink asking to hear once everything before it is written.
#[derive(Debug)]
enum CallerRow {
    Row(Box<weft_journal::ExecEvent>),
    Drained(tokio::sync::oneshot::Sender<()>),
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

impl BrokerCallerJournal {
    /// Build the sink and start its window clock. The clock stops when
    /// the exchange ends or when the connection drops the sink,
    /// whichever comes first, so a conversation never leaves a task
    /// behind.
    fn start(
        journal: Arc<dyn weft_journal::JournalClient>,
        replica: String,
        policy: weft_core::stream_journal::JournalPolicy,
    ) -> Arc<Self> {
        let (rows, mut incoming) = tokio::sync::mpsc::unbounded_channel();
        let degraded: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        // The writer. One task, one row at a time, awaited: the queue IS
        // the ordering. It ends when the sink drops and the channel
        // closes, so a conversation never leaves a task behind.
        let writer_degraded = degraded.clone();
        tokio::spawn(async move {
            while let Some(row) = incoming.recv().await {
                let event = match row {
                    CallerRow::Row(event) => *event,
                    CallerRow::Drained(done) => {
                        let _ = done.send(());
                        continue;
                    }
                };
                if let Err(e) = journal.record_event(&event, Some(&replica)).await {
                    tracing::error!(
                        target: "weft_engine::caller_conn",
                        error = %e,
                        "caller journal write failed; the exchange is no longer being recorded"
                    );
                    let mut slot = writer_degraded.lock().expect("caller journal degraded");
                    // Keep the FIRST reason: it is the one that explains
                    // the gap, and the ones after it are its echoes.
                    slot.get_or_insert_with(|| format!("journal write failed: {e}"));
                }
            }
        });
        let sink = Arc::new(Self {
            rows,
            degraded,
            policy,
            pending: std::sync::Mutex::new(PendingCallerWindow::default()),
        });
        let weak = Arc::downgrade(&sink);
        let window = policy.window;
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
        sink
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

    /// Hand one row to the writer. The send only fails once the writer
    /// is gone, which happens when this sink is being dropped, so there
    /// is nothing left to tell.
    fn emit(&self, event: weft_journal::ExecEvent) {
        let _ = self.rows.send(CallerRow::Row(Box::new(event)));
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

impl crate::caller_conn::CallerJournalSink for BrokerCallerJournal {
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
    fn close(&self) -> futures::future::BoxFuture<'static, ()> {
        self.flush();
        self.pending.lock().expect("caller journal buffer").closed = true;
        let (done, drained) = tokio::sync::oneshot::channel();
        // The writer holds its receiver for as long as this sink lives,
        // and this sink is alive here, so the ask always reaches it.
        self.rows.send(CallerRow::Drained(done)).expect("the caller journal writer outlives its sink");
        Box::pin(async move {
            drained.await.expect("the caller journal writer answers every drain it is handed");
        })
    }
}

/// Resolver over the worker's per-execution live-config map. The connection
/// server calls this once the run a caller claimed is ready for them
/// (`LiveStarter::start`), to learn the protocol and caps it builds the
/// connection with.
struct LiveConfigResolver {
    live_configs: LiveConfigMap,
    replica: String,
}

impl crate::caller_conn::ConnConfigResolver for LiveConfigResolver {
    fn resolve(&self, execution_id: ExecutionId) -> Option<crate::caller_conn::ResolvedLiveStart> {
        let start = self
            .live_configs
            .lock()
            .expect("live_configs poisoned")
            .get(&execution_id)
            .cloned()?;
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> = BrokerCallerJournal::start(
            start.journal.clone(),
            self.replica.clone(),
            start.runtime.journal,
        );
        Some(crate::caller_conn::ResolvedLiveStart {
            config: start.runtime.clone(),
            heartbeat_secs: start.heartbeat_secs,
            request: start.request.clone(),
            journal: sink,
        })
    }
}

/// Canceller over the worker's per-execution cancel registry. The connection
/// server fires it when a caller drops in a caller-tied (cancel) run.
struct RegistryCanceller {
    cancel_registry: CancelRegistry,
}

impl crate::caller_conn::ExecutionCanceller for RegistryCanceller {
    fn cancel(&self, execution_id: ExecutionId, cause: weft_core::exec::CancelCause) {
        // Block-in-place is wrong here (sync trait method on an async
        // mutex); use try_lock in a short spin via the blocking handle.
        // The cancel registry is a tokio Mutex; grab it with a dedicated
        // runtime-blocking section. In practice it's never contended.
        let reg = self.cancel_registry.clone();
        tokio::spawn(async move {
            if let Some(flag) = reg.lock().await.get(&execution_id).cloned() {
                flag.cancel_because(cause);
            }
        });
    }
}


/// The cancel wait: one held call to the broker for the executions this worker
/// drives, re-asked whenever that set changes. A cancel it hears fires the
/// execution's flag at once.
fn spawn_cancel_wait(worker: Worker) {
    tokio::spawn(async move {
        loop {
            // Armed before the set is read, so a change between the read
            // and the wait still ends the wait.
            let changed = worker.driving_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let execution_ids: Vec<String> = worker.cancel_registry.lock().await.keys().map(|c| c.to_string()).collect();
            if execution_ids.is_empty() {
                changed.await;
                continue;
            }
            let heard = tokio::select! {
                _ = &mut changed => continue,
                heard = worker.clients.tasks.wait_cancels(worker.project_id, execution_ids, weft_task_store::pg_signal::MAX_HOLD) => heard,
            };
            match heard {
                Ok(cancels) => {
                    for asked in cancels {
                        match asked.execution_id.parse::<ExecutionId>() {
                            Ok(execution_id) => cancel_execution_id(&worker.cancel_registry, execution_id, asked.cause).await,
                            Err(e) => tracing::error!(target: "weft_engine::worker", execution_id = %asked.execution_id, error = %e, "a cancel named no execution"),
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "weft_engine::worker", error = %format!("{e:#}"), "cancel wait failed; asking again");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    });
}

/// A short run's self-imposed end, just before the platform's cap: the
/// execution is cancelled with a cause that names the lever, instead of
/// being cut mid-node with no ending written. A run with a live caller has
/// no long form (a long run cannot take a connection), so its cause names
/// what to do instead.
fn guard_short_run(
    worker: &Worker,
    execution_id: ExecutionId,
    flag: Arc<CancellationFlag>,
    class: weft_core::run_class::RunClass,
    live: bool,
) {
    let Some(cap) = worker.short_run_cap.filter(|_| class.is_short()) else { return };
    let stop_at = cap.saturating_sub(SHORT_RUN_CAP_MARGIN);
    tokio::spawn(async move {
        tokio::time::sleep(stop_at).await;
        if flag.is_cancelled() {
            return;
        }
        flag.cancel_because(weft_core::exec::CancelCause::Runtime { detail: short_run_cap_reached(execution_id, cap, live) });
    });
}

/// Why a short run was stopped at the platform's cap, and what to do.
fn short_run_cap_reached(execution_id: ExecutionId, cap: std::time::Duration, live: bool) -> String {
    let minutes = cap.as_secs() / 60;
    if live {
        format!(
            "execution {execution_id} reached the {minutes} minute limit this platform puts on one \
             connection. A conversation meant to last longer keeps its state outside the run and \
             has its client reconnect, each connection its own run; work that takes longer runs \
             as a run of its own (`run_class: long`) that the conversation starts"
        )
    } else {
        format!(
            "execution {execution_id} reached the {minutes} minute limit this platform puts on a short run. \
             Start it as a long run: set `run_class: long` on the trigger that starts it, or \
             use `weft run --long`"
        )
    }
}

fn new_worker(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: &WorkerConfig) -> Worker {
    Worker {
        project_id: config.project_id,
        catalog,
        clients,
        replica: config.replica.clone(),
        tenant_id: config.tenant_id.clone(),
        cancel_registry: Arc::new(Mutex::new(HashMap::new())),
        driving_changed: Arc::new(tokio::sync::Notify::new()),
        project_cache: Arc::new(Mutex::new(BoundedProjectCache::new())),
        caller_registry: crate::caller_conn::CallerRegistry::new(),
        live_configs: Arc::new(std::sync::Mutex::new(HashMap::new())),
        live_ready: Arc::new(std::sync::Mutex::new(HashMap::new())),
        background: weft_core::in_flight::InFlight::new("worker execution"),
        short_run_cap: config.short_run_cap,
    }
}

#[derive(Clone)]
struct ServerState {
    worker: Worker,
    door: WorkerDoor,
}

async fn run_handler(State(state): State<ServerState>, headers: HeaderMap, Path(execution_id): Path<String>) -> Response {
    if !state.door.admits_headers(&headers) {
        return (StatusCode::UNAUTHORIZED, "this worker answers the install only").into_response();
    }
    let Ok(execution_id) = execution_id.parse::<ExecutionId>() else {
        return (StatusCode::BAD_REQUEST, "not an execution id").into_response();
    };
    match state.worker.run_execution_id(execution_id).await {
        Ok(answer) => Json(answer).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:#}")).into_response(),
    }
}

/// Serve this worker until the platform stops it, then wind down: cancel
/// every execution it drives, wait for their endings to be written, and
/// settle the money.
pub async fn serve(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: WorkerConfig) -> Result<()> {
    weft_core::net::install_crypto_provider();
    weft_core::time_scale::announce();
    let worker = new_worker(catalog, clients, &config);
    spawn_cancel_wait(worker.clone());
    // SYNC: the `/_weft` prefix <-> weft_core::route::RESERVED_SEGMENT
    let own = Router::new()
        .route("/_weft/run/{execution_id}", post(run_handler))
        .route("/_weft/healthz", get(|| async { StatusCode::OK }))
        .with_state(ServerState { worker: worker.clone(), door: config.door.clone() });
    let app = match &config.caller_token_secret {
        Some(secret) => own.merge(crate::caller_conn::connection_router(crate::caller_conn::ConnServerState {
            registry: worker.caller_registry.clone(),
            token_secret: Arc::new(secret.clone()),
            project_id: worker.project_id,
            resolver: Arc::new(LiveConfigResolver { live_configs: worker.live_configs.clone(), replica: worker.replica.clone() }),
            clock: worker.clients.clock.clone(),
            canceller: Arc::new(RegistryCanceller { cancel_registry: worker.cancel_registry.clone() }),
            starter: Arc::new(LiveStarter { worker: worker.clone() }),
        })),
        None => {
            tracing::info!(
                target: "weft_engine::caller_conn",
                "no caller-token secret provisioned; this worker takes no live callers"
            );
            own
        }
    };
    crate::caller_conn::serve(app.layer(axum::middleware::map_response(mark_worker_answer)), config.port, shutdown_signal()).await?;
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

/// Run one execution as a job of its own (a long run) and exit when it
/// ends.
pub async fn run_long(catalog: Arc<dyn NodeCatalog>, clients: EngineClients, config: WorkerConfig, execution_id: ExecutionId) -> Result<()> {
    weft_core::net::install_crypto_provider();
    weft_core::time_scale::announce();
    let worker = new_worker(catalog, clients, &WorkerConfig { short_run_cap: None, ..config });
    spawn_cancel_wait(worker.clone());
    let answer = tokio::select! {
        answer = worker.run_execution_id(execution_id) => answer?,
        _ = shutdown_signal() => {
            wind_down(&worker).await;
            anyhow::bail!("the platform stopped the job running execution {execution_id} before it ended");
        }
    };
    wind_down(&worker).await;
    match answer {
        RunAnswer::Completed => Ok(()),
        RunAnswer::NothingToRun => anyhow::bail!("execution {execution_id} had no task to run here (already running elsewhere, or done)"),
        RunAnswer::LeaseLost => anyhow::bail!("execution {execution_id}'s claim was lost mid-run; whoever claims it next runs it"),
        RunAnswer::Failed { error } => anyhow::bail!("execution {execution_id} failed: {error}"),
    }
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

/// Stop what this worker drives the one way a cancel stops anything,
/// then wait for those executions to write their endings, and settle the
/// money. No deadline: what is waited on is the tail of work already told
/// to stop, each node task bounded by the cancel it was just handed.
async fn wind_down(worker: &Worker) {
    let execution_ids: Vec<ExecutionId> = worker.cancel_registry.lock().await.keys().copied().collect();
    for execution_id in execution_ids {
        cancel_execution_id(
            &worker.cancel_registry,
            execution_id,
            weft_core::exec::CancelCause::Runtime { detail: "the worker running this execution was stopped by its platform".into() },
        )
        .await;
    }
    worker.background.wait_zero().await;
    // A charge still open once the executions ended is a call that spent
    // and whose amount no response ever stated: booked as the unknown it
    // is, before the wait, so the record still lands.
    worker.clients.open_charges.flush("the worker stopped before the job was read back");
    worker.clients.pending_costs.wait_zero().await;
}

/// Claims and starts the run a live caller arrived for (the connection
/// server calls it once the caller's ticket checks out).
struct LiveStarter {
    worker: Worker,
}

#[async_trait::async_trait]
impl crate::caller_conn::LiveStarter for LiveStarter {
    async fn start(&self, execution_id: ExecutionId) -> Result<crate::caller_conn::LiveClaim> {
        let Some(claimed) = self.worker.claim_execution_id(execution_id).await? else {
            return Ok(crate::caller_conn::LiveClaim::NotHere);
        };
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        self.worker.live_ready.lock().expect("live_ready poisoned").insert(execution_id, ready_tx);
        let mut starting = Starting { drive: Some(self.worker.drive_detached(claimed)), execution_id, live_ready: self.worker.live_ready.clone() };
        let came = {
            let drive = starting.drive.as_mut().expect("just started");
            tokio::select! {
                told = ready => Ok(told),
                ended = drive => Err(ended),
            }
        };
        // Ready or ended, the drive is no longer this call's to stop.
        starting.drive = None;
        match came {
            Ok(told) => {
                // Only `attach_live_caller` takes the sender, and it sends;
                // the guard that could drop it is this call's own.
                told.expect("a readiness sender is only taken to send on it");
                Ok(crate::caller_conn::LiveClaim::Ready)
            }
            Err(ended) => match ended.context("the drive panicked outside its guard")? {
                RunAnswer::Failed { error } => anyhow::bail!("{error}"),
                other => anyhow::bail!("the run of {execution_id} ended ({other:?}) before its caller could attach"),
            },
        }
    }
}

/// A claimed live run's drive until it is ready for its caller. Dropped
/// before then (the caller hung up while it started, and the request went
/// with them) it stops the drive, before any of the program has run: the
/// run is then left to the reaper, which cancels a live run whose worker
/// let its claim lapse, rather than running with nobody on the line. It
/// always takes back the run's readiness slot.
struct Starting {
    drive: Option<tokio::task::JoinHandle<RunAnswer>>,
    execution_id: ExecutionId,
    live_ready: LiveReadyMap,
}

impl Drop for Starting {
    fn drop(&mut self) {
        self.live_ready.lock().expect("live_ready poisoned").remove(&self.execution_id);
        if let Some(drive) = self.drive.take() {
            drive.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_driver::engine_test_rig::{clients, MemJournal};
    use async_trait::async_trait;

    /// A run stopped at the platform's cap is told the way out that fits
    /// it: a long run for one nobody is talking to, and for one with a
    /// caller (which has no long form) a reconnect over kept state.
    #[test]
    fn the_cap_names_the_way_out_that_fits_the_run() {
        let cap = std::time::Duration::from_secs(3600);
        let live = short_run_cap_reached(ExecutionId::nil(), cap, true);
        assert!(live.contains("60 minute limit") && live.contains("reconnect"), "{live}");
        assert!(!live.contains("weft run --long"), "{live}");
        let plain = short_run_cap_reached(ExecutionId::nil(), cap, false);
        assert!(plain.contains("run_class: long") && plain.contains("weft run --long"), "{plain}");
    }

    #[test]
    fn a_worker_door_admits_only_its_key() {
        let key = "0123456789abcdef0123456789abcdef";
        let door = WorkerDoor::parse(&format!("key:{key}")).unwrap();
        let mut headers = HeaderMap::new();
        assert!(!door.admits_headers(&headers), "no bearer");
        headers.insert(axum::http::header::AUTHORIZATION, format!("Bearer {key}").parse().unwrap());
        assert!(!door.admits_headers(&headers), "a caller's own Authorization is the program's, never the door's");
        headers.insert(weft_platform_traits::WORKER_AUTH_HEADER, format!("Bearer {key}").parse().unwrap());
        assert!(door.admits_headers(&headers));
        headers.insert(weft_platform_traits::WORKER_AUTH_HEADER, "Bearer 00112233445566778899aabbccddeeff".parse().unwrap());
        assert!(!door.admits_headers(&headers), "another key");
        assert!(WorkerDoor::parse("platform").unwrap().admits_headers(&HeaderMap::new()));
        assert!(WorkerDoor::parse("key:abcd").is_err(), "too short to guard anything");
        assert!(WorkerDoor::parse("open").is_err(), "no third way");
    }

    #[test]
    fn a_run_answer_says_how_it_ended() {
        for a in [
            RunAnswer::Completed,
            RunAnswer::Failed { error: "boom".into() },
            RunAnswer::LeaseLost,
            RunAnswer::NothingToRun,
        ] {
            let v = serde_json::to_value(&a).unwrap();
            assert_eq!(serde_json::from_value::<RunAnswer>(v).unwrap(), a);
        }
        assert_eq!(serde_json::to_value(RunAnswer::NothingToRun).unwrap(), serde_json::json!({ "ended": "nothing_to_run" }));
    }

    /// Answers one scripted cancel for the first execution it is asked about,
    /// then holds each later wait for its full length.
    struct OneCancel {
        asked: std::sync::Mutex<Vec<Vec<String>>>,
        given: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl weft_task_store::TaskStoreClient for OneCancel {
        async fn wait_cancels(&self, _p: uuid::Uuid, execution_ids: Vec<String>, wait: std::time::Duration) -> anyhow::Result<Vec<weft_task_store::tasks::CancelAsked>> {
            self.asked.lock().unwrap().push(execution_ids.clone());
            if !self.given.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Ok(vec![weft_task_store::tasks::CancelAsked { execution_id: execution_ids[0].clone(), cause: weft_core::exec::CancelCause::User }]);
            }
            tokio::time::sleep(wait).await;
            Ok(Vec::new())
        }
        async fn enqueue_dedup(&self, _s: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!()
        }
        async fn wait_for_terminal(&self, _t: uuid::Uuid, _to: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!()
        }
        async fn claim_execution(&self, _p: &str, _project: uuid::Uuid, _execution: &str) -> anyhow::Result<Option<ClaimedExecution>> {
            Ok(None)
        }
        async fn heartbeat(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn requeue(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(&self, _t: uuid::Uuid, _p: &str, _r: serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(&self, _t: uuid::Uuid, _p: &str, _e: String) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn worker_over(tasks: Arc<dyn weft_task_store::TaskStoreClient>) -> Worker {
        let mut clients = clients(Arc::new(MemJournal::default()));
        clients.tasks = tasks;
        struct NoCatalog;
        impl NodeCatalog for NoCatalog {
            fn lookup(&self, _t: &str) -> Option<&'static dyn weft_core::Node> {
                None
            }
            fn all(&self) -> Vec<&'static str> {
                Vec::new()
            }
        }
        new_worker(
            Arc::new(NoCatalog),
            clients,
            &WorkerConfig {
                project_id: uuid::Uuid::from_u128(1),
                tenant_id: "t".into(),
                replica: "worker-1".into(),
                door: WorkerDoor::Platform,
                caller_token_secret: None,
                port: 0,
                short_run_cap: None,
            },
        )
    }

    // A cancel the broker answers reaches the flag of the execution being
    // driven, and the wait asks for exactly the executions driven: it starts
    // only once an execution is registered, whichever of the two comes first.
    weft_core::stress_test! {
        name: a_heard_cancel_fires_the_flag_of_the_execution_id_driven,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let tasks = Arc::new(OneCancel { asked: std::sync::Mutex::new(Vec::new()), given: false.into() });
            let worker = worker_over(tasks.clone());
            spawn_cancel_wait(worker.clone());
            let execution_id = ExecutionId::new_v4();
            let flag = CancellationFlag::new_arc();
            worker.cancel_registry.lock().await.insert(execution_id, flag.clone());
            worker.driving_changed.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !flag.is_cancelled() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the cancel reached the flag");
            assert_eq!(tasks.asked.lock().unwrap()[0], vec![execution_id.to_string()]);
        }
    }

    #[tokio::test]
    async fn a_call_for_an_execution_with_nothing_to_claim_says_so() {
        let tasks = Arc::new(OneCancel { asked: std::sync::Mutex::new(Vec::new()), given: true.into() });
        let worker = worker_over(tasks);
        assert_eq!(worker.run_execution_id(ExecutionId::new_v4()).await.unwrap(), RunAnswer::NothingToRun);
    }
}
