//! Trigger setup captures SignalSpec and input ports in the journal without
//! contacting a listener. Activation arms the completed capture. Suspensions
//! arm immediately and return the token the worker waits on. Both arming paths
//! have the listener compute the signal's row (`prepare`, which starts
//! nothing), write it, then have the listener bring the signal up
//! (`start`), so whatever the signal starts always finds its row.
//!
//! Idempotency: dedup keyed on `(execution, node_id, frames, is_resume,
//! call_index)` so a process-crash retry converges on the same task. The
//! task's body is itself idempotent: entry rows reuse a stable token
//! per `(project_id, node_id)`, resume rows mint per-suspension.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_core::frames::LoopFrames;
use weft_core::primitive::SignalSpec;
use weft_core::signal as core_signal;
use weft_core::signal::listener_protocol::StartMode;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSignalPayload {
    pub execution_id: String,
    pub node_id: String,
    pub frames: LoopFrames,
    pub spec: SignalSpec,
    pub is_resume: bool,
    /// 0-based ordinal of the `await_signal` call within this
    /// (execution, node_id, frames). Set by the worker; the dispatcher
    /// stamps it on the SuspensionRegistered event so replay can
    /// rebuild the per-(node, frames) sequence in order. Must not
    /// vary across replays of the same body, so the dedup key
    /// includes it. Required: a missing field would silently default
    /// to 0 and collide every await on the same frame stack.
    pub call_index: u32,
    /// The trigger's delivered port values at registration time (an
    /// entry registration only; `None` on resumes). Stored on the
    /// signal row and replayed onto the trigger's ports at every fire.
    #[serde(default)]
    pub port_snapshot: Option<serde_json::Value>,
    /// When the registration was asked for (ms since the epoch): the
    /// worker stamps the moment the node called in, activation stamps
    /// its own start. Handed to the listener so a relative wait counts
    /// from here, whatever the trip to the listener cost.
    pub asked_at_unix_ms: i64,
}

pub struct RegisterSignalExecutor;

/// The stored mount path for a public-entry surface, namespaced by the owning
/// tenant: `/<tenant>/<pattern>`. The tenant prefix walls each account into
/// its own path space, so two tenants can both claim `chat` without
/// colliding, and one tenant claiming a path never blocks another (the old
/// global unique index did both wrong). Callers reach it at
/// `/connect/<tenant>/<path>` (live) or `POST /<tenant>/<path>` (public
/// fire), the tenant segment is in the URL. A guessable URL is fine here:
/// live/public endpoints are API surfaces whose callers bring their own
/// auth; the tenant prefix is for COLLISION, not secrecy. The path is a
/// route PATTERN (`chat/{room}`), stored as written; the dispatcher matches
/// calls against it in Rust.
fn mount_path_for(
    surface: &weft_core::primitive::SignalSurface,
    tenant: &str,
) -> Option<String> {
    match surface {
        weft_core::primitive::SignalSurface::PublicEntry { path, .. } => {
            let path = path.trim_start_matches('/');
            Some(if path.is_empty() {
                format!("/{tenant}")
            } else {
                format!("/{tenant}/{path}")
            })
        }
        weft_core::primitive::SignalSurface::TaskCallback
        | weft_core::primitive::SignalSurface::Internal => None,
    }
}

/// The stored method list for a surface: a public entry's methods
/// (empty = any), empty for every other surface.
fn mount_methods_for(surface: &weft_core::primitive::SignalSurface) -> Vec<String> {
    match surface {
        weft_core::primitive::SignalSurface::PublicEntry { methods, .. } => methods.clone(),
        weft_core::primitive::SignalSurface::TaskCallback
        | weft_core::primitive::SignalSurface::Internal => Vec::new(),
    }
}

/// The pattern under the tenant prefix a stored mount path carries
/// (`/alice/chat/{room}` -> `chat/{room}`, `/alice` -> ``). Every row of a
/// tenant is prefixed the same way, so the strip is exact.
pub(crate) fn pattern_of_mount_path(mount_path: &str, tenant: &str) -> String {
    let prefix = format!("/{tenant}");
    let rest = mount_path.strip_prefix(&prefix).unwrap_or(mount_path);
    rest.trim_start_matches('/').to_string()
}

/// One other public entry of the tenant, as the overlap check sees it.
pub(crate) struct RegisteredRoute {
    pub pattern: String,
    pub methods: Vec<String>,
    pub project_id: uuid::Uuid,
    pub node_id: String,
}

/// The identity a captured trigger is registered under: its address,
/// the way the source spells it (`door` at the top of the program,
/// `caption.door` for the `door` inside the file the site `caption`
/// includes). An included file is a group with a different lifetime,
/// so a trigger inside one is as ordinary as a trigger inside a group:
/// the call frames it registers under name the site, and the address
/// is what a fire resolves back to the node and its site. Two sites
/// including one file are two triggers with two addresses. What is
/// refused is a trigger inside a LOOP (an entry cannot fire per
/// iteration) and a second registration from one node (the engine
/// caps it too; belt and braces).
pub(crate) fn captured_trigger_address(
    project: &weft_core::ProjectDefinition,
    node_id: &str,
    frames: &LoopFrames,
    call_index: u32,
) -> Result<String> {
    anyhow::ensure!(
        frames.iter().all(|frame| frame.loop_index().is_none()),
        "entry capture cannot occur inside a loop"
    );
    anyhow::ensure!(call_index == 0, "entry capture cannot register more than once");
    anyhow::ensure!(
        project.nodes.iter().any(|node| node.id == node_id && node.features.is_trigger),
        "entry capture must come from a trigger in its original program"
    );
    let call_path: Vec<String> = weft_core::frames::call_path(frames).into_iter().map(str::to_string).collect();
    Ok(weft_core::project::address_of(project, node_id, &call_path))
}

/// The place a registration is for, spelled the way a person writes it
/// (`door`, `one.door` for the `door` inside the file the site `one`
/// includes), and the node behind it. The spelling is what the signal
/// row carries as its `node_id` (see `SignalRegistration::node_id`),
/// for an entry and a resume alike: a node inside a file called from
/// two places is two places, each with its own registrations, and the
/// spelling is the one key that tells them apart. A person then names
/// a registration everywhere (`weft wake`, a display, the task list)
/// the way they name the node.
///
/// An entry arrives already spelled ([`captured_trigger_address`], which
/// the activate loop hands over with no frames), so the spelling is a
/// fixed point for it. A resume arrives as the compiled id under the
/// frames it fired at, and the call sites among those frames place it;
/// the loop iterations do not, because a place is not an iteration.
pub(crate) fn registered_place<'a>(
    project: &'a weft_core::ProjectDefinition,
    node_id: &str,
    frames: &LoopFrames,
) -> Result<(String, &'a weft_core::project::NodeDefinition)> {
    let (id, mut path) = weft_core::project::resolve_address(project, node_id);
    path.extend(weft_core::frames::call_path(frames).into_iter().map(str::to_string));
    let node = project
        .nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| anyhow::anyhow!("node_id='{node_id}' not in project"))?;
    Ok((weft_core::project::address_of(project, &id, &path), node))
}

/// Is there a registered route that shares a call with this one and
/// that nothing can arbitrate between? Sharing a call is fine when one
/// of the two is the more specific: it serves what it spells out and
/// the other serves the rest, which is how `users/me` lives beside
/// `users/{id}`. What has to be refused is the pair where neither
/// wins, because a shared call would then have two equal claims.
/// Pure: the first offender, or `None`. The stored pattern of another
/// row that no longer parses is skipped, never a reason to refuse this
/// one (that row's own register validated it).
pub(crate) fn ambiguous_route<'a>(
    pattern: &weft_core::route::RoutePattern,
    methods: &[String],
    others: impl IntoIterator<Item = &'a RegisteredRoute>,
) -> Option<&'a RegisteredRoute> {
    others.into_iter().find(|other| {
        weft_core::route::RoutePattern::parse(&other.pattern)
            .map(|theirs| {
                weft_core::route::compare_patterns(pattern, &theirs)
                    == weft_core::route::PatternOrder::Ambiguous
                    && weft_core::route::methods_overlap(methods, &other.methods)
            })
            .unwrap_or(false)
    })
}

#[async_trait]
impl TaskExecutor<DispatcherState> for RegisterSignalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RegisterSignalPayload = serde_json::from_value(task.payload.clone())?;
        if !payload.is_resume {
            core_signal::validate_spec(&payload.spec).map_err(anyhow::Error::msg)?;
            let execution_id = payload.execution_id.parse()?;
            let rows = state.journal.events_log(execution_id).await?;
            anyhow::ensure!(matches!(rows.first(), Some(weft_journal::ExecEvent::ExecutionStarted {
                phase: weft_core::context::Phase::TriggerSetup, program: Some(_), ..
            })), "entry capture requires a trigger-setup run with a pinned program");
            let found = crate::projection::execution_program(state, execution_id).await?;
            let project = found.program().context("entry capture has no original program")?;
            let node_id = captured_trigger_address(&project, &payload.node_id, &payload.frames, payload.call_index)?;
            let ports = payload.port_snapshot.context("entry capture requires its input port snapshot")?;
            anyhow::ensure!(ports.is_object(), "entry capture ports must be an object");
            state.journal.record_event_dedup(&weft_journal::ExecEvent::TriggerCaptured {
                execution_id, node_id: node_id.clone(), spec: payload.spec, port_snapshot: ports,
                at_unix: crate::lease::now_unix() as u64,
            }, &format!("trigger_capture:{execution_id}:{node_id}")).await?;
            return Ok(serde_json::to_value(weft_core::primitive::RegisterSignalResult::Captured)?);
        }
        let token = Self::arm(state, payload).await?.token;
        Ok(serde_json::to_value(weft_core::primitive::RegisterSignalResult::Registered { token })?)
    }
}

impl RegisterSignalExecutor {
    /// Arm a captured entry or a live suspension through the same path.
    /// The answer says what the arm did to the row, so a caller whose
    /// later step fails can take it back ([`disarm`]).
    pub(crate) async fn arm(state: &DispatcherState, payload: RegisterSignalPayload) -> Result<Armed> {
        let execution_id: weft_core::ExecutionId = payload
            .execution_id
            .parse()
            .map_err(|e| anyhow::anyhow!("bad execution: {e}"))?;

        // Per-kind validation: each `Signal` impl owns its rules
        // (cron parses, path well-formed, url is http(s), etc).
        // Surfacing this here, before the listener round-trip, keeps
        // the failure attached to the worker that asked, with a clean
        // message instead of a half-set-up signal row.
        if let Err(e) = core_signal::validate_spec(&payload.spec) {
            anyhow::bail!("invalid signal spec: {e}");
        }

        // Resolve tenant + project_id. Token reuse for entry rows keeps the
        // registration stable across reactivates; resume rows always mint
        // fresh.
        let Some(owner) = state.journal.execution_owner(execution_id).await? else {
            anyhow::bail!("no execution row for execution {execution_id}")
        };
        let project_id = owner.project_id;
        // The tenant stamped on the execution, not one re-derived from
        // the project store: same answer while the project lives, and
        // still an answer once it does not.
        let tenant = owner.tenant;
        // Whose signal: the run's instance (an instance's trigger setup arms
        // that instance's copy; an instance's run waits as that instance).
        let instance = owner.instance;
        let fired_by = owner.fired_by;

        // The place this registration is for, spelled: the row's key.
        // Read off the original program, the one this registration's
        // node was compiled into, so the spelling is the one that
        // program's source reads.
        let project_def = crate::projection::execution_program(state, execution_id).await?
            .program().context("register_signal: original program is unavailable")?;
        let (place, node) = registered_place(&project_def, &payload.node_id, &payload.frames)
            .with_context(|| format!("register_signal: project_id={project_id}"))?;
        // Tags drive the signal-token enumeration filter; charset
        // already validated at parse time.
        let tags = node.tags();

        // Resume tokens derive from the suspension identity so a retry of
        // this task converges on the same token. The identity (execution,
        // node_id, frames, call_index) is what the engine's fold uses to
        // match resumes. Entry tokens are reused across reactivates and
        // read off the existing row below.
        let resume_token = payload.is_resume.then(|| {
            let mut hasher = Sha256::new();
            hasher.update(execution_id.to_string().as_bytes());
            hasher.update(b":");
            hasher.update(payload.node_id.as_bytes());
            hasher.update(b":");
            for frame in &payload.frames {
                hasher.update(frame.text().as_bytes());
                hasher.update(b"/");
            }
            hasher.update(b":");
            hasher.update(payload.call_index.to_le_bytes());
            let bytes = hasher.finalize();
            let mut buf = [0u8; 16];
            buf.copy_from_slice(&bytes[..16]);
            uuid::Uuid::from_bytes(buf).to_string()
        });

        let resume_execution_id = payload.is_resume.then(|| execution_id.to_string());
        let spec_json = serde_json::to_string(&payload.spec)?;

        let events = state.journal.events_log(execution_id).await?;
        let (program, source_version) = match events.first() {
            Some(weft_journal::ExecEvent::ExecutionStarted { program, source_version, .. }) => (program.clone(), source_version.clone()),
            _ => anyhow::bail!("register_signal: execution has no birth"),
        };

        // From the overlap check to the signal row's insert, this tenant's
        // registrations run one at a time install-wide: the check reads the
        // rows the insert writes, so two routes registering at once would
        // each pass the check before the other's row existed and both arm
        // (`chat/{a}` next to `chat/{b}`). The lock comes from the lock
        // pool, so a waiter pins no work connection.
        let mount_key = crate::lease::advisory_key(crate::lease::SIGNAL_MOUNT_DOMAIN, tenant.as_str());
        crate::lease::with_advisory_lock_blocking(&state.lock_pool, mount_key, || async move {
            // The row's kind state is computed from the state it replaces,
            // and written only while the row is still at the version that
            // state was read at (`signal_insert` is a compare-and-set). A
            // wake claiming a moment in between moves the row, so this
            // reads again and asks the kind again; each round lost is a
            // claim some wake made, so running out of rounds means the
            // signal's wakes never stop claiming, which is a bug to see.
            const ROUNDS: usize = 5;
            let mut round = 0;
            let (token, prior) = loop {
                round += 1;
                let (token, prior) =
                    read_prior(state.journal.as_ref(), resume_token.as_deref(), project_id, &place, instance.as_ref()).await?;
                let prior_seq = prior.as_ref().map_or(0, |row| row.kind_state_seq);
                // A resume starts its kind state afresh; an entry carries
                // its row's forward (a feed cursor keeps its place).
                let prior_kind_state = prior.as_ref().filter(|row| !row.is_resume).map(|row| row.kind_state.clone());
                let prepared = state
                    .listener
                    .prepare(&weft_core::signal::listener_protocol::PrepareRequest {
                        token: token.clone(),
                        tenant_id: tenant.as_str().to_string(),
                        spec: payload.spec.clone(),
                        node_id: place.clone(),
                        is_resume: payload.is_resume,
                        execution_id: resume_execution_id.clone(),
                        source: weft_core::signal::listener_protocol::PrepareSource {
                            prior_kind_state,
                            asked_at_unix_ms: payload.asked_at_unix_ms,
                        },
                    })
                    .await?;
                refuse_unarmable_route(state, &prepared.routing.surface, instance.as_ref(), tenant.as_str(), project_id, &place).await?;

                let written = state
                    .journal
                    .signal_insert(&crate::journal::SignalRegistration {
                        instance: instance.clone(),
                        // An entry signal is its own trigger's; a wait is the
                        // activation's of the trigger that fired its run (none
                        // for a run started by hand).
                        activation_trigger: if payload.is_resume { fired_by.clone() } else { Some(place.clone()) },
                        setup_execution_id: (!payload.is_resume).then_some(execution_id),
                        source_version: source_version.clone(),
                        program: program.clone(),
                        token: token.clone(),
                        tenant_id: tenant.to_string(),
                        project_id,
                        execution_id: if payload.is_resume { Some(execution_id) } else { None },
                        node_id: place.clone(),
                        is_resume: payload.is_resume,
                        spec_json: spec_json.clone(),
                        access_id: payload.spec.access.as_ref().map(|a| a.id.clone()),
                        consumer_kind: payload.spec.consumer_kind.clone(),
                        tags: tags.clone(),
                        port_snapshot: payload.port_snapshot.clone(),
                        consumer_payload: (!prepared.rendered.is_null()).then_some(prepared.rendered),
                        surface_kind: prepared.routing.surface.kind_tag().to_string(),
                        mount_path: mount_path_for(&prepared.routing.surface, tenant.as_str()),
                        mount_methods: mount_methods_for(&prepared.routing.surface),
                        auth_kind: prepared.routing.auth.kind_tag().to_string(),
                        auth_config: (!prepared.routing.auth_config.is_null()).then_some(prepared.routing.auth_config),
                        kind_state: prepared.kind_state,
                        kind_state_seq: prior_seq,
                    })
                    .await?;
                match written {
                    crate::journal::SignalWrite::Written => break (token, prior),
                    crate::journal::SignalWrite::StateMoved if round < ROUNDS => continue,
                    crate::journal::SignalWrite::StateMoved => anyhow::bail!(
                        "signal {token}'s state moved under each of {ROUNDS} registrations in a row: \
                         its wakes keep claiming it while it is being registered"
                    ),
                }
            };

            // The row is committed, so whatever the signal starts (its
            // first wake, its held connection) finds it. A signal that
            // cannot come up is undone: a row this call created goes, and
            // the listener forgets whatever part of it did come up; a row
            // it replaced goes back to what the listener still runs.
            let undo = match prior {
                None => ArmUndo::Created,
                Some(prior) => ArmUndo::Replaced(Box::new(prior)),
            };
            if let Err(e) = state.listener.start(&token, StartMode::New).await {
                // A start that failed left the listener on what it ran
                // before, so a replaced row goes back without a restart
                // (unless a wake ran the new row meanwhile, see
                // `undo_registration`).
                undo_arm(state.journal.as_ref(), &state.listener, &token, undo, Restart::No).await.map_err(|undo_err| {
                    anyhow::anyhow!(
                        "signal {token} could not start ({e:#}), and undoing its registration failed too, \
                         so its row may not match what the listener runs until the trigger is \
                         deactivated or the run ends: {undo_err:#}"
                    )
                })?;
                return Err(e);
            }

            if payload.is_resume {
                // Suspension state lives on the signal row; we also
                // journal SuspensionRegistered so the engine's fold can
                // rebuild the awaited-sequence replay structure on
                // worker restart. Sequenced AFTER signal_insert so the
                // signal row exists by the time anything reads the
                // journal entry. Dedup key collapses retries on the
                // same (execution, node_id, frames, call_index); a failure
                // here triggers the task framework to retry, and the
                // registration above converges on the same row.
                let now = crate::lease::now_unix() as u64;
                let frames_key = payload
                    .frames
                    .iter()
                    .map(weft_core::frames::Frame::text)
                    .collect::<Vec<_>>()
                    .join("/");
                state
                    .journal
                    .record_event_dedup(
                        &weft_journal::ExecEvent::SuspensionRegistered {
                            execution_id,
                            node_id: payload.node_id.clone(),
                            frames: payload.frames.clone(),
                            token: token.clone(),
                            spec: payload.spec.clone(),
                            call_index: payload.call_index,
                            at_unix: now,
                        },
                        &format!(
                            "register_signal:{execution_id}:{node_id}:{frames_key}:{call_index}",
                            execution_id = execution_id,
                            node_id = payload.node_id,
                            call_index = payload.call_index
                        ),
                    )
                    .await?;
            }

            Ok(Armed { token, undo })
        })
        .await
    }
}

/// A signal [`RegisterSignalExecutor::arm`] armed, and how to take it back.
#[derive(Debug, Clone)]
pub(crate) struct Armed {
    pub token: String,
    pub undo: ArmUndo,
}

/// What an arm did to its signal's row.
#[derive(Debug, Clone)]
pub(crate) enum ArmUndo {
    /// No row was there: taking the arm back removes it.
    Created,
    /// This row was there (an entry's reused token, a resume's retry):
    /// taking the arm back writes it again.
    Replaced(Box<crate::journal::SignalRegistration>),
}

/// Whether taking an arm back has the listener run the restored row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Restart {
    /// The replacement never started, so the listener still runs the
    /// row as it was.
    No,
    /// The replacement started, so the listener runs it now and has to
    /// be brought back to the restored row.
    Yes,
}

/// The listener calls taking an arm back makes. `ListenerClient` in
/// production; a recorder in unit tests, which have no listener.
#[async_trait]
pub(crate) trait SignalHolder: Send + Sync {
    async fn start(&self, token: &str, mode: StartMode) -> Result<()>;
    async fn unregister_many(&self, signals: &[crate::journal::SignalRegistration]);
}

#[async_trait]
impl SignalHolder for crate::listener::ListenerClient {
    async fn start(&self, token: &str, mode: StartMode) -> Result<()> {
        crate::listener::ListenerClient::start(self, token, mode).await
    }
    async fn unregister_many(&self, signals: &[crate::journal::SignalRegistration]) {
        crate::listener::ListenerClient::unregister_many(self, signals).await
    }
}

/// Take back arms that STARTED, newest first, so each signal ends as it
/// was before any of them: a created row goes and the listener forgets
/// it; a replaced row is written back and the listener brings that row
/// up again in place of the one it was started with. Every arm is
/// attempted even after one fails, and the failures come back together,
/// naming each signal the listener did not confirm it runs again.
pub(crate) async fn disarm(
    journal: &dyn crate::journal::Journal,
    listener: &dyn SignalHolder,
    armed: Vec<Armed>,
) -> Result<()> {
    let mut failed = Vec::new();
    for Armed { token, undo } in armed.into_iter().rev() {
        if let Err(e) = undo_arm(journal, listener, &token, undo, Restart::Yes).await {
            failed.push(format!("signal {token}: {e:#}"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "could not put back {} armed signal(s): {}", failed.len(), failed.join("; "));
    Ok(())
}

async fn undo_arm(
    journal: &dyn crate::journal::Journal,
    listener: &dyn SignalHolder,
    token: &str,
    undo: ArmUndo,
    restart: Restart,
) -> Result<()> {
    let prior = match undo {
        ArmUndo::Created => None,
        ArmUndo::Replaced(prior) => Some(*prior),
    };
    match undo_registration(journal, token, prior, restart).await? {
        Undone::Removed(removed) => listener.unregister_many(&removed).await,
        // A put-back: whatever the listener runs under the token came from
        // the replacement, so it is replaced; the row ran before this arm,
        // so a failure bringing it back is the listener's to retry, never
        // a refusal that turns a live trigger off.
        Undone::Restored { rerun: true } => listener
            .start(token, StartMode::PutBack)
            .await
            .context("its row is back, but the listener did not confirm it runs it (a listener that answered stopped the replacement and keeps retrying the row, shown as down)")?,
        Undone::Restored { rerun: false } => {}
    }
    Ok(())
}

/// The token a registration writes and the row it replaces, when there
/// is one. A resume's token is its own (`resume_token`), and its row,
/// when a retry finds one, is the one it replaces. An entry reuses the
/// token of the row already armed at its place; with none it mints one.
async fn read_prior(
    journal: &dyn crate::journal::Journal,
    resume_token: Option<&str>,
    project_id: uuid::Uuid,
    place: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(String, Option<crate::journal::SignalRegistration>)> {
    if let Some(token) = resume_token {
        return Ok((token.to_string(), journal.signal_get(token).await?));
    }
    Ok(match journal.signal_entry_at(project_id, place, instance).await? {
        Some(row) => (row.token.clone(), Some(row)),
        None => (uuid::Uuid::new_v4().to_string(), None),
    })
}

/// What undoing a registration that could not start did.
#[derive(Debug)]
enum Undone {
    /// The row was this registration's own and is gone; the listener
    /// is to forget these.
    Removed(Vec<crate::journal::SignalRegistration>),
    /// The row existed before (an entry's reused token, a resume's
    /// retry) and is back to it. `rerun` when the listener has to bring
    /// the restored row up again to match it (see [`undo_registration`]).
    Restored { rerun: bool },
}

/// Undo a registration whose row is written. `prior` is the row it
/// replaced (see [`read_prior`]); `None` means it created the row. A
/// restore is a compare-and-set, retried when a claim moved the row
/// under it.
///
/// The prior row's kind state always goes back with the rest of it. A
/// claim that moved the row since the registration wrote it was made
/// for the NEW spec whatever `restart` says: only a kind that wakes
/// claims its state, and its wake reads the row as it is now (a wake
/// that had read the prior row loses its claim, since the registration
/// moved the version under it). So that claim's state, and the next wake
/// it set, belong to a spec that is being taken back.
///
/// The listener then runs the restored row again (`rerun`) when it has
/// run anything of the new one: `Restart::Yes` (the replacement
/// started), or a moved version (a wake ran the new spec and set its
/// next wake from it; bringing the prior row up sets the wake its own
/// state asks for). With `Restart::No` and nothing moved, the listener
/// still runs exactly the prior row.
async fn undo_registration(
    journal: &dyn crate::journal::Journal,
    token: &str,
    prior: Option<crate::journal::SignalRegistration>,
    restart: Restart,
) -> Result<Undone> {
    let Some(prior) = prior else {
        return Ok(Undone::Removed(journal.signal_remove_many(&[token.to_string()]).await?));
    };
    // The row this registration wrote sits one past the version it read.
    let written_seq = prior.kind_state_seq + 1;
    const ROUNDS: usize = 5;
    for _ in 0..ROUNDS {
        let current = journal
            .signal_get(token)
            .await?
            .with_context(|| format!("signal {token}'s row vanished before it could be put back"))?;
        let rerun = restart == Restart::Yes || current.kind_state_seq != written_seq;
        let restored = crate::journal::SignalRegistration { kind_state_seq: current.kind_state_seq, ..prior.clone() };
        match journal.signal_restore(&restored).await? {
            crate::journal::SignalWrite::Written => return Ok(Undone::Restored { rerun }),
            crate::journal::SignalWrite::StateMoved => continue,
        }
    }
    anyhow::bail!("signal {token}'s state moved under each of {ROUNDS} attempts to put its row back")
}

/// Refuse a route that cannot be armed here, naming the way that works.
async fn refuse_unarmable_route(
    state: &DispatcherState,
    surface: &weft_core::primitive::SignalSurface,
    instance: Option<&weft_core::instance::InstanceId>,
    tenant: &str,
    project_id: uuid::Uuid,
    place: &str,
) -> Result<()> {
    let weft_core::primitive::SignalSurface::PublicEntry { path, methods } = surface else {
        return Ok(());
    };
    // A public address is one per trigger: the instance of a call to it is
    // named per call (the `Weft-Instance` header on a gated route, or an
    // instance token), never by giving each instance a copy of the route.
    if let Some(instance) = instance {
        anyhow::bail!(
            "trigger '{place}' serves the public address '{path}', and an address is shared by \
             every instance, so it cannot be armed for instance '{instance}'. Keep the route shared \
             and name the instance per call: the {} header on a route gated by a connection, or \
             an instance token",
            weft_core::instance::INSTANCE_HEADER,
        );
    }

    // Route overlap check: another (project, node) of THIS tenant already
    // serves a call this route would claim (`chat/{room}` against
    // `chat/{x}`, or `chat/general`, on a shared method). Refuse with a
    // clear error rather than let the gateway pick one at call time. Same
    // (project, node) reclaiming its route on reactivate is fine because
    // it reuses the existing token. The rows are already tenant-prefixed,
    // so only the caller's own account is in play; another tenant's
    // identical pattern has a different prefix and cannot collide.
    let mine = weft_core::route::RoutePattern::parse(path).map_err(anyhow::Error::msg)?;
    let others: Vec<RegisteredRoute> = sqlx::query_as::<_, (String, Vec<String>, uuid::Uuid, String)>(
        "SELECT mount_path, mount_methods, project_id, node_id \
         FROM signal \
         WHERE tenant_id = $1 AND mount_path IS NOT NULL \
           AND NOT (project_id = $2 AND node_id = $3)",
    )
    .bind(tenant)
    .bind(project_id)
    .bind(place)
    .fetch_all(&state.pg_pool)
    .await?
    .into_iter()
    .map(|(mp, ms, project_id, node_id)| RegisteredRoute {
        pattern: pattern_of_mount_path(&mp, tenant),
        methods: ms,
        project_id,
        node_id,
    })
    .collect();
    if let Some(taken) = ambiguous_route(&mine, methods, &others) {
        let method_words = |m: &[String]| if m.is_empty() { "any method".to_string() } else { m.join("/") };
        anyhow::bail!(
            "route `{}` ({}) and `{}` ({}), already registered by \
             project='{}' node='{}', can both be reached by one \
             call and neither is the more specific, so that call \
             has no answer. Both projects are yours, so: make one \
             of them spell out what the other captures, change \
             this route's `path` or `method`, or free the other \
             with `weft deactivate --project {}` (a project you \
             are done with can also go entirely, `weft rm {}`)",
            mine.as_str(),
            method_words(methods),
            taken.pattern,
            method_words(&taken.methods),
            taken.project_id,
            taken.node_id,
            taken.project_id,
            taken.project_id,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ambiguous_route, captured_trigger_address, mount_methods_for, mount_path_for, registered_place,
        pattern_of_mount_path, RegisteredRoute,
    };
    use weft_core::frames::Frame;
    use weft_core::primitive::SignalSurface;
    use weft_core::route::RoutePattern;

    /// A program with a trigger at the top and the same body included
    /// through two sites, each holding a trigger.
    fn program_with_included_triggers() -> weft_core::ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                {"id": "top", "nodeType": "Route", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "features": {"isTrigger": true}, "requiresInfra": false},
                {"id": "Api.door", "nodeType": "Route", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "scope": ["Api"], "features": {"isTrigger": true}, "requiresInfra": false},
                {"id": "Api.work", "nodeType": "Debug", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "scope": ["Api"], "features": {"isTrigger": false}, "requiresInfra": false}
            ],
            "edges": [],
            "groups": [
                {"id": "one", "kind": "call", "body": "Api", "nodeIds": []},
                {"id": "two", "kind": "call", "body": "Api", "nodeIds": []},
                {"id": "Api", "kind": "body", "nodeIds": ["Api.door", "Api.work"]}
            ]
        }))
        .expect("a valid program")
    }

    #[test]
    fn an_included_trigger_is_captured_under_its_address_like_one_in_a_group() {
        let p = program_with_included_triggers();
        assert_eq!(captured_trigger_address(&p, "top", &vec![], 0).unwrap(), "top");
        let under_one = vec![Frame::Call { site: "one".into() }];
        let under_two = vec![Frame::Call { site: "two".into() }];
        assert_eq!(captured_trigger_address(&p, "Api.door", &under_one, 0).unwrap(), "one.door");
        assert_eq!(captured_trigger_address(&p, "Api.door", &under_two, 0).unwrap(), "two.door");
        // The address a fire resolves lands back on the node and its site.
        assert_eq!(
            weft_core::project::resolve_address(&p, "two.door"),
            ("Api.door".to_string(), vec!["two".to_string()])
        );
    }

    /// A registration is stored under the place it was made at, spelled:
    /// an entry arrives spelled already, a resume arrives as the compiled
    /// id under its frames, and both land on the same spelling.
    #[test]
    fn a_registration_is_stored_under_the_place_it_was_made_at() {
        let p = program_with_included_triggers();
        let under = |site: &str| vec![Frame::Call { site: site.into() }];
        let (place, node) = registered_place(&p, "one.door", &Vec::new()).unwrap();
        assert_eq!((place.as_str(), node.id.as_str()), ("one.door", "Api.door"));
        let (place, node) = registered_place(&p, "top", &Vec::new()).unwrap();
        assert_eq!((place.as_str(), node.id.as_str()), ("top", "top"));
        let (place, node) = registered_place(&p, "Api.work", &under("one")).unwrap();
        assert_eq!((place.as_str(), node.id.as_str()), ("one.work", "Api.work"));
        // The same node under the other site is another place.
        assert_eq!(registered_place(&p, "Api.work", &under("two")).unwrap().0, "two.work");
        // An iteration is not a place: a resume inside a loop under a
        // site is spelled by the site alone.
        let looped = vec![Frame::Call { site: "two".into() }, Frame::Loop { index: 3 }];
        assert_eq!(registered_place(&p, "Api.work", &looped).unwrap().0, "two.work");
        let err = registered_place(&p, "one.nothing", &Vec::new()).unwrap_err().to_string();
        assert!(err.contains("'one.nothing' not in project"), "{err}");
    }

    #[test]
    fn a_trigger_inside_a_loop_or_registering_twice_is_refused() {
        let p = program_with_included_triggers();
        let in_loop = vec![Frame::Call { site: "one".into() }, Frame::Loop { index: 0 }];
        let err = captured_trigger_address(&p, "Api.door", &in_loop, 0).unwrap_err().to_string();
        assert!(err.contains("inside a loop"), "{err}");
        let err = captured_trigger_address(&p, "top", &vec![], 1).unwrap_err().to_string();
        assert!(err.contains("more than once"), "{err}");
        let err = captured_trigger_address(&p, "Api.work", &vec![], 0).unwrap_err().to_string();
        assert!(err.contains("must come from a trigger"), "{err}");
    }

    fn entry(path: &str) -> SignalSurface {
        SignalSurface::PublicEntry { path: path.into(), methods: Vec::new() }
    }

    #[test]
    fn public_entry_path_is_tenant_namespaced() {
        let s = entry("chat");
        assert_eq!(mount_path_for(&s, "alice").as_deref(), Some("/alice/chat"));
        // Same path, different tenant -> different mount path (no collision).
        assert_eq!(mount_path_for(&s, "bob").as_deref(), Some("/bob/chat"));
    }

    #[test]
    fn public_entry_empty_path_is_just_the_tenant() {
        assert_eq!(mount_path_for(&entry(""), "alice").as_deref(), Some("/alice"));
    }

    #[test]
    fn leading_slash_in_path_is_normalized() {
        let s = entry("/webhooks/stripe");
        assert_eq!(mount_path_for(&s, "acme").as_deref(), Some("/acme/webhooks/stripe"));
    }

    #[test]
    fn a_pattern_is_stored_as_written_and_read_back_without_the_tenant() {
        let s = SignalSurface::PublicEntry { path: "chat/{room}".into(), methods: vec!["POST".into()] };
        let stored = mount_path_for(&s, "alice").unwrap();
        assert_eq!(stored, "/alice/chat/{room}");
        assert_eq!(pattern_of_mount_path(&stored, "alice"), "chat/{room}");
        assert_eq!(pattern_of_mount_path("/alice", "alice"), "");
        assert_eq!(mount_methods_for(&s), vec!["POST".to_string()]);
        assert!(mount_methods_for(&SignalSurface::TaskCallback).is_empty());
    }

    fn registered(pattern: &str, methods: &[&str]) -> RegisteredRoute {
        RegisteredRoute {
            pattern: pattern.into(),
            methods: methods.iter().map(|m| m.to_string()).collect(),
            project_id: uuid::Uuid::from_u128(0x106),
            node_id: "other".into(),
        }
    }

    #[test]
    fn a_route_is_refused_only_when_nothing_can_arbitrate_the_shared_call() {
        let mine = RoutePattern::parse("chat/{room}").unwrap();
        let post = vec!["POST".to_string()];
        let others = vec![registered("chat/{x}", &["POST"])];
        assert!(ambiguous_route(&mine, &post, &others).is_some(), "two captures, same method");
        let others = vec![registered("chat/{x}", &["GET"])];
        assert!(ambiguous_route(&mine, &post, &others).is_none(), "two captures, disjoint methods");
        let others = vec![registered("chat/{x}", &[])];
        assert!(ambiguous_route(&mine, &post, &others).is_some(), "any-method claims every method");
        // The pair the refusal used to reject and now allows: the
        // literal serves `chat/general`, the capture serves the rest.
        let others = vec![registered("chat/general", &["POST"])];
        assert!(ambiguous_route(&mine, &post, &others).is_none(), "a literal is the more specific");
        let others = vec![registered("mail/{x}", &["POST"])];
        assert!(ambiguous_route(&mine, &post, &others).is_none(), "a different literal");
        let others = vec![registered("chat/{x}/y", &["POST"])];
        assert!(ambiguous_route(&mine, &post, &others).is_none(), "a different length");
        let others = vec![registered("chat/{", &["POST"])];
        assert!(ambiguous_route(&mine, &post, &others).is_none(), "an unparseable row is skipped");
        // Each wins a position, so `a/b/c` has two equal claims and
        // neither can be preferred. Counting captures would tie here,
        // which is why the comparison reads positions.
        let split = RoutePattern::parse("a/{x}/c").unwrap();
        let others = vec![registered("a/b/{y}", &["POST"])];
        assert!(ambiguous_route(&split, &post, &others).is_some(), "each wins a position");
        // The same address twice is the plainest ambiguity there is.
        let exact = RoutePattern::parse("chat/general").unwrap();
        let others = vec![registered("chat/general", &["POST"])];
        assert!(ambiguous_route(&exact, &post, &others).is_some(), "the same pattern twice");
    }

    use super::{read_prior, undo_registration, Restart, StartMode, Undone};
    use crate::journal::fake::tests::registration;
    use crate::journal::{FakeJournal, Journal, SignalWrite};

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    /// An entry registered at a place with no row mints its token and
    /// replaces nothing; at a place already armed it reuses the row's
    /// token and names that row as the one it replaces.
    #[tokio::test]
    async fn an_entry_reuses_the_armed_row_and_a_fresh_place_mints() {
        let j = FakeJournal::new();
        let (minted, prior) = read_prior(&j, None, PROJECT, "n", None).await.unwrap();
        assert!(prior.is_none());
        let mut row = registration(&minted);
        row.kind_state = serde_json::json!({ "cursor": 7 });
        j.signal_insert(&row).await.unwrap();
        let (token, prior) = read_prior(&j, None, PROJECT, "n", None).await.unwrap();
        assert_eq!(token, minted);
        assert_eq!(prior.unwrap().kind_state, serde_json::json!({ "cursor": 7 }));
        let (token, prior) = read_prior(&j, Some("tok-r"), PROJECT, "n", None).await.unwrap();
        assert_eq!((token.as_str(), prior.is_none()), ("tok-r", true));
    }

    /// A row the failed registration created goes, handed back for the
    /// listener to forget.
    #[tokio::test]
    async fn undoing_a_created_row_removes_it() {
        let j = FakeJournal::new();
        j.signal_insert(&registration("tok")).await.unwrap();
        let Undone::Removed(removed) = undo_registration(&j, "tok", None, Restart::No).await.unwrap() else {
            panic!("a created row is removed");
        };
        assert_eq!(removed.len(), 1);
        assert!(j.signal_get("tok").await.unwrap().is_none());
    }

    /// A row the failed registration replaced goes back exactly to what
    /// the listener still runs, at a new version.
    #[tokio::test]
    async fn undoing_a_replaced_row_puts_it_back() {
        let j = FakeJournal::new();
        let mut live = registration("tok");
        live.spec_json = r#"{"old":true}"#.into();
        live.mount_path = Some("/t/old".into());
        live.kind_state = serde_json::json!({ "cursor": 7 });
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("tok").await.unwrap().unwrap();
        let mut replacing = registration("tok");
        replacing.spec_json = r#"{"new":true}"#.into();
        replacing.mount_path = Some("/t/new".into());
        replacing.kind_state = serde_json::json!({ "cursor": 0 });
        replacing.kind_state_seq = prior.kind_state_seq;
        assert_eq!(j.signal_insert(&replacing).await.unwrap(), SignalWrite::Written);

        assert!(matches!(
            undo_registration(&j, "tok", Some(prior.clone()), Restart::No).await.unwrap(),
            Undone::Restored { rerun: false }
        ));
        let row = j.signal_get("tok").await.unwrap().unwrap();
        assert_eq!(row.spec_json, prior.spec_json);
        assert_eq!(row.mount_path, prior.mount_path);
        assert_eq!(row.kind_state, prior.kind_state);
        assert_eq!(row.kind_state_seq, prior.kind_state_seq + 2);
    }

    /// A replacement that never started, but whose row a wake claimed
    /// meanwhile: the wake read the new row, so its claim (and the next
    /// wake it set) follow the new spec. The prior state goes back, and
    /// the listener runs the prior row again so its wake follows it.
    #[tokio::test]
    async fn a_restore_drops_a_wake_claim_made_for_the_spec_that_never_started() {
        let j = FakeJournal::new();
        let mut live = registration("tok");
        live.kind_state = serde_json::json!({ "next_at": 100 });
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("tok").await.unwrap().unwrap();
        let mut replacing = registration("tok");
        replacing.spec_json = r#"{"new":true}"#.into();
        replacing.kind_state = serde_json::json!({ "next_at": 5 });
        replacing.kind_state_seq = prior.kind_state_seq;
        j.signal_insert(&replacing).await.unwrap();
        // A wake reads the new row and claims its next moment.
        let mut claimed = j.signal_get("tok").await.unwrap().unwrap();
        claimed.kind_state = serde_json::json!({ "next_at": 6 });
        j.signal_insert(&claimed).await.unwrap();

        assert!(matches!(
            undo_registration(&j, "tok", Some(prior.clone()), Restart::No).await.unwrap(),
            Undone::Restored { rerun: true }
        ));
        let row = j.signal_get("tok").await.unwrap().unwrap();
        assert_eq!(row.spec_json, prior.spec_json);
        assert_eq!(row.kind_state, prior.kind_state);
    }

    /// When the replacement ran, a claim that moved the row was its own,
    /// made for the new spec: the restore puts the prior row's state back
    /// with the rest of it.
    #[tokio::test]
    async fn a_restore_after_the_replacement_ran_drops_its_claims() {
        let j = FakeJournal::new();
        let mut live = registration("tok");
        live.kind_state = serde_json::json!({ "next_at": 100 });
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("tok").await.unwrap().unwrap();
        let mut replacing = registration("tok");
        replacing.spec_json = r#"{"new":true}"#.into();
        replacing.kind_state = serde_json::json!({ "next_at": 5 });
        replacing.kind_state_seq = prior.kind_state_seq;
        j.signal_insert(&replacing).await.unwrap();
        // The running replacement claims its next moment.
        let mut claimed = j.signal_get("tok").await.unwrap().unwrap();
        claimed.kind_state = serde_json::json!({ "next_at": 6 });
        j.signal_insert(&claimed).await.unwrap();

        undo_registration(&j, "tok", Some(prior.clone()), Restart::Yes).await.unwrap();
        let row = j.signal_get("tok").await.unwrap().unwrap();
        assert_eq!(row.spec_json, prior.spec_json);
        assert_eq!(row.kind_state, prior.kind_state);
    }

    use super::{disarm, ArmUndo, Armed, SignalHolder};

    /// A listener that records what it is told and refuses to start the
    /// tokens named in `refuse`.
    #[derive(Default)]
    struct Recorder {
        started: std::sync::Mutex<Vec<(String, StartMode)>>,
        forgotten: std::sync::Mutex<Vec<String>>,
        refuse: Vec<String>,
    }

    #[async_trait::async_trait]
    impl SignalHolder for Recorder {
        async fn start(&self, token: &str, mode: StartMode) -> anyhow::Result<()> {
            anyhow::ensure!(!self.refuse.iter().any(|t| t == token), "refused {token}");
            self.started.lock().unwrap().push((token.to_string(), mode));
            Ok(())
        }
        async fn unregister_many(&self, signals: &[crate::journal::SignalRegistration]) {
            self.forgotten.lock().unwrap().extend(signals.iter().map(|s| s.token.clone()));
        }
    }

    /// Three live triggers, re-armed with a new spec each, as the activation
    /// window does it: write the new row, start it. The first starts, the
    /// second fails to start (its own undo puts its row back), the third is
    /// never reached. Taking back what armed leaves all three rows as they
    /// were, and the listener is told to run the first one's prior row again.
    #[tokio::test]
    async fn a_rearm_that_fails_midway_leaves_every_trigger_as_it_was() {
        let j = FakeJournal::new();
        let mut priors = Vec::new();
        for (i, tok) in ["t1", "t2", "t3"].into_iter().enumerate() {
            let mut live = registration(tok);
            live.node_id = format!("n{i}");
            live.spec_json = format!(r#"{{"old":{i}}}"#);
            live.kind_state = serde_json::json!({ "cursor": i });
            j.signal_insert(&live).await.unwrap();
            priors.push(j.signal_get(tok).await.unwrap().unwrap());
        }
        let rearm = |prior: &crate::journal::SignalRegistration| {
            let mut next = prior.clone();
            next.spec_json = r#"{"new":true}"#.into();
            next.kind_state = serde_json::json!({ "cursor": 0 });
            next
        };
        let listener = Recorder { refuse: vec!["t2".into()], ..Recorder::default() };

        // Key 1 arms and starts.
        assert_eq!(j.signal_insert(&rearm(&priors[0])).await.unwrap(), SignalWrite::Written);
        listener.start("t1", StartMode::New).await.unwrap();
        let armed = vec![Armed { token: "t1".into(), undo: ArmUndo::Replaced(Box::new(priors[0].clone())) }];
        // Key 2 is written, fails to start, and undoes itself.
        assert_eq!(j.signal_insert(&rearm(&priors[1])).await.unwrap(), SignalWrite::Written);
        assert!(listener.start("t2", StartMode::New).await.is_err());
        super::undo_arm(&j, &listener, "t2", ArmUndo::Replaced(Box::new(priors[1].clone())), super::Restart::No).await.unwrap();

        disarm(&j, &listener, armed).await.unwrap();

        for prior in &priors {
            let row = j.signal_get(&prior.token).await.unwrap().unwrap();
            assert_eq!((&row.spec_json, &row.kind_state), (&prior.spec_json, &prior.kind_state), "{}", prior.token);
        }
        assert_eq!(
            *listener.started.lock().unwrap(),
            vec![("t1".to_string(), StartMode::New), ("t1".to_string(), StartMode::PutBack)],
            "t1 started with the new spec, then restored with its prior row",
        );
        assert!(listener.forgotten.lock().unwrap().is_empty());
    }

    /// Arms are taken back newest first; a created row is removed and
    /// forgotten; one that cannot be run again is named, and the rest
    /// are still put back.
    #[tokio::test]
    async fn disarm_takes_back_every_arm_and_names_the_ones_it_could_not() {
        let j = FakeJournal::new();
        let mut live = registration("old");
        live.node_id = "a".into();
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("old").await.unwrap().unwrap();
        let mut next = prior.clone();
        next.spec_json = r#"{"new":true}"#.into();
        j.signal_insert(&next).await.unwrap();
        let mut fresh = registration("fresh");
        fresh.node_id = "b".into();
        j.signal_insert(&fresh).await.unwrap();

        let listener = Recorder { refuse: vec!["old".into()], ..Recorder::default() };
        let armed = vec![
            Armed { token: "old".into(), undo: ArmUndo::Replaced(Box::new(prior.clone())) },
            Armed { token: "fresh".into(), undo: ArmUndo::Created },
        ];
        let error = disarm(&j, &listener, armed).await.unwrap_err();
        assert!(format!("{error:#}").contains("signal old"), "{error:#}");
        assert!(j.signal_get("fresh").await.unwrap().is_none());
        assert_eq!(*listener.forgotten.lock().unwrap(), vec!["fresh"]);
        assert_eq!(j.signal_get("old").await.unwrap().unwrap().spec_json, prior.spec_json);
    }

    #[test]
    fn non_public_surfaces_have_no_mount_path() {
        assert!(mount_path_for(&SignalSurface::TaskCallback, "alice").is_none());
        assert!(mount_path_for(&SignalSurface::Internal, "alice").is_none());
    }
}
