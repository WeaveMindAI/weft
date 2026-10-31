//! Trigger setup captures a trigger's SignalSpec and input ports without
//! contacting a listener: this checks the capture and answers the place it
//! is for, and the worker writes it into its run's record. Activation arms
//! the completed capture. Suspensions arm immediately and return the token
//! the worker waits on, which the worker records as its wait. Both arming paths
//! have the listener compute the signal's row (`prepare`, which starts
//! nothing), write it, then have the listener bring the signal up
//! (`start`), so whatever the signal starts always finds its row.
//!
//! Idempotency: dedup keyed on `(run, node_id, frames, is_resume,
//! call_index)` so a process-crash retry converges on the same task. The
//! task's body is itself idempotent: entry rows reuse a stable token
//! per `(project_id, node_id)`, resume rows mint per-suspension.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_core::frames::LoopFrames;
use weft_core::signal as core_signal;
use weft_core::signal::listener_protocol::StartMode;
use weft_task_store::db::Client as _;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::RegisterSignalPayload;

use crate::state::DispatcherState;

pub struct RegisterSignalExecutor;

/// The stored mount path for a public-entry surface: its pattern under the
/// project on the install's shared address (`/<tenant>/<project id>/<pattern>`,
/// [`weft_core::route::SharedMount`]), so each project has its own path
/// space and two projects can both serve `chat`. Callers reach it at
/// `/connect/<tenant>/<project id>/<path>` (live) or `POST /<tenant>/<project
/// id>/<path>` (public fire). A guessable URL is fine here: live and public
/// endpoints are API surfaces whose callers bring their own auth; the
/// prefix is for collision, not secrecy. The path is a route PATTERN
/// (`chat/{room}`), stored as written; the dispatcher matches calls against
/// it in Rust.
fn mount_path_for(
    surface: &weft_core::primitive::SignalSurface,
    mount: weft_core::route::SharedMount<'_>,
) -> Option<String> {
    match surface {
        weft_core::primitive::SignalSurface::PublicEntry { path, .. } => Some(mount.mount_path(path)),
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

/// One other public entry of the tenant, as the overlap check sees it.
pub(crate) struct RegisteredRoute {
    pub pattern: String,
    pub methods: Vec<String>,
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

/// What a registration is filed under: its place (the row's key, see
/// [`registered_place`]), the tags its node carries (the signal-token
/// enumeration filter reads them), and, for an entry, the project's
/// defaults its trigger's unset inputs take (`weft.toml` `[runs]`,
/// `[triggers]`).
#[derive(Debug, PartialEq)]
pub(crate) struct Filed {
    pub place: String,
    pub tags: Vec<String>,
    pub entry_defaults: Option<weft_core::project::ProjectDefaults>,
}

/// Where the registration of `node_id` at `frames` is filed, read off the
/// program its run runs (`found`).
///
/// A run with no program (a node test's, which runs a package's image and
/// never a project definition) files its wait under the node's own id,
/// with no tags: there is no program to spell a place in or to read tags
/// off. It has no call sites either, so a call frame on it is refused, and
/// an entry is refused, since an entry's address and its defaults are its
/// program's. A run whose program is recorded but does not read is refused
/// with why.
pub(crate) fn filed_under(
    found: &crate::projection::ProgramLookup,
    node_id: &str,
    frames: &LoopFrames,
    is_resume: bool,
) -> Result<Filed> {
    use crate::projection::ProgramLookup;
    match found {
        ProgramLookup::Found(program) => {
            let (place, node) = registered_place(program, node_id, frames)?;
            Ok(Filed { place, tags: node.tags(), entry_defaults: (!is_resume).then_some(program.defaults) })
        }
        ProgramLookup::NoProgram => {
            anyhow::ensure!(is_resume, "an entry registers only from a run with a program, and this run has none");
            let sites = weft_core::frames::call_path(frames);
            anyhow::ensure!(
                sites.is_empty(),
                "node '{node_id}' waits under the call site(s) {sites:?}, but its run has no program, so it has no call sites"
            );
            Ok(Filed { place: node_id.to_string(), tags: Vec::new(), entry_defaults: None })
        }
        ProgramLookup::Unreadable(why) | ProgramLookup::ProgramGone(why) => {
            anyhow::bail!("the run's original program is unavailable: {why}")
        }
    }
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

/// Whether an entry's trigger listened the whole time its row was not
/// served, from how its activation stood just before the claim that arms
/// it again (`before`, `None` when it was never activated), at `now_unix`:
/// it was on, or down behind a door that still holds what arrives
/// (`weft_core::arrival`), or down for `weft maintenance on`. A door that
/// refuses, or whose window ended, listened to nothing. Maintenance's door
/// refuses too, but nobody refused anything: it is weft's own pause, so
/// what arrived meanwhile is caught up like any other downtime of weft's.
/// A door that refuses takes the places of its rows as it is set
/// (`crate::take_down::places_go`), and one whose window ends as the
/// reaper closes it (`crate::reaper::close_door_windows_over`); this is
/// what still holds between a window's end by the clock and that close,
/// which a claim made in between keeps from matching. Pure.
pub(crate) fn listened_while_down(before: Option<&crate::activation_store::ActivationLifecycle>, now_unix: i64) -> bool {
    let Some(before) = before else { return false };
    before.went_down == Some(weft_broker_client::activation::WentDown::Maintenance)
        || before.standing().arrival(now_unix) != weft_core::arrival::Arrival::Refused
}

#[async_trait]
impl TaskExecutor<DispatcherState> for RegisterSignalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RegisterSignalPayload = serde_json::from_value(task.payload.clone())?;
        if !payload.is_resume {
            core_signal::validate_spec(&payload.spec).map_err(anyhow::Error::msg)?;
            let run_id = payload.run_id;
            let owner = state.runs.run_owner(run_id).await?.context("entry capture has no run")?;
            anyhow::ensure!(
                owner.phase == weft_core::context::Phase::TriggerSetup && owner.binary_hash.is_some(),
                "entry capture requires a trigger-setup run with a pinned program"
            );
            let found = crate::projection::program_of_owner(state, run_id, &owner).await?;
            let project = found.program().context("entry capture has no original program")?;
            let node_id = captured_trigger_address(&project, &payload.node_id, &payload.frames, payload.call_index)?;
            let ports = payload.port_snapshot.as_ref().context("entry capture requires its input port snapshot")?;
            anyhow::ensure!(ports.is_object(), "entry capture ports must be an object");
            // The worker writes the capture into its run's record: nobody
            // else writes a run its worker drives.
            return Ok(serde_json::to_value(weft_core::primitive::RegisterSignalResult::Captured { node_id })?);
        }
        let token = Self::arm(state, payload, false).await?.token;
        Ok(serde_json::to_value(weft_core::primitive::RegisterSignalResult::Registered { token })?)
    }
}

impl RegisterSignalExecutor {
    /// Arm a captured entry or a live suspension through the same path.
    /// The answer says what the arm did to the row, so a caller whose
    /// later step fails can take it back ([`disarm`]).
    ///
    /// `listened` says whether the row an entry replaces was listened for
    /// the whole time: read off how its activation stood as the claim that
    /// arms it found it ([`listened_while_down`], kept on the activation's
    /// row: `crate::activation_store::Activating::listened`); `false` for a
    /// resume.
    pub(crate) async fn arm(state: &DispatcherState, mut payload: RegisterSignalPayload, listened: bool) -> Result<Armed> {
        let run_id = payload.run_id;

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
        let Some(owner) = state.runs.run_owner(run_id).await? else {
            anyhow::bail!("no run row for run {run_id}")
        };
        let project_id = owner.project_id;
        // The tenant stamped on the run, not one re-derived from
        // the project store: same answer while the project lives, and
        // still an answer once it does not.
        let tenant = owner.tenant.clone();
        // Whose signal: the run's instance (an instance's trigger setup arms
        // that instance's copy; an instance's run waits as that instance).
        let instance = owner.instance.clone();
        let fired_by = owner.fired_by.clone();

        // The place this registration is for, spelled: the row's key.
        // Read off the original program, the one this registration's
        // node was compiled into, so the spelling is the one that
        // program's source reads. Tags drive the signal-token enumeration
        // filter; charset already validated at parse time.
        let found = crate::projection::program_of_owner(state, run_id, &owner).await?;
        let Filed { place, tags, entry_defaults } = filed_under(&found, &payload.node_id, &payload.frames, payload.is_resume)
            .with_context(|| format!("register_signal: project_id={project_id}"))?;
        // An entry takes its project's defaults where its trigger's own
        // inputs left them unset, so the worker's door and every run it
        // starts read them off the one spec.
        if let Some(defaults) = entry_defaults {
            payload.spec.limits = payload.spec.limits.under(&defaults.triggers);
            payload.spec.settings = payload.spec.settings.keeping_for(Some(payload.spec.settings.kept_for(defaults.keep_for())));
        }

        // Resume tokens derive from the suspension identity so a retry of
        // this task converges on the same token. The identity (run,
        // node_id, frames, call_index) is what the engine's fold uses to
        // match resumes. Entry tokens are reused across reactivates and
        // read off the existing row below.
        let resume_token = payload.is_resume.then(|| {
            let mut hasher = Sha256::new();
            hasher.update(run_id.to_string().as_bytes());
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

        let resume_run_id = payload.is_resume.then_some(run_id);
        let spec_json = serde_json::to_string(&payload.spec)?;

        let program = match (owner.registered_hash(), &owner.binary_hash) {
            (Some(definition_hash), Some(binary_hash)) => Some(
                state
                    .projects
                    .program_identity(project_id, definition_hash, binary_hash)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("run {run_id} ran on worker {binary_hash}, which its project no longer records"))?,
            ),
            _ => None,
        };
        let source_version = owner.source_version.clone();

        // From the overlap check to the signal row's insert, this tenant's
        // registrations run one at a time install-wide: the check reads the
        // rows the insert writes, so two routes registering at once would
        // each pass the check before the other's row existed and both arm
        // (`chat/{a}` next to `chat/{b}`). A lease, not a lock: it is held
        // across the listener's start, which may wait on a provider's
        // subscribe, and a lock held that long would hold one of the few
        // lock connections the whole time.
        let mount = format!("{}/{tenant}", crate::lease::SIGNAL_MOUNT_LEASE);
        let waiting_for = format!("tenant {tenant}'s route registrations");
        let brought_up: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<Armed>>>> = std::sync::Mutex::new(None);
        let brought_up_slot = &brought_up;
        let leased = weft_task_store::locks::with_lease_waiting(&state.pg_pool, &state.signals, &mount, &waiting_for, || async move {
            // The row's kind state is computed from the state it replaces,
            // and written only while the row is still at the version that
            // state was read at (`signal_insert` is a compare-and-set). A
            // wake claiming a moment in between moves the row, so this
            // reads again and asks the kind again; each round lost is a
            // claim some wake made, so running out of rounds means the
            // signal's wakes never stop claiming, which is a bug to see.
            const ROUNDS: usize = 5;
            let mut round = 0;
            let (token, prior, holds, generation) = loop {
                round += 1;
                let (token, prior) =
                    read_prior(state.registrations.as_ref(), resume_token.as_deref(), project_id, &place, instance.as_ref()).await?;
                let prior_seq = prior.as_ref().map_or(0, |row| row.kind_state_seq);
                // What the new row carries over of this one is the
                // listener's to decide, the same way for every kind.
                let prior_row = match &prior {
                    Some(row) => Some(weft_core::signal::listener_protocol::PriorRow {
                        spec: serde_json::from_str(&row.spec_json)
                            .with_context(|| format!("signal {token}'s row holds a spec that does not read"))?,
                        kind_state: row.kind_state.clone(),
                        listened,
                    }),
                    None => None,
                };
                let prepared = state
                    .listener
                    .prepare(&weft_core::signal::listener_protocol::PrepareRequest {
                        token: token.clone(),
                        tenant_id: tenant.as_str().to_string(),
                        spec: payload.spec.clone(),
                        node_id: place.clone(),
                        is_resume: payload.is_resume,
                        run_id: resume_run_id,
                        source: weft_core::signal::listener_protocol::PrepareSource {
                            prior: prior_row,
                            asked_at_unix_ms: payload.asked_at_unix_ms,
                        },
                        for_instance: instance
                            .clone()
                            .map(|instance| weft_core::instance::InstanceScope { project_id, instance }),
                    })
                    .await?;
                refuse_unarmable_route(state, &prepared.routing.surface, instance.as_ref(), tenant.as_str(), project_id, &place).await?;

                let holds = prepared.holds;
                let written = state
                    .registrations
                    .signal_insert(&crate::store::SignalRegistration {
                        instance: instance.clone(),
                        // An entry signal is its own trigger's; a wait is the
                        // activation's of the trigger that fired its run (none
                        // for a run started by hand).
                        activation_trigger: if payload.is_resume { fired_by.clone() } else { Some(place.clone()) },
                        setup_run_id: (!payload.is_resume).then_some(run_id),
                        source_version: source_version.clone(),
                        program: program.clone(),
                        token: token.clone(),
                        tenant_id: tenant.to_string(),
                        project_id,
                        run_id: if payload.is_resume { Some(run_id) } else { None },
                        node_id: place.clone(),
                        is_resume: payload.is_resume,
                        spec_json: spec_json.clone(),
                        access_id: payload.spec.access.as_ref().map(|a| a.id.clone()),
                        consumer_kind: payload.spec.consumer_kind.clone(),
                        tags: tags.clone(),
                        port_snapshot: payload.port_snapshot.clone(),
                        consumer_payload: (!prepared.rendered.is_null()).then_some(prepared.rendered),
                        surface_kind: prepared.routing.surface.kind_tag().to_string(),
                        mount_path: mount_path_for(&prepared.routing.surface, weft_core::route::SharedMount::new(tenant.as_str(), project_id)),
                        mount_methods: mount_methods_for(&prepared.routing.surface),
                        auth_kind: prepared.routing.auth.kind_tag().to_string(),
                        auth_config: (!prepared.routing.auth_config.is_null()).then_some(prepared.routing.auth_config),
                        kind_state: prepared.kind_state,
                        kind_state_seq: prior_seq,
                        holds,
                        generation: 0,
                    })
                    .await?;
                match written {
                    crate::store::SignalWrite::Written { generation } => break (token, prior, holds, generation),
                    crate::store::SignalWrite::StateMoved if round < ROUNDS => continue,
                    crate::store::SignalWrite::StateMoved => anyhow::bail!(
                        "signal {token}'s state moved under each of {ROUNDS} registrations in a row: \
                         its wakes keep claiming it while it is being registered"
                    ),
                }
            };

            // The row is written, so from here on the arm is not stopped
            // half-way, even when the lease is lost (a database too slow
            // to renew it): a replaced row already stopped and the new one
            // not started would be a signal nobody runs, which a later arm
            // does not repair (its prior is then the new row, so it never
            // stops the old serving). So the rest runs on a task of its
            // own, which this waits for while the lease is held. The lease
            // kept the overlap check and the write together; what is left
            // touches only this token's row, and only while it is still
            // the registration this wrote (`generation`): the task that
            // called this can be dropped (its claim lost) while the rest
            // runs on, and its retry may arm the row again meanwhile.
            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            let rest = bring_up(state.registrations.clone(), state.listener.clone(), token, generation, prior, holds);
            *brought_up_slot.lock().expect("the slot is never poisoned") = Some(tokio::spawn(async move {
                let armed = rest.await;
                let _ = done_tx.send(());
                armed
            }));
            // An error only when the task panicked, which its handle says.
            let _ = done_rx.await;
            Ok(())
        })
        .await;
        // The arm's answer is the bringing-up's whenever the row was
        // written; the lease's error only when it never was.
        let handle = brought_up.into_inner().expect("the slot is never poisoned");
        match (leased, handle) {
            (leased, Some(handle)) => {
                if let Err(e) = leased {
                    tracing::warn!(
                        target: "weft_dispatcher::register_signal",
                        error = %format!("{e:#}"),
                        "the registration lease was lost after the signal's row was written; bringing it up all the same"
                    );
                }
                match handle.await {
                    Ok(armed) => armed,
                    Err(join) if join.is_panic() => std::panic::resume_unwind(join.into_panic()),
                    Err(join) => Err(anyhow::anyhow!("bringing up the signal's row was cancelled: {join}")),
                }
            }
            (Err(e), None) => Err(e),
            (Ok(()), None) => unreachable!("the lease's work ends only after it handed over the bringing-up"),
        }
    }
}

/// Bring up the signal [`RegisterSignalExecutor::arm`] just wrote (`token`
/// as the registration `generation`, replacing `prior` when there was a
/// row, served held or not as `holds` says), undoing the row when it
/// cannot come up and nothing has registered it again since.
async fn bring_up(
    registrations: std::sync::Arc<dyn crate::store::SignalStore>,
    listener: crate::listener::ListenerClient,
    token: String,
    generation: i64,
    prior: Option<crate::store::SignalRegistration>,
    holds: bool,
) -> Result<Armed> {
    // The row is committed, so whatever the signal starts (its
    // first wake, its held connection) finds it. A signal that
    // cannot come up is undone: a row this call created goes, and
    // the listener forgets whatever part of it did come up; a row
    // it replaced goes back to what the listener still runs.
    let undo = match prior {
        None => ArmUndo::Created,
        Some(prior) => ArmUndo::Replaced(Box::new(prior)),
    };
    // A replaced row served the other way round (held where the
    // new one is not, or the reverse) is unregistered first, from
    // its own spec and state: bringing the new row up never reaches
    // the old serving, so nothing else would tear down what it
    // arranged outside (a wake's provider subscription). A held
    // connection in this process stops here; one a holder elsewhere
    // runs lost its claim when the row was written, so the broker
    // refuses its fires from then on and it stops at its next look.
    let served_otherwise = matches!(&undo, ArmUndo::Replaced(prior) if prior.holds != holds);
    let stopped = match &undo {
        ArmUndo::Replaced(prior) if served_otherwise => listener.unregister_replaced(prior).await,
        _ => Ok(false),
    };
    // Whether the replaced row may have been stopped, so an undo
    // brings it back up (harmless when it was not: a put-back
    // re-arms a wake that subscribes only when nothing is live, or
    // retakes a held row). Only a row the listener could not read,
    // or whose kind it does not know, was surely left untouched.
    let untouched = match &stopped {
        Ok(stopped) => !stopped,
        Err(e) => e
            .downcast_ref::<crate::listener::ListenerRefused>()
            .is_some_and(|r| r.status == reqwest::StatusCode::SERVICE_UNAVAILABLE),
    };
    let replaced_stopped = served_otherwise && !untouched;
    let replaced = match &undo {
        ArmUndo::Replaced(prior) => Some(prior.as_ref()),
        ArmUndo::Created => None,
    };
    let started = match stopped {
        Ok(_) => listener.start(&token, StartMode::New, replaced).await,
        Err(e) => Err(e.context("stopping the row it replaces, which was served the other way")),
    };
    if let Err(e) = started {
        // A start that failed left the listener on what it ran
        // before, so a replaced row goes back without a restart
        // (unless a wake ran the new row meanwhile, see
        // `undo_registration`). A replaced row it was told to stop
        // is brought back up.
        let restart = if replaced_stopped { Restart::Yes } else { Restart::No };
        undo_arm(registrations.as_ref(), &listener, &token, generation, undo, restart).await.map_err(|undo_err| {
            anyhow::anyhow!(
                "signal {token} could not start ({e:#}), and undoing its registration failed too, \
                 so its row may not match what the listener runs until the trigger is \
                 deactivated or the run ends: {undo_err:#}"
            )
        })?;
        return Err(e);
    }

    Ok(Armed { token, generation, undo })
}

/// A signal [`RegisterSignalExecutor::arm`] armed, and how to take it back.
#[derive(Debug, Clone)]
pub(crate) struct Armed {
    pub token: String,
    /// The registration the arm wrote: taking it back touches the row
    /// only while it is still this one.
    pub generation: i64,
    pub undo: ArmUndo,
}

/// What an arm did to its signal's row.
#[derive(Debug, Clone)]
pub(crate) enum ArmUndo {
    /// No row was there: taking the arm back removes it.
    Created,
    /// This row was there (an entry's reused token, a resume's retry):
    /// taking the arm back writes it again.
    Replaced(Box<crate::store::SignalRegistration>),
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
    async fn start(&self, token: &str, mode: StartMode, replaced: Option<&crate::store::SignalRegistration>) -> Result<()>;
    async fn unregister_many(&self, signals: &[crate::store::SignalRegistration]);
}

#[async_trait]
impl SignalHolder for crate::listener::ListenerClient {
    async fn start(&self, token: &str, mode: StartMode, replaced: Option<&crate::store::SignalRegistration>) -> Result<()> {
        crate::listener::ListenerClient::start(self, token, mode, replaced).await
    }
    async fn unregister_many(&self, signals: &[crate::store::SignalRegistration]) {
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
    registrations: &dyn crate::store::SignalStore,
    listener: &dyn SignalHolder,
    armed: Vec<Armed>,
) -> Result<()> {
    let mut failed = Vec::new();
    for Armed { token, generation, undo } in armed.into_iter().rev() {
        if let Err(e) = undo_arm(registrations, listener, &token, generation, undo, Restart::Yes).await {
            failed.push(format!("signal {token}: {e:#}"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "could not put back {} armed signal(s): {}", failed.len(), failed.join("; "));
    Ok(())
}

async fn undo_arm(
    registrations: &dyn crate::store::SignalStore,
    listener: &dyn SignalHolder,
    token: &str,
    generation: i64,
    undo: ArmUndo,
    restart: Restart,
) -> Result<()> {
    let prior = match undo {
        ArmUndo::Created => None,
        ArmUndo::Replaced(prior) => Some(*prior),
    };
    match undo_registration(registrations, token, generation, prior, restart).await? {
        Undone::Removed(removed) => listener.unregister_many(&removed).await,
        // A later arm owns the row and what the listener runs of it; both
        // stay its.
        Undone::Superseded => tracing::debug!(
            target: "weft_dispatcher::register_signal",
            %token, generation,
            "not taking back registration {generation} of signal {token}: a later arm registered it again and owns it"
        ),
        // A put-back: whatever the listener runs under the token came from
        // the replacement, so it is replaced (and its wakes cancelled); the
        // row ran before this arm, so a failure bringing it back is the
        // listener's to retry, never a refusal that turns a live trigger
        // off.
        Undone::Restored { rerun: Some(replacement) } => listener
            .start(token, StartMode::PutBack, Some(&replacement))
            .await
            .context("its row is back, but the listener did not confirm it runs it (a listener that answered stopped the replacement and keeps retrying the row, shown as down)")?,
        Undone::Restored { rerun: None } => {}
    }
    Ok(())
}

/// The token a registration writes and the row it replaces, when there
/// is one. A resume's token is its own (`resume_token`), and its row,
/// when a retry finds one, is the one it replaces. An entry reuses the
/// token of the row already armed at its place; with none it mints one.
async fn read_prior(
    registrations: &dyn crate::store::SignalStore,
    resume_token: Option<&str>,
    project_id: uuid::Uuid,
    place: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(String, Option<crate::store::SignalRegistration>)> {
    if let Some(token) = resume_token {
        return Ok((token.to_string(), registrations.signal_get(token).await?));
    }
    Ok(match registrations.signal_entry_at(project_id, place, instance).await? {
        Some(row) => (row.token.clone(), Some(row)),
        None => (uuid::Uuid::new_v4().to_string(), None),
    })
}

/// What undoing a registration that could not start did.
#[derive(Debug)]
enum Undone {
    /// The row was this registration's own and is gone; the listener
    /// is to forget these.
    Removed(Vec<crate::store::SignalRegistration>),
    /// The row existed before (an entry's reused token, a resume's
    /// retry) and is back to it. `rerun` when the listener has to bring
    /// the restored row up again to match it (see [`undo_registration`]):
    /// the replacement it runs instead, as it last read.
    Restored { rerun: Option<Box<crate::store::SignalRegistration>> },
    /// The row is no longer the registration being undone: a later arm
    /// (a retry of the task, a new activation) wrote it again, so it and
    /// what the listener runs of it are left as they are.
    Superseded,
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
///
/// All of it only while the row is still the registration `generation`
/// this undoes: a wake's claim moves the row's version but never its
/// registration, while every write that registers it again (another arm,
/// a restore, a registration afresh for its connection) moves both. A row
/// registered again since belongs to that later write (`Superseded`). The
/// removal checks the registration in its own statement; the restore
/// checks it on the row it read, and its compare-and-set on the version
/// refuses a write that registered the row again after that read.
async fn undo_registration(
    registrations: &dyn crate::store::SignalStore,
    token: &str,
    generation: i64,
    prior: Option<crate::store::SignalRegistration>,
    restart: Restart,
) -> Result<Undone> {
    let Some(prior) = prior else {
        return Ok(match registrations.signal_remove_registration(token, generation).await? {
            Some(removed) => Undone::Removed(vec![removed]),
            None => Undone::Superseded,
        });
    };
    // The row this registration wrote sits one past the version it read.
    let written_seq = prior.kind_state_seq + 1;
    const ROUNDS: usize = 5;
    for _ in 0..ROUNDS {
        let current = registrations
            .signal_get(token)
            .await?
            .with_context(|| format!("signal {token}'s row vanished before it could be put back"))?;
        if current.generation != generation {
            return Ok(Undone::Superseded);
        }
        let rerun = restart == Restart::Yes || current.kind_state_seq != written_seq;
        let restored = crate::store::SignalRegistration { kind_state_seq: current.kind_state_seq, ..prior.clone() };
        match registrations.signal_restore(&restored).await? {
            crate::store::SignalWrite::Written { .. } => return Ok(Undone::Restored { rerun: rerun.then(|| Box::new(current)) }),
            crate::store::SignalWrite::StateMoved => continue,
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

    // Route overlap check: another trigger of THIS project already serves
    // a call this route would claim (`chat/{room}` against `chat/{x}`, or
    // `chat/general`, on a shared method). Refuse with a clear error rather
    // than let the door pick one at call time. The same node reclaiming its
    // route on reactivate is fine because it reuses the existing token.
    // Each project has its own path space (`SharedMount`), so another
    // project's routes are never in play.
    let mount = weft_core::route::SharedMount::new(tenant, project_id);
    let mine = weft_core::route::RoutePattern::parse(path).map_err(anyhow::Error::msg)?;
    let others: Vec<RegisteredRoute> = state
        .pg_pool
        .all::<(String, Vec<String>, String)>(
            "SELECT mount_path, mount_methods, node_id \
             FROM signal \
             WHERE project_id = $1 AND mount_path IS NOT NULL AND node_id <> $2",
            &[&project_id, &place],
        )
        .await?
        .into_iter()
        .map(|(mp, ms, node_id)| RegisteredRoute { pattern: mount.pattern_of(&mp), methods: ms, node_id })
        .collect();
    if let Some(taken) = ambiguous_route(&mine, methods, &others) {
        let method_words = |m: &[String]| if m.is_empty() { "any method".to_string() } else { m.join("/") };
        anyhow::bail!(
            "route `{}` ({}) and `{}` ({}) of trigger '{}' can both be reached by one call and \
             neither is the more specific, so that call has no answer. Make one of them spell out \
             what the other captures, or change this route's `path` or `method`",
            mine.as_str(),
            method_words(methods),
            taken.pattern,
            method_words(&taken.methods),
            taken.node_id,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ambiguous_route, captured_trigger_address, filed_under, mount_methods_for, mount_path_for, registered_place,
        Filed, RegisteredRoute,
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

    /// A wait of a run with a program is filed under its place with its
    /// node's tags, and an entry carries its project's defaults; a wait of
    /// a run with no program (a node test's) is filed under the node's own
    /// id, with nothing read off a program.
    #[test]
    fn a_wait_is_filed_under_its_programs_place_or_its_own_id_without_one() {
        use crate::projection::ProgramLookup;
        let mut p = program_with_included_triggers();
        p.nodes.iter_mut().find(|n| n.id == "Api.work").unwrap().config = serde_json::json!({ weft_core::tag::TAGS_CONFIG_KEY: ["billing"] });
        let found = ProgramLookup::Found(std::sync::Arc::new(p));
        let under_one = vec![Frame::Call { site: "one".into() }];
        let filed = filed_under(&found, "Api.work", &under_one, true).unwrap();
        assert_eq!(filed, Filed { place: "one.work".into(), tags: vec!["billing".into()], entry_defaults: None });
        assert!(filed_under(&found, "top", &vec![], false).unwrap().entry_defaults.is_some(), "an entry takes its project's defaults");

        let looped = vec![Frame::Loop { index: 2 }];
        let filed = filed_under(&ProgramLookup::NoProgram, "node", &looped, true).unwrap();
        assert_eq!(filed, Filed { place: "node".into(), tags: Vec::new(), entry_defaults: None });
    }

    /// With no program, an entry and a call site have nothing to be read
    /// off; a program that is recorded but does not read says why.
    #[test]
    fn what_a_run_without_a_readable_program_cannot_register_is_refused() {
        use crate::projection::ProgramLookup;
        let err = filed_under(&ProgramLookup::NoProgram, "node", &vec![], false).unwrap_err().to_string();
        assert!(err.contains("an entry registers only from a run with a program"), "{err}");
        let under_one = vec![Frame::Call { site: "one".into() }];
        let err = filed_under(&ProgramLookup::NoProgram, "node", &under_one, true).unwrap_err().to_string();
        assert!(err.contains("has no call sites"), "{err}");
        let err = filed_under(&ProgramLookup::ProgramGone("pruned".into()), "node", &vec![], true).unwrap_err().to_string();
        assert!(err.contains("unavailable: pruned"), "{err}");
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

    const ONE: uuid::Uuid = uuid::Uuid::from_u128(1);
    const TWO: uuid::Uuid = uuid::Uuid::from_u128(2);

    fn under(tenant: &str, project: uuid::Uuid) -> weft_core::route::SharedMount<'_> {
        weft_core::route::SharedMount::new(tenant, project)
    }

    #[test]
    fn a_public_entry_is_stored_under_its_tenant_and_project() {
        let s = entry("chat");
        assert_eq!(mount_path_for(&s, under("alice", ONE)), Some(format!("/alice/{ONE}/chat")));
        // The same path in another project or another tenant is another
        // mount path, so neither blocks the other.
        assert_eq!(mount_path_for(&s, under("alice", TWO)), Some(format!("/alice/{TWO}/chat")));
        assert_eq!(mount_path_for(&s, under("bob", ONE)), Some(format!("/bob/{ONE}/chat")));
        // The project's root, and a leading slash written in the path.
        assert_eq!(mount_path_for(&entry(""), under("alice", ONE)), Some(format!("/alice/{ONE}")));
        assert_eq!(mount_path_for(&entry("/webhooks/stripe"), under("acme", ONE)), Some(format!("/acme/{ONE}/webhooks/stripe")));
    }

    #[test]
    fn a_pattern_is_stored_as_written_and_read_back_without_the_prefix() {
        let s = SignalSurface::PublicEntry { path: "chat/{room}".into(), methods: vec!["POST".into()] };
        let stored = mount_path_for(&s, under("alice", ONE)).unwrap();
        assert_eq!(stored, format!("/alice/{ONE}/chat/{{room}}"));
        assert_eq!(under("alice", ONE).pattern_of(&stored), "chat/{room}");
        assert_eq!(under("alice", ONE).pattern_of(&format!("/alice/{ONE}")), "");
        assert_eq!(mount_methods_for(&s), vec!["POST".to_string()]);
        assert!(mount_methods_for(&SignalSurface::TaskCallback).is_empty());
    }

    fn registered(pattern: &str, methods: &[&str]) -> RegisteredRoute {
        RegisteredRoute {
            pattern: pattern.into(),
            methods: methods.iter().map(|m| m.to_string()).collect(),
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
    use crate::store::fake::tests::registration;
    use crate::store::{FakeStore, SignalStore, SignalWrite};

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    /// The registration a write that had to land made.
    async fn written(j: &FakeStore, row: &crate::store::SignalRegistration) -> i64 {
        let SignalWrite::Written { generation } = j.signal_insert(row).await.unwrap() else {
            panic!("signal {}'s write did not land", row.token)
        };
        generation
    }

    /// An entry registered at a place with no row mints its token and
    /// replaces nothing; at a place already armed it reuses the row's
    /// token and names that row as the one it replaces.
    #[tokio::test]
    async fn an_entry_reuses_the_armed_row_and_a_fresh_place_mints() {
        let j = FakeStore::new();
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
        let j = FakeStore::new();
        let generation = written(&j, &registration("tok")).await;
        // A wake's claim moves the row's version, never its registration.
        j.claim_kind_state("tok", serde_json::json!({ "next_at": 6 }));
        let Undone::Removed(removed) = undo_registration(&j, "tok", generation, None, Restart::No).await.unwrap() else {
            panic!("a created row is removed");
        };
        assert_eq!(removed.len(), 1);
        assert!(j.signal_get("tok").await.unwrap().is_none());
    }

    /// The task that armed a row was dropped (its claim lost) while its
    /// bringing-up ran on; its retry armed the row again; then the first
    /// one's start failed. Its undo finds the row registered again and
    /// leaves it, and what the listener runs of it, to the retry: a
    /// created row is not removed, a replaced one is not put back.
    #[tokio::test]
    async fn an_undo_leaves_a_row_a_later_arm_registered_again() {
        let j = FakeStore::new();
        let listener = Recorder::default();
        // Created: the first attempt wrote the row, the retry replaced it.
        let first = written(&j, &registration("tok")).await;
        let mut retry = j.signal_get("tok").await.unwrap().unwrap();
        retry.spec_json = r#"{"retry":true}"#.into();
        written(&j, &retry).await;
        super::undo_arm(&j, &listener, "tok", first, ArmUndo::Created, Restart::No).await.unwrap();
        let row = j.signal_get("tok").await.unwrap().expect("the retry's row stays");
        assert_eq!(row.spec_json, r#"{"retry":true}"#);

        // Replaced: the first attempt replaced `live`, the retry replaced
        // the first attempt's row.
        let mut live = registration("old");
        live.node_id = "m".into();
        live.spec_json = r#"{"old":true}"#.into();
        written(&j, &live).await;
        let prior = j.signal_get("old").await.unwrap().unwrap();
        let mut next = prior.clone();
        next.spec_json = r#"{"new":true}"#.into();
        let first = written(&j, &next).await;
        let mut retry = j.signal_get("old").await.unwrap().unwrap();
        retry.spec_json = r#"{"retry":true}"#.into();
        let retried = written(&j, &retry).await;
        super::undo_arm(&j, &listener, "old", first, ArmUndo::Replaced(Box::new(prior)), Restart::Yes).await.unwrap();
        let row = j.signal_get("old").await.unwrap().unwrap();
        assert_eq!((row.spec_json.as_str(), row.generation), (r#"{"retry":true}"#, retried), "the retry's row, untouched");

        assert!(listener.started.lock().unwrap().is_empty(), "the listener is not told to put anything back");
        assert!(listener.forgotten.lock().unwrap().is_empty(), "nor to forget anything");
    }

    /// A row the failed registration replaced goes back exactly to what
    /// the listener still runs, at a new version.
    #[tokio::test]
    async fn undoing_a_replaced_row_puts_it_back() {
        let j = FakeStore::new();
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
        let generation = written(&j, &replacing).await;

        assert!(matches!(
            undo_registration(&j, "tok", generation, Some(prior.clone()), Restart::No).await.unwrap(),
            Undone::Restored { rerun: None }
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
        let j = FakeStore::new();
        let mut live = registration("tok");
        live.kind_state = serde_json::json!({ "next_at": 100 });
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("tok").await.unwrap().unwrap();
        let mut replacing = registration("tok");
        replacing.spec_json = r#"{"new":true}"#.into();
        replacing.kind_state = serde_json::json!({ "next_at": 5 });
        replacing.kind_state_seq = prior.kind_state_seq;
        let generation = written(&j, &replacing).await;
        // A wake reads the new row and claims its next moment.
        j.claim_kind_state("tok", serde_json::json!({ "next_at": 6 }));

        let Undone::Restored { rerun: Some(replacement) } =
            undo_registration(&j, "tok", generation, Some(prior.clone()), Restart::No).await.unwrap()
        else {
            panic!("the listener runs the prior row again")
        };
        assert_eq!(replacement.kind_state, serde_json::json!({ "next_at": 6 }), "over the replacement as the wake left it");
        let row = j.signal_get("tok").await.unwrap().unwrap();
        assert_eq!(row.spec_json, prior.spec_json);
        assert_eq!(row.kind_state, prior.kind_state);
    }

    /// When the replacement ran, a claim that moved the row was its own,
    /// made for the new spec: the restore puts the prior row's state back
    /// with the rest of it.
    #[tokio::test]
    async fn a_restore_after_the_replacement_ran_drops_its_claims() {
        let j = FakeStore::new();
        let mut live = registration("tok");
        live.kind_state = serde_json::json!({ "next_at": 100 });
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("tok").await.unwrap().unwrap();
        let mut replacing = registration("tok");
        replacing.spec_json = r#"{"new":true}"#.into();
        replacing.kind_state = serde_json::json!({ "next_at": 5 });
        replacing.kind_state_seq = prior.kind_state_seq;
        let generation = written(&j, &replacing).await;
        // The running replacement claims its next moment.
        j.claim_kind_state("tok", serde_json::json!({ "next_at": 6 }));

        assert!(matches!(
            undo_registration(&j, "tok", generation, Some(prior.clone()), Restart::Yes).await.unwrap(),
            Undone::Restored { rerun: Some(_) }
        ));
        let row = j.signal_get("tok").await.unwrap().unwrap();
        assert_eq!(row.spec_json, prior.spec_json);
        assert_eq!(row.kind_state, prior.kind_state);
    }

    use super::{disarm, ArmUndo, Armed, SignalHolder};

    /// A listener that records what it is told (each start with the spec
    /// of the row it replaced) and refuses to start the tokens named in
    /// `refuse`.
    #[derive(Default)]
    struct Recorder {
        started: std::sync::Mutex<Vec<(String, StartMode, Option<String>)>>,
        forgotten: std::sync::Mutex<Vec<String>>,
        refuse: Vec<String>,
    }

    #[async_trait::async_trait]
    impl SignalHolder for Recorder {
        async fn start(&self, token: &str, mode: StartMode, replaced: Option<&crate::store::SignalRegistration>) -> anyhow::Result<()> {
            anyhow::ensure!(!self.refuse.iter().any(|t| t == token), "refused {token}");
            self.started.lock().unwrap().push((token.to_string(), mode, replaced.map(|r| r.spec_json.clone())));
            Ok(())
        }
        async fn unregister_many(&self, signals: &[crate::store::SignalRegistration]) {
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
        let j = FakeStore::new();
        let mut priors = Vec::new();
        for (i, tok) in ["t1", "t2", "t3"].into_iter().enumerate() {
            let mut live = registration(tok);
            live.node_id = format!("n{i}");
            live.spec_json = format!(r#"{{"old":{i}}}"#);
            live.kind_state = serde_json::json!({ "cursor": i });
            j.signal_insert(&live).await.unwrap();
            priors.push(j.signal_get(tok).await.unwrap().unwrap());
        }
        let rearm = |prior: &crate::store::SignalRegistration| {
            let mut next = prior.clone();
            next.spec_json = r#"{"new":true}"#.into();
            next.kind_state = serde_json::json!({ "cursor": 0 });
            next
        };
        let listener = Recorder { refuse: vec!["t2".into()], ..Recorder::default() };

        // Key 1 arms and starts.
        let generation = written(&j, &rearm(&priors[0])).await;
        listener.start("t1", StartMode::New, Some(&priors[0])).await.unwrap();
        let armed = vec![Armed { token: "t1".into(), generation, undo: ArmUndo::Replaced(Box::new(priors[0].clone())) }];
        // Key 2 is written, fails to start, and undoes itself.
        let generation = written(&j, &rearm(&priors[1])).await;
        assert!(listener.start("t2", StartMode::New, Some(&priors[1])).await.is_err());
        super::undo_arm(&j, &listener, "t2", generation, ArmUndo::Replaced(Box::new(priors[1].clone())), Restart::No).await.unwrap();

        disarm(&j, &listener, armed).await.unwrap();

        for prior in &priors {
            let row = j.signal_get(&prior.token).await.unwrap().unwrap();
            assert_eq!((&row.spec_json, &row.kind_state), (&prior.spec_json, &prior.kind_state), "{}", prior.token);
        }
        assert_eq!(
            *listener.started.lock().unwrap(),
            vec![
                ("t1".to_string(), StartMode::New, Some(r#"{"old":0}"#.to_string())),
                ("t1".to_string(), StartMode::PutBack, Some(r#"{"new":true}"#.to_string())),
            ],
            "t1 started with the new spec over its prior row, then restored with its prior row over the new one",
        );
        assert!(listener.forgotten.lock().unwrap().is_empty());
    }

    /// Arms are taken back newest first; a created row is removed and
    /// forgotten; one that cannot be run again is named, and the rest
    /// are still put back.
    #[tokio::test]
    async fn disarm_takes_back_every_arm_and_names_the_ones_it_could_not() {
        let j = FakeStore::new();
        let mut live = registration("old");
        live.node_id = "a".into();
        j.signal_insert(&live).await.unwrap();
        let prior = j.signal_get("old").await.unwrap().unwrap();
        let mut next = prior.clone();
        next.spec_json = r#"{"new":true}"#.into();
        let replaced = written(&j, &next).await;
        let mut fresh = registration("fresh");
        fresh.node_id = "b".into();
        let created = written(&j, &fresh).await;

        let listener = Recorder { refuse: vec!["old".into()], ..Recorder::default() };
        let armed = vec![
            Armed { token: "old".into(), generation: replaced, undo: ArmUndo::Replaced(Box::new(prior.clone())) },
            Armed { token: "fresh".into(), generation: created, undo: ArmUndo::Created },
        ];
        let error = disarm(&j, &listener, armed).await.unwrap_err();
        assert!(format!("{error:#}").contains("signal old"), "{error:#}");
        assert!(j.signal_get("fresh").await.unwrap().is_none());
        assert_eq!(*listener.forgotten.lock().unwrap(), vec!["fresh"]);
        assert_eq!(j.signal_get("old").await.unwrap().unwrap().spec_json, prior.spec_json);
    }

    /// A trigger listened while it was down when its door held what
    /// arrived (within its window), or when weft's maintenance took it
    /// down; never behind a door that refused, past its window, or when it
    /// was never activated.
    #[test]
    fn a_trigger_listened_while_down_only_behind_a_door_that_held_or_for_maintenance() {
        use super::listened_while_down;
        use crate::activation_store::ActivationLifecycle;
        use weft_broker_client::activation::WentDown;
        use weft_core::take_down::{Door, Held, Running, TakeDown};
        let down = |door: Door, door_for_minutes: Option<u32>| {
            ActivationLifecycle::down(&TakeDown { door, door_for_minutes, held: Held::Keep, running: Running::Wait }, 0)
        };
        assert!(listened_while_down(Some(&ActivationLifecycle::active()), 10), "on: a re-arm");
        assert!(listened_while_down(Some(&down(Door::Take, None)), 10), "a door that takes");
        assert!(listened_while_down(Some(&down(Door::Hide, None)), 10), "a door that hides");
        assert!(listened_while_down(Some(&down(Door::Take, Some(1))), 60), "within the door's window");
        assert!(!listened_while_down(Some(&down(Door::Take, Some(1))), 61), "past the door's window");
        assert!(!listened_while_down(Some(&down(Door::Refuse, None)), 10), "a door that refuses");
        let maintenance = ActivationLifecycle { went_down: Some(WentDown::Maintenance), ..down(Door::Refuse, None) };
        assert!(listened_while_down(Some(&maintenance), 10), "weft's own maintenance, whose door refuses");
        let health = ActivationLifecycle { went_down: Some(WentDown::Health), ..down(Door::Refuse, None) };
        assert!(!listened_while_down(Some(&health), 10), "any other refusing door listened to nothing");
        assert!(!listened_while_down(Some(&ActivationLifecycle::registered()), 10), "never switched on");
        assert!(!listened_while_down(None, 10), "never activated");
    }

    #[test]
    fn non_public_surfaces_have_no_mount_path() {
        assert!(mount_path_for(&SignalSurface::TaskCallback, under("alice", ONE)).is_none());
        assert!(mount_path_for(&SignalSurface::Internal, under("alice", ONE)).is_none());
    }
}
