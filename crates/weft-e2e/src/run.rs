//! Start a run, wait for it to settle, fetch its replay.
//!
//! A "run" here is one execution identified by its `execution_id` (a UUID). The rig
//! fires it through the real CLI (`weft run`, which builds + registers + fires,
//! exactly as a user does) and reads the execution back from the CLI's `--json`
//! progress stream. From then on, the run is observed purely through the
//! dispatcher's public API: poll `/executions/{execution_id}` until a terminal status,
//! then fetch `/executions/{execution_id}/replay` for the full event log the
//! assertions read.
//!
//! For runs that DON'T start with a plain `weft run` (a trigger fire, a live
//! caller, a form submission), the test obtains the execution from that path and
//! calls [`SettledRun::observe`] directly.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::client::{poll_until, poll_until_describing, Dispatcher};
use crate::event::{Replay, TERMINAL_KINDS};
use crate::project::Project;

/// How long the rig waits for an execution to reach a terminal status. This is
/// an INTERNAL transition the rig controls the inputs to (a small fixture run),
/// so a bound is correct: a run that never settles is a bug, not legitimate
/// long-running user work. Generous enough to cover a cold worker spawn.
pub const RUN_SETTLE_DEADLINE: Duration = Duration::from_secs(120);
const RUN_SETTLE_POLL: Duration = Duration::from_millis(300);

/// Fire a plain (non-triggered) run of `project` via `weft run` and return its
/// execution. Builds + registers as a side effect. Does NOT wait for the run to
/// finish; pair with [`SettledRun::observe`].
pub async fn start(project: &mut Project) -> Result<Uuid> {
    start_with(project, &[]).await
}

/// [`start`], as a LONG run (`weft run --long`): the run gets a worker of
/// its own that lives until the run ends.
pub async fn start_long(project: &mut Project) -> Result<Uuid> {
    start_with(project, &["--long"]).await
}

async fn start_with(project: &mut Project, flags: &[&str]) -> Result<Uuid> {
    // `--json` makes the CLI emit one progress event per line and detach (it
    // does not stream logs), so we get the execution without holding the run open.
    let mut args = vec!["run", "--json"];
    args.extend_from_slice(flags);
    let stdout = project.weft(&args).await?;
    parse_execution_id(&stdout).context("parse execution from `weft run --json` output")
}

/// Convenience: start a plain run and wait for it to settle, returning the
/// observed run ready for assertions.
pub async fn run_and_settle(project: &mut Project) -> Result<SettledRun> {
    let execution_id = start(project).await?;
    SettledRun::observe(project.dispatcher(), execution_id).await
}

/// Fire an AIMED run (`weft run --target <node>` per target) and wait
/// for it to settle. The dispatcher kicks only the roots the targets
/// need; pulses then run whatever those roots reach.
pub async fn run_targeted_and_settle(
    project: &mut Project,
    targets: &[&str],
) -> Result<SettledRun> {
    let mut args = vec!["run", "--json"];
    for t in targets {
        args.push("--target");
        args.push(t);
    }
    let stdout = project.weft(&args).await?;
    let execution_id =
        parse_execution_id(&stdout).context("parse execution from `weft run --target --json` output")?;
    SettledRun::observe(project.dispatcher(), execution_id).await
}

/// Extract the execution from `weft run --json` NDJSON. The CLI emits a
/// `dispatcher_call_done` event whose `detail` carries `{ execution_id, project_id }`
/// (see crates/weft-cli/src/commands/run.rs). We scan for the first event that
/// carries a `execution_id`, which is unambiguous across the build/register noise.
fn parse_execution_id(stdout: &str) -> Result<Uuid> {
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(ev): std::result::Result<Value, _> = serde_json::from_str(line) else {
            // Non-JSON line (shouldn't happen under --json, but tolerate it
            // rather than fail the whole parse).
            continue;
        };
        // The execution rides in the event's `detail` object.
        if let Some(execution_id) = ev
            .get("detail")
            .and_then(|d| d.get("execution_id"))
            .and_then(Value::as_str)
        {
            return Uuid::parse_str(execution_id)
                .with_context(|| format!("invalid execution uuid '{execution_id}'"));
        }
    }
    bail!("no execution found in `weft run --json` output:\n{stdout}")
}

/// Snapshot the set of executions that currently exist for `project_id`.
/// Take this BEFORE firing an external trigger, then pass it to
/// [`wait_for_triggered_execution`] so the rig waits for a genuinely NEW
/// execution (the Fire), not a pre-existing one (e.g. the TriggerSetup run that
/// activation created). Returns an empty set if the project has no executions.
pub async fn executions(disp: &Dispatcher, project_id: &Uuid) -> Result<HashSet<Uuid>> {
    // `/executions` is paginated (`{ executions, total }`) with a dispatcher-side
    // project filter; walk the pages so the snapshot is complete.
    let mut execution_ids = HashSet::new();
    let mut offset = 0u32;
    loop {
        let page: Value = disp
            .get_json(&format!(
                "/executions?project_id={project_id}&limit=200&offset={offset}"
            ))
            .await?;
        let batch = page
            .get("executions")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("/executions returned no `executions` array: {page}"))?;
        let n = batch.len() as u32;
        execution_ids.extend(batch.iter().filter_map(|e| {
            e.get("execution_id")
                .and_then(Value::as_str)
                .and_then(|c| Uuid::parse_str(c).ok())
        }));
        offset += n;
        let total = page.get("total").and_then(Value::as_u64).unwrap_or(0) as u32;
        if n == 0 || offset >= total {
            return Ok(execution_ids);
        }
    }
}

/// Wait for a NEW execution to appear for `project_id` that is not in `known`
/// (the snapshot taken before firing the trigger), and return its execution. Used
/// where the run is started by an external event (a reach-out feed, a timer)
/// rather than by `weft run`: a trigger's activation creates a TriggerSetup
/// execution, so "latest" alone is ambiguous; excluding the pre-existing executions
/// pins the result to the actual Fire execution.
pub async fn wait_for_triggered_execution(
    disp: &Dispatcher,
    project_id: &Uuid,
    known: &HashSet<Uuid>,
    deadline: Duration,
) -> Result<Uuid> {
    let mut execution_ids = wait_for_triggered_executions(disp, project_id, known, 1, deadline).await?;
    Ok(execution_ids.pop().expect("exactly one execution"))
}

/// Wait for exactly `n` NEW executions (not in `known`) to exist for
/// `project_id`, and return their executions in no particular order. This is
/// [`wait_for_triggered_execution`] for a burst: `n` events pushed back to
/// back, each starting its own run. Fewer than `n` = not yet (retry). More
/// than `n` = the snapshot/fire contract is violated (a stray extra
/// execution); bail loudly with the set instead of silently picking `n`.
pub async fn wait_for_triggered_executions(
    disp: &Dispatcher,
    project_id: &Uuid,
    known: &HashSet<Uuid>,
    n: usize,
    deadline: Duration,
) -> Result<Vec<Uuid>> {
    // The timeout says how many of `n` had appeared ("1 of 2 came"), so
    // the reader knows whether the push was lost or only half the burst.
    let seen = std::sync::atomic::AtomicUsize::new(0);
    poll_until_describing(
        &format!("{n} new triggered execution(s) to appear for project {project_id}"),
        deadline,
        RUN_SETTLE_POLL,
        || {
            let disp = disp.clone();
            let known = known.clone();
            let seen = &seen;
            async move {
                let current = executions(&disp, project_id).await?;
                // Collect ALL executions not in the snapshot rather than pick
                // (a HashSet has no order, so `find` would return a random
                // new execution and the test would assert against the wrong run).
                let new: Vec<Uuid> = current.difference(&known).copied().collect();
                seen.store(new.len(), std::sync::atomic::Ordering::Relaxed);
                match new.len().cmp(&n) {
                    std::cmp::Ordering::Less => Ok(None),
                    std::cmp::Ordering::Equal => Ok(Some(new)),
                    std::cmp::Ordering::Greater => bail!(
                        "expected exactly {n} new execution(s) from the fire for project \
                         {project_id}, found {}: {new:?}",
                        new.len()
                    ),
                }
            }
        },
        || format!("{} of {n} had appeared", seen.load(std::sync::atomic::Ordering::Relaxed)),
    )
    .await
}

/// The current status string of `execution_id` from `/executions/{execution_id}`
/// (`running`, `waiting_for_input`, `completed`, `failed`, `cancelled`).
pub async fn status_of(disp: &Dispatcher, execution_id: Uuid) -> Result<String> {
    let v: Value = disp.get_json(&format!("/executions/{execution_id}")).await?;
    v.get("status")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("/executions/{execution_id} returned no status: {v}"))
}

/// Poll until `execution_id` reports `status`. For the NON-terminal states a test
/// wants to observe a run sitting in (`running` on a held node,
/// `waiting_for_input` on a form) before acting on it; a terminal state is
/// what [`SettledRun::observe`] waits for. Bails as soon as the run reaches
/// a terminal state other than `status`, since it can never come back.
pub async fn wait_for_status(disp: &Dispatcher, execution_id: Uuid, status: &str) -> Result<()> {
    // The timeout names the status last observed (parked too early,
    // still processing, never started).
    let last = std::sync::Mutex::new(String::new());
    poll_until_describing(
        &format!("execution {execution_id} to reach status '{status}'"),
        RUN_SETTLE_DEADLINE,
        RUN_SETTLE_POLL,
        || {
            let disp = disp.clone();
            let last = &last;
            async move {
                let now = status_of(&disp, execution_id).await?;
                *last.lock().unwrap() = now.clone();
                if now == status {
                    return Ok(Some(()));
                }
                if matches!(now.as_str(), "completed" | "failed" | "cancelled") {
                    bail!("execution {execution_id} settled as '{now}' while waiting for '{status}'");
                }
                Ok(None)
            }
        },
        || format!("last status observed: '{}'", last.lock().unwrap()),
    )
    .await
}

/// A settled execution: its terminal status is known and its full replay is
/// fetched. All [`crate::assert`] helpers operate on this.
pub struct SettledRun {
    pub execution_id: Uuid,
    /// The terminal status string from `/executions/{execution_id}` (`completed` /
    /// `failed` / `cancelled`).
    pub status: String,
    /// The event log from `/executions/{execution_id}/replay`, snapshotted at
    /// terminal. The journal is APPEND-ONLY and a run's trailing bookkeeping
    /// (a metered call's cost record, a late log line) lands AFTER the
    /// terminal event by design; an assertion about those events refreshes
    /// this snapshot via [`Self::refresh_replay_until`] instead of assuming
    /// it is complete.
    pub replay: Replay,
    /// The dispatcher the run was observed through, kept so the replay can
    /// be re-read (see `replay`).
    disp: Dispatcher,
    /// The program the run is read through, when the test gave one: a
    /// node in an assertion is then spelled the way the source reads
    /// (`triage.up`), and resolves to the compiled id and the call it
    /// runs under. Without one, a name is the compiled id.
    pub(crate) definition: Option<weft_core::ProjectDefinition>,
}

impl SettledRun {
    /// Poll `/executions/{execution_id}` until a terminal status, then fetch the
    /// replay. Errors loudly on timeout (the run never settled) so a hung
    /// execution surfaces as a clear failure, never a silently-passing test.
    pub async fn observe(disp: &Dispatcher, execution_id: Uuid) -> Result<Self> {
        let status = wait_for_terminal(disp, execution_id).await?;
        let replay = fetch_replay(disp, execution_id).await?;
        Ok(Self {
            execution_id,
            status,
            replay,
            disp: disp.clone(),
            definition: None,
        })
    }

    /// Observe a run with an explicit deadline (e.g. a live-caller run that
    /// only settles after the test closes the connection).
    pub async fn observe_within(
        disp: &Dispatcher,
        execution_id: Uuid,
        deadline: Duration,
    ) -> Result<Self> {
        let status = wait_for_terminal_within(disp, execution_id, deadline).await?;
        let replay = fetch_replay(disp, execution_id).await?;
        Ok(Self {
            execution_id,
            status,
            replay,
            disp: disp.clone(),
            definition: None,
        })
    }

    /// Read this run through `definition`: see [`Self::definition`].
    pub fn reading(mut self, definition: weft_core::ProjectDefinition) -> Self {
        self.definition = Some(definition);
        self
    }

    /// Where a spelled node lives: its compiled id and the call path
    /// its rows carry. Through the program when one was given, else the
    /// name is the id.
    pub fn locate(&self, spelled: &str) -> (String, Vec<String>) {
        match &self.definition {
            Some(definition) => weft_core::project::resolve_address(definition, spelled),
            None => (spelled.to_string(), Vec::new()),
        }
    }

    /// Every event of the run about the spelled node, under its call.
    pub fn events_of<'a>(&'a self, spelled: &str) -> impl Iterator<Item = &'a crate::event::Event> + 'a {
        let (id, path) = self.locate(spelled);
        self.replay.events.iter().filter(move |e| e.is_node_at(&id, &path))
    }

    /// Re-fetch the replay until `present` holds for it, updating
    /// `self.replay`; errors loudly when `what` is still absent at
    /// `deadline`.
    ///
    /// This is how an assertion reads the journal's TRAILING events. The
    /// terminal event marks the user's work done, but bookkeeping that is
    /// deliberately detached from the run (a metered call's cost resolve,
    /// which may make its own provider follow-up query) journals after it.
    /// Waiting on the condition here asserts the real contract ("the record
    /// lands, bounded") instead of racing the at-terminal snapshot.
    pub(crate) async fn refresh_replay_until(
        &mut self,
        what: &str,
        deadline: Duration,
        present: impl Fn(&Replay) -> bool,
    ) -> Result<()> {
        if present(&self.replay) {
            return Ok(());
        }
        let disp = self.disp.clone();
        let execution_id = self.execution_id;
        let present = &present;
        self.replay = poll_until(what, deadline, RUN_SETTLE_POLL, || {
            let disp = disp.clone();
            async move {
                let replay = fetch_replay(&disp, execution_id).await?;
                Ok(present(&replay).then_some(replay))
            }
        })
        .await?;
        Ok(())
    }
}

/// Poll the execution status until it is terminal, returning the terminal
/// status string. Uses the default settle deadline.
async fn wait_for_terminal(disp: &Dispatcher, execution_id: Uuid) -> Result<String> {
    wait_for_terminal_within(disp, execution_id, RUN_SETTLE_DEADLINE).await
}

async fn wait_for_terminal_within(
    disp: &Dispatcher,
    execution_id: Uuid,
    deadline: Duration,
) -> Result<String> {
    let path = format!("/executions/{execution_id}");
    poll_until(
        &format!("execution {execution_id} to reach a terminal status"),
        deadline,
        RUN_SETTLE_POLL,
        || {
            let disp = disp.clone();
            let path = path.clone();
            async move {
                let v: Value = disp.get_json(&path).await?;
                let status = v
                    .get("status")
                    .and_then(Value::as_str)
                    .context("execution status response missing `status`")?;
                if is_terminal_status(status) {
                    Ok(Some(status.to_string()))
                } else {
                    Ok(None)
                }
            }
        },
    )
    .await
}

/// Whether a `/executions/{execution_id}` status string is terminal. SYNC with the
/// dispatcher's status derivation (ExecutionSummary.status), which is one of
/// running / completed / failed / cancelled.
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

/// Fetch + parse the replay event array for `execution_id`.
async fn fetch_replay(disp: &Dispatcher, execution_id: Uuid) -> Result<Replay> {
    let path = format!("/executions/{execution_id}/replay");
    let arr: Vec<Value> = disp.get_json(&path).await?;
    let replay = Replay::from_array(arr);
    // Sanity: a settled run must carry exactly one terminal event. If the
    // status says terminal but the replay has none, the two read paths
    // disagree, which is a real bug we want loud, not a silent pass.
    if !replay.has_any_kind(&TERMINAL_KINDS) {
        bail!(
            "execution {execution_id} reported a terminal status but its replay has no terminal event; \
             status/replay disagree"
        );
    }
    Ok(replay)
}
