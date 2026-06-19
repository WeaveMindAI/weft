//! Start a run, wait for it to settle, fetch its replay.
//!
//! A "run" here is one execution identified by its `color` (a UUID). The rig
//! fires it through the real CLI (`weft run`, which builds + registers + fires,
//! exactly as a user does) and reads the color back from the CLI's `--json`
//! progress stream. From then on, the run is observed purely through the
//! dispatcher's public API: poll `/executions/{color}` until a terminal status,
//! then fetch `/executions/{color}/replay` for the full event log the
//! assertions read.
//!
//! For runs that DON'T start with a plain `weft run` (a trigger fire, a live
//! caller, a form submission), the test obtains the color from that path and
//! calls [`SettledRun::observe`] directly.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::client::{poll_until, Dispatcher};
use crate::event::{Replay, TERMINAL_KINDS};
use crate::project::Project;

/// How long the rig waits for an execution to reach a terminal status. This is
/// an INTERNAL transition the rig controls the inputs to (a small fixture run),
/// so a bound is correct: a run that never settles is a bug, not legitimate
/// long-running user work. Generous enough to cover a cold worker spawn.
const RUN_SETTLE_DEADLINE: Duration = Duration::from_secs(120);
const RUN_SETTLE_POLL: Duration = Duration::from_millis(300);

/// Fire a plain (non-triggered) run of `project` via `weft run` and return its
/// color. Builds + registers as a side effect (so the project is marked
/// registered for teardown). Does NOT wait for the run to finish; pair with
/// [`SettledRun::observe`].
pub async fn start(project: &mut Project) -> Result<Uuid> {
    // `--json` makes the CLI emit one progress event per line and detach (it
    // does not stream logs), so we get the color without holding the run open.
    let stdout = project.weft(&["run", "--json"]).await?;
    project.mark_registered();
    parse_color(&stdout).context("parse color from `weft run --json` output")
}

/// Convenience: start a plain run and wait for it to settle, returning the
/// observed run ready for assertions.
pub async fn run_and_settle(project: &mut Project) -> Result<SettledRun> {
    let color = start(project).await?;
    SettledRun::observe(project.dispatcher(), color).await
}

/// Extract the execution color from `weft run --json` NDJSON. The CLI emits a
/// `dispatcher_call_done` event whose `detail` carries `{ color, project_id }`
/// (see crates/weft-cli/src/commands/run.rs). We scan for the first event that
/// carries a `color`, which is unambiguous across the build/register noise.
fn parse_color(stdout: &str) -> Result<Uuid> {
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
        // The color rides in the event's `detail` object.
        if let Some(color) = ev
            .get("detail")
            .and_then(|d| d.get("color"))
            .and_then(Value::as_str)
        {
            return Uuid::parse_str(color)
                .with_context(|| format!("invalid color uuid '{color}'"));
        }
    }
    bail!("no color found in `weft run --json` output:\n{stdout}")
}

/// Snapshot the set of execution colors that currently exist for `project_id`.
/// Take this BEFORE firing an external trigger, then pass it to
/// [`wait_for_triggered_execution`] so the rig waits for a genuinely NEW
/// execution (the Fire), not a pre-existing one (e.g. the TriggerSetup run that
/// activation created). Returns an empty set if the project has no executions.
pub async fn execution_colors(disp: &Dispatcher, project_id: &Uuid) -> Result<HashSet<Uuid>> {
    let all: Vec<Value> = disp.get_json("/executions").await?;
    let pid = project_id.to_string();
    Ok(all
        .into_iter()
        .filter(|e| e.get("project_id").and_then(Value::as_str) == Some(pid.as_str()))
        .filter_map(|e| e.get("color").and_then(Value::as_str).and_then(|c| Uuid::parse_str(c).ok()))
        .collect())
}

/// Wait for a NEW execution to appear for `project_id` that is not in `known`
/// (the snapshot taken before firing the trigger), and return its color. Used
/// where the run is started by an external event (a reach-out feed, a timer)
/// rather than by `weft run`: a trigger's activation creates a TriggerSetup
/// execution, so "latest" alone is ambiguous; excluding the pre-existing colors
/// pins the result to the actual Fire execution.
pub async fn wait_for_triggered_execution(
    disp: &Dispatcher,
    project_id: &Uuid,
    known: &HashSet<Uuid>,
    deadline: Duration,
) -> Result<Uuid> {
    poll_until(
        &format!("a new triggered execution to appear for project {project_id}"),
        deadline,
        RUN_SETTLE_POLL,
        || {
            let disp = disp.clone();
            let known = known.clone();
            async move {
                let current = execution_colors(&disp, project_id).await?;
                // The snapshot/fire contract is "exactly one new execution".
                // Collect ALL colors not in the snapshot rather than pick an
                // arbitrary one (a HashSet has no order, so `find` would return
                // a random new color and the test would assert against the wrong
                // run). Zero new = not yet (retry). More than one new = the
                // contract is violated (a stray extra execution); bail loudly
                // with the set instead of silently pinning one.
                let new: Vec<Uuid> = current.difference(&known).copied().collect();
                match new.as_slice() {
                    [] => Ok(None),
                    [color] => Ok(Some(*color)),
                    many => bail!(
                        "expected exactly one new execution from the fire for project \
                         {project_id}, found {}: {many:?}",
                        many.len()
                    ),
                }
            }
        },
    )
    .await
}

/// A settled execution: its terminal status is known and its full replay is
/// fetched. All [`crate::assert`] helpers operate on this.
pub struct SettledRun {
    pub color: Uuid,
    /// The terminal status string from `/executions/{color}` (`completed` /
    /// `failed` / `cancelled`).
    pub status: String,
    /// The full event log from `/executions/{color}/replay`.
    pub replay: Replay,
}

impl SettledRun {
    /// Poll `/executions/{color}` until a terminal status, then fetch the
    /// replay. Errors loudly on timeout (the run never settled) so a hung
    /// execution surfaces as a clear failure, never a silently-passing test.
    pub async fn observe(disp: &Dispatcher, color: Uuid) -> Result<Self> {
        let status = wait_for_terminal(disp, color).await?;
        let replay = fetch_replay(disp, color).await?;
        Ok(Self {
            color,
            status,
            replay,
        })
    }

    /// Observe a run with an explicit deadline (e.g. a live-caller run that
    /// only settles after the test closes the connection).
    pub async fn observe_within(
        disp: &Dispatcher,
        color: Uuid,
        deadline: Duration,
    ) -> Result<Self> {
        let status = wait_for_terminal_within(disp, color, deadline).await?;
        let replay = fetch_replay(disp, color).await?;
        Ok(Self {
            color,
            status,
            replay,
        })
    }
}

/// Poll the execution status until it is terminal, returning the terminal
/// status string. Uses the default settle deadline.
async fn wait_for_terminal(disp: &Dispatcher, color: Uuid) -> Result<String> {
    wait_for_terminal_within(disp, color, RUN_SETTLE_DEADLINE).await
}

async fn wait_for_terminal_within(
    disp: &Dispatcher,
    color: Uuid,
    deadline: Duration,
) -> Result<String> {
    let path = format!("/executions/{color}");
    poll_until(
        &format!("execution {color} to reach a terminal status"),
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

/// Whether a `/executions/{color}` status string is terminal. SYNC with the
/// dispatcher's status derivation (ExecutionSummary.status), which is one of
/// running / completed / failed / cancelled.
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

/// Fetch + parse the replay event array for `color`.
async fn fetch_replay(disp: &Dispatcher, color: Uuid) -> Result<Replay> {
    let path = format!("/executions/{color}/replay");
    let arr: Vec<Value> = disp.get_json(&path).await?;
    let replay = Replay::from_array(arr);
    // Sanity: a settled run must carry exactly one terminal event. If the
    // status says terminal but the replay has none, the two read paths
    // disagree, which is a real bug we want loud, not a silent pass.
    if !replay.has_any_kind(&TERMINAL_KINDS) {
        bail!(
            "execution {color} reported a terminal status but its replay has no terminal event; \
             status/replay disagree"
        );
    }
    Ok(replay)
}
