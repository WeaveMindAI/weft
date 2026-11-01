//! Shared scaffolding for the e2e suites that drive a project through
//! verbs and read what the CLI printed: the `lifecycle` fixture's graph
//! shapes, the HoldGate release protocol and the drive/observe helpers
//! built on them (`lifecycle_transitions.rs`), and the readers of a
//! command's output (a run id, a summary, the warnings, the version tree)
//! that the version-tree, working-copy, instance and loop suites share.
//! One definition, used by every module that needs it.
//!
//! Without the `e2e` feature no test module is compiled, so nothing uses
//! these; the allow keeps that from spraying dead-code warnings.
#![allow(dead_code)]

use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;
use weft_e2e::client::{cli, CliOutput};
use weft_e2e::fakes::SseFake;
use weft_e2e::{run, Dispatcher, Project};

pub const INFRA_NODE: &str = "svc";
/// Bodies the HoldGate polls for: anything without "release" holds.
pub const HOLD: &str = "hold";
pub const RELEASE: &str = "release";

/// Graph: MiniService + the HoldGate chain, both roots. A run fires both; the
/// svc branch settles fast, the hold branch keeps the run running until
/// the rig releases it.
pub fn graph_infra_hold(release_url: &str) -> String {
    format!(
        "svc = MiniService\n\
         svc_out = Debug\n\
         svc_out.data = svc.status\n\
         kick = Text {{ value: \"go\" }}\n\
         hold = HoldGate {{ url: \"{release_url}\" }}\n\
         hold.arm = kick.value\n\
         out = Debug\n\
         out.data = hold.done\n"
    )
}

/// Graph: SSE trigger -> HoldGate -> Debug. A pushed event starts a Fire
/// run that holds until released.
pub fn graph_trigger_hold(sse_url: &str, event_name: &str, release_url: &str) -> String {
    format!(
        "start = TestSseTrigger -> (value: String) {{\n\
         \x20 url: \"{sse_url}\"\n\
         \x20 event_name: \"{event_name}\"\n\
         }}\n\
         hold = HoldGate {{ url: \"{release_url}\" }}\n\
         hold.arm = start.value\n\
         out = Debug\n\
         out.data = hold.done\n"
    )
}

/// Spawn a `weft` invocation in the background (for verbs that BLOCK on a
/// drain the test is about to observe from the outside). Returns the join
/// handle; the test joins it once the drain resolves.
pub fn spawn_weft(
    project: &Project,
    args: Vec<String>,
) -> tokio::task::JoinHandle<anyhow::Result<CliOutput>> {
    let disp = project.dispatcher().clone();
    let dir = project.dir().to_path_buf();
    tokio::spawn(async move {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        cli(&disp, &dir, &arg_refs).await
    })
}

/// Assert a whole-graph RUN is refused, and by the gate the test means.
///
/// Through the CLI, which builds and sends the real definition hash, so
/// the refusal reaches the gate under test: a bare POST with a made-up
/// hash would be turned away for the hash, three lines further down the
/// same handler, and a test that only checked the code would pass with
/// the gate deleted. `gate` is the fragment of the refusal that names
/// it: `LIFECYCLE_GATE` for the reconciliation table (a transitional
/// project), `INFRA_GATE` for the run's own infra pre-flight (declared
/// infra not running).
pub async fn assert_run_rejected(project: &Project, why: &str, gate: &str) -> anyhow::Result<()> {
    let out = cli(project.dispatcher(), project.dir(), &["run", "--json"]).await?;
    anyhow::ensure!(!out.success, "`weft run` must be refused ({why}), but it ran:\n{}", out.stdout);
    let text = format!("{}\n{}", out.stdout, out.stderr);
    anyhow::ensure!(
        text.contains(gate),
        "`weft run` must be refused by the gate ({why}: `{gate}`), but it was refused for \
         another reason:\n{text}"
    );
    Ok(())
}

/// The reconciliation table's refusal (`require_action`).
pub const LIFECYCLE_GATE: &str = "is not available right now";
/// The run's own infra pre-flight (`versions::run`), which names the
/// places that are down.
pub const INFRA_GATE: &str = "infra not running for";

/// The current status string of one run (`running` /
/// `waiting_for_input` / `completed` / `cancelled` / `failed`), read live
/// (no settle wait).
pub async fn run_status(disp: &Dispatcher, run_id: Uuid) -> anyhow::Result<String> {
    let v: Value = disp.get_json(&format!("/runs/{run_id}")).await?;
    Ok(v.get("status").and_then(Value::as_str).unwrap_or("").to_string())
}

/// Fire the SSE trigger and wait for the ONE new run it starts.
///
/// The push happens after `wait_for_subscriber` confirms a connection is reading,
/// so a single push is delivered (not lost on a stale reader). We then wait the
/// FULL worker-settle deadline for the run to appear, because a trigger fire
/// cold-starts a worker (spawn + fold + run) and is legitimately
/// slow. We do NOT re-push on a short timeout: pushing again while the first push's
/// run is still cold-starting would start a SECOND run and make the
/// "exactly one new run" contract fail. A second push is attempted ONLY after
/// a full-deadline timeout with zero new runs (evidence the push was genuinely
/// dropped, e.g. a reactivation reconnect blip), re-confirming a reader first.
pub async fn fire_until_run(
    feed: &SseFake,
    disp: &Dispatcher,
    pid: &Uuid,
    event: &str,
    before: &std::collections::HashSet<Uuid>,
) -> anyhow::Result<Uuid> {
    let mut last_err = None;
    for _ in 0..2 {
        feed.wait_for_subscriber(Duration::from_secs(60)).await?;
        feed.push_event(event, &json!({ "value": "go" }).to_string())?;
        // Full settle deadline: only a genuine "nothing ever started" times out
        // here, so a timeout is real evidence the push was dropped, not just a slow
        // cold start.
        match run::wait_for_triggered_run(disp, pid, before, run::RUN_SETTLE_DEADLINE).await {
            Ok(run_id) => return Ok(run_id),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap().context("no run started after repeated trigger pushes"))
}

/// The project's runs a trigger's fire (or a run by hand) started, setup
/// runs left out: an activate starts a setup run of its own, so a test
/// that activates between its snapshot and its check compares these.
pub async fn fire_runs(disp: &Dispatcher, pid: &Uuid) -> anyhow::Result<std::collections::HashSet<Uuid>> {
    let page: weft_core::program::RunPage = disp.get_json(&format!("/runs?project_id={pid}&limit=200")).await?;
    Ok(page.runs.into_iter().filter(|run| run.phase == weft_core::primitive::Phase::Fire).map(|run| run.run_id).collect())
}

/// Wait for exactly one fire run not in `before` ([`fire_runs`]), and
/// return it. A second one is a run nothing asked for, and fails.
pub async fn wait_for_one_new_fire(
    disp: &Dispatcher,
    pid: &Uuid,
    before: &std::collections::HashSet<Uuid>,
) -> anyhow::Result<Uuid> {
    weft_e2e::poll_until("one new fire run", run::RUN_SETTLE_DEADLINE, Duration::from_millis(500), || async {
        let new: Vec<Uuid> = fire_runs(disp, pid).await?.difference(before).copied().collect();
        anyhow::ensure!(new.len() <= 1, "one event, one run; found {}: {new:?}", new.len());
        Ok(new.first().copied())
    })
    .await
}

/// Push `event` on the SSE feed while `trigger` (a shared one) is down
/// with a door that holds, and wait until its activation holds it as one
/// arrival. Pushed again only when the first push held nothing in a whole
/// minute (lost on a reconnect), the way [`fire_until_run`] does.
pub async fn push_until_held(feed: &SseFake, disp: &Dispatcher, pid: &Uuid, event: &str, trigger: &str) -> anyhow::Result<()> {
    let mut last_err = None;
    for _ in 0..2 {
        feed.wait_for_subscriber(Duration::from_secs(60)).await?;
        feed.push_event(event, &json!({ "value": "go" }).to_string())?;
        match weft_e2e::status::wait_until(disp, pid, "the event to be held", Duration::from_secs(60), |s| {
            s.trigger_held(trigger).0 >= 1
        })
        .await
        {
            Ok(s) => {
                let held = s.trigger_held(trigger).0;
                anyhow::ensure!(held == 1, "one event pushed, {held} held");
                return Ok(());
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap().context("the trigger held nothing after repeated pushes"))
}

// ----- reading a `--json` command's NDJSON --------------------------------
//
// One definition of each, used by every version-tree suite. They were a
// verbatim copy in two test files, which is how two readers of one
// output format stop agreeing.

/// The run returned by run or bake. A bake answers one setup per owner
/// of the triggers it prepared (`baked`), so it names one run only when
/// it prepared one owner's; a bake of several is refused here, since
/// "the" run would be a guess.
pub fn run_id_of(stdout: &str) -> anyhow::Result<Uuid> {
    for line in stdout.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line.trim()) else { continue };
        if v.get("verb").and_then(Value::as_str) == Some("bake") {
            let Some(baked) = v.pointer("/detail/result/baked").and_then(Value::as_array) else { continue };
            let runs: Vec<&str> = baked.iter().filter_map(|bake| bake.get("runId").and_then(Value::as_str)).collect();
            match runs.as_slice() {
                [run] => return Ok(run.parse()?),
                _ => anyhow::bail!("expected the bake to run one setup, it ran {}:\n{stdout}", runs.len()),
            }
        }
        if let Some(c) = v.pointer("/detail/runId").and_then(Value::as_str) {
            return Ok(c.parse()?);
        }
    }
    anyhow::bail!("no run in command output:\n{stdout}")
}

/// The one-line summary on the terminal event.
pub fn summary_of(stdout: &str) -> String {
    for line in stdout.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line.trim()) else { continue };
        if v.get("phase").and_then(Value::as_str) == Some("complete") {
            return v.pointer("/detail/summary").and_then(Value::as_str).unwrap_or("").to_string();
        }
    }
    String::new()
}

/// The version tree, as `weft tree --json` answers it: the install's
/// tree plus `workingCopy`, the id of the folder's own working copy.
/// Reading it syncs the folder first, as every command does.
pub async fn tree_of(project: &weft_e2e::project::Project) -> anyhow::Result<Value> {
    Ok(serde_json::from_str(project.weft(&["tree", "--json"]).await?.trim())?)
}

/// The folder's own working copy in a [`tree_of`] answer: its
/// `headVersion`, `headRun`, `workingVersion` and `revision`.
pub fn my_copy(tree: &Value) -> anyhow::Result<&Value> {
    let id = tree["workingCopy"].as_str().ok_or_else(|| anyhow::anyhow!("the tree names no working copy of this folder: {tree}"))?;
    tree["workingCopies"]
        .as_array()
        .and_then(|copies| copies.iter().find(|copy| copy["id"] == json!(id)))
        .ok_or_else(|| anyhow::anyhow!("the folder's working copy {id} is not in the tree: {tree}"))
}

/// The id of every version a [`tree_of`] answer holds.
pub fn version_ids(tree: &Value) -> Vec<String> {
    tree["versions"]
        .as_array()
        .map(|versions| versions.iter().filter_map(|v| v["id"].as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

/// The project's working copies as the install holds them right now,
/// read over `GET /projects/{id}/working-copies`. Unlike [`tree_of`] this
/// runs no command in the folder, so nothing syncs on the way: what it
/// shows of a save is what the runtime's own sync already landed.
pub async fn working_copies(disp: &Dispatcher, project: &Uuid) -> anyhow::Result<Vec<weft_core::working_copy::WorkingCopy>> {
    disp.get_json(&format!("/projects/{project}/working-copies")).await
}

/// Whether a node's rows in a run came from the seed. The node is
/// spelled the way the source reads (`triage.up` is the file's node
/// under the call `triage`).
pub fn inherited(run: &weft_e2e::run::SettledRun, node: &str) -> bool {
    run.events_of(node).any(|e| e.field("inheritedFrom").is_some())
}
