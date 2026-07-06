//! Shared scaffolding for the lifecycle-transition e2e suites
//! (`lifecycle_transitions.rs`, `multi_worker.rs`): the `lifecycle`
//! fixture's graph shapes, the HoldGate release protocol, and the
//! drive/observe helpers built on them. One definition, used by every
//! test binary that evolves a `lifecycle` project through verbs.
//!
//! Rust compiles each integration-test file as its own binary, so not
//! every binary uses every item; the allow keeps that from spraying
//! dead-code warnings per binary.
#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;
use weft_e2e::client::{cli, CliOutput};
use weft_e2e::fakes::SseFake;
use weft_e2e::{run, Dispatcher};

pub const INFRA_NODE: &str = "svc";
/// Bodies the HoldGate polls for: anything without "release" holds.
pub const HOLD: &str = "hold";
pub const RELEASE: &str = "release";

/// Graph: kick -> HoldGate -> Debug. No infra, no trigger. The base shape.
pub fn graph_hold(release_url: &str) -> String {
    format!(
        "kick = Text {{ value: \"go\" }}\n\
         hold = HoldGate {{ url: \"{release_url}\" }}\n\
         hold.arm = kick.value\n\
         out = Debug\n\
         out.data = hold.done\n"
    )
}

/// Graph: MiniService only (the infra_min shape).
pub fn graph_infra() -> String {
    "svc = MiniService\nout = Debug\nout.data = svc.status\n".to_string()
}

/// Graph: MiniService + the HoldGate chain, both roots. A run fires both; the
/// svc branch settles fast, the hold branch keeps the execution running until
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
/// execution that holds until released.
pub fn graph_trigger_hold(sse_url: &str, event_name: &str, release_url: &str) -> String {
    format!(
        "start = TestSseTrigger {{\n\
         \x20 url: \"{sse_url}\"\n\
         \x20 event_name: \"{event_name}\"\n\
         }}\n\
         hold = HoldGate {{ url: \"{release_url}\" }}\n\
         hold.arm = start.value\n\
         out = Debug\n\
         out.data = hold.done\n"
    )
}

/// Graph: trigger + hold chain + MiniService (all three concerns at once).
pub fn graph_trigger_infra_hold(sse_url: &str, event_name: &str, release_url: &str) -> String {
    format!(
        "{}svc = MiniService\nsvc_out = Debug\nsvc_out.data = svc.status\n",
        graph_trigger_hold(sse_url, event_name, release_url)
    )
}

/// Spawn a `weft` invocation in the background (for verbs that BLOCK on a
/// drain the test is about to observe from the outside). Returns the join
/// handle; the test joins it once the drain resolves.
pub fn spawn_weft(
    dir: PathBuf,
    args: Vec<String>,
) -> tokio::task::JoinHandle<anyhow::Result<CliOutput>> {
    tokio::spawn(async move {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        cli(&dir, &arg_refs).await
    })
}

/// Assert a POST to a project verb is REJECTED (a REJ cell): non-2xx, with
/// the offending status in the failure message.
pub async fn assert_verb_rejected(disp: &Dispatcher, path: &str, why: &str) -> anyhow::Result<()> {
    let (code, body) = disp.post_raw(path, &json!({})).await?;
    anyhow::ensure!(
        !code.is_success(),
        "POST {path} must be rejected ({why}), but got HTTP {code}: {body}"
    );
    Ok(())
}

/// The current status string of one execution (`running` /
/// `waiting_for_input` / `completed` / `cancelled` / `failed`), read live
/// (no settle wait).
pub async fn exec_status(disp: &Dispatcher, color: Uuid) -> anyhow::Result<String> {
    let v: Value = disp.get_json(&format!("/executions/{color}")).await?;
    Ok(v.get("status").and_then(Value::as_str).unwrap_or("").to_string())
}

/// Fire the SSE trigger and wait for the ONE new execution it starts.
///
/// The push happens after `wait_for_subscriber` confirms a connection is reading,
/// so a single push is delivered (not lost on a stale reader). We then wait the
/// FULL worker-settle deadline for the execution to appear, because a trigger fire
/// cold-starts a worker pod (spawn + image pull + fold + run) and is legitimately
/// slow. We do NOT re-push on a short timeout: pushing again while the first push's
/// execution is still cold-starting would start a SECOND execution and make the
/// "exactly one new execution" contract fail. A second push is attempted ONLY after
/// a full-deadline timeout with zero new executions (evidence the push was genuinely
/// dropped, e.g. a reactivation reconnect blip), re-confirming a reader first.
pub async fn fire_until_execution(
    feed: &SseFake,
    disp: &Dispatcher,
    pid: &Uuid,
    event: &str,
    before: &std::collections::HashSet<Uuid>,
) -> anyhow::Result<Uuid> {
    let mut last_err = None;
    for _ in 0..2 {
        feed.wait_for_subscriber(Duration::from_secs(60)).await?;
        feed.push_event(event, &json!({ "value": "go" }).to_string());
        // Full settle deadline: only a genuine "nothing ever started" times out
        // here, so a timeout is real evidence the push was dropped, not just a slow
        // cold start.
        match run::wait_for_triggered_execution(disp, pid, before, run::RUN_SETTLE_DEADLINE).await {
            Ok(color) => return Ok(color),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap().context("no execution started after repeated trigger pushes"))
}
