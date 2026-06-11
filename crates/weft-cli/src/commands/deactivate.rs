//! `weft deactivate [project]`. Without arg: discover cwd project.
//! No build required; deactivate just drops trigger URLs (or
//! preserves them, per --mode).
//!
//! Also home to [`prompt_trigger_deactivation`]: the shared helper
//! used by every CLI verb that takes triggers down as a side effect
//! (`weft infra stop / terminate / upgrade`). One UX surface for
//! trigger deactivation means improvements propagate everywhere.

use super::Ctx;
use crate::commands::ensure::{parse_running_policy_flag, RunningPolicy};
use crate::progress::ActionVerb;

/// Default hibernate grace window when the user picks hibernate
/// but doesn't specify --grace. Mirrors the dispatcher's default;
/// keeping a single number isolated to one place per side avoids
/// drift if either side wants to bump it later.
const DEFAULT_GRACE_MINUTES: u32 = 15;

/// Resolve the trigger-deactivation choice (mode + grace + running
/// policy) from explicit flags, falling back to interactive prompts
/// on a human terminal. In `--json` mode, missing values are an
/// error: the caller (the extension) must pass them explicitly.
///
/// Returns a JSON object matching the wire
/// `DeactivateSpec` shape (`mode`, `runningPolicy`, optional
/// `graceMinutes`), ready to embed under `triggerDeactivation`
/// in the request body or to POST verbatim to `/deactivate`.
///
/// `verb_label` is used in the JSON-mode error message so the user
/// sees which verb is failing (e.g. "infra stop requires --mode").
pub fn prompt_trigger_deactivation(
    json: bool,
    verb_label: &str,
    mode: Option<&str>,
    grace: Option<u32>,
    running_policy: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    // Mode resolution priority: explicit --mode flag > interactive
    // prompt (human terminal only) > error (json mode).
    let mode = match mode {
        Some(m) => m.to_string(),
        None if json => anyhow::bail!(
            "{verb_label}: project is active, --mode required \
             (one of: wipe, hibernate, park)"
        ),
        None => prompt_mode()?,
    };
    if !["wipe", "hibernate", "park"].contains(&mode.as_str()) {
        anyhow::bail!("invalid mode '{mode}'; must be one of: wipe, hibernate, park");
    }

    // Grace window: only meaningful for hibernate. Prompt if the
    // user picked hibernate interactively without --grace; otherwise
    // fall back to default. Non-hibernate mode ignores grace entirely.
    let grace_minutes = match (mode.as_str(), grace) {
        ("hibernate", Some(g)) => Some(g),
        ("hibernate", None) if json => Some(DEFAULT_GRACE_MINUTES),
        ("hibernate", None) => Some(prompt_grace()?),
        _ => None,
    };

    // Running-policy: wait by default for preservation modes; wipe
    // forces cancel because waiting before wiping is contradictory.
    // Parse through the shared `RunningPolicy` so the accepted set
    // can't drift from the other verbs.
    let running_policy = match parse_running_policy_flag(running_policy)? {
        Some(p) => p,
        None if mode == "wipe" => RunningPolicy::Cancel,
        None => RunningPolicy::Wait,
    };
    if mode == "wipe" && running_policy == RunningPolicy::Wait {
        anyhow::bail!(
            "wipe requires running-policy=cancel; waiting before wiping is contradictory"
        );
    }

    let mut obj = serde_json::Map::new();
    obj.insert("mode".into(), serde_json::json!(mode));
    obj.insert("runningPolicy".into(), serde_json::json!(running_policy));
    if let Some(g) = grace_minutes {
        obj.insert("graceMinutes".into(), serde_json::json!(g));
    }
    Ok(serde_json::Value::Object(obj))
}

/// Read the project's current lifecycle.status from the dispatcher.
/// Returns `Ok(true)` when status == "active". Propagates errors
/// so callers don't silently skip trigger-deactivation prompts on
/// a network blip and then eat a 412 from the dispatcher.
pub async fn project_is_active(
    client: &crate::client::DispatcherClient,
    project_id: &str,
) -> anyhow::Result<bool> {
    // Surfaces real errors (network blip, dispatcher down) so the
    // caller doesn't silently skip the trigger-deactivation prompt
    // and then eat a 412 from the dispatcher with no context.
    let status: serde_json::Value = client
        .get_json(&format!("/projects/{project_id}/status"))
        .await?;
    Ok(status
        .get("status")
        .and_then(|s| s.as_str())
        .is_some_and(|s| s == "active"))
}

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    mode: Option<String>,
    grace: Option<u32>,
    running_policy: Option<String>,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Deactivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, mode, grace, running_policy).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    mode: Option<String>,
    grace: Option<u32>,
    running_policy: Option<String>,
) -> anyhow::Result<()> {
    let (client, id, name) = match project {
        Some(id) => (ctx.client(), id.clone(), id),
        None => super::resolve_project(ctx)?,
    };

    // Standalone deactivate has its own --json behavior: when no
    // --mode is given in json mode, default to wipe (preserves
    // backwards-compat with the extension's existing dispatch
    // path, which always passes --mode anyway). prompt_trigger_deactivation
    // would error in json mode without --mode; absorb that here by
    // pre-filling.
    let resolved_mode = match mode.as_deref() {
        Some(m) => Some(m.to_string()),
        None if ctx.json() => Some("wipe".to_string()),
        None => None,
    };
    let deactivation = prompt_trigger_deactivation(
        ctx.json(),
        "deactivate",
        resolved_mode.as_deref(),
        grace,
        running_policy.as_deref(),
    )?;

    let path = format!("/projects/{id}/deactivate");
    // The dispatcher's `/deactivate` endpoint takes the canonical
    // `DeactivateSpec` shape, same field names as the embedded
    // `triggerDeactivation` body (`mode`, `runningPolicy`,
    // `graceMinutes`). We send `deactivation` verbatim.
    let mode_str = deactivation
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("wipe")
        .to_string();
    let running_policy_str = deactivation
        .get("runningPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("cancel")
        .to_string();
    let grace_minutes = deactivation
        .get("graceMinutes")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);
    progress.dispatcher_call_start(&path);
    client.post_with_body(&path, &deactivation).await?;
    let mut done = serde_json::Map::new();
    done.insert("mode".into(), serde_json::json!(mode_str));
    done.insert("runningPolicy".into(), serde_json::json!(running_policy_str));
    if let Some(g) = grace_minutes {
        done.insert("graceMinutes".into(), serde_json::json!(g));
    }
    progress.dispatcher_call_done(serde_json::Value::Object(done));
    if !ctx.json() {
        let suffix = match grace_minutes {
            Some(g) => format!("[mode: {mode_str}, running: {running_policy_str}, grace: {g}min]"),
            None => format!("[mode: {mode_str}, running: {running_policy_str}]"),
        };
        println!("deactivated {name} ({id}) {suffix}");
    }
    progress.complete(&format!("deactivated {name} ({mode_str}/{running_policy_str})"));
    Ok(())
}

fn prompt_grace() -> anyhow::Result<u32> {
    println!(
        "Hibernate grace window in minutes (default {DEFAULT_GRACE_MINUTES}): \
         submissions arriving after this point will be refused. Press Enter for default."
    );
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_GRACE_MINUTES);
    }
    trimmed.parse::<u32>().map_err(|_| {
        anyhow::anyhow!("grace must be a non-negative integer (minutes); got '{trimmed}'")
    })
}

fn prompt_mode() -> anyhow::Result<String> {
    println!("Choose preservation mode for in-flight signals:");
    println!("  1) wipe       drop all signals, cancel suspended runs (fully fresh on reactivate)");
    println!("  2) hibernate  keep signals; hide pending tasks from extension; park late submissions");
    println!("  3) park       keep signals visible; queue new submissions for reactivate");
    println!("Enter 1, 2, or 3:");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(match line.trim() {
        "1" | "wipe" => "wipe".into(),
        "2" | "hibernate" => "hibernate".into(),
        "3" | "park" => "park".into(),
        other => anyhow::bail!("aborted: unrecognized choice '{other}'"),
    })
}
