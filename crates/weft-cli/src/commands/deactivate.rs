//! `weft deactivate [project]`. Without arg: discover cwd project.
//! No build required; deactivate just drops trigger URLs (or
//! preserves them, per --mode).

use super::Ctx;
use crate::progress::ActionVerb;

/// Default hibernate grace window when the user picks hibernate
/// but doesn't specify --grace. Mirrors the dispatcher's default;
/// keeping a single number isolated to one place per side avoids
/// drift if either side wants to bump it later.
const DEFAULT_GRACE_MINUTES: u32 = 15;

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

    // Mode resolution priority: explicit --mode flag > interactive
    // prompt (human terminal only) > default wipe (when --json or
    // stdin isn't a terminal). The extension always passes --mode
    // since it can't answer stdin prompts.
    let mode = match mode {
        Some(m) => m,
        None if ctx.json() => "wipe".to_string(),
        None => prompt_mode()?,
    };
    if !["wipe", "hibernate", "park"].contains(&mode.as_str()) {
        anyhow::bail!(
            "invalid mode '{mode}'; must be one of: wipe, hibernate, park"
        );
    }

    // Grace window: only meaningful for hibernate. Prompt if the
    // user picked hibernate interactively without --grace; otherwise
    // fall back to default.
    let grace_minutes = match (mode.as_str(), grace) {
        ("hibernate", Some(g)) => Some(g),
        ("hibernate", None) if ctx.json() => Some(DEFAULT_GRACE_MINUTES),
        ("hibernate", None) => Some(prompt_grace()?),
        _ => None,
    };

    // Running-policy: wait by default for preservation modes;
    // wipe forces cancel because waiting before wiping is
    // contradictory (you've asked to drop everything anyway).
    let running_policy = match running_policy.as_deref() {
        Some(p) if p == "wait" || p == "cancel" => p.to_string(),
        Some(other) => anyhow::bail!(
            "invalid --running-policy '{other}'; must be wait|cancel"
        ),
        None if mode == "wipe" => "cancel".to_string(),
        None => "wait".to_string(),
    };
    if mode == "wipe" && running_policy == "wait" {
        anyhow::bail!(
            "wipe requires --running-policy cancel; waiting before wiping is contradictory"
        );
    }

    let path = format!("/projects/{id}/deactivate");
    let mut body = serde_json::Map::new();
    body.insert("preservationMode".into(), serde_json::json!(mode));
    body.insert("runningPolicy".into(), serde_json::json!(running_policy));
    if let Some(g) = grace_minutes {
        body.insert("graceMinutes".into(), serde_json::json!(g));
    }
    let body = serde_json::Value::Object(body);
    progress.dispatcher_call_start(&path);
    // Dispatcher returns 204 No Content on success: idempotent
    // state mutation, no body to deserialize. post_json would
    // EOF-error on the empty body; post_with_body discards it.
    client.post_with_body(&path, &body).await?;
    let mut done = serde_json::Map::new();
    done.insert("mode".into(), serde_json::json!(mode));
    done.insert("runningPolicy".into(), serde_json::json!(running_policy));
    if let Some(g) = grace_minutes {
        done.insert("graceMinutes".into(), serde_json::json!(g));
    }
    progress.dispatcher_call_done(serde_json::Value::Object(done));
    if !ctx.json() {
        let suffix = match grace_minutes {
            Some(g) => format!("[mode: {mode}, running: {running_policy}, grace: {g}min]"),
            None => format!("[mode: {mode}, running: {running_policy}]"),
        };
        println!("deactivated {name} ({id}) {suffix}");
    }
    progress.complete(&format!("deactivated {name} ({mode}/{running_policy})"));
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
