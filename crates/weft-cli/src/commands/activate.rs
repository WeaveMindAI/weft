//! `weft activate [project]`. Without arg: discover cwd project,
//! ensure registered, activate. With arg: treat it as a project id
//! and activate directly (assume already registered).
//!
//! When the project is currently inactive AND preserved state
//! exists (parked + suspended counts both non-zero), the user is
//! prompted to pick one of three reactivate choices. The choice is
//! sent in the activate body (`reactivateChoice`); the dispatcher's
//! activate handler decides whether to drain/wipe/keep based on it.
//!
//! A worker built from an older image is replaced on the way:
//! `--running-policy` says whether its running executions are
//! cancelled now (the default) or waited for, up to `--drain-timeout`.

use anyhow::Context;

use super::ensure::{parse_running_choice, running_choice_fields};
use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    reactivate_choice_flag: Option<String>,
    running_policy: Option<String>,
    drain_timeout: Option<u64>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Activate, |progress| async move {
        run_inner(
            &ctx_inner,
            &progress,
            project,
            reactivate_choice_flag,
            running_policy,
            drain_timeout,
            scope,
        )
        .await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    reactivate_choice_flag: Option<String>,
    running_policy: Option<String>,
    drain_timeout: Option<u64>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    // Parsed before anything is built: a misspelt policy, or a cap with
    // nothing to bound, is refused here, not after a build that took a
    // minute.
    let (running_policy, drain_timeout) =
        parse_running_choice(running_policy.as_deref(), drain_timeout)?;
    let (client, id, name, binary_hash, definition_hash, infra_hash) = match project {
        // Activate-by-id skips the build/discover step entirely.
        Some(id) => (ctx.client()?, id.clone(), id, None, None, None),
        None => {
            // The build makes every place's images, a member's copies
            // included, so a program starting a member's copy later finds
            // its images there.
            let handle = super::ensure::ensure_registered(ctx, progress, weft_compiler::codegen::NodeSet::Full).await?;
            (
                handle.client,
                handle.id,
                handle.name,
                Some(handle.built.binary_hash),
                Some(handle.built.definition_hash),
                Some(handle.built.infra_hash),
            )
        }
    };

    // --reactivate-choice always wins. Otherwise:
    //   - JSON mode: detect preserved state via /status; bail loud
    //     if present so the caller (extension) is forced to pass an
    //     explicit choice. No silent default.
    //   - TTY mode: interactive prompt iff preserved state.
    let reactivate_choice = if let Some(c) = reactivate_choice_flag {
        validate_reactivate_choice(&c)?;
        Some(c)
    } else if ctx.json() {
        require_choice_when_preserved(&client, &id).await?
    } else {
        prompt_reactivate_choice(&client, &id).await?
    };

    let path = format!("/projects/{id}/activate");
    let mut body = serde_json::Map::new();
    // The hashes of the build just made, which the dispatcher checks is
    // still the registered one. The "activate by id" path builds nothing
    // and sends none: it activates whatever is registered.
    super::ensure::inject_hash_fields_opt(
        &mut body,
        binary_hash.as_deref(),
        definition_hash.as_deref(),
        infra_hash.as_deref(),
    );
    if let Some(choice) = reactivate_choice {
        body.insert("reactivateChoice".into(), serde_json::Value::String(choice));
    }
    body.extend(running_choice_fields(running_policy, drain_timeout));
    body.insert("scope".into(), serde_json::to_value(&scope)?);
    // The one line that says the call may now sit for a while (only
    // under a wait: the progress reads the policy off the body), so a
    // quiet terminal is a wait and not a hang.
    progress.drain_wait(&serde_json::Value::Object(body.clone()), drain_timeout);
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let answer: serde_json::Value = client.post_json(&path, &serde_json::Value::Object(body)).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    progress.trigger_register_done();
    // SYNC: infra_not_running <-> crates/weft-dispatcher/src/api/project.rs ActivateResponse::infra_not_running
    if let Some(note) = answer.get("infra_not_running").and_then(|v| v.as_str()) {
        progress.warn(note);
    }
    if !ctx.json() {
        println!("activated {name} ({id})");
    }
    progress.complete(&format!("activated {name}"));
    Ok(())
}

/// Read the project's preserved state from `/status`. Returns
/// `Some((parked, suspended))` only when the project is `inactive`
/// AND at least one count is non-zero; `None` when the project is in no
/// state that preserves anything, or preserved nothing. A status that
/// cannot be read, or an inactive project whose answer carries no
/// preservation counts, is an error: guessing "nothing preserved" would
/// skip the choice and drop the parked work on the default.
async fn fetch_preserved_state(
    client: &crate::client::DispatcherClient,
    id: &str,
) -> anyhow::Result<Option<(u64, u64)>> {
    let path = format!("/projects/{id}/status");
    let resp: serde_json::Value = client
        .get_json(&path)
        .await
        .context("read the project's status to see what its inactive window preserved")?;
    let mismatch = || {
        anyhow::anyhow!(
            "the dispatcher's status for {id} does not say what it preserved: {resp}; upgrade the \
             dispatcher or this CLI so the versions match"
        )
    };
    let status = resp.get("status").and_then(|v| v.as_str()).ok_or_else(mismatch)?;
    if status != "inactive" {
        return Ok(None);
    }
    let count = |key: &str| resp.pointer(&format!("/preservation/{key}")).and_then(|v| v.as_u64()).ok_or_else(mismatch);
    let (parked, suspended) = (count("parked")?, count("suspended")?);
    if parked == 0 && suspended == 0 {
        return Ok(None);
    }
    Ok(Some((parked, suspended)))
}

/// JSON-mode preserved-state check. Bails with a clear message
/// when there's preserved state so callers must pass an explicit
/// `--reactivate-choice` instead of falling through to a default.
async fn require_choice_when_preserved(
    client: &crate::client::DispatcherClient,
    id: &str,
) -> anyhow::Result<Option<String>> {
    let Some((parked, suspended)) = fetch_preserved_state(client, id).await? else {
        return Ok(None);
    };
    anyhow::bail!(
        "project {id} has preserved state (parked={parked}, suspended={suspended}); \
         pass --reactivate-choice (execute_parked_keep_suspended | keep_suspended_only | wipe_all)"
    )
}

fn validate_reactivate_choice(choice: &str) -> anyhow::Result<()> {
    match choice {
        "execute_parked_keep_suspended" | "keep_suspended_only" | "wipe_all" => Ok(()),
        other => anyhow::bail!(
            "invalid --reactivate-choice '{other}'; expected one of: \
             execute_parked_keep_suspended, keep_suspended_only, wipe_all"
        ),
    }
}

/// TTY mode: if the project has preserved state, prompt the user.
/// Otherwise return None and let the dispatcher default
/// (`execute_parked_keep_suspended`) kick in.
async fn prompt_reactivate_choice(
    client: &crate::client::DispatcherClient,
    id: &str,
) -> anyhow::Result<Option<String>> {
    let Some((parked, suspended)) = fetch_preserved_state(client, id).await? else {
        return Ok(None);
    };
    println!("Preserved during inactive window:");
    println!("  - {parked} parked signal(s) (queued submissions, will execute on reactivate)");
    println!("  - {suspended} pending suspension(s) (registered, no submission yet)");
    println!();
    println!("Choose:");
    println!("  1) execute_parked_keep_suspended  drain parked + keep suspensions");
    println!("  2) keep_suspended_only            drop parked, keep suspensions");
    println!("  3) wipe_all                       drop everything, fresh start");
    let line = crate::prompt::prompt_line("> ", "--reactivate-choice <choice>")?;
    let choice = match line.as_str() {
        "1" | "execute_parked_keep_suspended" => "execute_parked_keep_suspended",
        "2" | "keep_suspended_only" => "keep_suspended_only",
        "3" | "wipe_all" => "wipe_all",
        _ => anyhow::bail!("invalid reactivate choice '{line}'; expected 1, 2, or 3"),
    };
    Ok(Some(choice.to_string()))
}
