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

use weft_core::activation::{ActivateRequest, ActivateResponse, ActivationTarget, ReactivateChoice};

use super::ensure::{parse_running_choice, running_choice};
use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    reactivate_choice_flag: Option<ReactivateChoice>,
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
    reactivate_choice_flag: Option<ReactivateChoice>,
    running_policy: Option<String>,
    drain_timeout: Option<u64>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    // Parsed before anything is built: a misspelt policy, or a cap with
    // nothing to bound, is refused here, not after a build that took a
    // minute.
    let (running_policy, drain_timeout) =
        parse_running_choice(running_policy.as_deref(), drain_timeout)?;
    // The hashes of the build just made, which the dispatcher checks is
    // still the registered one. The "activate by id" path builds nothing
    // and sends none: it activates whatever is registered.
    let (client, id, name, target) = match project {
        // Activate-by-id skips the build/discover step entirely.
        Some(id) => (ctx.client()?, id.clone(), id, ActivationTarget::default()),
        None => {
            // The build makes every place's images, an instance's copies
            // included, so a program starting an instance's copy later finds
            // its images there.
            let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;
            let target = handle.activation_target();
            (handle.client, handle.id, handle.name, target)
        }
    };

    // --reactivate-choice always wins. Otherwise a terminal is asked when
    // the triggers being turned on kept work while they were off. With
    // nobody to ask (JSON mode), no choice is sent, and the dispatcher
    // refuses an activate over kept work that names none: never a silent
    // default.
    let reactivate_choice = if reactivate_choice_flag.is_some() {
        reactivate_choice_flag
    } else if ctx.json() {
        None
    } else {
        prompt_reactivate_choice(&client, &id, &scope).await?
    };

    let path = format!("/projects/{id}/activate");
    let body = ActivateRequest {
        target: ActivationTarget { reactivate_choice, ..target },
        running: running_choice(running_policy, drain_timeout),
        scope,
    };
    // The one line that says the call may now sit for a while (only
    // under a wait), so a quiet terminal is a wait and not a hang.
    progress.drain_wait(running_policy, drain_timeout);
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let answer: ActivateResponse = serde_json::from_value(client.post_json(&path, &serde_json::to_value(&body)?).await?)
        .context("read the activate answer")?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    progress.trigger_register_done();
    if let Some(note) = &answer.infra_not_running {
        progress.warn(note);
    }
    let left_out = answer.per_instance_left_out;
    if let Some(note) = left_out_note(&left_out) {
        progress.warn(&note);
    }
    progress.complete_with(&format!("activated {name} ({id})"), serde_json::json!({ "per_instance_left_out": left_out }));
    Ok(())
}

/// What a person is told about the per-instance triggers a plain activate
/// left off: each exists once per instance, so an instance has to be named.
fn left_out_note(left_out: &[String]) -> Option<String> {
    if left_out.is_empty() {
        return None;
    }
    Some(format!(
        "left off: {} run once per instance, so they stay off until an instance is named. \
         Switch one instance's on with `weft activate --instance <id>`, or have the program switch \
         them on itself once it knows the instance (a node like ActivateInstanceTriggers does it)",
        left_out.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", ")
    ))
}

/// What the triggers `scope` names kept while they were off, as the
/// dispatcher counts it for that scope (`/status` asked about it): the
/// parked fires and the runs waiting on a person that a reactivate's choice
/// decides about. `None` when they kept nothing. A status that cannot be
/// read is an error: guessing "nothing kept" would skip the question.
async fn fetch_preserved_state(
    client: &crate::client::DispatcherClient,
    id: &str,
    scope: &weft_core::activation::ActivationScope,
) -> anyhow::Result<Option<(usize, usize)>> {
    let query = weft_core::projects::StatusQuery::for_scope(scope).to_query_string();
    let path = format!("/projects/{id}/status{query}");
    let resp: weft_core::projects::ProjectStatusResponse = serde_json::from_value(
        client
            .get_json(&path)
            .await
            .context("read the project's status to see what the triggers kept while they were off")?,
    )
    .with_context(|| {
        format!(
            "the dispatcher's status for {id} does not read as this CLI expects; upgrade the \
             dispatcher or this CLI so the versions match"
        )
    })?;
    let weft_core::projects::PreservationCounts { parked, suspended } = resp.preservation;
    if parked == 0 && suspended == 0 {
        return Ok(None);
    }
    Ok(Some((parked, suspended)))
}

/// TTY mode: when the triggers being turned on kept work while they were
/// off, ask what to do with it; `None` when they kept nothing.
async fn prompt_reactivate_choice(
    client: &crate::client::DispatcherClient,
    id: &str,
    scope: &weft_core::activation::ActivationScope,
) -> anyhow::Result<Option<ReactivateChoice>> {
    let Some((parked, suspended)) = fetch_preserved_state(client, id, scope).await? else {
        return Ok(None);
    };
    println!("Kept while these triggers were off:");
    println!("  - {parked} parked signal(s) (queued submissions, will execute on reactivate)");
    println!("  - {suspended} pending suspension(s) (registered, no submission yet)");
    println!();
    println!("Choose:");
    println!("  1) execute_parked_keep_suspended  drain parked + keep suspensions");
    println!("  2) keep_suspended_only            drop parked, keep suspensions");
    println!("  3) wipe_all                       drop everything, fresh start");
    let line = crate::prompt::prompt_line("> ", "--reactivate-choice <choice>")?;
    let choice = match line.as_str() {
        "1" => ReactivateChoice::ExecuteParkedKeepSuspended,
        "2" => ReactivateChoice::KeepSuspendedOnly,
        "3" => ReactivateChoice::WipeAll,
        named => named.parse().map_err(|_| anyhow::anyhow!("invalid reactivate choice '{line}'; expected 1, 2, or 3"))?,
    };
    Ok(Some(choice))
}
