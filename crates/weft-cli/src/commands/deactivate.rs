//! `weft deactivate [project]`. Without arg: discover cwd project.
//! No build required; deactivate just drops trigger URLs (or
//! preserves them, per --mode).
//!
//! Also home to [`prompt_trigger_deactivation`]: the shared helper
//! used by every CLI verb that takes triggers down as a side effect
//! (`weft infra stop / terminate / upgrade`). One UX surface for
//! trigger deactivation means improvements propagate everywhere.

use super::Ctx;
use crate::commands::ensure::parse_running_choice;
use crate::progress::ActionVerb;
use weft_core::{DeactivateSpec, DeactivationMode, RunningPolicy, DEFAULT_GRACE_MINUTES};

/// Resolve the trigger-deactivation choice (mode + grace + running
/// policy) from explicit flags, falling back to interactive prompts
/// on a human terminal. Off a terminal, or in `--json` mode, a missing
/// `--mode` is `wipe`. A missing grace takes the default in `--json`
/// mode; a terminal is asked (Enter accepts the default); off a terminal
/// the grace prompt refuses, naming `--grace`.
///
/// `wipe` is the default rather than a refusal, and rather than one of
/// the preserving modes, because those keep signals and suspended runs
/// alive across the change: on a project somebody is still building,
/// that is how a run ends up waiting on something nobody will ever
/// answer. Starting fresh is the safe reading of "I just changed the
/// program", and anybody who means to keep the in-flight work says so
/// with `--mode hibernate` or `--mode park`. This was a refusal until
/// the agents hit it on every `weft resync`, where a human never did.
///
/// Returns the wire `DeactivateSpec` itself, validated, ready to
/// embed under `triggerDeactivation` in a request body or to POST
/// verbatim to `/deactivate`: the CLI builds the type the dispatcher
/// reads, so the two cannot disagree on a mode or a rule.
pub fn prompt_trigger_deactivation(
    json: bool,
    mode: Option<&str>,
    grace: Option<u32>,
    running_policy: RunningPolicy,
    drain_timeout_secs: Option<u64>,
) -> anyhow::Result<DeactivateSpec> {
    // Mode resolution priority: explicit --mode flag > interactive
    // prompt (human terminal only) > error (a script, or `--json`).
    let scripted = json || !crate::prompt::is_interactive();
    let mode = match mode {
        Some(m) => DeactivationMode::parse(m).ok_or_else(|| {
            anyhow::anyhow!(
                "invalid mode '{m}'; must be one of: {}",
                DeactivationMode::VARIANTS.iter().map(|m| m.as_str()).collect::<Vec<_>>().join(", ")
            )
        })?,
        None if scripted => DeactivationMode::Wipe,
        None => prompt_mode()?,
    };

    // Grace window: only meaningful for hibernate. `--json` (the editor)
    // takes the documented default; anyone else without --grace is
    // asked, and `prompt_grace` refuses off a terminal naming the flag,
    // so a shell script never gets a window it did not choose. The
    // other modes carry the default the wire would fill in anyway.
    let grace_minutes = match (mode, grace) {
        (DeactivationMode::Hibernate, Some(g)) => g,
        (DeactivationMode::Hibernate, None) if json => DEFAULT_GRACE_MINUTES,
        (DeactivationMode::Hibernate, None) => prompt_grace()?,
        _ => DEFAULT_GRACE_MINUTES,
    };

    // The running-work pair arrives parsed (`parse_running_choice`, the
    // one reading every verb uses: cancel unless the flag says wait, a
    // cap only beside a wait). The spec's own validator refuses the one
    // contradictory pair (wipe + wait) here, before anything is sent.
    let spec = DeactivateSpec { mode, grace_minutes, running_policy, drain_timeout_secs };
    spec.validate().map_err(|refusal| anyhow::anyhow!("{refusal}"))?;
    Ok(spec)
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
    drain_timeout: Option<u64>,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Deactivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, mode, grace, running_policy, drain_timeout)
            .await
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
    drain_timeout: Option<u64>,
) -> anyhow::Result<()> {
    // No `--mode` off a terminal (or under `--json`) is `wipe` with the
    // running executions cancelled, the standing answer while building;
    // on a terminal `prompt_trigger_deactivation` asks.
    let (running_policy, drain_timeout) = parse_running_choice(running_policy.as_deref(), drain_timeout)?;
    let deactivation = prompt_trigger_deactivation(ctx.json(), mode.as_deref(), grace, running_policy, drain_timeout)?;

    let (client, id, name) = match project {
        Some(id) => (ctx.client(), id.clone(), id),
        None => super::resolve_project(ctx)?,
    };

    let path = format!("/projects/{id}/deactivate");
    // The dispatcher's `/deactivate` endpoint takes the `DeactivateSpec`
    // itself, the same shape the infra verbs embed under
    // `triggerDeactivation`.
    let body = serde_json::to_value(&deactivation)?;
    let mode_str = deactivation.mode.as_str();
    let running_policy_str = deactivation.running_policy.as_str();
    let grace_minutes =
        (deactivation.mode == DeactivationMode::Hibernate).then_some(deactivation.grace_minutes);
    progress.drain_wait(&body, deactivation.drain_timeout_secs);
    progress.dispatcher_call_start(&path);
    client.post_with_body(&path, &body).await?;
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
    let line = crate::prompt::prompt_line("> ", "--grace <minutes>")?;
    if line.is_empty() {
        return Ok(DEFAULT_GRACE_MINUTES);
    }
    line.parse::<u32>().map_err(|_| {
        anyhow::anyhow!("grace must be a non-negative integer (minutes); got '{line}'")
    })
}

fn prompt_mode() -> anyhow::Result<DeactivationMode> {
    println!("Choose preservation mode for in-flight signals:");
    println!("  1) wipe       drop all signals, cancel suspended runs (fully fresh on reactivate)");
    println!("  2) hibernate  keep signals; hide pending tasks from extension; park late submissions");
    println!("  3) park       keep signals visible; queue new submissions for reactivate");
    let line = crate::prompt::prompt_line("Enter 1, 2, or 3: ", "--mode wipe | hibernate | park")?;
    Ok(match line.as_str() {
        "1" | "wipe" => DeactivationMode::Wipe,
        "2" | "hibernate" => DeactivationMode::Hibernate,
        "3" | "park" => DeactivationMode::Park,
        other => anyhow::bail!("aborted: unrecognized choice '{other}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::prompt_trigger_deactivation;
    use crate::commands::ensure::parse_running_choice;
    use weft_core::{DeactivateSpec, DeactivationMode, RunningPolicy};

    /// The flags as a verb reads them: parsed once, then handed to the
    /// prompt, exactly the two steps every verb takes.
    fn prompt(mode: Option<&str>, policy: Option<&str>, cap: Option<u64>) -> anyhow::Result<DeactivateSpec> {
        let (running_policy, drain_timeout) = parse_running_choice(policy, cap)?;
        prompt_trigger_deactivation(true, mode, None, running_policy, drain_timeout)
    }

    /// A script (and every agent) gets `wipe` with no flag: the
    /// preserving modes carry signals and suspended runs across a
    /// change to the program, which is how a run ends up waiting on
    /// something nobody will answer. Keeping the in-flight work is the
    /// thing you ask for, and so is waiting on the running work: with
    /// no `--running-policy` nothing waits, whatever the mode.
    #[test]
    fn a_script_gets_a_fresh_start_unless_it_asks_to_keep_the_in_flight_work() {
        let fresh = prompt(None, None, None).unwrap();
        assert_eq!(fresh.mode, DeactivationMode::Wipe);
        assert_eq!(fresh.running_policy, RunningPolicy::Cancel, "waiting before a wipe is contradictory");

        let park = prompt(Some("park"), None, None).unwrap();
        assert_eq!(park.mode, DeactivationMode::Park);
        assert_eq!(park.running_policy, RunningPolicy::Cancel, "a wait is asked for, never assumed");

        let patient = prompt(Some("hibernate"), Some("wait"), Some(30)).unwrap();
        assert_eq!(patient.running_policy, RunningPolicy::Wait);
        assert_eq!(patient.drain_timeout_secs, Some(30), "the cap rides the wait it was asked with");
        assert_eq!(patient.grace_minutes, 15, "`--json` takes the documented grace");

        let err = prompt(Some("park"), None, Some(30))
            .expect_err("a cap with nothing to bound is refused, never dropped")
            .to_string();
        assert!(err.contains("--drain-timeout") && err.contains("wait"), "{err}");

        let err = prompt(Some("wipe"), Some("wait"), None)
            .expect_err("wipe + wait is the wire type's own refusal")
            .to_string();
        assert!(err.contains("contradictory"), "{err}");

        let err = prompt(Some("nuke"), None, None)
            .expect_err("an unknown mode is refused, naming the legal ones")
            .to_string();
        assert!(err.contains("wipe, hibernate, park"), "{err}");
    }
}
