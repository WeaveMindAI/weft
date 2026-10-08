//! `weft deactivate [project]`. Without arg: discover cwd project.
//! No build required; deactivate just drops trigger URLs (or
//! preserves them, per --mode).
//!
//! Also home to [`prompt_trigger_deactivation`] and
//! [`post_with_trigger_choice`]: the shared helpers used by every CLI
//! verb that takes triggers down as a side effect (`weft resync`, `weft
//! infra stop / terminate / upgrade`). One UX surface for trigger
//! deactivation means improvements propagate everywhere.

use super::Ctx;
use crate::commands::ensure::parse_running_choice;
use crate::progress::ActionVerb;
use weft_core::deactivation::{whose_triggers, DeactivateRequest, DeactivateResponse, ResyncRequest};
use weft_core::infra::wire::{StopRequest, UpgradeRequest};
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
    // prompt (human terminal only) > `wipe` (a script, or `--json`).
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

/// The trigger-deactivation flags as a verb receives them from the
/// command line, before they are read into a [`DeactivateSpec`]: the
/// mode and grace, and the running-work pair that rides inside the
/// choice (and answers the verb's own running work when no trigger
/// comes down).
#[derive(Debug, Default, Clone)]
pub struct TriggerChoiceFlags {
    pub mode: Option<String>,
    pub grace: Option<u32>,
    pub running_policy: Option<String>,
    pub drain_timeout: Option<u64>,
}

/// The trigger-deactivation choice the person gave on the command line,
/// `None` when they gave neither `--mode` nor `--grace`. A verb that
/// takes triggers down only when some are on (the infra verbs) sends a
/// given choice every time and lets the dispatcher use it or not; it is
/// the dispatcher that knows whose triggers read what. `--running-policy`
/// and `--drain-timeout` alone do not make a choice: they are the verb's
/// own running-work answer, and they ride inside the choice when it is
/// made.
pub fn given_trigger_deactivation(
    json: bool,
    mode: Option<&str>,
    grace: Option<u32>,
    running_policy: RunningPolicy,
    drain_timeout_secs: Option<u64>,
) -> anyhow::Result<Option<DeactivateSpec>> {
    if mode.is_none() && grace.is_none() {
        return Ok(None);
    }
    prompt_trigger_deactivation(json, mode, grace, running_policy, drain_timeout_secs).map(Some)
}

/// A request that carries the person's answer to "how do the triggers
/// come down" (`triggerDeactivation`) when the verb takes them down.
pub trait CarriesTriggerChoice: serde::Serialize {
    fn set_trigger_deactivation(&mut self, spec: DeactivateSpec);
}

impl CarriesTriggerChoice for ResyncRequest {
    fn set_trigger_deactivation(&mut self, spec: DeactivateSpec) {
        self.trigger_deactivation = Some(spec);
    }
}

impl CarriesTriggerChoice for UpgradeRequest {
    fn set_trigger_deactivation(&mut self, spec: DeactivateSpec) {
        self.trigger_deactivation = Some(spec);
    }
}

impl CarriesTriggerChoice for StopRequest {
    fn set_trigger_deactivation(&mut self, spec: DeactivateSpec) {
        self.trigger_deactivation = Some(spec);
    }
}

/// POST `body` to `path`, carrying the trigger-deactivation choice when
/// there is one to carry. `given` (see [`given_trigger_deactivation`])
/// always goes along. Without it the body goes as it is, and when the
/// dispatcher answers that triggers are on and it needs the choice, a
/// person at a terminal (not `--json`) is asked and the request is sent
/// once more with the answer; anyone else gets the dispatcher's refusal,
/// which names the flags to pass.
pub async fn post_with_trigger_choice<B: CarriesTriggerChoice>(
    client: &crate::client::DispatcherClient,
    path: &str,
    mut body: B,
    json: bool,
    given: Option<DeactivateSpec>,
    running_policy: RunningPolicy,
    drain_timeout_secs: Option<u64>,
) -> anyhow::Result<serde_json::Value> {
    if let Some(spec) = given {
        body.set_trigger_deactivation(spec);
        return client.post_json(path, &serde_json::to_value(&body)?).await;
    }
    let refusal = match client.post_json_or_choice_needed(path, &serde_json::to_value(&body)?).await? {
        Ok(answer) => return Ok(answer),
        Err(refusal) => refusal,
    };
    if json || !crate::prompt::is_interactive() {
        return Err(NeedsTriggerChoice(refusal).into());
    }
    println!("Triggers are on and come down first.");
    body.set_trigger_deactivation(prompt_trigger_deactivation(false, None, None, running_policy, drain_timeout_secs)?);
    client.post_json(path, &serde_json::to_value(&body)?).await
}

/// The dispatcher's refusal when triggers are on and nobody here could
/// be asked how they come down: its message names the flags. A type of
/// its own so the `--json` error event can say so as a fact
/// (`needsTriggerChoice`, see `progress::error_detail`), which is what
/// lets the editor open its picker instead of showing the refusal.
#[derive(Debug)]
pub struct NeedsTriggerChoice(pub String);

impl std::fmt::Display for NeedsTriggerChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NeedsTriggerChoice {}

/// The line a plain deactivate adds when instances still have triggers
/// on: how many, which, and the flag that takes theirs down too. `None`
/// when no instance's are on.
fn instances_still_on_line(instances: &[weft_core::instance::InstanceId], on: Option<&str>) -> Option<String> {
    if instances.is_empty() {
        return None;
    }
    let names = instances.iter().map(|m| m.as_str()).collect::<Vec<_>>().join(", ");
    let (count, have) = match instances.len() {
        1 => ("1 instance".to_string(), "has"),
        n => (format!("{n} instances"), "have"),
    };
    Some(format!(
        "{count} still {have} triggers on ({names}); `{}` takes theirs down too",
        super::weft_on(on, "deactivate --all-instances")
    ))
}

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    flags: TriggerChoiceFlags,
    scope: weft_core::activation::ActivationScope,
    all_instances: bool,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Deactivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, flags, scope, all_instances).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    flags: TriggerChoiceFlags,
    scope: weft_core::activation::ActivationScope,
    all_instances: bool,
) -> anyhow::Result<()> {
    // No `--mode` off a terminal (or under `--json`) is `wipe` with the
    // running executions cancelled, the standing answer while building;
    // on a terminal `prompt_trigger_deactivation` asks.
    let (running_policy, drain_timeout) = parse_running_choice(flags.running_policy.as_deref(), flags.drain_timeout)?;
    let deactivation =
        prompt_trigger_deactivation(ctx.json(), flags.mode.as_deref(), flags.grace, running_policy, drain_timeout)?;

    let (client, id, name) = match project {
        Some(id) => (ctx.client()?, id.clone(), id),
        None => super::resolve_project(ctx)?,
    };

    let path = format!("/projects/{id}/deactivate");
    // The dispatcher's `/deactivate` endpoint takes the `DeactivateSpec`
    // itself (the same shape the infra verbs embed under
    // `triggerDeactivation`), plus which activations it takes down.
    let mode_str = deactivation.mode.as_str();
    let running_policy_str = deactivation.running_policy.as_str();
    let grace_minutes =
        (deactivation.mode == DeactivationMode::Hibernate).then_some(deactivation.grace_minutes);
    progress.drain_wait(deactivation.running_policy, deactivation.drain_timeout_secs);
    let body = DeactivateRequest { spec: deactivation, scope, all_instances };
    progress.dispatcher_call_start(&path);
    let answer: DeactivateResponse = serde_json::from_value(client.post_json(&path, &serde_json::to_value(&body)?).await?)
        .map_err(|e| anyhow::anyhow!("deactivate: the dispatcher's answer does not read as one ({e}); upgrade the dispatcher or this CLI so the versions match"))?;
    let mut done = serde_json::Map::new();
    done.insert("mode".into(), serde_json::json!(mode_str));
    done.insert("runningPolicy".into(), serde_json::json!(running_policy_str));
    if let Some(g) = grace_minutes {
        done.insert("graceMinutes".into(), serde_json::json!(g));
    }
    done.insert("deactivated".into(), serde_json::to_value(&answer.deactivated)?);
    done.insert("instancesStillOn".into(), serde_json::to_value(&answer.instances_still_on)?);
    progress.dispatcher_call_done(serde_json::Value::Object(done));
    if !ctx.json() {
        if let Some(line) = instances_still_on_line(&answer.instances_still_on, ctx.on()) {
            println!("{line}");
        }
    }
    let suffix = match grace_minutes {
        Some(g) => format!("[mode: {mode_str}, running: {running_policy_str}, grace: {g}min]"),
        None => format!("[mode: {mode_str}, running: {running_policy_str}]"),
    };
    // The one line saying what happened: `complete` prints it on a
    // terminal and carries it in `--json`.
    let summary = if answer.deactivated.is_empty() {
        format!("no trigger in {name} ({id}) was on; nothing to deactivate {suffix}")
    } else {
        let whom = answer.deactivated.iter().map(whose_triggers).collect::<Vec<_>>().join(", ");
        format!("deactivated {whom} in {name} ({id}) {suffix}")
    };
    progress.complete(&summary);
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
    println!("  2) hibernate  like park for a grace window, questions hidden; after it, nothing new is taken");
    println!("  3) park       nothing is dropped: calls, answers and the triggers' own fires wait and run once they are back");
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

    /// Only `--mode` or `--grace` make a choice to send up front; the
    /// running-work flags alone stay the verb's own answer, so a verb
    /// with no trigger on is never refused over a choice it did not need.
    #[test]
    fn a_choice_is_given_by_mode_or_grace_only() {
        let (cancel, none) = parse_running_choice(None, None).unwrap();
        assert_eq!(super::given_trigger_deactivation(true, None, None, cancel, none).unwrap(), None);
        let (wait, cap) = parse_running_choice(Some("wait"), Some(30)).unwrap();
        assert_eq!(super::given_trigger_deactivation(true, None, None, wait, cap).unwrap(), None);
        let park = super::given_trigger_deactivation(true, Some("park"), None, wait, cap).unwrap().expect("given");
        assert_eq!((park.mode, park.running_policy, park.drain_timeout_secs), (DeactivationMode::Park, RunningPolicy::Wait, Some(30)));
    }

    #[test]
    fn a_plain_deactivate_names_the_instances_still_on() {
        assert_eq!(super::instances_still_on_line(&[], None), None);
        let one = super::instances_still_on_line(&[weft_core::instance::InstanceId::new("ada").unwrap()], None).unwrap();
        assert!(one.starts_with("1 instance still has triggers on (ada)") && one.contains("--all-instances"), "{one}");
        let two = super::instances_still_on_line(&[
            weft_core::instance::InstanceId::new("ada").unwrap(),
            weft_core::instance::InstanceId::new("bob").unwrap(),
        ], Some("prod"))
        .unwrap();
        assert!(two.contains("weft deactivate --all-instances --on prod"), "the hint acts on the same install: {two}");
        assert!(two.starts_with("2 instances still have triggers on (ada, bob)"), "{two}");
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
