//! `weft activate [project]`. Without arg: discover cwd project,
//! ensure registered, activate. With arg: treat it as a project id
//! and activate directly (assume already registered).
//!
//! The activation is carried out by a task of the install's, which the
//! request answers at once; this follows it in short looks until it ends
//! (`super::follow::follow_task`), saying what it waits on as that
//! changes. A look the install could not answer (its copy restarting, its
//! gateway cutting it) is tried again, and the activation goes on
//! meanwhile; Ctrl+C stops following, never the activation, which `weft
//! status` still shows and `weft cancel-activate` stops.
//!
//! It turns on every trigger it names that is off, new ones included, and
//! leaves the ones on alone: with nothing off it succeeds, saying so, so a
//! script can always run it. What the triggers held while they were down
//! is released (`--held run`, the default, paced at half each trigger's
//! own limits so new arrivals still get through) or dropped (`--held
//! drop`). It never touches a run going right now.

use anyhow::Context;

use weft_core::activation::{ActivateRequest, ActivateResponse, ActivateStarted};
use weft_core::ReleaseHeld;

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    held: ReleaseHeld,
    port: Option<u16>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Activate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, held, port, scope).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    held: ReleaseHeld,
    port: Option<u16>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    // The hashes of the build just made, which the dispatcher checks is
    // still the registered one. The "activate by id" path builds nothing
    // and sends none: it activates whatever is registered.
    let (client, id, name, build) = match project {
        Some(id) => (ctx.client()?, id.clone(), id, Default::default()),
        None => {
            // The build makes every place's images, an instance's copies
            // included, so a program starting an instance's copy later finds
            // its images there.
            let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;
            let build = handle.built.named();
            (handle.client, handle.id, handle.name, build)
        }
    };
    let path = format!("/projects/{id}/activations");
    let body = ActivateRequest { build, scope, held, port };
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let started: ActivateStarted =
        serde_json::from_value(client.post_json(&path, &serde_json::to_value(&body)?).await?).context("read the activate answer")?;
    // What it waits on and for how long, on the progress channel, each
    // time that changes and again every so often while it does not: an
    // activation may wait on its setup run for as long as a person takes,
    // and the time said is the time as of the look.
    let following = std::time::Instant::now();
    let mut said: (Option<String>, crate::progress::SayAgain) = (None, Default::default());
    let going = async |where_it_is: Option<serde_json::Value>| {
        let Some(where_it_is) = where_it_is else { return Ok(()) };
        let now: weft_core::infra::wire::ChangeProgress = crate::client::read_answer(where_it_is, "where the activation is")?;
        let (waiting, again) = &mut said;
        if waiting.as_deref() != now.waiting.as_deref() || again.due(following.elapsed()) {
            progress.trigger_register_wait(&now);
            again.said(following.elapsed());
            *waiting = now.waiting;
        }
        Ok(())
    };
    let answer: ActivateResponse =
        crate::client::read_answer(super::follow::follow_task(&client, progress, &id, started.task, going).await?, "the activate answer")?;
    progress.dispatcher_call_done(serde_json::json!({ "projectId": id }));
    progress.trigger_register_done();
    if let Some(note) = &answer.infra_not_running {
        progress.warn(note);
    }
    let left_out = &answer.per_instance_left_out;
    if let Some(note) = left_out_note(left_out, ctx.on()) {
        progress.warn(&note);
    }
    let did = what_it_did(&answer);
    let summary = match &answer.address {
        Some(weft_core::projects::ProjectAddress::Serving { url }) => format!("{name} ({id}): {did}; its routes answer at {url}"),
        // A port the person asked for, checked free before activating,
        // that another program took as the project activated fails the
        // command, like any server refused its port.
        Some(weft_core::projects::ProjectAddress::Unavailable { why }) if port.is_some() => {
            anyhow::bail!("{name} ({id}): {did}, and its own address could not open: {why}")
        }
        Some(weft_core::projects::ProjectAddress::Unavailable { why }) => {
            progress.warn(&format!("the project's own address is unavailable: {why}"));
            format!("{name} ({id}): {did}")
        }
        None => format!("{name} ({id}): {did}"),
    };
    super::ensure::warn_broken(progress, &answer.broken);
    progress.complete_with(&summary, serde_json::to_value(&answer)?);
    Ok(())
}

/// What an activate did, as a person reads it: which triggers it turned
/// on (or that every one was on already), and what became of what they
/// held.
fn what_it_did(answer: &ActivateResponse) -> String {
    if answer.turned_on.is_empty() {
        return "every trigger it names is on already; nothing to turn on".into();
    }
    let held = match &answer.held {
        Some(released) if !released.held.is_empty() => match released.release {
            ReleaseHeld::Run => format!("; {} released, at half each trigger's limits", released.held.describe()),
            ReleaseHeld::Drop => format!("; {} dropped", released.held.describe()),
        },
        _ => String::new(),
    };
    format!("turned on {}{held}", weft_core::activation::OwnerTriggers::list_words(&answer.turned_on))
}

/// What a person is told about the per-instance triggers a plain activate
/// left off: each exists once per instance, so an instance has to be named.
fn left_out_note(left_out: &[String], on: Option<&str>) -> Option<String> {
    if left_out.is_empty() {
        return None;
    }
    Some(format!(
        "left off: {} run once per instance, so they stay off until an instance is named. \
         Switch one instance's on with `{}`, or have the program switch \
         them on itself once it knows the instance (an ActivateTriggers node given the instance does it)",
        left_out.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", "),
        super::weft_on(on, "activate --instance <id>")
    ))
}

#[cfg(test)]
mod tests {
    use weft_core::activation::{ActivateResponse, HeldReleased, OwnerTriggers};
    use weft_core::instance::{InstanceId, Owner};
    use weft_core::projects::HeldWork;
    use weft_core::ReleaseHeld;

    /// An activate says what it turned on, whose, and what became of what
    /// they held; with nothing off it says so, which is no error.
    #[test]
    fn an_activate_says_what_it_turned_on_and_what_it_released() {
        assert_eq!(super::what_it_did(&ActivateResponse::default()), "every trigger it names is on already; nothing to turn on");
        let answer = ActivateResponse {
            turned_on: vec![
                OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] },
                OwnerTriggers { owner: Owner::Instance(InstanceId::new("ada").unwrap()), triggers: vec!["inbox".into()] },
            ],
            held: Some(HeldReleased { held: HeldWork { arrivals: 3, paused: 0 }, release: ReleaseHeld::Run }),
            ..Default::default()
        };
        let said = super::what_it_did(&answer);
        assert!(said.starts_with("turned on 'door'; 'inbox' of instance 'ada'; "), "{said}");
        assert!(said.ends_with("released, at half each trigger's limits"), "{said}");
    }
}
