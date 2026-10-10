//! Seeing a build through: asking the builder how each running build is
//! doing and recording how it ended, then registering each waiting
//! version whose builds all ended (`super::waiting`).
//!
//! The dispatcher's build loop ([`drain_loop`]) does it, whichever
//! dispatcher wakes. A build claimed, or a version written down as
//! waiting, wakes the loop ([`BUILD_CLAIMED_CHANNEL`]), which then looks
//! every [`look_every`] while a build runs or a version waits, and sleeps
//! after. It watches a build from its claim, before it is made on the
//! builder, so a start whose dispatcher went away is ended as soon as its
//! project's build heartbeat goes stale (`ledger::end_lost_starts`). No
//! process holds a build, and the request that started one answered as
//! soon as it ran, so none has to stay up for it to finish.

use anyhow::Result;
use sqlx::PgPool;
use weft_platform_traits::{BuildHandle, BuildStatus, ImageBuilder};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use super::ledger;
use crate::state::DispatcherState;

/// How often the loop looks at the running builds while there are some,
/// in real time at this install's pace.
fn look_every() -> std::time::Duration {
    weft_core::time_scale::scaled(std::time::Duration::from_secs(5))
}

/// The channel a build announces on the moment its row says it runs
/// ([`ledger::claim`]), and a version the moment it waits
/// (`super::waiting::ask`); the payload is the image, or the version's id.
pub const BUILD_CLAIMED_CHANNEL: &str = "weft_build_claimed";

pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(BUILD_CLAIMED_CHANNEL)];

/// One look at every running build: the starts nobody drives any more
/// ended (`ledger::end_lost_starts`), then each asked about and its end
/// recorded when it ended. Answers whether any build was running. One
/// build's failure, or a failed sweep, never skips the others; the
/// failures are answered together after.
pub async fn look(pool: &PgPool, images: &dyn ImageBuilder) -> Result<bool> {
    let now = crate::lease::now_unix();
    let mut failed = Vec::new();
    let stale_before = now - crate::transition::heartbeat_stale_secs();
    let swept = match pool.acquire().await {
        Ok(mut conn) => ledger::end_lost_starts(&mut conn, ledger::LostStarts::Undriven { stale_before }, now).await,
        Err(e) => Err(e.into()),
    };
    if let Err(e) = swept {
        failed.push(format!("ending the starts nobody drives: {e:#}"));
    }
    let builds = ledger::running(pool, None).await?;
    for build in &builds {
        if let Err(e) = advance_build(pool, images, build).await {
            failed.push(format!("{}: {e:#}", build.image_ref));
        }
    }
    anyhow::ensure!(failed.is_empty(), "could not move these builds forward: {}", failed.join("; "));
    Ok(!builds.is_empty())
}

/// Ask the builder about `build` and record its end when it ended. Errs
/// only when the ledger cannot be read or written: a builder that does not
/// answer is looked at again, and given up on (the build ends failed) once
/// it has not answered for [`ledger::UNANSWERED_GIVE_UP_SECS`].
async fn advance_build(pool: &PgPool, images: &dyn ImageBuilder, build: &ledger::RunningBuild) -> Result<()> {
    let now = crate::lease::now_unix();
    // Not made on the builder yet: nothing to ask. Its starter still
    // drives it, or `ledger::end_lost_starts` ended it.
    let Some(builder_id) = &build.builder_id else { return Ok(()) };
    let handle = BuildHandle::named(builder_id.clone());
    let status = match images.poll(&handle).await {
        Ok(status) => {
            ledger::answered(pool, &build.image_ref, &build.build_name).await?;
            status
        }
        Err(e) => {
            let since = ledger::unanswered(pool, &build.image_ref, &build.build_name, now).await?;
            if now - since < ledger::UNANSWERED_GIVE_UP_SECS {
                tracing::warn!(
                    target: "weft_dispatcher::build",
                    image = %build.image_ref, build = %builder_id, error = %format!("{e:#}"),
                    "the builder did not answer about a build; looking again"
                );
                return Ok(());
            }
            let outcome = ledger::Outcome::Failed(format!(
                "the builder has not answered about build {builder_id} for {} seconds: {e:#}",
                now - since
            ));
            if ledger::finish(pool, &build.image_ref, &build.build_name, outcome, now).await? {
                images.release(&handle).await;
            }
            return Ok(());
        }
    };
    let outcome = match status {
        BuildStatus::Pending => return Ok(()),
        BuildStatus::Gone => ledger::Outcome::Failed(format!(
            "the build {builder_id} is gone: its builder knows nothing of it any more (it was deleted, or the \
             process that ran it ended). Build again"
        )),
        BuildStatus::Succeeded => ledger::Outcome::Succeeded,
        BuildStatus::Failed { reason } => ledger::Outcome::Failed(reason),
    };
    // Whoever recorded the end frees the build, once.
    if ledger::finish(pool, &build.image_ref, &build.build_name, outcome, now).await? {
        images.release(&handle).await;
    }
    Ok(())
}

/// The dispatcher's build loop: every running build moved forward, then
/// every waiting version whose builds ended registered or ended
/// (`super::waiting::advance`), then every project whose build transition
/// nothing holds any more let out of it (`crate::transition::settle`), then
/// a look again soon while a build runs or a version waits. One dispatcher
/// at a time (`crate::reaper`'s install-wide lock), so the builder is asked
/// once per look, not once per replica.
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, WAKE_ON, "image_builds", |state| async move {
        let ran = look(&state.pg_pool, state.builder.images.as_ref()).await;
        // Advanced whatever the look answered: a build whose end was
        // recorded before another failed still lets its version register.
        let advanced = super::waiting::advance(&state.pg_pool, state.projects.as_ref()).await;
        if let Ok(advanced) = &advanced {
            for summary in &advanced.registered {
                super::waiting::announce(&state, summary).await;
            }
        }
        crate::transition::settle(&state, None).await?;
        let advanced = advanced?;
        anyhow::ensure!(
            advanced.failed.is_empty(),
            "could not move these versions forward: {}",
            advanced.failed.join("; ")
        );
        Ok(if ran? || advanced.waiting { DrainStep::RetryIn(look_every()) } else { DrainStep::Done })
    })
}
