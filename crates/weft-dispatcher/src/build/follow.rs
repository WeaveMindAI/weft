//! Seeing a build through: asking the builder how each running build is
//! doing and recording how it ended.
//!
//! Anyone who looks moves a build forward ([`advance`]): the verb waiting
//! on it, and otherwise the dispatcher's build loop ([`drain_loop`]),
//! whichever dispatcher wakes. A build starting wakes the loop
//! ([`BUILD_STARTED_CHANNEL`]), which then looks every [`look_every`]
//! until no build runs, and sleeps after. No process holds a build, so
//! none has to stay up for it to finish.

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

/// The channel a build announces on once it runs on its builder
/// ([`ledger::started`]); the payload is its image.
pub const BUILD_STARTED_CHANNEL: &str = "weft_build_started";

pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(BUILD_STARTED_CHANNEL)];

/// Ask the builder about the build running for `image_ref` and record its
/// end when it ended. Errs only when the ledger cannot be read or
/// written: a builder that does not answer is looked at again, and given
/// up on (the build ends failed) once it has not answered for
/// [`ledger::UNANSWERED_GIVE_UP_SECS`].
pub async fn advance(pool: &PgPool, images: &dyn ImageBuilder, image_ref: &str) -> Result<()> {
    let Some(build) = ledger::running_for(pool, image_ref).await? else { return Ok(()) };
    advance_build(pool, images, &build).await
}

async fn advance_build(pool: &PgPool, images: &dyn ImageBuilder, build: &ledger::RunningBuild) -> Result<()> {
    let now = crate::lease::now_unix();
    let Some(builder_id) = &build.builder_id else {
        // Not made on the builder yet: nothing to ask. A start that never
        // recorded the build's id there went with its starter.
        if now > build.held_until {
            let outcome = ledger::Outcome::Failed(format!(
                "the build {} was never made on the builder: its start did not finish within {} seconds (the \
                 dispatcher starting it went away). Build again",
                build.build_name,
                ledger::START_HOLD_SECS
            ));
            ledger::finish(pool, &build.image_ref, &build.build_name, outcome, now).await?;
        }
        return Ok(());
    };
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

/// The dispatcher's build loop: every running build moved forward, then a
/// look again soon while any still runs. One dispatcher at a time
/// (`crate::reaper`'s install-wide lock), so the builder is asked once per
/// look, not once per replica.
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, WAKE_ON, "image_builds", |state| async move {
        let builds = ledger::running(&state.pg_pool, None).await?;
        // One build's failure never skips the others; the failures are
        // answered together after.
        let mut failed = Vec::new();
        for build in &builds {
            if let Err(e) = advance_build(&state.pg_pool, state.builder.images.as_ref(), build).await {
                failed.push(format!("{}: {e:#}", build.image_ref));
            }
        }
        anyhow::ensure!(failed.is_empty(), "could not move these builds forward: {}", failed.join("; "));
        Ok(if builds.is_empty() { DrainStep::Done } else { DrainStep::RetryIn(look_every()) })
    })
}
