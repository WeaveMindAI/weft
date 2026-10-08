//! Erasing the runs kept long enough.
//!
//! A run is kept for its `keep_for` once it ends (the project's
//! `[runs] keep_for`, or what started it said: a trigger's `keepRunsFor`,
//! `weft run --keep-for`), and its row says until when (`run.keep_until`,
//! set as it ends). This loop erases the runs past it, with everything of
//! theirs, a round of [`ROUND`] at a time oldest first through the
//! `run_expiry` index, pausing between rounds so a backlog never holds the
//! database. A parked or queued run has not ended, so it is never erased
//! however old it is.

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep};

use crate::state::DispatcherState;

/// How many runs one round erases.
const ROUND: i64 = 5_000;

/// The pause between two full rounds.
const BETWEEN_ROUNDS: Duration = Duration::from_secs(1);

/// How often the loop looks when nothing was due: once a minute, at this
/// install's pace (`weft_core::time_scale`).
fn every() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("retention", &[], every(), move || {
        let state = state.clone();
        async move {
            let (erased, removed) = state.journal.erase_expired(crate::lease::now_unix(), ROUND).await?;
            state.listener.unregister_many(&removed).await;
            if erased > 0 {
                tracing::info!(target: "weft_dispatcher::retention", erased, "erased runs kept long enough");
            }
            Ok(if erased as i64 == ROUND { DrainStep::RetryIn(BETWEEN_ROUNDS) } else { DrainStep::Done })
        }
    })
}
