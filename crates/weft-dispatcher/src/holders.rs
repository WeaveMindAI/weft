//! How many holders run (`weft_platform_traits::holder_pool`): counted
//! from the held signals and set on the platform, so the holders start as
//! the first held signal is registered and stop with the last.

use weft_broker_client::protocol::{ACTIVATION_LISTENS, SIGNAL_ACTIVATION_JOIN};
use weft_platform_traits::HolderPool;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// Announced when a held signal comes or goes (the `signal` table's
/// triggers), and when an activation changes status, whether it takes
/// work, or until when (the `trigger_activation` table's): a trigger
/// switched on, wiped, or past its hibernation's grace window changes which
/// signals are held without a write to their rows.
// SYNC: HELD_SIGNALS_CHANNEL <-> crates/weft-dispatcher/src/journal/postgres.rs (signal_held_notify)
pub const HELD_SIGNALS_CHANNEL: &str = "weft_held_signals";

pub(crate) static ON_HELD_SIGNALS: &[WakeOn] = &[WakeOn::any(HELD_SIGNALS_CHANNEL)];

/// How many signals a holder holds now: every held row whose governing
/// activation is one a listener holds, the same rows a holder claims from.
pub async fn held_count(pool: &sqlx::PgPool) -> anyhow::Result<u64> {
    let n: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM signal s {SIGNAL_ACTIVATION_JOIN} \
         WHERE s.holds AND {ACTIVATION_LISTENS}"
    ))
    .fetch_one(pool)
    .await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// Run as many holders as the held signals need, `per_copy` to a holder.
/// Every change to that count is announced, so nothing is looked at again
/// until the next one.
pub async fn size(pool: &sqlx::PgPool, holders: &dyn HolderPool, per_copy: u32) -> anyhow::Result<DrainStep> {
    let copies = weft_platform_traits::holder_pool::copies_for(held_count(pool).await?, per_copy);
    holders.resize(copies).await?;
    Ok(DrainStep::Done)
}

/// The holder sizing as a loop of the dispatcher: woken when a held signal
/// comes or goes or its activation changes ([`HELD_SIGNALS_CHANNEL`]).
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, ON_HELD_SIGNALS, "holders", |state| async move {
        size(&state.pg_pool, state.holder_pool.as_ref(), state.holder_settings.signals_per_copy).await
    })
}
