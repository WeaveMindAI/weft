//! How many holders run (`weft_platform_traits::holder_pool`): counted
//! from the held signals and moved above that count by the load the
//! holders say they carry, then set on the platform, so the holders start
//! as the first held signal is registered and stop with the last. The
//! holders themselves look only when told (`weft_platform_traits::holders`):
//! every change of the held set is told once the pool is sized for it, and
//! every connection made again or removed as it is announced
//! ([`ACCESS_REMADE_CHANNEL`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use weft_broker_client::protocol::{ACTIVATION_LISTENS, SIGNAL_ACTIVATION_JOIN};
use weft_platform_traits::holder_pool::{copies_wanted, Loads};
use weft_platform_traits::config::HolderSettings;
use weft_platform_traits::{HolderNews, HolderNote, HolderPool};
use weft_task_store::db::{Client as _, Pool};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// Announced when the held set changes: a held signal comes, goes, gives
/// up or loses its holder (the `signal` table's triggers), and when an
/// activation changes status, whether it takes work, or until when (the
/// `trigger_activation` table's): a trigger switched on, taken down with
/// its door refusing, or past its door's window changes which signals are
/// held without a write to their rows. Also when a holder says a new
/// load or room, or goes (the `holder_load` table's, [`GROUP`]).
// SYNC: HELD_SIGNALS_CHANNEL <-> crates/weft-dispatcher/src/store/postgres.rs (signal_held_notify)
pub const HELD_SIGNALS_CHANNEL: &str = "weft_held_signals";

pub(crate) static ON_HELD_SIGNALS: &[WakeOn] = &[WakeOn::any(HELD_SIGNALS_CHANNEL)];

/// The loop that tells the holders a connection's access changed.
pub(crate) const HOLDER_ACCESS: &str = "holder_access";

/// Announced when a connection is made again (a reconnect or a rotate
/// bumps its `reconnects`) or removed, with its id: what a listener
/// keeps of its credentials is then stale. A token refresh is not
/// announced here: what a listener keeps carries the moment the broker
/// said it stops being good, and is fetched again before it.
/// [`crate::held::ACCESS_CHANNEL`] announces every write to a connection,
/// a refresh included, for the copies that keep its row.
// SYNC: ACCESS_REMADE_CHANNEL <-> crates/weft-dispatcher/src/store/postgres.rs (signal_access_stale)
pub const ACCESS_REMADE_CHANNEL: &str = "weft_access_remade";

pub(crate) static ON_ACCESS: &[WakeOn] = &[WakeOn::any(ACCESS_REMADE_CHANNEL)];

/// SQL: the signals a holder holds, as `s`, from `FROM` on: every held
/// row whose governing activation is one a listener holds and that no
/// holder gave up on, the same rows a holder claims from.
fn held_rows_sql() -> String {
    format!("FROM signal s {SIGNAL_ACTIVATION_JOIN} WHERE s.holds AND s.held_failed IS NULL AND {ACTIVATION_LISTENS}")
}

/// The `holder_pool` table: how many holders weft last set the platform's
/// pool to run, so a sizing that wants the same count asks the platform
/// nothing (on Google Cloud, not even to read the pool). In Postgres, since
/// any dispatcher replica may size the pool next; the sizing runs under
/// its loop's install-wide lock, so the count written is the one set.
///
/// What it says is trusted only once this process has set the pool itself
/// ([`size`]'s `set_here`): the pool can change behind weft's back (an
/// apply that makes it afresh at none, a count changed by hand, a database
/// kept across a reinstall), and the first sizing of every process puts
/// it right. A process starts whenever something wakes the dispatcher, so
/// that costs one call to the platform per start, never a timer.
///
/// The `holder_load` table: what each holder in the pool last said of its
/// load (`weft_broker::held_signals::report_load`), one row per holder
/// that said something, gone with the holder's claims
/// (`weft_broker::held_signals::release`). It is written only when a
/// holder's load moves a band, so a pool whose load holds still writes
/// nothing; each write that changes what the sizing reads announces
/// itself, and so does a holder going. A take marks the taker's row as
/// behind what it holds without announcing it: that only keeps the next
/// sizing from taking a copy away, and the holder's next report wakes it.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "holder_pool",
    tables: &["holder_pool", "holder_load"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS holder_pool (
            -- At most one row, written by the first sizing that went
            -- through: before it nobody knows the pool's count, so that
            -- sizing asks the platform.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            -- The count the platform's pool was last set to.
            copies BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE IF NOT EXISTS holder_load (
            -- The holder, by the name its claims are taken in.
            replica TEXT PRIMARY KEY,
            -- The CPU it said it uses, in percent of the CPUs it is
            -- given, averaged over a minute.
            cpu_percent INTEGER NOT NULL CHECK (cpu_percent >= 0),
            -- Whether that was measured on what it holds now: false once
            -- it took signals since, or handed some over in saying it,
            -- until it says its load again.
            settled BOOLEAN NOT NULL,
            -- The most signals it takes now: under the pool's per-copy
            -- room while it keeps less after handing signals over.
            room BIGINT NOT NULL
        )"#,
        // The function is the run group's (`crate::store::postgres::GROUP`),
        // which applies first. A row marked behind wakes nobody: that only
        // holds the count where it is until the holder says its load.
        r#"DROP TRIGGER IF EXISTS holder_load_on_insert ON holder_load"#,
        r#"CREATE TRIGGER holder_load_on_insert
            AFTER INSERT ON holder_load
            FOR EACH ROW
            EXECUTE FUNCTION signal_held_notify()"#,
        r#"DROP TRIGGER IF EXISTS holder_load_on_change ON holder_load"#,
        r#"CREATE TRIGGER holder_load_on_change
            AFTER UPDATE OF cpu_percent, settled, room ON holder_load
            FOR EACH ROW
            WHEN (NEW.cpu_percent IS DISTINCT FROM OLD.cpu_percent OR NEW.room IS DISTINCT FROM OLD.room OR (NEW.settled AND NOT OLD.settled))
            EXECUTE FUNCTION signal_held_notify()"#,
        r#"DROP TRIGGER IF EXISTS holder_load_on_delete ON holder_load"#,
        r#"CREATE TRIGGER holder_load_on_delete
            AFTER DELETE ON holder_load
            FOR EACH ROW
            EXECUTE FUNCTION signal_held_notify()"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// The count to set the pool to, when `wanted` copies are needed and the
/// pool was last set to `last_set` (`None`: never, as far as this
/// database knows), `set_here` saying whether this process set it since
/// it started: `None` when it is known to run that many already.
fn resize_to(wanted: u32, last_set: Option<u32>, set_here: bool) -> Option<u32> {
    (!set_here || last_set != Some(wanted)).then_some(wanted)
}

/// Run as many holders as the held signals and the holders' load need
/// (`weft_platform_traits::holder_pool::copies_wanted`), as `settings`
/// share them, then tell the holders the held set changed, so each looks
/// once. Every change is announced, so nothing is looked at again until
/// the next one. The pool is set only when the count it needs differs from
/// the one last set ([`GROUP`]), or when this process has not set it yet
/// (`set_here`, which a sizing that goes through sets): the held set
/// changes far more often than the count, and every call to the platform
/// is one that can fail. A resize leaves every load said as it is: each
/// stays true until its holder's load moves a band, or the holder takes
/// signals, which marks it behind until it says it again. A tell that
/// fails fails the pass: a holder that missed it would not take a new
/// signal.
pub async fn size(
    pool: &Pool,
    holders: &dyn HolderPool,
    news: &dyn HolderNews,
    settings: &HolderSettings,
    set_here: &AtomicBool,
) -> anyhow::Result<DrainStep> {
    // The held signals (how many, how many no holder claims), the count
    // last set, the room the holders gave up, and of the holders that
    // claim signals, the load they said and how many have not said it on
    // what they hold, in one round trip.
    let (held, unclaimed, last_set, short, cpu_percent, unsettled): (i64, i64, Option<i64>, i64, i64, i64) = pool
        .one(
            &format!(
                "WITH held AS (SELECT s.held_by {}), \
                      claimants AS (SELECT DISTINCT held_by AS replica FROM held WHERE held_by IS NOT NULL) \
                 SELECT (SELECT count(*) FROM held), \
                        (SELECT count(*) FROM held WHERE held_by IS NULL), \
                        (SELECT copies FROM holder_pool), \
                        (SELECT coalesce(sum(greatest($1 - room, 0)), 0)::bigint FROM holder_load), \
                        (SELECT coalesce(sum(l.cpu_percent), 0)::bigint FROM claimants c JOIN holder_load l USING (replica)), \
                        (SELECT count(*) FROM claimants c LEFT JOIN holder_load l USING (replica) WHERE l.settled IS NOT TRUE)",
                held_rows_sql()
            ),
            &[&i64::from(settings.signals_per_copy)],
        )
        .await?;
    // Counts and sums of columns the schema keeps at zero or more: one out
    // of range is a corrupt row, and sizing the pool on it would hide that.
    fn counted<T: TryFrom<i64>>(what: &str, n: i64) -> anyhow::Result<T> {
        T::try_from(n).map_err(|_| anyhow::anyhow!("the holder sizing read {what} = {n}, which no holder row can make"))
    }
    let loads = Loads {
        cpu_percent: counted("the load said", cpu_percent)?,
        short: counted("the room given up", short)?,
        unclaimed: counted("the unclaimed signals", unclaimed)?,
        unsettled: counted("the holders behind on their load", unsettled)?,
    };
    let last_set = last_set.map(|n| counted("holder_pool.copies", n)).transpose()?;
    // The install config refuses both at 0 (`InstallConfig::validate`).
    let target = std::num::NonZeroU8::new(settings.target_cpu_percent)
        .ok_or_else(|| anyhow::anyhow!("holders.targetCpuPercent is 0, which the install config refuses"))?;
    let per_copy = std::num::NonZeroU32::new(settings.signals_per_copy)
        .ok_or_else(|| anyhow::anyhow!("holders.signalsPerCopy is 0, which the install config refuses"))?;
    let wanted = copies_wanted(counted("the held signals", held)?, per_copy, target, last_set, loads);
    if let Some(copies) = resize_to(wanted, last_set, set_here.load(Ordering::Acquire)) {
        holders.resize(copies).await?;
        pool.execute(
            "INSERT INTO holder_pool (copies) VALUES ($1) ON CONFLICT (one) DO UPDATE SET copies = EXCLUDED.copies",
            &[&i64::from(copies)],
        )
        .await?;
        set_here.store(true, Ordering::Release);
    }
    news.tell(HolderNote::HeldSetChanged).await?;
    Ok(DrainStep::Done)
}

/// The holder sizing as a loop of the dispatcher: woken when the held set
/// changes or a holder says a new load ([`HELD_SIGNALS_CHANNEL`]).
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    let set_here = Arc::new(AtomicBool::new(false));
    crate::reaper::woken(state, ON_HELD_SIGNALS, "holders", "size the pool of holders", move |state| {
        let set_here = set_here.clone();
        async move { size(&state.pg_pool, state.holder_pool.as_ref(), state.holder_news.as_ref(), &state.holder_settings, &set_here).await }
    })
}

/// Tell the holders a connection was made again or removed, and register
/// again the signals no holder holds whose connection it was, so none
/// keeps a stale credential: woken by each one announced
/// ([`ACCESS_REMADE_CHANNEL`]).
pub fn access_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, ON_ACCESS, HOLDER_ACCESS, "tell the listeners a connection was made again", |state| async move {
        state.holder_news.tell(HolderNote::AccessChanged).await?;
        reregister_stale_access(&state).await?;
        Ok(DrainStep::Done)
    })
}

/// Register again every signal no holder holds whose connection changed
/// or went (`signal.access_stale`, marked by the change itself, so only
/// the signals acting as that connection). A listener copy keeps a
/// connection's credentials by registration (`weft_listener::reach`) and
/// hears no news, so the new registration is what makes it take them
/// afresh. It comes up through the listener's `/start` naming the row it
/// replaced, whose wakes are cancelled first. A listener that answers
/// that it could not bring one up owns it from then on (it retries, then
/// gives up on the node); one that cannot be reached fails the pass, and
/// the mark stays for the next.
pub async fn reregister_stale_access(state: &DispatcherState) -> anyhow::Result<()> {
    use crate::store::postgres::{access_taken_afresh, reregister_for_access, signals_with_stale_access};
    for stale in signals_with_stale_access(&state.pg_pool).await? {
        let Some(fresh) = reregister_for_access(&state.pg_pool, &stale).await? else { continue };
        let started = state.listener.start(&fresh.token, weft_core::signal::listener_protocol::StartMode::Restore, Some(&stale)).await;
        match started {
            Ok(()) => {}
            Err(e) if e.downcast_ref::<crate::listener::ListenerRefused>().is_some() => tracing::warn!(
                target: "weft_dispatcher::holders", token = %fresh.token, error = %format!("{e:#}"),
                "a signal registered again for its changed connection did not come up; the listener retries it"
            ),
            Err(e) => return Err(e.context(format!("bring up signal {} again for its changed connection", fresh.token))),
        }
        access_taken_afresh(&state.pg_pool, &fresh.token, fresh.generation).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pool_is_set_only_to_a_count_it_was_not_last_set_to() {
        assert_eq!(resize_to(0, None, true), Some(0), "never set: the platform is asked, even for none");
        assert_eq!(resize_to(2, Some(2), true), None, "the same count: nothing to ask");
        assert_eq!(resize_to(0, Some(0), true), None);
        assert_eq!(resize_to(3, Some(2), true), Some(3));
        assert_eq!(resize_to(0, Some(1), true), Some(0));
    }

    /// The pool may have changed behind weft's back since the count was
    /// written, so a process that has not set it yet sets it even to the
    /// same count.
    #[test]
    fn a_process_that_has_not_set_the_pool_sets_it_whatever_was_last_set() {
        assert_eq!(resize_to(2, Some(2), false), Some(2));
        assert_eq!(resize_to(0, None, false), Some(0));
    }
}
