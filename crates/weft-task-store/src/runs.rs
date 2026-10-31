//! What every role shares about runs being worked on: the ONE rule for
//! "a worker is driving this run right now", and how a cancel reaches the
//! worker that drives a run. A run's row (`run`) is its whole life; it is
//! written by `weft_journal::record`, and the worker leases this rule reads
//! are this crate's (`crate::worker_door`).

/// Until when (unix seconds) a running run's owner holds it, as a SQL
/// expression over a `run` row aliased `$run`: the later of its owner's
/// lease (`worker_lease`) and the grace its claim or birth gave it before
/// that lease is written (`run.delivered_until`,
/// `weft_journal::record::delivered_until`); 0 when it has neither. Only
/// a running run's reads this way: a queued run's `delivered_until` is its
/// delivery's. The run is worked on while this is ahead of the clock
/// ([`in_flight_sql!`]) and lost once it is a margin behind; the
/// dispatcher's lost-run sweep, the look it books at the earliest of
/// these, and a drain's next count all read it from here.
#[macro_export]
macro_rules! run_held_until_sql {
    ($run:literal) => {
        concat!(
            "GREATEST(COALESCE(",
            $run,
            ".delivered_until, 0), COALESCE((SELECT held_lease.leased_until_unix FROM worker_lease held_lease WHERE held_lease.replica = ",
            $run,
            ".owner), 0))"
        )
    };
}

/// THE rule for "this run is being worked on", as a SQL predicate over a
/// `run` row aliased `$run`: it is running and its owner still holds it
/// ([`run_held_until_sql!`] is ahead of the database's clock). A run whose
/// owner let it lapse is lost (the lost-run sweep ends or requeues it); a
/// parked or queued run is not being worked on. Every reader that asks (a
/// drain's count, a cancel, the infra-setup guard) spells it through here,
/// or through [`in_flight`].
#[macro_export]
macro_rules! in_flight_sql {
    ($run:literal) => {
        concat!(
            "(",
            $run,
            ".state = 'running' AND ",
            $crate::run_held_until_sql!($run),
            " >= EXTRACT(EPOCH FROM NOW())::BIGINT)"
        )
    };
}

/// Whether a worker is driving `run_id` right now
/// ([`in_flight_sql!`]).
pub async fn in_flight(db: &impl crate::db::Client, run_id: weft_core::RunId) -> anyhow::Result<bool> {
    Ok(db.scalar(concat!("SELECT EXISTS (SELECT 1 FROM run r WHERE r.run_id = $1 AND ", in_flight_sql!("r"), ")"), &[&run_id]).await?)
}

/// Whether `run_id` was ever queued: its row is there, ended or not.
pub async fn born(db: &impl crate::db::Client, run_id: weft_core::RunId) -> anyhow::Result<bool> {
    Ok(db.scalar::<bool>("SELECT EXISTS (SELECT 1 FROM run WHERE run_id = $1)", &[&run_id]).await?)
}

/// Whether `run_id` has ended (or is gone).
pub async fn ended(db: &impl crate::db::Client, run_id: weft_core::RunId) -> anyhow::Result<bool> {
    Ok(!db.scalar::<bool>("SELECT EXISTS (SELECT 1 FROM run WHERE run_id = $1 AND state <> 'ended')", &[&run_id]).await?)
}

/// The channel a run that is queued for a worker is announced on, with its
/// project's id, in the transaction that queues it: the dispatcher's
/// delivery wakes on it and hands the run to one of the project's workers.
/// Queuing is rare (a run started by hand or for setup, an answer to a
/// parked run, a run handed back or whose worker was lost), never a
/// batch's work.
// SYNC: RUN_QUEUED_CHANNEL <-> weft_journal::record::notify_queued_in
pub const RUN_QUEUED_CHANNEL: &str = "weft_run_queued";

/// The channel a cancel of a running run is announced on, with its run
/// ([`run_payload`]), in the same transaction that sets the run's
/// `cancel_requested`: the broker pushes it down the lines of the project's
/// workers, and the one driving the run fires its flag. A worker that may
/// have missed one (its line came back, it just claimed a run) reads
/// `cancel_requested` of the runs it drives.
// SYNC: CANCEL_CHANNEL <-> weft_broker_client::line::LINE_CHANNELS, crates/weft-broker/src/line.rs (audience)
pub const CANCEL_CHANNEL: &str = "weft_cancel";

/// The channel the dispatchers relay the events that ride no record row
/// on (`weft_core::live_event::LiveEvent`, as JSON): each dispatcher pushes
/// what it hears to whoever follows the event's project. Written by the
/// dispatchers themselves and by the broker as it parks a run.
pub const EVENTS_CHANNEL: &str = "weft_dispatcher_events";

/// The most an event announced on [`EVENTS_CHANNEL`] may weigh, in bytes:
/// Postgres NOTIFY refuses a payload past 8000.
pub const EVENT_MAX_BYTES: usize = 7800;

/// Announce `event` on [`EVENTS_CHANNEL`] through `db`: inside the
/// caller's transaction it is heard only if that commits. `Err` when the
/// event weighs more than [`EVENT_MAX_BYTES`] (nobody would hear it, and no
/// record replays it) or the database refused.
pub async fn announce_event(db: &impl crate::db::Client, event: &weft_core::live_event::LiveEvent) -> anyhow::Result<()> {
    let payload = serde_json::to_string(event)?;
    anyhow::ensure!(
        payload.len() <= EVENT_MAX_BYTES,
        "the event weighs {} bytes, more than the {EVENT_MAX_BYTES} a NOTIFY carries",
        payload.len()
    );
    db.execute("SELECT pg_notify($1, $2)", &[&EVENTS_CHANNEL, &payload]).await?;
    Ok(())
}

/// How an announcement names a run: `<project id> <run id>`.
// SYNC: run_payload <-> parked_fire_notify in crates/weft-task-store/src/parked_fires.rs (GROUP)
pub fn run_payload(project_id: uuid::Uuid, run_id: weft_core::RunId) -> String {
    format!("{project_id} {run_id}")
}

/// The project and run a [`run_payload`] names.
pub fn parse_run_payload(payload: &str) -> Option<(uuid::Uuid, weft_core::RunId)> {
    let (project, run) = payload.split_once(' ')?;
    Some((project.parse().ok()?, run.parse().ok()?))
}

#[cfg(test)]
mod tests {
    /// A run named in an announcement reads back as the project and run.
    #[test]
    fn a_run_payload_reads_back() {
        let (project, run) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        assert_eq!(super::parse_run_payload(&super::run_payload(project, run)), Some((project, run)));
        assert_eq!(super::parse_run_payload("not a run"), None);
        assert_eq!(super::parse_run_payload(&project.to_string()), None, "a project alone names no run");
    }
}
