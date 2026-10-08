//! What every role shares about runs being worked on: the ONE rule for
//! "a worker is driving this run right now", and how a cancel reaches the
//! worker that drives a run. A run's row (`run`) is its whole life; it is
//! written by `weft_journal::record`, and the worker leases this rule reads
//! are this crate's (`crate::worker_door`).

/// THE rule for "this run is being worked on", as a SQL predicate over a
/// `run` row aliased `$run`: it is running and its owner's lease
/// (`worker_lease`) is alive. A run whose owner's lease lapsed is lost
/// (the lost-run sweep ends or requeues it); a parked or queued run is
/// not being worked on. Every reader that asks (a drain's count, a
/// cancel, the infra-setup guard) spells it through here, or through
/// [`in_flight`].
#[macro_export]
macro_rules! in_flight_sql {
    ($run:literal) => {
        concat!(
            "(",
            $run,
            ".state = 'running' AND EXISTS (SELECT 1 FROM worker_lease in_flight_lease WHERE in_flight_lease.replica = ",
            $run,
            ".owner AND ",
            $crate::worker_alive!("in_flight_lease"),
            "))"
        )
    };
}

/// Whether a worker is driving `execution_id` right now
/// ([`in_flight_sql!`]).
pub async fn in_flight<'e, E: sqlx::PgExecutor<'e>>(executor: E, execution_id: weft_core::ExecutionId) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar(concat!("SELECT EXISTS (SELECT 1 FROM run r WHERE r.execution_id = $1 AND ", in_flight_sql!("r"), ")"))
        .bind(execution_id)
        .fetch_one(executor)
        .await?)
}

/// The channel a run that is queued for a worker is announced on, with its
/// project's id, in the transaction that queues it: the dispatcher's
/// delivery wakes on it and hands the run to one of the project's workers.
/// Queuing is rare (a run started by hand or for setup, an answer to a
/// parked run, a run handed back or whose worker was lost), never a
/// batch's work.
// SYNC: RUN_QUEUED_CHANNEL <-> weft_journal::record::notify_queued_in
pub const RUN_QUEUED_CHANNEL: &str = "weft_run_queued";

/// The channel a cancel of a running run is announced on, with
/// `<project id> <execution id>` ([`cancel_payload`]), in the same
/// transaction that sets the run's `cancel_requested`: the broker pushes it
/// down the lines of the project's workers, and the one driving the run
/// fires its flag. A worker that may have missed one (its line came back,
/// it just claimed a run) reads `cancel_requested` of the runs it drives.
// SYNC: CANCEL_CHANNEL <-> weft_broker_client::line::LINE_CHANNELS, crates/weft-broker/src/line.rs (audience)
pub const CANCEL_CHANNEL: &str = "weft_cancel";

/// A [`CANCEL_CHANNEL`] payload.
pub fn cancel_payload(project_id: uuid::Uuid, execution_id: weft_core::ExecutionId) -> String {
    format!("{project_id} {execution_id}")
}

/// The project and execution a [`CANCEL_CHANNEL`] payload names.
pub fn parse_cancel_payload(payload: &str) -> Option<(uuid::Uuid, weft_core::ExecutionId)> {
    let (project, execution) = payload.split_once(' ')?;
    Some((project.parse().ok()?, execution.parse().ok()?))
}

#[cfg(test)]
mod tests {
    /// A cancel's payload reads back as the project and run it named.
    #[test]
    fn a_cancel_payload_reads_back() {
        let (project, run) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        assert_eq!(super::parse_cancel_payload(&super::cancel_payload(project, run)), Some((project, run)));
        assert_eq!(super::parse_cancel_payload("not a cancel"), None);
    }
}
