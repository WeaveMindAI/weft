//! A run's record: its events (`ExecEvent`), how they are stored
//! (`stored`), carried (`frame`) and written and read (`record`), and how
//! folding them over the program rebuilds the run (`fold`). Shared by the
//! dispatcher, the engine and the broker, which all need the same events
//! and the same writes. The listener never touches it: an event it holds
//! reaches a run through the worker's door or a `fire_signal` task.

pub mod birth;
pub mod events;
pub mod fold;
pub mod frame;
pub mod record;
pub mod search;
pub mod seed;
pub mod stored;
pub mod tags;
pub mod traits;
pub mod unrecorded;

pub use events::{redacted, ExecEvent, Seed};
pub use fold::{fold_to_snapshot, EndedWait, FiringView, Fold, FoldEffects};
pub use seed::{fold_seeded, seed_chain, LiveFold, SeedChain};
pub use traits::{BatchError, JournalClient, JournalRow, NoopJournal, Position, RecordClient};

/// The channel a run's new rows are announced on (by the broker after a
/// batch of records commits, at most every few milliseconds; by the
/// dispatcher after it writes a run nobody drives): the live view reads
/// them for the projects somebody watches. A wake-up only. Each payload is
/// one project and runs of it that got rows ([`run_log_payloads`]), so a
/// dispatcher where nobody watches that project reads one id and stops.
// SYNC: RUN_LOG_CHANNEL <-> crates/weft-broker/src/notices.rs, crates/weft-journal/src/record.rs (append_locked_in), crates/weft-dispatcher/src/live_view.rs
pub const RUN_LOG_CHANNEL: &str = "weft_run_log";

/// The channel runs that ended with somebody to tell (watched, or holding
/// signals to take down) are announced on: the dispatcher handles them at
/// once. A wake-up only: the dispatcher also rescans for such runs
/// (`run_end_unhandled`), so a lost one only waits. The payload is the run
/// ids ([`run_ended_payloads`]).
// SYNC: RUN_ENDED_CHANNEL <-> crates/weft-broker/src/notices.rs, crates/weft-journal/src/record.rs (append_locked_in), crates/weft-dispatcher/src/run_ends.rs
pub const RUN_ENDED_CHANNEL: &str = "weft_run_ended";

/// Postgres refuses a notification payload of 8,000 bytes or more.
const PAYLOAD_MAX: usize = 7_900;

/// `runs` of `project` as [`RUN_LOG_CHANNEL`] payloads, each under the
/// limit Postgres sets and each starting with the project.
pub fn run_log_payloads<'a>(project: &uuid::Uuid, runs: impl IntoIterator<Item = &'a uuid::Uuid>) -> Vec<String> {
    payloads(Some(project), runs)
}

/// What a [`RUN_LOG_CHANNEL`] payload names: its project and its runs.
/// `None` when it names no project; a run id that does not parse is left
/// out.
pub fn run_log_of_payload(payload: &str) -> Option<(uuid::Uuid, Vec<uuid::Uuid>)> {
    let mut ids = payload.split(' ');
    let project = ids.next()?.parse().ok()?;
    Some((project, ids.filter_map(|id| id.parse().ok()).collect()))
}

/// `runs` as [`RUN_ENDED_CHANNEL`] payloads, each under the limit Postgres
/// sets.
pub fn run_ended_payloads<'a>(runs: impl IntoIterator<Item = &'a uuid::Uuid>) -> Vec<String> {
    payloads(None, runs)
}

/// The runs a [`RUN_ENDED_CHANNEL`] payload names; one that does not
/// parse is left out.
pub fn run_ended_of_payload(payload: &str) -> Vec<uuid::Uuid> {
    payload.split(' ').filter_map(|id| id.parse().ok()).collect()
}

/// `ids` space-separated after `head`, cut into payloads under
/// [`PAYLOAD_MAX`] bytes that each start with `head`.
fn payloads<'a>(head: Option<&uuid::Uuid>, ids: impl IntoIterator<Item = &'a uuid::Uuid>) -> Vec<String> {
    // A uuid and its space.
    const ONE: usize = 37;
    let start = || head.map(ToString::to_string).unwrap_or_default();
    let mut out = Vec::new();
    let mut current = start();
    let mut held = 0usize;
    for id in ids {
        if current.len() + ONE > PAYLOAD_MAX {
            out.push(std::mem::replace(&mut current, start()));
            held = 0;
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(&id.to_string());
        held += 1;
    }
    if held > 0 {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Any number of runs goes out in payloads under the limit, every run
    /// once, in order, each payload naming the project first.
    #[test]
    fn payloads_split_under_the_limit_and_read_back() {
        let project = uuid::Uuid::from_u128(u128::MAX);
        let runs: Vec<uuid::Uuid> = (0..1000u128).map(uuid::Uuid::from_u128).collect();
        let logged = run_log_payloads(&project, &runs);
        assert!(logged.len() > 1);
        assert!(logged.iter().all(|payload| payload.len() < 8_000));
        let mut back = Vec::new();
        for payload in &logged {
            let (named, of) = run_log_of_payload(payload).expect("names its project");
            assert_eq!(named, project);
            back.extend(of);
        }
        assert_eq!(back, runs);
        let ended = run_ended_payloads(&runs);
        assert!(ended.iter().all(|payload| payload.len() < 8_000));
        assert_eq!(ended.iter().flat_map(|payload| run_ended_of_payload(payload)).collect::<Vec<_>>(), runs);
        assert!(run_log_payloads(&project, &[]).is_empty());
        assert!(run_ended_payloads(&[]).is_empty());
    }
}
