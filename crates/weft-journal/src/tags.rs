//! Run tags: the selectable copy of `ctx.tag_run`.
//!
//! A tag is recorded as an `RunTagged` event (the record of the act,
//! written by the run's own worker) AND kept in `run_tag`, one row per
//! (run, tag), which the broker writes once the event is on record. An
//! unrecorded run writes its birth instead of the event
//! (`crate::unrecorded`), so it is tagged and stopped through its run row
//! like any run, and its tag rows go with that row.
//! The table exists because `ctx.stop_tagged` has to answer "which live
//! runs of this project carry tag T" on every inbound message, and
//! folding every open run's record to find out would be O(all history).
//! Every piece of SQL that touches the table lives here (write, read-back,
//! live selector, delete) so the dispatcher and the broker, which both act
//! on it, can never disagree on it. The one exception is the DDL itself,
//! which the dispatcher owns in its journal schema group.
//!
//! `seq` is a `BIGSERIAL`: the order tags were written, gap-tolerant,
//! never tied. It is what the last-one-wins rule compares. Two runs of
//! the same user a few milliseconds apart both say "stop the others,
//! keep me"; each only stops runs whose tag `seq` is BELOW its own, so
//! the later one survives. Unix seconds could not do this (a tie inside
//! one second would let both live, or both die).

use weft_core::RunId;
use weft_task_store::db::{self, Client};

/// One live run carrying a tag, as the selector reads it: which
/// run, and the sequence its tag row got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedRun {
    pub run_id: RunId,
    pub seq: i64,
}

/// Insert `run_id`'s tag rows, on the caller's connection.
/// Re-tagging an existing (run, tag) keeps the original row (and its
/// `seq`): a body replayed after a durable wait lands on the same state,
/// and a tag's position in the order is the FIRST time the run claimed it.
pub async fn tag_run_in(db: &impl Client, run_id: RunId, tags: &[String], at_unix: u64) -> db::Result<()> {
    db.execute(
        "INSERT INTO run_tag (run_id, tag, tagged_at_unix) SELECT $1::uuid, t, $3::bigint FROM unnest($2::text[]) AS t \
         ON CONFLICT (run_id, tag) DO NOTHING",
        &[&run_id, &tags, &(at_unix as i64)],
    )
    .await?;
    Ok(())
}

/// The tag column every run-summary read selects: the run's tags
/// in the order it claimed them, `{}` for an untagged run, so
/// `Vec<String>` decodes straight off the row. Spliced into queries
/// that already have the run in scope as `run r`.
pub const TAGS_LATERAL: &str =
    "(SELECT COALESCE(array_agg(tag ORDER BY seq), '{}') FROM run_tag WHERE run_id = r.run_id)";

/// Delete the tag rows of `run_ids`. Runs on the caller's
/// transaction, inside the one-shot delete of the runs' whole footprint:
/// tag rows must never outlive the run they select on, or a half-applied
/// delete leaves exactly the row set `live_tagged_runs` matches for
/// a run that is gone.
pub async fn delete_for_run_ids(tx: &impl Client, run_ids: &[RunId]) -> db::Result<u64> {
    tx.execute("DELETE FROM run_tag WHERE run_id = ANY($1)", &[&run_ids]).await
}

/// The `seq` of `run_id`'s `tag` row, if the run carries that tag.
pub async fn tag_seq(db: &impl Client, run_id: RunId, tag: &str) -> db::Result<Option<i64>> {
    db.scalar_opt("SELECT seq FROM run_tag WHERE run_id = $1 AND tag = $2", &[&run_id, &tag]).await
}

/// The highest `seq` any tag row has, or 0 when the table is empty. The
/// anchor for a `Keep` stop by a run that never carried the tag: every
/// row written so far is below `max + 1`, every row written from now on
/// is not.
pub async fn max_tag_seq(db: &impl Client) -> db::Result<i64> {
    let max: Option<i64> = db.scalar("SELECT MAX(seq) FROM run_tag", &[]).await?;
    Ok(max.unwrap_or(0))
}

/// Every LIVE run of `project_id` carrying `tag`, with its tag
/// row's `seq`: a project run that has not ended (never a node test),
/// recorded or not (an unrecorded run that tagged itself has its row). The
/// ordering and self rules are applied afterwards by
/// [`select_stop_targets`], which is pure, so the SQL stays a plain read
/// and the rule has a layer-1 test.
pub async fn live_tagged_runs(db: &impl Client, project_id: uuid::Uuid, tag: &str) -> db::Result<Vec<TaggedRun>> {
    let rows: Vec<(RunId, i64)> = db
        .all(
            "SELECT et.run_id, et.seq FROM run_tag et JOIN run r ON r.run_id = et.run_id \
             WHERE r.project_id = $1 AND et.tag = $2 AND r.kind = 'project' AND r.state <> 'ended' \
             ORDER BY et.seq ASC",
            &[&project_id, &tag],
        )
        .await?;
    Ok(rows.into_iter().map(|(run_id, seq)| TaggedRun { run_id, seq }).collect())
}

/// THE stop rule, pure. Out of the live runs carrying the tag,
/// which ones does a stop asked by `by` reach?
///
/// - `before_seq: Some(n)`: only runs whose tag row came before `n`.
///   For a `Keep` stop this is the asker's own `seq` (or, if it never
///   carried the tag, one past the newest row at ask time), which is
///   what makes two concurrent "stop the others" calls leave the later
///   one alive instead of killing each other.
/// - `before_seq: None`: no ordering, every live run carrying the tag.
/// - `StopSelf::Keep` never returns `by` itself, whatever the seqs say.
pub fn select_stop_targets(
    candidates: &[TaggedRun],
    by: RunId,
    before_seq: Option<i64>,
    stop_self: weft_core::StopSelf,
) -> Vec<RunId> {
    candidates
        .iter()
        .filter(|c| before_seq.is_none_or(|n| c.seq < n))
        .filter(|c| stop_self == weft_core::StopSelf::Include || c.run_id != by)
        .map(|c| c.run_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::StopSelf;

    fn tagged(seq: i64) -> TaggedRun {
        TaggedRun { run_id: RunId::new_v4(), seq }
    }

    /// Two messages a few milliseconds apart, both "stop the others,
    /// keep me": the first (seq 1) stops nothing newer than itself; the
    /// second (seq 2) stops the first and survives. Neither stops
    /// itself.
    #[test]
    fn concurrent_keep_stops_leave_the_later_run_alive() {
        let first = tagged(1);
        let second = tagged(2);
        let live = vec![first.clone(), second.clone()];
        assert_eq!(
            select_stop_targets(&live, first.run_id, Some(first.seq), StopSelf::Keep),
            Vec::<RunId>::new()
        );
        assert_eq!(
            select_stop_targets(&live, second.run_id, Some(second.seq), StopSelf::Keep),
            vec![first.run_id]
        );
    }

    /// A stop by a run that never carried the tag (anchor = one past
    /// the newest row) reaches everything currently tagged, and only
    /// that.
    #[test]
    fn keep_stop_without_own_tag_reaches_every_current_row() {
        let a = tagged(1);
        let b = tagged(2);
        let by = RunId::new_v4();
        assert_eq!(
            select_stop_targets(&[a.clone(), b.clone()], by, Some(3), StopSelf::Keep),
            vec![a.run_id, b.run_id]
        );
    }

    /// `Include` with no anchor takes the whole batch down, the asker too.
    #[test]
    fn include_stop_takes_the_asker_too() {
        let a = tagged(1);
        let me = tagged(2);
        let c = tagged(3);
        assert_eq!(
            select_stop_targets(&[a.clone(), me.clone(), c.clone()], me.run_id, None, StopSelf::Include),
            vec![a.run_id, me.run_id, c.run_id]
        );
    }

    /// `Keep` never returns the asker even when the seq filter would.
    #[test]
    fn keep_never_returns_the_asker() {
        let me = tagged(5);
        assert_eq!(
            select_stop_targets(std::slice::from_ref(&me), me.run_id, Some(10), StopSelf::Keep),
            Vec::<RunId>::new()
        );
    }
}
