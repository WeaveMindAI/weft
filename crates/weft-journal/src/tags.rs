//! Execution tags: the selectable copy of `ctx.tag_execution`.
//!
//! A tag is journaled as an `ExecutionTagged` event (the record of the
//! act) AND written to `execution_tag`, one row per (color, tag), in the
//! same transaction. The table exists because `ctx.stop_tagged` has to
//! answer "which live executions of this project carry tag T" on every
//! inbound message, and folding every open color's journal to find out
//! would be O(all history). Every piece of SQL that touches the table
//! lives here (write, read-back, live selector, delete) so the
//! dispatcher and the broker, which both act on it, can never disagree
//! on it. The one exception is the DDL itself, which the dispatcher
//! owns in its journal schema group.
//!
//! `seq` is a `BIGSERIAL`: the order tags were written, gap-tolerant,
//! never tied. It is what the last-one-wins rule compares. Two runs of
//! the same user a few milliseconds apart both say "stop the others,
//! keep me"; each only stops runs whose tag `seq` is BELOW its own, so
//! the later one survives. Unix seconds could not do this (a tie inside
//! one second would let both live, or both die).

use weft_core::Color;

use crate::events::ExecEvent;
use crate::write::{record_event_in, RecordError};

/// One live execution carrying a tag, as the selector reads it: which
/// run, and the sequence its tag row got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedExecution {
    pub color: Color,
    pub seq: i64,
}

/// Journal `ExecutionTagged` and insert the tag rows, on the caller's
/// transaction. Re-tagging an existing (color, tag) keeps the original
/// row (and its `seq`): a body re-run after a crash lands on the same
/// state, and a tag's position in the order is the FIRST time the run
/// claimed it. `pod_name` stamps the event for the fencing trigger,
/// exactly like every other worker-originated write.
pub async fn tag_execution_in(
    tx: &mut sqlx::PgConnection,
    color: Color,
    tags: &[String],
    at_unix: u64,
    pod_name: Option<&str>,
) -> Result<(), RecordError> {
    let event = ExecEvent::ExecutionTagged { color, tags: tags.to_vec(), at_unix };
    record_event_in(&mut *tx, &event, pod_name, None).await?;
    for tag in tags {
        sqlx::query(
            "INSERT INTO execution_tag (color, tag, tagged_at_unix) VALUES ($1, $2, $3) \
             ON CONFLICT (color, tag) DO NOTHING",
        )
        .bind(color.to_string())
        .bind(tag)
        .bind(at_unix as i64)
        .execute(&mut *tx)
        .await?;
    }
    Ok(())
}

/// The tag column every execution-summary read selects: the run's tags
/// in the order it claimed them, `{}` for an untagged run, so
/// `Vec<String>` decodes straight off the row. Spliced into queries
/// that already have the color in scope as `execution_color ec`.
pub const TAGS_LATERAL: &str =
    "(SELECT COALESCE(array_agg(tag ORDER BY seq), '{}') FROM execution_tag WHERE color = ec.color)";

/// Delete `color`'s tag rows. Runs on the caller's transaction, inside
/// `delete_execution`'s one-shot clean of the color's whole footprint:
/// tag rows must never outlive the journal they select on, or a
/// half-applied clean leaves exactly the row set `live_tagged_executions`
/// matches for a run whose history is gone.
pub async fn delete_for_color(tx: &mut sqlx::PgConnection, color: Color) -> Result<u64, sqlx::Error> {
    let res = sqlx::query("DELETE FROM execution_tag WHERE color = $1")
        .bind(color.to_string())
        .execute(&mut *tx)
        .await?;
    Ok(res.rows_affected())
}

/// The `seq` of `color`'s `tag` row, if the run carries that tag.
pub async fn tag_seq<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    color: Color,
    tag: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT seq FROM execution_tag WHERE color = $1 AND tag = $2")
            .bind(color.to_string())
            .bind(tag)
            .fetch_optional(executor)
            .await?;
    Ok(row.map(|(s,)| s))
}

/// The highest `seq` any tag row has, or 0 when the table is empty. The
/// anchor for a `Keep` stop by a run that never carried the tag: every
/// row written so far is below `max + 1`, every row written from now on
/// is not.
pub async fn max_tag_seq<'e, E: sqlx::PgExecutor<'e>>(executor: E) -> Result<i64, sqlx::Error> {
    let (max,): (Option<i64>,) = sqlx::query_as("SELECT MAX(seq) FROM execution_tag")
        .fetch_one(executor)
        .await?;
    Ok(max.unwrap_or(0))
}

/// Every LIVE execution of `project_id` carrying `tag`, with its tag
/// row's `seq`. Live means a project execution (not a node test) whose
/// journal holds no terminal event. The ordering and self rules are
/// applied afterwards by [`select_stop_targets`], which is pure, so the
/// SQL stays a plain read and the rule has a layer-1 test.
pub async fn live_tagged_executions<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    project_id: &str,
    tag: &str,
) -> Result<Vec<TaggedExecution>, sqlx::Error> {
    // The NOT EXISTS kind list below is the SQL copy of the terminal set;
    // a fourth terminal kind must land here too or a run that ended in it
    // keeps matching this read as live forever.
    // SYNC: terminal kind list <-> crates/weft-journal/src/events.rs ExecEvent::is_execution_terminal,
    // crates/weft-dispatcher/src/api/execution.rs terminal_outcome (SQL kind list),
    // crates/weft-cli/src/commands/follow.rs is_terminal (SSE kind list)
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT et.color, et.seq \
         FROM execution_tag et \
         JOIN execution_color ec ON ec.color = et.color \
         WHERE ec.project_id = $1 \
           AND et.tag = $2 \
           AND ec.kind = 'execution' \
           AND NOT EXISTS ( \
               SELECT 1 FROM exec_event t \
               WHERE t.color = ec.color \
                 AND t.kind IN ('execution_completed', \
                                'execution_failed', \
                                'execution_cancelled') \
           ) \
         ORDER BY et.seq ASC",
    )
    .bind(project_id)
    .bind(tag)
    .fetch_all(executor)
    .await?;
    rows.into_iter()
        .map(|(color, seq)| {
            let color: Color = color.parse().map_err(|e: uuid::Error| {
                sqlx::Error::Decode(
                    format!("execution_tag row holds a non-uuid color '{color}': {e}").into(),
                )
            })?;
            Ok(TaggedExecution { color, seq })
        })
        .collect()
}

/// THE stop rule, pure. Out of the live executions carrying the tag,
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
    candidates: &[TaggedExecution],
    by: Color,
    before_seq: Option<i64>,
    stop_self: weft_core::StopSelf,
) -> Vec<Color> {
    candidates
        .iter()
        .filter(|c| before_seq.is_none_or(|n| c.seq < n))
        .filter(|c| stop_self == weft_core::StopSelf::Include || c.color != by)
        .map(|c| c.color)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::StopSelf;

    fn tagged(seq: i64) -> TaggedExecution {
        TaggedExecution { color: Color::new_v4(), seq }
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
            select_stop_targets(&live, first.color, Some(first.seq), StopSelf::Keep),
            Vec::<Color>::new()
        );
        assert_eq!(
            select_stop_targets(&live, second.color, Some(second.seq), StopSelf::Keep),
            vec![first.color]
        );
    }

    /// A stop by a run that never carried the tag (anchor = one past
    /// the newest row) reaches everything currently tagged, and only
    /// that.
    #[test]
    fn keep_stop_without_own_tag_reaches_every_current_row() {
        let a = tagged(1);
        let b = tagged(2);
        let by = Color::new_v4();
        assert_eq!(
            select_stop_targets(&[a.clone(), b.clone()], by, Some(3), StopSelf::Keep),
            vec![a.color, b.color]
        );
    }

    /// `Include` with no anchor takes the whole batch down, the asker too.
    #[test]
    fn include_stop_takes_the_asker_too() {
        let a = tagged(1);
        let me = tagged(2);
        let c = tagged(3);
        assert_eq!(
            select_stop_targets(&[a.clone(), me.clone(), c.clone()], me.color, None, StopSelf::Include),
            vec![a.color, me.color, c.color]
        );
    }

    /// `Keep` never returns the asker even when the seq filter would.
    #[test]
    fn keep_never_returns_the_asker() {
        let me = tagged(5);
        assert_eq!(
            select_stop_targets(std::slice::from_ref(&me), me.color, Some(10), StopSelf::Keep),
            Vec::<Color>::new()
        );
    }
}
