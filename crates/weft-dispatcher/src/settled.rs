//! Reading a table's new rows without ever passing one that is still
//! being written.
//!
//! A `BIGSERIAL` id is drawn when a row is inserted, but the row only
//! becomes visible when its transaction commits, and transactions commit
//! out of id order. A reader that remembers "everything up to id 60 is
//! done" can see row 60 while row 50 is still in flight, and would never
//! look at 50 again.
//!
//! So a table read this way stamps each row with the transaction that
//! wrote it (`writer_xid`, a 64-bit `xid8` that never wraps, filled by
//! the column's `DEFAULT pg_current_xact_id()`), and the reader goes in
//! `(writer_xid, id)` order instead of id order. It passes a row only
//! when its writer is below the horizon, the oldest transaction still
//! running (`pg_snapshot_xmin(pg_current_snapshot())`). Every transaction
//! below the horizon has ended, so each of its rows is either visible or
//! rolled back for good, and a transaction that has not written yet will
//! be handed an xid at or above the horizon. Nothing can ever appear
//! behind the cursor, so a row is never skipped, whatever order ids are
//! drawn and transactions commit in.
//!
//! A long transaction still holds the horizon, and every row written
//! after it began waits until it ends. The reader backs off while it
//! does and, once the backoff reaches the safety tick, logs which
//! transaction is in the way.

// Every table read this way, and its reader:
// SYNC: settled-read tables <-> exec_event (weft_dispatcher::journal::postgres::GROUP),
//       read by crate::journal_bridge; infra_event (crate::infra_event::GROUP),
//       read by crate::infra_event_bridge. Each has `writer_xid` and an index on
//       (writer_xid, id).
//
// exec_event is also read in order PER COLOR: the bridge must apply one
// color's rows in write order, or a terminal lands before the event it
// closes and the run reopens. `(writer_xid, id)` order gives that only
// if a transaction holds the color's lock before it gets its xid (at its
// first write), which is the invariant on `weft_journal::write`. Every
// transaction that writes something else before its exec_event rows
// takes `weft_journal::lock_colors` first:
// SYNC: color lock before first write <-> crate::journal::postgres
//       (record_with_seed, start_execution, start_live_execution,
//       cancel_execution). Transactions whose first write is the
//       exec_event insert are covered by the lock that insert takes:
//       weft_journal::tags::tag_execution_in (broker execution_tag) and
//       every single-statement record_event_* call.

use std::time::{Duration, Instant};

use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::pg_wake::{self, DrainStep};

/// How soon to look again the first time a row is held back: its writer
/// is usually a short transaction, nothing announces its end, and waiting
/// for the next wake could take a whole safety tick.
pub const HELD_BACK_RETRY: Duration = Duration::from_millis(100);

/// The longest a held-back cursor waits between looks: the drain loop's
/// own safety tick, which would look anyway.
const HELD_BACK_LONGEST_RETRY: Duration = pg_wake::SAFETY_POLL_INTERVAL;

/// Where a cursor stands: the last row it passed, as `(writer_xid, id)`.
/// The xid is an `xid8`, carried as a bigint (`::text::bigint`), which
/// holds it until the 64-bit counter passes 2^63.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Position {
    pub xid: i64,
    pub id: i64,
}

impl Position {
    /// The position of a row a settled read returned.
    pub fn of(row: &PgRow) -> anyhow::Result<Self> {
        Ok(Self { xid: row.try_get("writer_xid")?, id: row.try_get("id")? })
    }
}

/// One cursor's settled reads, and what it remembers between them: the
/// horizon it is held back on, to back off while that transaction stays
/// open and to say so in the log. RAM only; a pod that starts fresh just
/// starts at the shortest retry.
pub struct SettledReader {
    /// The tracing target the stall warning is logged under.
    target: &'static str,
    held: Option<Held>,
}

struct Held {
    horizon_xid: i64,
    retry: Duration,
    since: Instant,
}

/// What one settled read found.
pub struct SettledBatch {
    /// The settled rows after the cursor, in `(writer_xid, id)` order,
    /// each carrying `id` and `writer_xid` (read its place with
    /// [`Position::of`]).
    pub rows: Vec<PgRow>,
    /// What the drain loop does next: look again at once (the read hit
    /// its limit), after a backoff (a committed row waits on the
    /// horizon), or on the next wake.
    pub next: DrainStep,
}

impl SettledReader {
    pub const fn new(target: &'static str) -> Self {
        Self { target, held: None }
    }

    /// The settled rows of `table` after `after`, at most `limit`, each
    /// carrying `id`, `writer_xid` and `columns`.
    pub async fn read(
        &mut self,
        conn: &mut sqlx::PgConnection,
        table: &str,
        columns: &str,
        after: Position,
        limit: i64,
    ) -> anyhow::Result<SettledBatch> {
        // The horizon is read ONCE and both statements below are cut at
        // that value. Each statement takes its own snapshot, so letting
        // the check read the horizon again would compare against a later
        // one: a row held back by a transaction that has since ended
        // (and that wrote nothing here, so sent no wake) would sit below
        // the new horizon, above the cursor, and be reported as nothing
        // waiting.
        let horizon_xid: i64 =
            sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
                .fetch_one(&mut *conn)
                .await?;
        let rows = sqlx::query(&format!(
            "SELECT id, writer_xid::text::bigint AS writer_xid, {columns} \
             FROM {table} \
             WHERE writer_xid < $4::text::xid8 \
               AND (writer_xid, id) > ($1::text::xid8, $2) \
             ORDER BY writer_xid, id LIMIT $3"
        ))
        .bind(after.xid.to_string())
        .bind(after.id)
        .bind(limit)
        .bind(horizon_xid.to_string())
        .fetch_all(&mut *conn)
        .await?;
        if rows.len() as i64 >= limit {
            self.held = None;
            return Ok(SettledBatch { rows, next: DrainStep::More });
        }
        // Fewer than asked: is a committed row waiting on the horizon? A
        // row still in flight is not, its commit wakes the loop. Every
        // row at or above the horizon is past the cursor, since the
        // cursor only ever passed rows below it.
        let waiting: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {table} WHERE writer_xid >= $1::text::xid8)"
        ))
        .bind(horizon_xid.to_string())
        .fetch_one(&mut *conn)
        .await?;
        let next = if waiting {
            DrainStep::RetryIn(self.held_back(horizon_xid, !rows.is_empty(), after))
        } else {
            self.held = None;
            DrainStep::Done
        };
        Ok(SettledBatch { rows, next })
    }

    /// How long to wait after a read held back on `horizon_xid`: the
    /// shortest retry when the cursor moved or the blocker is new,
    /// otherwise twice the last wait, up to the safety tick. Once it
    /// reaches that, every look logs which transaction is in the way.
    fn held_back(&mut self, horizon_xid: i64, advanced: bool, after: Position) -> Duration {
        let held = match &mut self.held {
            Some(held) if held.horizon_xid == horizon_xid && !advanced => {
                held.retry = (held.retry * 2).min(HELD_BACK_LONGEST_RETRY);
                held
            }
            _ => self.held.insert(Held { horizon_xid, retry: HELD_BACK_RETRY, since: Instant::now() }),
        };
        if held.retry == HELD_BACK_LONGEST_RETRY {
            tracing::warn!(
                target: "weft_dispatcher::settled",
                cursor = self.target,
                horizon_xid,
                backend_xid = horizon_xid as u32,
                held_secs = held.since.elapsed().as_secs(),
                after_xid = after.xid,
                after_id = after.id,
                "cursor held back by the horizon, the oldest open Postgres transaction: rows \
                 written after it began are not passed until it commits or aborts. \
                 `SELECT pid, xact_start, state, query FROM pg_stat_activity WHERE backend_xid = \
                 '<backend_xid>'` names it."
            );
        }
        held.retry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P0: Position = Position { xid: 0, id: 0 };
    const P5: Position = Position { xid: 3, id: 5 };

    #[test]
    fn a_blocker_backs_off_to_the_safety_tick_and_resets_when_the_cursor_moves() {
        let mut reader = SettledReader::new("test");
        assert_eq!(reader.held_back(7, false, P0), HELD_BACK_RETRY);
        assert_eq!(reader.held_back(7, false, P0), HELD_BACK_RETRY * 2);
        assert_eq!(reader.held_back(7, false, P0), HELD_BACK_RETRY * 4);
        for _ in 0..20 {
            reader.held_back(7, false, P0);
        }
        assert_eq!(reader.held_back(7, false, P0), HELD_BACK_LONGEST_RETRY);
        assert_eq!(reader.held_back(7, true, P5), HELD_BACK_RETRY, "the cursor moved");
        reader.held_back(7, false, P5);
        assert_eq!(reader.held_back(8, false, P5), HELD_BACK_RETRY, "a new blocker");
    }
}

#[cfg(all(test, feature = "db-tests"))]
mod db_tests {
    use super::*;
    use sqlx::PgPool;

    async fn table(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE settled_probe (id BIGSERIAL PRIMARY KEY, v TEXT NOT NULL, \
             writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id())",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert(executor: impl sqlx::PgExecutor<'_>, v: &str) {
        sqlx::query("INSERT INTO settled_probe (v) VALUES ($1)").bind(v).execute(executor).await.unwrap();
    }

    fn values(batch: &SettledBatch) -> Vec<String> {
        batch.rows.iter().map(|r| r.get::<String, _>("v")).collect()
    }

    /// Read until the batch holds `want`. The horizon is the whole
    /// server's, so another test's open transaction can hold a row back
    /// for a moment; only this test's own writers may hold it for good.
    async fn read_until(pool: &PgPool, after: Position, limit: i64, want: &[&str]) -> SettledBatch {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut conn = pool.acquire().await.unwrap();
        loop {
            let batch =
                SettledReader::new("test").read(&mut conn, "settled_probe", "v", after, limit).await.unwrap();
            if values(&batch) == want || Instant::now() > deadline {
                return batch;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn last(batch: &SettledBatch) -> Position {
        Position::of(batch.rows.last().unwrap()).unwrap()
    }

    #[sqlx::test]
    async fn the_read_stops_at_the_horizon(pool: PgPool) {
        table(&pool).await;
        insert(&pool, "a").await;
        let mut open = pool.begin().await.unwrap();
        insert(&mut *open, "b").await;
        insert(&pool, "c").await;

        let batch = read_until(&pool, Position::default(), 100, &["a"]).await;
        assert_eq!(values(&batch), ["a"], "b is in flight, so c waits behind it");
        assert_eq!(batch.next, DrainStep::RetryIn(HELD_BACK_RETRY));

        open.commit().await.unwrap();
        let batch = read_until(&pool, last(&batch), 100, &["b", "c"]).await;
        assert_eq!(values(&batch), ["b", "c"]);
        assert_eq!(batch.next, DrainStep::Done);
    }

    /// The case id order cannot handle: the lower id commits last. T1
    /// takes its xid first; T2 takes a later one and draws the lower id;
    /// T1 then draws the higher id and commits while T2 is still open.
    /// A read passes T1's row, and T2's lower id still arrives after it.
    #[sqlx::test]
    async fn a_lower_id_committing_later_is_never_skipped(pool: PgPool) {
        table(&pool).await;
        // T1 holds an older xid, then draws a HIGHER id than T2.
        let mut t1 = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_current_xact_id()").execute(&mut *t1).await.unwrap();
        // T2 draws the lower id and stays open while T1 commits.
        let mut t2 = pool.begin().await.unwrap();
        insert(&mut *t2, "t2-lower-id").await;
        insert(&mut *t1, "t1-higher-id").await;
        t1.commit().await.unwrap();

        // T1's row is below the horizon (T2 is newer), T2's is in flight.
        let first = read_until(&pool, Position::default(), 100, &["t1-higher-id"]).await;
        assert_eq!(values(&first), ["t1-higher-id"]);
        let cursor = last(&first);

        // An id cursor at T1's id would never look at T2's lower id again.
        t2.commit().await.unwrap();
        let second = read_until(&pool, cursor, 100, &["t2-lower-id"]).await;
        assert_eq!(values(&second), ["t2-lower-id"], "the lower id committed later is still read");
        assert!(Position::of(&second.rows[0]).unwrap().id < cursor.id);
        assert_eq!(second.next, DrainStep::Done);
    }

    /// The xid of the row holding `v`.
    async fn xid_of(pool: &PgPool, v: &str) -> i64 {
        sqlx::query_scalar("SELECT writer_xid::text::bigint FROM settled_probe WHERE v = $1")
            .bind(v)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Two writers of one color, the second of which writes something
    /// else first (as `cancel_execution` strips signals before its
    /// terminals). Locked before that first write, the second writer's
    /// xid follows the first writer's, so the settled read applies the
    /// color's rows in write order. Locked after it, the second writer
    /// already holds the older xid and its later row reads first: the
    /// reopened-run bug `weft_journal::lock_colors` exists for.
    #[sqlx::test]
    async fn locking_the_color_before_any_write_keeps_its_rows_in_xid_order(pool: PgPool) {
        table(&pool).await;
        for lock_first in [true, false] {
            let color = uuid::Uuid::new_v4();
            let (first, second) = (format!("first-{lock_first}"), format!("second-{lock_first}"));
            let mut t1 = pool.begin().await.unwrap();
            weft_journal::lock_colors(&mut t1, &[color]).await.unwrap();

            let (pool2, second2) = (pool.clone(), second.clone());
            let other = format!("other-{lock_first}");
            let t2 = tokio::spawn(async move {
                let mut t2 = pool2.begin().await.unwrap();
                if lock_first {
                    weft_journal::lock_colors(&mut t2, &[color]).await.unwrap();
                    insert(&mut *t2, &other).await;
                } else {
                    insert(&mut *t2, &other).await;
                    weft_journal::lock_colors(&mut t2, &[color]).await.unwrap();
                }
                insert(&mut *t2, &second2).await;
                t2.commit().await.unwrap();
            });
            // Let T2 reach the lock (and, unlocked-first, its early write).
            tokio::time::sleep(Duration::from_millis(300)).await;
            insert(&mut *t1, &first).await;
            t1.commit().await.unwrap();
            t2.await.unwrap();

            let in_order = xid_of(&pool, &first).await < xid_of(&pool, &second).await;
            assert_eq!(in_order, lock_first, "lock_first = {lock_first}");
        }
    }

    #[sqlx::test]
    async fn a_full_read_asks_for_more(pool: PgPool) {
        table(&pool).await;
        for v in ["a", "b", "c"] {
            insert(&pool, v).await;
        }
        let batch = read_until(&pool, Position::default(), 2, &["a", "b"]).await;
        assert_eq!(values(&batch), ["a", "b"]);
        assert_eq!(batch.next, DrainStep::More);
    }
}
