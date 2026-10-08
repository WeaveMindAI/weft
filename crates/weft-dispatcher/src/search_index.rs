//! Finding a run by what went through it.
//!
//! When a person traces an incident ("this customer says their order
//! failed at noon"), what they hold is a word that went through the run:
//! an email, an order id, a phrase from an error. Each finished recorded
//! run gets one search entry: the words of every value its record holds
//! (the trigger's input, what every node sent on, every error and log
//! line, `weft_journal::search::SearchText`) and the nodes it started. The
//! runs listing matches it (`--search`, `--through`), beside its other
//! filters.
//!
//! Built behind the writes, never on a run's path: a run's ending queues it
//! (`run_search_queue`, in the write that ends it), and a loop here takes
//! the queue a thousand at a time, reads their records, and writes their
//! entries, at most [`DEFAULT_RATE`] runs a second (`WEFT_SEARCH_RATE`),
//! pausing while workers' records are being written: an entry costs the
//! database about ten times what a record does, and must never compete
//! with one. A run becomes searchable a few seconds after it ends, longer
//! under heavy load; a search meanwhile answers from what is indexed, as
//! if the newest runs had not happened yet.
//!
//! Words, not substrings: `ada@example.com` finds the runs that carried that
//! address, and several words find the runs carrying all of them (a
//! quoted phrase, the runs carrying it as written).

use std::time::Duration;

use sqlx::PgPool;
use weft_core::ExecutionId;
use weft_journal::ExecEvent;
use weft_task_store::drain::{DrainLoop, DrainStep};

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "execution_search",
    tables: &["run_search_queue", "run_search"],
    ddl: &[
        // The ended recorded runs whose entry is not built yet.
        r#"CREATE TABLE IF NOT EXISTS run_search_queue (
            execution_id UUID PRIMARY KEY
        )"#,
        // One row per indexed run: its words, in the `simple` configuration
        // (every word as written, lowercased, no language's stemming or
        // stop words: an id or a name is found as it is), and the nodes it
        // started, spelled the way the record names them.
        r#"CREATE TABLE IF NOT EXISTS run_search (
            execution_id UUID PRIMARY KEY,
            words TSVECTOR NOT NULL,
            nodes TEXT[] NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_words ON run_search USING GIN (words)"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_nodes ON run_search USING GIN (nodes)"#,
    ],
    seed: &[],
};

/// How many runs a second the loop indexes when nothing says otherwise.
pub const DEFAULT_RATE: u32 = 2_000;

/// How many queued runs one round takes.
const ROUND: i64 = 1_000;

/// How long the loop waits before looking again when the queue is empty,
/// or the records are busy.
const IDLE: Duration = Duration::from_secs(1);

/// How often the loop looks when it found nothing to do: a run that just
/// ended waits about this long to be searchable, on a quiet install.
const QUIET_LOOK: Duration = Duration::from_secs(2);

/// What one run's entry holds.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Entry {
    pub words: String,
    pub nodes: Vec<String>,
}

impl Entry {
    /// The entry of a run whose record is `events`.
    pub fn of(events: &[ExecEvent]) -> Self {
        let mut words = weft_journal::search::SearchText::default();
        let mut nodes = std::collections::BTreeSet::new();
        for event in events {
            words.push(event);
            if let ExecEvent::NodeStarted { node_id, .. } = event {
                nodes.insert(node_id.clone());
            }
        }
        Self { words: words.into_text(), nodes: nodes.into_iter().collect() }
    }
}

/// `WEFT_SEARCH_RATE`, or [`DEFAULT_RATE`].
pub fn rate_from_env() -> anyhow::Result<u32> {
    match std::env::var("WEFT_SEARCH_RATE") {
        Ok(raw) => {
            let rate: u32 = raw.trim().parse().map_err(|e| anyhow::anyhow!("WEFT_SEARCH_RATE is not a whole number of runs a second: {e}"))?;
            anyhow::ensure!(rate > 0, "WEFT_SEARCH_RATE must be at least 1");
            Ok(rate)
        }
        Err(_) => Ok(DEFAULT_RATE),
    }
}

/// What one round found.
#[derive(Debug, PartialEq, Eq)]
pub enum Round {
    /// Workers' records are being written: nothing was indexed.
    Busy,
    /// This many runs indexed.
    Indexed(usize),
}

/// The loop (see the module doc), paced to `rate` runs a second. Several
/// dispatchers run it side by side: each round takes its runs with
/// `FOR UPDATE SKIP LOCKED`, so no run is indexed twice.
pub fn drain_loop(state: crate::state::DispatcherState, rate: u32) -> DrainLoop {
    DrainLoop::new("search_index", &[], QUIET_LOOK, move || {
        let pool = state.pg_pool.clone();
        async move {
            Ok(match round(&pool, rate).await? {
                Round::Busy => DrainStep::RetryIn(IDLE),
                Round::Indexed(0) => DrainStep::Done,
                Round::Indexed(n) if n as i64 == ROUND => DrainStep::More,
                Round::Indexed(_) => DrainStep::RetryIn(IDLE),
            })
        }
    })
}

/// One round: up to [`ROUND`] queued runs indexed, paced to `rate` runs a
/// second, unless workers' records are being written right now.
pub async fn round(pool: &PgPool, rate: u32) -> anyhow::Result<Round> {
    if records_busy(pool).await? {
        return Ok(Round::Busy);
    }
    let started = tokio::time::Instant::now();
    let mut tx = pool.begin().await?;
    let ids: Vec<ExecutionId> = sqlx::query_scalar(
        "SELECT execution_id FROM run_search_queue ORDER BY execution_id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(ROUND)
    .fetch_all(&mut *tx)
    .await?;
    for execution_id in &ids {
        let record = weft_journal::record::read_record(&mut tx, *execution_id, None).await?;
        let entry = match record.events(*execution_id) {
            Ok(events) => Entry::of(&events),
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::search", %execution_id, error = %e, "a run's record does not read; it is not searchable");
                Entry::default()
            }
        };
        sqlx::query(
            "INSERT INTO run_search (execution_id, words, nodes) VALUES ($1, to_tsvector('simple', $2), $3) \
             ON CONFLICT (execution_id) DO NOTHING",
        )
        .bind(execution_id)
        .bind(&entry.words)
        .bind(&entry.nodes)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM run_search_queue WHERE execution_id = ANY($1)").bind(&ids).execute(&mut *tx).await?;
    tx.commit().await?;
    let paced = Duration::from_secs_f64(ids.len() as f64 / f64::from(rate));
    tokio::time::sleep_until(started + paced).await;
    Ok(Round::Indexed(ids.len()))
}

/// Whether workers' records wait to be written: some broker has every
/// connection of its record pool in use (they carry its name,
/// `weft_task_store::db::RECORD_POOL_APPLICATION`), so its next batch
/// waits for one. The loop waits for them then; under any lighter load it
/// goes on, paced, and never starves.
async fn records_busy(pool: &PgPool) -> anyhow::Result<bool> {
    let busiest: Option<i64> = sqlx::query_scalar(
        "SELECT max(going) FROM (SELECT count(*) AS going FROM pg_stat_activity \
           WHERE state <> 'idle' AND starts_with(application_name, $1) GROUP BY application_name) AS broker",
    )
    .bind(format!("{}/", weft_task_store::db::RECORD_POOL_APPLICATION))
    .fetch_one(pool)
    .await?;
    Ok(busiest.unwrap_or(0) >= i64::from(weft_task_store::db::RECORD_POOL_CONNECTIONS))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An entry holds the run's values and the nodes it started, each once.
    #[test]
    fn an_entry_is_the_runs_words_and_started_nodes() {
        let execution_id = weft_core::ExecutionId::from_u128(7);
        let events = vec![
            ExecEvent::NodeStarted { execution_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            ExecEvent::LogLine {
                execution_id,
                node_id: "lookup".into(),
                frames: vec![],
                level: "info".into(),
                message: "ada@example.com".into(),
                at_unix_ms: None,
                seq: None,
                at_unix: 0,
            },
            ExecEvent::NodeStarted { execution_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            ExecEvent::NodeStarted { execution_id, node_id: "cards.post".into(), frames: vec![], at_unix: 0 },
        ];
        let entry = Entry::of(&events);
        assert!(entry.words.contains("ada@example.com"), "{}", entry.words);
        assert_eq!(entry.nodes, ["cards.post", "lookup"]);
    }
}
