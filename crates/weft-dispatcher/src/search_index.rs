//! Finding a run by what went through it.
//!
//! When a person traces an incident ("this customer says their order
//! failed at noon"), what they hold is a word that went through the run:
//! an email, an order id, a phrase from an error. Each finished recorded
//! run gets one search entry: the words of every value its record holds
//! (the trigger's input, what every node sent on, every error and log
//! line, `weft_journal::search::SearchText`) and the nodes it started,
//! each spelled the way a person names it in the program the run was
//! started on (`one.strip` for the `strip` the site `one` includes, the
//! loop `each` for its boundaries), which is the name `--through` asks
//! with. The runs listing matches it (`--search`, `--through`), beside its
//! other filters.
//!
//! Built when somebody searches, never on a run's path and never on a
//! timer: a run's ending queues it (`run_search_queue`, in the write that
//! ends it), and a listing that reads the entries first builds the ones
//! queued, then builds again the ones an older weft built at an older
//! [`FORMAT`] ([`index_queued`]). So an install where nobody searches
//! spends nothing on it, and the first search after many runs ended takes
//! longer.
//!
//! Words, not substrings: `ada@example.com` finds the runs that carried that
//! address, and several words find the runs carrying all of them (a
//! quoted phrase, the runs carrying it as written).

use std::collections::HashMap;
use std::sync::Arc;

use weft_core::project::graph::{GraphView, ProjectGraph};
use weft_core::{ProjectDefinition, RunId};
use weft_journal::RunEvent;
use weft_task_store::db::{self, Client};

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "run_search",
    tables: &["run_search_queue", "run_search"],
    ddl: &[
        // The ended recorded runs whose entry is not built yet.
        r#"CREATE TABLE IF NOT EXISTS run_search_queue (
            run_id UUID PRIMARY KEY
        )"#,
        // One row per indexed run: its words, in the `simple` configuration
        // (every word as written, lowercased, no language's stemming or
        // stop words: an id or a name is found as it is), and the nodes it
        // started, spelled the way its program reads (`Entry::of`), built
        // at the entry shape `format` ([`FORMAT`]).
        r#"CREATE TABLE IF NOT EXISTS run_search (
            run_id UUID PRIMARY KEY,
            words TSVECTOR NOT NULL,
            nodes TEXT[] NOT NULL,
            format INTEGER NOT NULL DEFAULT 1
        )"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_words ON run_search USING GIN (words)"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_nodes ON run_search USING GIN (nodes)"#,
        // A search finds the entries an older weft built.
        r#"CREATE INDEX IF NOT EXISTS run_search_format ON run_search (format)"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// How many runs one round takes.
const ROUND: i64 = 1_000;

/// The shape of the entries this weft builds. An entry an older weft
/// built (at a lower format) is built again by the next search, after the
/// queued runs, so a change to what an entry holds reaches every run
/// already indexed. 1 held the nodes' compiled ids; 2 spells them.
const FORMAT: i32 = 2;

/// Where the runs whose entry is to be built wait, in the order a search
/// builds them ([`Pending::BOTH`]): queued since they ended, then holding
/// an entry an older weft built.
#[derive(Clone, Copy)]
enum Pending {
    Queued,
    Older,
}

impl Pending {
    const BOTH: [Pending; 2] = [Pending::Queued, Pending::Older];

    /// Up to [`ROUND`] of the runs waiting here, read without locking
    /// them: a round reads each run's whole record, which is no time to
    /// hold a transaction open. Two searches at once may read the same
    /// runs; only one writes each entry ([`Pending::write`]).
    async fn waiting(self, db: &impl Client) -> db::Result<Vec<RunId>> {
        match self {
            Pending::Queued => db.scalars("SELECT run_id FROM run_search_queue ORDER BY run_id LIMIT $1", &[&ROUND]).await,
            Pending::Older => {
                db.scalars("SELECT run_id FROM run_search WHERE format < $2 ORDER BY run_id LIMIT $1", &[&ROUND, &FORMAT]).await
            }
        }
    }

    /// Write `entry` as run `run_id`'s if the run still waits here, in one
    /// statement: a queued run leaves the queue in the same write that
    /// builds its entry, so a search that stops halfway leaves every run it
    /// did not write still waiting, and of two searches that built the same
    /// run, the one that writes second finds it waiting no more and writes
    /// nothing. A queued run erased meanwhile is not queued any more, so it
    /// gets no entry.
    async fn write(self, db: &impl Client, run_id: RunId, entry: &Entry) -> db::Result<()> {
        match self {
            Pending::Queued => db.execute(
                "WITH taken AS (DELETE FROM run_search_queue WHERE run_id = $1 RETURNING run_id) \
                 INSERT INTO run_search (run_id, words, nodes, format) \
                 SELECT run_id, to_tsvector('simple', $2::text), $3, $4 FROM taken \
                 ON CONFLICT (run_id) DO UPDATE SET words = EXCLUDED.words, nodes = EXCLUDED.nodes, format = EXCLUDED.format",
                &[&run_id, &entry.words, &entry.nodes, &FORMAT],
            )
            .await?,
            Pending::Older => db.execute(
                "UPDATE run_search SET words = to_tsvector('simple', $2::text), nodes = $3, format = $4 WHERE run_id = $1 AND format < $4",
                &[&run_id, &entry.words, &entry.nodes, &FORMAT],
            )
            .await?,
        };
        Ok(())
    }
}

/// What one run's entry holds.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Entry {
    pub words: String,
    pub nodes: Vec<String>,
}

impl Entry {
    /// The entry of a run whose record is `events` and whose registered
    /// program (the one it was started on, `RunOwner::registered_hash`) is
    /// `g`: each node it started spelled through the call sites its frames
    /// name (`weft_core::project::placed_address`), so a node of a file
    /// included twice is two names, the way `--through` tells them apart.
    /// With no program to spell them by, the entry holds the run's words
    /// alone: an id is not a name a person asks with.
    pub fn of(run_id: RunId, events: &[RunEvent], g: Option<&impl GraphView>) -> Self {
        let mut entry = Building::default();
        events.iter().for_each(|event| entry.push(run_id, event, g));
        entry.finish()
    }
}

/// An entry being built, its run's record fed in order (a page at a time:
/// it keeps the words and nodes so far, never the record).
#[derive(Default)]
struct Building {
    words: weft_journal::search::SearchText,
    nodes: std::collections::BTreeSet<String>,
}

impl Building {
    fn push(&mut self, run_id: RunId, event: &RunEvent, g: Option<&impl GraphView>) {
        self.words.push(event);
        let (RunEvent::NodeStarted { node_id, frames, .. }, Some(g)) = (event, g) else { return };
        let path: Vec<String> = weft_core::frames::call_path(frames).into_iter().map(str::to_string).collect();
        match weft_core::project::placed_address(g, node_id, &path) {
            Some(spelled) => {
                self.nodes.insert(spelled);
            }
            None => tracing::error!(
                target: "weft_dispatcher::search", %run_id, node = %node_id,
                "a run's record starts a node its program does not have; `--through` does not find the run by it"
            ),
        }
    }

    fn finish(self) -> Entry {
        Entry { words: self.words.into_text(), nodes: self.nodes.into_iter().collect() }
    }
}

/// The programs runs were started on, by project and hash, as a search
/// read them (`None`: there is none to spell its nodes by): kept for the
/// whole search, so each is read once.
type Programs = HashMap<(uuid::Uuid, String), Option<Arc<ProjectDefinition>>>;

/// Build the entry of every run queued, then build again every entry at
/// an older [`FORMAT`], a round of [`ROUND`] at a time, and answer how
/// many.
pub async fn index_queued(pool: &db::Pool) -> anyhow::Result<usize> {
    let mut programs = Programs::new();
    let mut indexed = 0;
    for pending in Pending::BOTH {
        loop {
            let round = round(pool, &mut programs, pending).await?;
            indexed += round;
            if (round as i64) < ROUND {
                break;
            }
        }
    }
    Ok(indexed)
}

/// One round: up to [`ROUND`] runs waiting at `pending` indexed, each
/// record read a page at a time outside any transaction and each entry
/// written on its own ([`Pending::write`]); answers how many were read.
/// Everything runs on the round's one connection.
async fn round(pool: &db::Pool, programs: &mut Programs, pending: Pending) -> anyhow::Result<usize> {
    let conn = pool.get().await?;
    let ids = pending.waiting(&conn).await?;
    for run_id in &ids {
        let id = *run_id;
        let first = weft_journal::record::RawRecord { rows: weft_journal::record::read_first_row(&conn, id).await?.into_iter().collect() };
        let birth = weft_journal::on_blocking_pool(move || first.decode(id)).await;
        let built = match birth {
            Ok(birth) => {
                let program = started_on(&conn, programs, id, birth.first().map(|row| &row.event)).await?;
                // A row that does not read leaves the run unsearchable, as
                // a record whose birth does not.
                weft_journal::record::fold_pages(|after| weft_journal::record::read_page(&conn, id, after), None, Ok(Building::default()), move |entry: &mut Result<Building, String>, page| {
                    if let Ok(building) = entry {
                        match page.decode(id) {
                            Ok(rows) => {
                                let g = program.as_deref().map(ProjectGraph::new);
                                rows.iter().for_each(|row| building.push(id, &row.event, g.as_ref()));
                            }
                            Err(e) => *entry = Err(e),
                        }
                    }
                    Ok(())
                })
                .await?
            }
            Err(e) => Err(e),
        };
        let entry = match built {
            Ok(entry) => entry.finish(),
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::search", %run_id, error = %e, "a run's record does not read; it is not searchable");
                Entry::default()
            }
        };
        pending.write(&conn, id, &entry).await?;
    }
    Ok(ids.len())
}

/// The program the run whose birth is `birth` was started on, read on `db`
/// and kept in `programs`: the registered one its birth names (the
/// program it was cut from, for a run of a cut, which leaves out what it
/// reuses). `None` when there is none to spell its nodes by (gone, never
/// named, or no longer reading), said in the log.
async fn started_on(
    db: &impl Client,
    programs: &mut Programs,
    run_id: RunId,
    birth: Option<&RunEvent>,
) -> anyhow::Result<Option<Arc<ProjectDefinition>>> {
    let Some(RunEvent::RunStarted { project_id, definition_hash, cut_from, .. }) = birth else {
        tracing::warn!(target: "weft_dispatcher::search", %run_id, "a run's record does not start with its birth; its nodes are not searchable");
        return Ok(None);
    };
    let Some(hash) = cut_from.as_ref().or(definition_hash.as_ref()) else { return Ok(None) };
    let key = (*project_id, hash.clone());
    if let Some(program) = programs.get(&key) {
        return Ok(program.clone());
    }
    let program = match crate::project_store::program_in(db, *project_id, hash).await {
        Ok(Some(program)) => Some(Arc::new(program)),
        Ok(None) => {
            tracing::warn!(target: "weft_dispatcher::search", %run_id, %hash, "a run's program is gone; its nodes are not searchable");
            None
        }
        Err(e) if e.downcast_ref::<weft_core::stored::DecodeError>().is_some() => {
            tracing::warn!(target: "weft_dispatcher::search", %run_id, error = %format!("{e:#}"), "a run's program no longer reads; its nodes are not searchable");
            None
        }
        Err(e) => return Err(e),
    };
    programs.insert(key, program.clone());
    Ok(program)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::frames::Frame;

    /// `lookup` at the top, the loop `each` (its two boundaries), and the
    /// file `@src:lib` (holding `strip`) included at the sites `one` and
    /// `two`.
    fn program() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str], boundary: serde_json::Value| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false
        });
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("lookup", &[], serde_json::Value::Null),
                node("@src:lib.strip", &["@src:lib"], serde_json::Value::Null),
                node("each__in", &[], serde_json::json!({"groupId": "each", "role": "In"})),
                node("each__out", &[], serde_json::json!({"groupId": "each", "role": "Out"})),
            ],
            "edges": [],
            "groups": [
                {"id": "@src:lib", "kind": "body", "nodeIds": ["@src:lib.strip"]},
                {"id": "one", "kind": "call", "body": "@src:lib", "nodeIds": []},
                {"id": "two", "kind": "call", "body": "@src:lib", "nodeIds": []},
                {"id": "each", "kind": "loop", "loopConfig": {}, "nodeIds": []}
            ]
        }))
        .unwrap()
    }

    /// An entry holds the run's values and the nodes it started, each once,
    /// spelled the way `--through` names them: a node of an included file
    /// through the site its frames name, a loop's boundary as the loop.
    #[test]
    fn an_entry_is_the_runs_words_and_started_nodes() {
        let run_id = weft_core::RunId::from_u128(7);
        let events = vec![
            RunEvent::NodeStarted { run_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            RunEvent::LogLine {
                run_id,
                node_id: "lookup".into(),
                frames: vec![],
                level: "info".into(),
                message: "ada@example.com".into(),
                at_unix_ms: None,
                seq: None,
                at_unix: 0,
            },
            RunEvent::NodeStarted { run_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "@src:lib.strip".into(), frames: vec![Frame::Call { site: "one".into() }], at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "each__in".into(), frames: vec![], at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "gone".into(), frames: vec![], at_unix: 0 },
        ];
        let program = program();
        let entry = Entry::of(run_id, &events, Some(&ProjectGraph::new(&program)));
        assert!(entry.words.contains("ada@example.com"), "{}", entry.words);
        assert_eq!(entry.nodes, ["each", "lookup", "one.strip"]);
        // With no program to spell by, the words alone.
        let bare = Entry::of(run_id, &events, None::<&ProjectGraph>);
        assert!(bare.words.contains("ada@example.com") && bare.nodes.is_empty());
    }
}
