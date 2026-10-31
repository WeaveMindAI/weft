//! A run's costs as rows of their own (`run_cost`): one per `CostReported`
//! its record holds, written by whoever writes the record, in the same
//! statement or transaction (`crate::record`), so `ctx.costs()` is one
//! query ([`list`]) and never a read of any record. A run's costs go with
//! the run (`weft_dispatcher`'s `erase_run_ids`).

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use weft_core::instance::InstanceId;
use weft_core::program::{CostFilter, CostRecord, PaidBy};
use weft_core::project::ProjectDefinition;
use weft_core::{CredentialOwner, RunId};
use weft_task_store::db::{self, Client, FromRow, Row, Transaction};

use crate::events::RunEvent;

/// One cost a write of a run's record reports, as its row keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cost {
    /// The cost's identity: a write sent again lands it once.
    pub cost_id: String,
    /// Its place among the events of the write that carries it: with the
    /// `seq` the write starts at, its place in the run's record, which is
    /// the order [`list`] answers a run's costs in.
    pub index: u32,
    pub node_id: String,
    /// The call sites the node ran under (`weft_core::frames::call_path`),
    /// which its address is spelled through on read.
    pub call_path: Vec<String>,
    pub service: String,
    pub model: Option<String>,
    /// `None` when the service's price could not be measured.
    pub amount_usd: Option<f64>,
    pub paid_by: CredentialOwner,
    pub at_unix: u64,
}

impl Cost {
    /// Every cost `events` (one write of one run) report, each with its
    /// place among them.
    pub fn all_of(events: &[RunEvent]) -> Vec<Self> {
        events.iter().enumerate().filter_map(|(index, event)| Self::of(index, event)).collect()
    }

    /// The cost `event` reports, at `index` among its write's events.
    fn of(index: usize, event: &RunEvent) -> Option<Self> {
        let RunEvent::CostReported { node_id, frames, cost_id, service, model, amount_usd, origin, at_unix, .. } = event else {
            return None;
        };
        Some(Self {
            cost_id: cost_id.clone(),
            index: u32::try_from(index).expect("a write holds fewer than 4 billion events"),
            node_id: node_id.clone(),
            call_path: weft_core::frames::call_path(frames).into_iter().map(str::to_string).collect(),
            service: service.clone(),
            model: model.clone(),
            amount_usd: *amount_usd,
            paid_by: origin.clone(),
            at_unix: *at_unix,
        })
    }
}

/// Costs as the aligned arrays `weft_record_costs` takes, each one's run
/// named by `K` (its id, or its ordinal in a batch of records, which
/// `weft_record_batch` turns into its id once it knows the run went in). A
/// call path has no fixed length, so the paths travel flattened: each site
/// with the ordinal (from 1) of the cost it belongs to, in order.
// SYNC: CostColumns <-> weft_record_costs's parameters, weft_record_batch's `p_cost_*` (crates/weft-dispatcher/src/store/postgres.rs)
#[derive(Debug, PartialEq)]
pub struct CostColumns<'a, K> {
    pub run: Vec<K>,
    pub seq: Vec<i32>,
    pub cost_id: Vec<&'a str>,
    pub index: Vec<i32>,
    pub node_id: Vec<&'a str>,
    pub service: Vec<&'a str>,
    pub model: Vec<Option<&'a str>>,
    pub amount_usd: Vec<Option<f64>>,
    pub paid_by: Vec<&'static str>,
    pub paid_by_instance: Vec<Option<&'a str>>,
    pub at_unix: Vec<i64>,
    pub path_cost: Vec<i32>,
    pub path_site: Vec<&'a str>,
}

impl<K> Default for CostColumns<'_, K> {
    fn default() -> Self {
        Self {
            run: Vec::new(),
            seq: Vec::new(),
            cost_id: Vec::new(),
            index: Vec::new(),
            node_id: Vec::new(),
            service: Vec::new(),
            model: Vec::new(),
            amount_usd: Vec::new(),
            paid_by: Vec::new(),
            paid_by_instance: Vec::new(),
            at_unix: Vec::new(),
            path_cost: Vec::new(),
            path_site: Vec::new(),
        }
    }
}

impl<'a, K> CostColumns<'a, K> {
    /// Add `cost`, of the run `run` names, carried by a write that starts
    /// at `seq`.
    pub fn push(&mut self, run: K, seq: i32, cost: &'a Cost) {
        self.run.push(run);
        self.seq.push(seq);
        self.cost_id.push(&cost.cost_id);
        self.index.push(i32::try_from(cost.index).expect("a write holds fewer than 2 billion events"));
        self.node_id.push(&cost.node_id);
        self.service.push(&cost.service);
        self.model.push(cost.model.as_deref());
        self.amount_usd.push(cost.amount_usd);
        self.paid_by.push(PaidBy::of(&cost.paid_by).as_str());
        self.paid_by_instance.push(match &cost.paid_by {
            CredentialOwner::Instance(instance) => Some(instance.as_str()),
            CredentialOwner::Platform | CredentialOwner::Author => None,
        });
        self.at_unix.push(cost.at_unix as i64);
        let ordinal = i32::try_from(self.run.len()).expect("a write holds fewer than 2 billion costs");
        for site in &cost.call_path {
            self.path_cost.push(ordinal);
            self.path_site.push(site);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.run.is_empty()
    }
}

impl<K: db::ToSql + Sync> CostColumns<'_, K> {
    /// The arrays, in `weft_record_costs`'s order.
    pub fn params(&self) -> [db::Param<'_>; 13] {
        [
            &self.run,
            &self.seq,
            &self.cost_id,
            &self.index,
            &self.node_id,
            &self.service,
            &self.model,
            &self.amount_usd,
            &self.paid_by,
            &self.paid_by_instance,
            &self.at_unix,
            &self.path_cost,
            &self.path_site,
        ]
    }
}

/// Write `costs`, of `run_id`, carried by its write at `seq`, on the
/// caller's transaction (the one writing that row of its record). A cost
/// already there stays as it is.
pub async fn insert_in(db: &impl Client, run_id: RunId, seq: i32, costs: &[Cost]) -> anyhow::Result<()> {
    let mut columns = CostColumns::default();
    for cost in costs {
        columns.push(run_id, seq, cost);
    }
    insert_columns_in(db, &columns).await
}

async fn insert_columns_in(db: &impl Client, columns: &CostColumns<'_, RunId>) -> anyhow::Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    db.execute(
        "SELECT weft_record_costs($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        &columns.params(),
    )
    .await?;
    Ok(())
}

/// The costs of `project_id`'s runs that `filter` reaches, in the order
/// the runs started and, within one, in its record's: each one's node
/// spelled through `project` (the address a person writes, as a filter
/// names it), or its id when the project has no program.
pub async fn list(
    db: &impl Client,
    project_id: uuid::Uuid,
    project: Option<&ProjectDefinition>,
    filter: &CostFilter,
) -> anyhow::Result<Vec<CostRecord>> {
    // A node is named by its address; its rows keep its id and the call
    // path it ran under, which the address resolves to. With no program
    // to resolve it against, the address is the id itself.
    let node: Option<(String, Option<Vec<String>>)> = filter.node.as_deref().map(|spelled| match project {
        Some(project) => {
            let (id, call_path) = weft_core::project::resolve_address(project, spelled);
            (id, Some(call_path))
        }
        None => (spelled.to_string(), None),
    });
    let rows: Vec<Listed> = db
        .all(
            "SELECT c.run_id, c.instance_id, c.node_id, c.call_path, c.service, c.model, c.amount_usd, c.paid_by, \
                    c.paid_by_instance, c.at_unix \
             FROM run_cost c JOIN run r ON r.run_id = c.run_id \
             WHERE c.project_id = $1 \
               AND ($2::text IS NULL OR c.instance_id = $2) \
               AND ($3::uuid IS NULL OR c.run_id = $3) \
               AND ($4::bigint IS NULL OR c.at_unix >= $4) \
               AND ($5::text IS NULL OR c.service = $5) \
               AND ($6::text IS NULL OR c.paid_by = $6) \
               AND ($7::text IS NULL OR c.node_id = $7) \
               AND ($8::text[] IS NULL OR c.call_path = $8) \
             ORDER BY r.started_at, c.run_id, c.seq, c.idx",
            &[
                &project_id,
                &filter.instance.as_ref().map(InstanceId::as_str),
                &filter.run,
                &filter.since_unix.map(|since| since as i64),
                &filter.service,
                &filter.paid_by.as_ref().map(PaidBy::as_str),
                &node.as_ref().map(|(id, _)| id.as_str()),
                &node.as_ref().and_then(|(_, call_path)| call_path.as_ref()),
            ],
        )
        .await?;
    let graph = project.map(weft_core::project::graph::ProjectGraph::new);
    rows.into_iter()
        .map(|row| {
            let paid_by = row.paid_by()?;
            let node = match &graph {
                Some(graph) => weft_core::project::address_of(graph, &row.node_id, &row.call_path),
                None => row.node_id,
            };
            Ok(CostRecord {
                run: row.run_id,
                instance: row.instance_id.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("a cost's instance: {e}"))?,
                node,
                service: row.service,
                model: row.model,
                amount_usd: row.amount_usd,
                paid_by,
                at_unix: row.at_unix as u64,
            })
        })
        .collect()
}

/// One row of [`list`]'s query.
struct Listed {
    run_id: RunId,
    instance_id: Option<String>,
    node_id: String,
    call_path: Vec<String>,
    service: String,
    model: Option<String>,
    amount_usd: Option<f64>,
    paid_by: String,
    paid_by_instance: Option<String>,
    at_unix: i64,
}

impl Listed {
    /// Whose credential the call spent, as its two columns say it.
    fn paid_by(&self) -> anyhow::Result<CredentialOwner> {
        Ok(match (PaidBy::parse(&self.paid_by), &self.paid_by_instance) {
            (Some(PaidBy::Platform), None) => CredentialOwner::Platform,
            (Some(PaidBy::Author), None) => CredentialOwner::Author,
            (Some(PaidBy::Instance), Some(instance)) => {
                CredentialOwner::Instance(InstanceId::new(instance.as_str()).map_err(|e| anyhow::anyhow!("a cost paid by an instance: {e}"))?)
            }
            (_, instance) => anyhow::bail!("a cost paid by '{}' (instance {instance:?}), which weft does not write", self.paid_by),
        })
    }
}

impl FromRow for Listed {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            run_id: row.try_get("run_id")?,
            instance_id: row.try_get("instance_id")?,
            node_id: row.try_get("node_id")?,
            call_path: row.try_get("call_path")?,
            service: row.try_get("service")?,
            model: row.try_get("model")?,
            amount_usd: row.try_get("amount_usd")?,
            paid_by: row.try_get("paid_by")?,
            paid_by_instance: row.try_get("paid_by_instance")?,
            at_unix: row.try_get("at_unix")?,
        })
    }
}

/// How many rows of the records one page of [`carry_from_records`] reads:
/// a page is decoded whole, so this bounds what it holds at once.
const CARRY_PAGE: i64 = 200;

/// The data step that fills `run_cost` from the records an older weft
/// left, which kept only each run's total (`run.cost_micro_usd`): every
/// cost every record reports, read page by page, decoded off the call
/// threads. Run by the boot while that column is there
/// (`weft_task_store::StepWork::Code`), so it lands once; a cost already
/// there stays as it is. A row that does not decode is said and left out,
/// as every reader of costs before it would have failed on it.
pub fn carry_from_records<'c>(tx: &'c Transaction<'c>) -> BoxFuture<'c, anyhow::Result<()>> {
    Box::pin(async move {
        let mut after = (RunId::nil(), -1);
        loop {
            let rows: Vec<(RunId, i32, Vec<u8>, weft_core::stored::Version)> = tx
                .all(
                    "SELECT run_id, seq, events, events_version FROM run_log \
                     WHERE (run_id, seq) > ($1, $2) ORDER BY run_id, seq LIMIT $3",
                    &[&after.0, &after.1, &CARRY_PAGE],
                )
                .await?;
            let Some(last) = rows.last() else { return Ok(()) };
            after = (last.0, last.1);
            let full = rows.len() as i64 == CARRY_PAGE;
            let costs = crate::on_blocking_pool(move || costs_of_rows(rows)).await;
            let mut columns = CostColumns::default();
            for (run_id, seq, costs) in &costs {
                for cost in costs {
                    columns.push(*run_id, *seq, cost);
                }
            }
            insert_columns_in(tx, &columns).await?;
            if !full {
                return Ok(());
            }
        }
    })
}

/// The costs each of `rows` (rows of records, with their run, seq and
/// version) reports, for the rows that report any.
fn costs_of_rows(rows: Vec<(RunId, i32, Vec<u8>, weft_core::stored::Version)>) -> Vec<(RunId, i32, Vec<Cost>)> {
    rows.into_iter()
        .filter_map(|(run_id, seq, events, version)| match crate::stored::decode(run_id, &events, version) {
            Ok(events) => Some((run_id, seq, Cost::all_of(&events))).filter(|(_, _, costs)| !costs.is_empty()),
            Err(e) => {
                tracing::error!(
                    target: "weft_journal::costs", %run_id, seq, error = %e,
                    "a row of a run's record does not read, so the costs it reports stay out of the cost table"
                );
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cost(cost_id: &str, amount_usd: Option<f64>, origin: CredentialOwner, frames: weft_core::LoopFrames) -> RunEvent {
        RunEvent::CostReported {
            run_id: RunId::from_u128(1),
            node_id: "One.llm".into(),
            frames,
            cost_id: cost_id.into(),
            service: "openai".into(),
            model: Some("m".into()),
            amount_usd,
            billed: false,
            origin,
            metadata: serde_json::Value::Null,
            charge: None,
            at_unix: 7,
        }
    }

    /// A write's costs are each at their place among its events, an
    /// unknown amount stays unknown, and each cost's call path lands under
    /// its own ordinal, however long.
    #[test]
    fn a_writes_costs_become_aligned_columns() {
        let ada = InstanceId::new("ada").unwrap();
        let site = |id: &str| weft_core::frames::Frame::Call { site: id.into() };
        let events = vec![
            RunEvent::RunCompleted { run_id: RunId::from_u128(1), at_unix: 1 },
            cost("a", Some(0.25), CredentialOwner::Platform, vec![site("one"), site("One.two")]),
            cost("b", None, CredentialOwner::Instance(ada.clone()), Vec::new()),
            cost("c", Some(1e-9), CredentialOwner::Author, vec![site("three")]),
        ];
        let costs = Cost::all_of(&events);
        assert_eq!(costs.iter().map(|cost| (cost.cost_id.as_str(), cost.index)).collect::<Vec<_>>(), [("a", 1), ("b", 2), ("c", 3)]);
        assert_eq!(costs[0].call_path, ["one", "One.two"]);
        let mut columns = CostColumns::default();
        for cost in &costs {
            columns.push(5, 9, cost);
        }
        assert_eq!(columns.run, [5, 5, 5]);
        assert_eq!(columns.seq, [9, 9, 9]);
        assert_eq!(columns.index, [1, 2, 3]);
        assert_eq!(columns.amount_usd, [Some(0.25), None, Some(1e-9)], "an unknown figure stays unknown, a tiny one exact");
        assert_eq!(columns.paid_by, ["platform", "instance", "author"]);
        assert_eq!(columns.paid_by_instance, [None, Some("ada"), None]);
        assert_eq!(columns.path_cost, [1, 1, 3]);
        assert_eq!(columns.path_site, ["one", "One.two", "three"]);
        assert_eq!(columns.at_unix, [7, 7, 7]);
    }

    /// A row that does not decode is left out and the others still give
    /// their costs.
    #[test]
    fn a_row_that_does_not_read_leaves_only_its_own_costs_out() {
        let run_id = RunId::from_u128(1);
        let version = crate::stored::STORED_EVENTS.version();
        let paid = cost("a", Some(0.5), CredentialOwner::Platform, Vec::new());
        let rows = vec![
            (run_id, 0, b"not zstd".to_vec(), version),
            (run_id, 1, crate::stored::encode(&[paid]), version),
            (run_id, 2, crate::stored::encode(&[RunEvent::RunCompleted { run_id, at_unix: 3 }]), version),
        ];
        let costs = costs_of_rows(rows);
        assert_eq!(costs.iter().map(|(run, seq, costs)| (*run, *seq, costs.len())).collect::<Vec<_>>(), [(run_id, 1, 1)]);
    }
}
