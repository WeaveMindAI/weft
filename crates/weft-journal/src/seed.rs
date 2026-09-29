//! Reconstruct chosen history under each ancestor's immutable program.
//! History never runs through the child's scheduling rules. Only the chosen
//! sources' outputs cross into the child's executable selection.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use weft_core::frames::Located;
use weft_core::primitive::ExecutionSnapshot;
use weft_core::project::ProjectDefinition;
use weft_core::ExecutionId;

use crate::events::{ExecEvent, Seed};
use crate::fold::{Fold, FoldEffects};
use crate::traits::JournalRow;

/// Full original context, including the birth row and unselected history.
#[derive(Debug, Clone)]
pub struct Ancestor {
    pub execution_id: ExecutionId,
    pub project: Arc<ProjectDefinition>,
    pub rows: Vec<ExecEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct SeedChain {
    /// Oldest first. Every origin must name a run already reconstructed.
    pub ancestors: Vec<Ancestor>,
}

pub fn seed_of(rows: &[ExecEvent]) -> Option<&Seed> {
    rows.iter().find_map(|event| match event {
        ExecEvent::ExecutionStarted { seed, .. } => seed.as_ref(),
        _ => None,
    })
}

impl SeedChain {
    pub fn is_empty(&self) -> bool { self.ancestors.is_empty() }

    /// Materialize ancestors once, retaining their source outputs for reuse.
    pub fn materialize(&self) -> Result<BTreeMap<ExecutionId, Fold>> {
        let mut sources = BTreeMap::new();
        for ancestor in &self.ancestors {
            let mut fold = Fold::new(ancestor.execution_id, ancestor.project.clone()).with_output_history();
            apply_history(&mut fold, ancestor.rows.iter(), &sources)?;
            // An ancestor is a finished run being read for its outputs:
            // a row its own program refuses means those outputs cannot
            // be trusted, so the child refuses to inherit. The child's
            // OWN corruptions stay on its snapshot for the driver to
            // word and journal as the run's terminal.
            anyhow::ensure!(
                fold.snapshot().corruptions.is_empty(),
                "seed ancestor {} has corrupt history: {:?}",
                fold.execution_id(),
                fold.snapshot().corruptions
            );
            sources.insert(ancestor.execution_id, fold);
        }
        Ok(sources)
    }

}

/// Import from already reconstructed runs, so callers painting several
/// ancestors do not reconstruct the same history once per ancestor.
pub fn import_origins(fold: &mut Fold, seed: &Seed, sources: &BTreeMap<ExecutionId, Fold>, at_unix: u64) -> Result<FoldEffects> {
    let mut by_origin: BTreeMap<ExecutionId, BTreeSet<Located>> = BTreeMap::new();
    for (place, origin) in &seed.origins { by_origin.entry(*origin).or_default().insert(place.clone()); }
    let mut effects = FoldEffects::default();
    for (origin, nodes) in by_origin {
        let source = sources.get(&origin).ok_or_else(|| anyhow::anyhow!("seed names origin {origin} outside its ancestor chain"))?;
        effects.emissions.extend(fold.inherit(source, &nodes)?.emissions);
    }
    let settled = fold.settle(at_unix);
    effects.emissions.extend(settled.emissions);
    effects.boundaries.extend(settled.boundaries);
    effects.rejections.extend(settled.rejections);
    Ok(effects)
}

fn apply_history<'a>(
    fold: &mut Fold,
    mut rows: impl Iterator<Item = &'a ExecEvent>,
    sources: &BTreeMap<ExecutionId, Fold>,
) -> Result<()> {
    let birth = rows.next().filter(|event| matches!(event, ExecEvent::ExecutionStarted { .. }))
        .ok_or_else(|| anyhow::anyhow!("run {} has no initial ExecutionStarted row", fold.execution_id()))?;
    fold.apply(birth);
    if let Some(seed) = seed_of(std::slice::from_ref(birth)) { import_origins(fold, seed, sources, birth.at_unix())?; }
    for row in rows { fold.apply(row); }
    Ok(())
}

/// Read original graphs by the immutable identity on each ancestor's birth.
/// An absent ancestor or cyclic chain is an error, never a fresh-run fallback.
pub async fn seed_chain<F, Fut, D, Def>(
    child_rows: &[ExecEvent],
    fetch_rows: F,
    fetch_definition: D,
) -> Result<SeedChain>
where
    F: Fn(ExecutionId) -> Fut,
    Fut: Future<Output = Result<Vec<ExecEvent>>>,
    D: Fn(uuid::Uuid, String) -> Def,
    Def: Future<Output = Result<Arc<ProjectDefinition>>>,
{
    let Some(seed) = seed_of(child_rows) else { return Ok(SeedChain::default()) };
    let mut next = Some(seed.parent);
    let mut seen = HashSet::new();
    if let Some(birth) = child_rows.first() { seen.insert(birth.execution_id()); }
    let mut nearest_first = Vec::new();
    while let Some(execution_id) = next {
        if !seen.insert(execution_id) { bail!("the seed chain loops back to {execution_id}"); }
        let rows = fetch_rows(execution_id).await.with_context(|| format!("read seed run {execution_id}"))?;
        anyhow::ensure!(!rows.is_empty(), "seed run {execution_id} has no journal rows; choose another seed or run without --seed");
        let (project_id, hash) = match rows.first() {
            Some(ExecEvent::ExecutionStarted { project_id, definition_hash: Some(hash), .. }) => (*project_id, hash.clone()),
            _ => bail!("seed run {execution_id} has no initial program identity"),
        };
        anyhow::ensure!(rows.iter().all(|row| row.execution_id() == execution_id), "seed run {execution_id} contains another run's rows");
        let project = fetch_definition(project_id, hash.clone()).await.with_context(|| format!("read original program of seed {execution_id}"))?;
        anyhow::ensure!(weft_core::project::hash::compute_definition_hash(&project)? == hash,
            "original program of seed {execution_id} does not match its recorded definition hash");
        next = seed_of(&rows).map(|seed| seed.parent);
        nearest_first.push(Ancestor { execution_id, project, rows });
    }
    nearest_first.reverse();
    Ok(SeedChain { ancestors: nearest_first })
}

pub fn fold_seeded(
    execution_id: ExecutionId,
    project: Arc<ProjectDefinition>,
    chain: &SeedChain,
    rows: &[ExecEvent],
) -> Result<ExecutionSnapshot> {
    let sources = chain.materialize()?;
    let mut fold = Fold::new(execution_id, project);
    apply_history(&mut fold, rows.iter(), &sources)?;
    Ok(fold.into_snapshot())
}

/// A run's fold kept current as its journal grows: built once from the
/// rows so far (on top of its seed chain), then fed only the rows after
/// the last one it applied. `Fold::apply` takes rows one at a time, so a
/// prefix and then the rest land exactly where all of them at once do,
/// and a long chatty run costs each new row once instead of refolding
/// the whole log on every wake.
pub struct LiveFold {
    fold: Fold,
    last_id: i64,
}

impl LiveFold {
    /// Fold `rows` (the run's log from its birth row) over `project`.
    pub fn start(
        execution_id: ExecutionId,
        project: Arc<ProjectDefinition>,
        chain: &SeedChain,
        rows: &[JournalRow],
    ) -> Result<Self> {
        let sources = chain.materialize()?;
        let mut fold = Fold::new(execution_id, project);
        apply_history(&mut fold, rows.iter().map(|row| &row.event), &sources)?;
        let mut live = Self { fold, last_id: 0 };
        live.last_id = live.checked_last_id(rows)?;
        Ok(live)
    }

    /// Fold the rows that came after the last one applied. A row at or
    /// before it would be folded twice, which no reader of an execution's log
    /// can produce, so it is refused rather than applied.
    pub fn apply(&mut self, rows: &[JournalRow]) -> Result<()> {
        let last_id = self.checked_last_id(rows)?;
        for row in rows {
            self.fold.apply(&row.event);
        }
        self.last_id = last_id;
        Ok(())
    }

    /// The id of the last row folded in; the next read resumes after it.
    pub fn last_id(&self) -> i64 {
        self.last_id
    }

    /// The run's state as the rows so far say.
    pub fn snapshot(&self) -> ExecutionSnapshot {
        self.fold.current_snapshot()
    }

    /// The last id of `rows`, once they are shown to follow the rows
    /// already folded, each after the one before.
    fn checked_last_id(&self, rows: &[JournalRow]) -> Result<i64> {
        let mut last = self.last_id;
        for row in rows {
            anyhow::ensure!(
                row.id > last,
                "journal row {} of run {} arrived after row {last}; the run's log is read in order, \
                 so this is a reader bug, not the journal's",
                row.id,
                self.fold.execution_id()
            );
            last = row.id;
        }
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use serde_json::json;
    use uuid::Uuid;
    use weft_core::project::selection::{RunSelection, SelectionBounds};

    fn execution_id(n: u8) -> ExecutionId { Uuid::from_bytes([n; 16]) }

    fn program() -> Arc<ProjectDefinition> {
        Arc::new(serde_json::from_value(json!({
            "id": Uuid::nil(), "nodes": (["a", "b", "c"].map(|id| json!({
                "id":id, "nodeType":"T", "label":null, "config":{},
                "position":{"x":0,"y":0},
                "inputs": if id == "a" { json!([]) } else { json!([{"name":"in","portType":"String","required":true}]) },
                "outputs":[{"name":"out","portType":"String","required":true}],
                "features":{}, "scope":[], "groupBoundary":null, "requiresInfra":false
            }))),
            "edges":[
                {"id":"ab","source":"a","target":"b","sourceHandle":"out","targetHandle":"in"},
                {"id":"bc","source":"b","target":"c","sourceHandle":"out","targetHandle":"in"}
            ], "groups":[]
        })).unwrap())
    }

    fn birth(execution_id: ExecutionId, seed: Option<Seed>, from: &str) -> ExecEvent {
        let mut selection = RunSelection::carve(&program(), &SelectionBounds { from: vec![from.into()], ..Default::default() }).unwrap();
        if let Some(seed) = &seed {
            selection.nodes.retain(|place| !seed.origins.contains_key(place));
            selection.suppliers.extend(seed.origins.keys().cloned());
        }
        ExecEvent::ExecutionStarted {
            execution_id, project_id: Uuid::nil(), entry_node: from.into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some(weft_core::project::hash::compute_definition_hash(&program()).unwrap()),
            program: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution, subgraph: Some(selection), seed, member: None, fired_trigger: None, run_class: weft_core::run_class::RunClass::Short, member_values: Default::default(), picks: Default::default(), at_unix: 0,
        }
    }

    fn result(execution_id: ExecutionId, node: &str, value: &str) -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeStarted { execution_id, node_id:node.into(), frames:vec![], at_unix:1 },
            ExecEvent::PortEmitted { execution_id, emission_id:Uuid::new_v4(), node_id:node.into(), frames:vec![],
                port:"out".into(), value:Arc::new(json!(value)), provided:false, at_unix:2 },
            ExecEvent::NodeCompleted { execution_id, node_id:node.into(), frames:vec![], at_unix:3 },
        ]
    }

    async fn chain(rows: &[ExecEvent], runs: &HashMap<ExecutionId, Vec<ExecEvent>>) -> Result<SeedChain> {
        seed_chain(rows, |execution_id| async move { Ok(runs.get(&execution_id).cloned().unwrap_or_default()) },
            |_, _| async { Ok(program()) }).await
    }

    #[tokio::test]
    async fn history_before_the_child_cut_survives_and_only_the_frontier_is_pending() {
        let (parent, child) = (execution_id(1), execution_id(2));
        let mut parent_rows = vec![birth(parent, None, "a")];
        parent_rows.extend(result(parent, "a", "A"));
        parent_rows.extend(result(parent, "b", "B"));
        let rows = vec![birth(child, Some(Seed { parent, origins:BTreeMap::from([(Located::top("a"), parent),(Located::top("b"), parent)]) }), "c")];
        let chain = chain(&rows, &HashMap::from([(parent,parent_rows)])).await.unwrap();
        assert!(matches!(chain.ancestors[0].rows[0], ExecEvent::ExecutionStarted { .. }));
        let snapshot = fold_seeded(child, program(), &chain, &rows).unwrap();
        assert_eq!(snapshot.executions["a"][0].inherited_from, Some(parent));
        assert_eq!(snapshot.executions["b"][0].received.input["in"].as_ref(), &json!("A"));
        assert!(!snapshot.pulses.contains_key("a") && !snapshot.pulses.contains_key("b"));
        assert_eq!(snapshot.pulses["c"][0].value.as_ref(), &json!("B"));
    }

    #[tokio::test]
    async fn explicit_origins_reconstruct_a_grandchild_without_replaying_parent_inputs() {
        let (a,b,c) = (execution_id(1),execution_id(2),execution_id(3));
        let mut a_rows = vec![birth(a,None,"a")];
        a_rows.extend(result(a,"a","A"));
        let mut b_rows = vec![birth(b,Some(Seed { parent:a, origins:BTreeMap::from([(Located::top("a"), a)]) }),"b")];
        b_rows.extend(result(b,"b","B"));
        let rows = vec![birth(c,Some(Seed { parent:b, origins:BTreeMap::from([(Located::top("a"), a),(Located::top("b"), b)]) }),"c")];
        let chain = chain(&rows,&HashMap::from([(a,a_rows),(b,b_rows)])).await.unwrap();
        let snapshot = fold_seeded(c,program(),&chain,&rows).unwrap();
        assert_eq!(snapshot.executions["a"][0].inherited_from,Some(a));
        assert_eq!(snapshot.executions["b"][0].inherited_from,Some(b));
        assert_eq!(snapshot.executions["b"][0].received.input["in"].as_ref(),&json!("A"));
    }

    #[tokio::test]
    async fn absent_and_cyclic_ancestors_are_refused() {
        let rows = vec![birth(execution_id(2),Some(Seed { parent:execution_id(1), origins:BTreeMap::new() }),"a")];
        assert!(chain(&rows,&HashMap::new()).await.unwrap_err().to_string().contains("no journal rows"));
        let ancestor = vec![birth(execution_id(1),Some(Seed { parent:execution_id(2), origins:BTreeMap::new() }),"a")];
        assert!(chain(&rows,&HashMap::from([(execution_id(1),ancestor)])).await.unwrap_err().to_string().contains("loops"));
    }

    #[tokio::test]
    async fn a_fresh_run_reads_no_ancestors() {
        let rows = vec![birth(execution_id(1),None,"a")];
        let chain = seed_chain(&rows, |_| async { bail!("unexpected journal read") },
            |_,_| async { bail!("unexpected definition read") }).await.unwrap();
        assert!(chain.is_empty());
    }

    #[tokio::test]
    async fn chosen_failed_result_is_refused_without_searching_for_older_success() {
        let parent = execution_id(1);
        let mut parent_rows = vec![birth(parent,None,"a")];
        parent_rows.push(ExecEvent::NodeStarted {execution_id:parent,node_id:"a".into(),frames:vec![],at_unix:1});
        parent_rows.push(ExecEvent::NodeFailed {execution_id:parent,node_id:"a".into(),frames:vec![],error:"failed".into(),at_unix:2});
        let rows = vec![birth(execution_id(2),Some(Seed {parent,origins:BTreeMap::from([(Located::top("a"), parent)])}),"b")];
        let chain = chain(&rows,&HashMap::from([(parent,parent_rows)])).await.unwrap();
        assert!(fold_seeded(execution_id(2),program(),&chain,&rows).err().unwrap().to_string().contains("no complete reusable result"));
    }

    /// What a snapshot says, in a form two folds can be compared by
    /// (pulse and record ids are minted by the fold, so they are left
    /// out).
    fn said(snap: &ExecutionSnapshot) -> serde_json::Value {
        let mut executions = serde_json::to_value(&snap.executions).unwrap();
        for records in executions.as_object_mut().unwrap().values_mut() {
            for record in records.as_array_mut().unwrap() {
                record.as_object_mut().unwrap().remove("id");
            }
        }
        let pulses: BTreeMap<&String, Vec<(&String, &serde_json::Value, bool)>> = snap
            .pulses
            .iter()
            .map(|(node, bucket)| (node, bucket.iter().map(|p| (&p.target_port, p.value.as_ref(), p.closed)).collect()))
            .collect();
        json!({
            "executions": executions,
            "pulses": serde_json::to_value(pulses).unwrap(),
            "corruptions": snap.corruptions.len(),
            "kicked": snap.kicked.len(),
        })
    }

    fn rows_of(events: Vec<ExecEvent>) -> Vec<crate::traits::JournalRow> {
        events.into_iter().enumerate().map(|(i, event)| crate::traits::JournalRow { id: 10 * (i as i64 + 1), event }).collect()
    }

    /// Folding a prefix and then the tail lands exactly where folding
    /// every row at once does, whatever the split, on a fresh run and on
    /// a seeded one.
    #[tokio::test]
    async fn a_prefix_then_the_tail_folds_like_the_whole_log() {
        let parent = execution_id(1);
        let mut parent_rows = vec![birth(parent, None, "a")];
        parent_rows.extend(result(parent, "a", "A"));
        let fresh = {
            let mut rows = vec![birth(execution_id(2), None, "a")];
            rows.extend(result(execution_id(2), "a", "A"));
            rows.extend(result(execution_id(2), "b", "B"));
            rows.extend(result(execution_id(2), "c", "C"));
            rows
        };
        let seeded = {
            let mut rows = vec![birth(execution_id(3), Some(Seed { parent, origins: BTreeMap::from([(Located::top("a"), parent)]) }), "b")];
            rows.extend(result(execution_id(3), "b", "B"));
            rows.extend(result(execution_id(3), "c", "C"));
            rows
        };
        let runs = HashMap::from([(parent, parent_rows)]);
        for events in [fresh, seeded] {
            let run = events[0].execution_id();
            let chain = chain(&events, &runs).await.unwrap();
            let whole = said(&fold_seeded(run, program(), &chain, &events).unwrap());
            let rows = rows_of(events);
            for split in 1..=rows.len() {
                let mut live = LiveFold::start(run, program(), &chain, &rows[..split]).unwrap();
                live.apply(&rows[split..]).unwrap();
                assert_eq!(said(&live.snapshot()), whole, "split at {split}");
                assert_eq!(live.last_id(), rows.last().unwrap().id);
            }
        }
    }

    /// A row that does not come after the last one folded is refused
    /// whole: folding it would count it twice.
    #[tokio::test]
    async fn a_row_already_folded_is_refused() {
        let mut events = vec![birth(execution_id(2), None, "a")];
        events.extend(result(execution_id(2), "a", "A"));
        let rows = rows_of(events);
        let chain = SeedChain::default();
        let mut live = LiveFold::start(execution_id(2), program(), &chain, &rows[..2]).unwrap();
        let before = said(&live.snapshot());
        assert!(live.apply(&rows[1..]).is_err(), "row 2 again");
        assert_eq!(said(&live.snapshot()), before, "nothing of a refused batch is folded");
        assert_eq!(live.last_id(), rows[1].id);
        live.apply(&rows[2..]).unwrap();
    }
}
