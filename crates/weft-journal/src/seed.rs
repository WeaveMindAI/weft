//! Reconstruct chosen history under each ancestor's immutable program.
//! History never runs through the child's scheduling rules: the child
//! imports the records of what it reuses, and what of them its cut needs
//! stands in for itself there (`weft_core::project::Given`).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use weft_core::frames::Located;
use weft_core::primitive::RunSnapshot;
use weft_core::project::ProjectDefinition;
use weft_core::RunId;

use crate::events::{RunEvent, Seed};
use crate::fold::{Fold, FoldEffects};

/// Full original context, including the birth row and unselected history.
#[derive(Debug, Clone)]
pub struct Ancestor {
    pub run_id: RunId,
    pub project: Arc<ProjectDefinition>,
    pub rows: Vec<RunEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct SeedChain {
    /// Oldest first. Every origin must name a run already reconstructed.
    pub ancestors: Vec<Ancestor>,
}

pub fn seed_of(rows: &[RunEvent]) -> Option<&Seed> {
    rows.iter().find_map(|event| match event {
        RunEvent::RunStarted { seed, .. } => seed.as_ref(),
        _ => None,
    })
}

impl SeedChain {
    pub fn is_empty(&self) -> bool { self.ancestors.is_empty() }

    /// Materialize ancestors once, retaining their source outputs for reuse.
    pub fn materialize(&self) -> Result<BTreeMap<RunId, Fold>> {
        let mut sources = BTreeMap::new();
        for ancestor in &self.ancestors {
            let mut fold = Fold::new(ancestor.run_id, ancestor.project.clone()).with_output_history();
            apply_history(&mut fold, ancestor.rows.iter(), &sources)?;
            // An ancestor is a finished run being read for its outputs:
            // a row its own program refuses means those outputs cannot
            // be trusted, so the child refuses to inherit. The child's
            // OWN corruptions stay on its snapshot for the driver to
            // word and journal as the run's terminal.
            anyhow::ensure!(
                fold.snapshot().corruptions.is_empty(),
                "seed ancestor {} has corrupt history: {:?}",
                fold.run_id(),
                fold.snapshot().corruptions
            );
            sources.insert(ancestor.run_id, fold);
        }
        Ok(sources)
    }

}

/// Import from already reconstructed runs, so callers painting several
/// ancestors do not reconstruct the same history once per ancestor.
pub fn import_origins(fold: &mut Fold, seed: &Seed, sources: &BTreeMap<RunId, Fold>, at_unix: u64) -> Result<FoldEffects> {
    let mut by_origin: BTreeMap<RunId, BTreeSet<Located>> = BTreeMap::new();
    for (place, origin) in &seed.origins { by_origin.entry(*origin).or_default().insert(place.clone()); }
    for (origin, nodes) in by_origin {
        let source = sources.get(&origin).ok_or_else(|| anyhow::anyhow!("seed names origin {origin} outside its ancestor chain"))?;
        fold.inherit(source, &nodes)?;
    }
    if !seed.answers.is_empty() {
        let parent = sources.get(&seed.parent).ok_or_else(|| anyhow::anyhow!("seed parent {} is outside its ancestor chain", seed.parent))?;
        fold.take_answers(parent, &seed.answers);
    }
    Ok(fold.settle(at_unix))
}

fn apply_history<'a>(
    fold: &mut Fold,
    mut rows: impl Iterator<Item = &'a RunEvent>,
    sources: &BTreeMap<RunId, Fold>,
) -> Result<()> {
    let birth = rows.next().filter(|event| matches!(event, RunEvent::RunStarted { .. }))
        .ok_or_else(|| anyhow::anyhow!("run {} has no initial RunStarted row", fold.run_id()))?;
    fold.apply(birth);
    if let Some(seed) = seed_of(std::slice::from_ref(birth)) { import_origins(fold, seed, sources, birth.at_unix())?; }
    for row in rows { fold.apply(row); }
    Ok(())
}

/// Read original graphs by the immutable identity on each ancestor's birth.
/// An absent ancestor or cyclic chain is an error, never a fresh-run fallback.
pub async fn seed_chain<F, Fut, D, Def>(
    child_rows: &[RunEvent],
    fetch_rows: F,
    fetch_definition: D,
) -> Result<SeedChain>
where
    F: Fn(RunId) -> Fut,
    Fut: Future<Output = Result<Vec<RunEvent>>>,
    D: Fn(uuid::Uuid, String) -> Def,
    Def: Future<Output = Result<Arc<ProjectDefinition>>>,
{
    let Some(seed) = seed_of(child_rows) else { return Ok(SeedChain::default()) };
    let mut next = Some(seed.parent);
    let mut seen = HashSet::new();
    if let Some(birth) = child_rows.first() { seen.insert(birth.run_id()); }
    let mut nearest_first = Vec::new();
    while let Some(run_id) = next {
        if !seen.insert(run_id) { bail!("the seed chain loops back to {run_id}"); }
        let rows = fetch_rows(run_id).await.with_context(|| format!("read seed run {run_id}"))?;
        anyhow::ensure!(!rows.is_empty(), "seed run {run_id} has no journal rows; choose another seed or run without --seed");
        let (project_id, hash) = match rows.first() {
            Some(RunEvent::RunStarted { project_id, definition_hash: Some(hash), .. }) => (*project_id, hash.clone()),
            _ => bail!("seed run {run_id} has no initial program identity"),
        };
        anyhow::ensure!(rows.iter().all(|row| row.run_id() == run_id), "seed run {run_id} contains another run's rows");
        // Read BY its hash, which is the check: a stored program keeps the
        // hash it was made under through every upgrade of its JSON, so
        // hashing it again here could refuse an upgraded one
        // (`weft_core::project::hash::compute_definition_hash`).
        let project = fetch_definition(project_id, hash.clone()).await.with_context(|| format!("read original program of seed {run_id}"))?;
        next = seed_of(&rows).map(|seed| seed.parent);
        nearest_first.push(Ancestor { run_id, project, rows });
    }
    nearest_first.reverse();
    Ok(SeedChain { ancestors: nearest_first })
}

pub fn fold_seeded(
    run_id: RunId,
    project: Arc<ProjectDefinition>,
    chain: &SeedChain,
    rows: &[RunEvent],
) -> Result<RunSnapshot> {
    let sources = chain.materialize()?;
    let mut fold = Fold::new(run_id, project);
    apply_history(&mut fold, rows.iter(), &sources)?;
    Ok(fold.into_snapshot())
}

/// A run's fold kept current as its drive goes: built once from the
/// record it starts from (on top of its seed chain), then caught up with
/// the record as it stands later (once the drive took an answer to a
/// wait, its record holds everything the run did since, the answer last).
/// `Fold::apply` takes events one at a time, so a start and then the rest
/// land exactly where all of them at once do.
pub struct LiveFold {
    fold: Fold,
    /// How many of the run's events are folded: the record's first ones.
    folded: usize,
}

impl LiveFold {
    /// Fold `events` (the run's record from its birth) over `project`.
    pub fn start(
        run_id: RunId,
        project: Arc<ProjectDefinition>,
        chain: &SeedChain,
        events: &[RunEvent],
    ) -> Result<Self> {
        let sources = chain.materialize()?;
        let mut fold = Fold::new(run_id, project);
        apply_history(&mut fold, events.iter(), &sources)?;
        Ok(Self { fold, folded: events.len() })
    }

    /// Fold what `record`, the run's whole record as it stands now, holds
    /// past what is folded already. A record shorter than what was folded
    /// is not this run's record.
    pub fn catch_up(&mut self, record: &[RunEvent]) -> Result<()> {
        let Some(new) = record.get(self.folded..) else {
            anyhow::bail!("the run's record holds {} events, fewer than the {} its fold already took", record.len(), self.folded);
        };
        for event in new {
            self.fold.apply(event);
        }
        self.folded = record.len();
        Ok(())
    }

    /// The run's state as the events so far say.
    pub fn snapshot(&self) -> RunSnapshot {
        self.fold.current_snapshot()
    }

    /// What the firing at `(node, frames)` handed out so far
    /// (`Fold::firing_output`).
    pub fn firing_output(&self, node_id: &str, frames: &weft_core::frames::LoopFrames) -> Option<&weft_core::run::postprocess::OutputBag> {
        self.fold.firing_output(node_id, frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use serde_json::json;
    use uuid::Uuid;
    use weft_core::project::{Given, GivenOrigin, GivenPort, StandIn};

    fn run_id(n: u8) -> RunId { Uuid::from_bytes([n; 16]) }

    /// `a -> b -> c`, each node one `out`.
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

    /// The program cut to `keep`, where each of `stand_ins` (a node, what
    /// it sent, the run it came from) stands in for itself, and what a run
    /// of it is given: the program a seeded run runs, and its values.
    fn cut(keep: &[&str], stand_ins: &[(&str, &str, RunId)]) -> (Arc<ProjectDefinition>, Given) {
        let mut program = program().as_ref().clone();
        program.nodes.retain(|node| keep.contains(&node.id.as_str()));
        program.edges.retain(|edge| keep.contains(&edge.source.as_str()) && keep.contains(&edge.target.as_str()));
        let mut given = Given::default();
        for (node, value, run) in stand_ins {
            given.outputs.insert(Located::top(*node), StandIn {
                ports: [("out".to_string(), GivenPort::Value { value: json!(value) })].into(),
                origin: GivenOrigin::Inherited { run: *run },
            });
        }
        program.given = given.places();
        (Arc::new(program), given)
    }

    fn seed(parent: RunId, origins: &[(&str, RunId)]) -> Option<Seed> {
        Some(Seed {
            parent,
            origins: origins.iter().map(|(node, run)| (Located::top(*node), *run)).collect(),
            answers: Default::default(),
        })
    }

    fn birth(run_id: RunId, seed: Option<Seed>, from: &str, program: &ProjectDefinition, given: &Given) -> RunEvent {
        RunEvent::RunStarted {
            given: Box::new(given.clone()),
            run_id, project_id: Uuid::nil(), entry_node: from.into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some(weft_core::project::hash::compute_definition_hash(program).unwrap()),
            binary_hash: None, cut_from: None, source_version: None, run_kind: weft_core::run::RunKind::Project, seed, instance: None, stand_in: None, fired_trigger: None, settings: Default::default(), instance_values: Default::default(), picks: Default::default(), at_unix: 0,
        }
    }

    fn result(run_id: RunId, node: &str, value: &str) -> Vec<RunEvent> {
        vec![
            RunEvent::NodeStarted { run_id, node_id:node.into(), frames:vec![], at_unix:1 },
            RunEvent::PortEmitted { run_id, emission_id:Uuid::new_v4(), node_id:node.into(), frames:vec![],
                port:"out".into(), value:Arc::new(json!(value)), at_unix:2 },
            RunEvent::NodeCompleted { run_id, node_id:node.into(), frames:vec![], at_unix:3 },
        ]
    }

    /// The seed chain of `rows`, reading each ancestor's program among
    /// `programs` by its hash.
    async fn chain(rows: &[RunEvent], runs: &HashMap<RunId, Vec<RunEvent>>, programs: &[Arc<ProjectDefinition>]) -> Result<SeedChain> {
        seed_chain(rows, |run_id| async move { Ok(runs.get(&run_id).cloned().unwrap_or_default()) },
            |_, hash| async move {
                programs.iter().find(|program| weft_core::project::hash::compute_definition_hash(program).unwrap() == hash)
                    .cloned().ok_or_else(|| anyhow::anyhow!("no program {hash}"))
            }).await
    }

    /// A child cut at `c` that reuses `a` and `b`: both records come over
    /// marked inherited, nothing of them goes on a wire, and `b`, which
    /// the cut reads, stands in and sends what it sent.
    #[tokio::test]
    async fn history_before_the_child_cut_survives_and_only_the_frontier_is_pending() {
        let (parent, child) = (run_id(1), run_id(2));
        let mut parent_rows = vec![birth(parent, None, "a", &program(), &Given::default())];
        parent_rows.extend(result(parent, "a", "A"));
        parent_rows.extend(result(parent, "b", "B"));
        let (cut, given) = cut(&["b", "c"], &[("b", "B", parent)]);
        let rows = vec![birth(child, seed(parent, &[("a", parent), ("b", parent)]), "c", &cut, &given)];
        let chain = chain(&rows, &HashMap::from([(parent, parent_rows)]), &[program()]).await.unwrap();
        assert!(matches!(chain.ancestors[0].rows[0], RunEvent::RunStarted { .. }));
        let snapshot = fold_seeded(child, cut, &chain, &rows).unwrap();
        assert_eq!(snapshot.firings["a"][0].inherited_from, Some(parent));
        assert_eq!(snapshot.firings["b"][0].received.input["in"].as_ref(), &json!("A"));
        assert!(!snapshot.pulses.contains_key("a") && !snapshot.pulses.contains_key("b"));
        assert_eq!(snapshot.pulses["c"][0].value.as_ref(), &json!("B"));
    }

    /// A grandchild names each result's original run, and each comes
    /// from there, never through the run in between.
    #[tokio::test]
    async fn explicit_origins_reconstruct_a_grandchild_without_replaying_parent_inputs() {
        let (a,b,c) = (run_id(1),run_id(2),run_id(3));
        let mut a_rows = vec![birth(a,None,"a", &program(), &Given::default())];
        a_rows.extend(result(a,"a","A"));
        let (b_cut, b_given) = cut(&["a","b","c"], &[("a","A",a)]);
        let mut b_rows = vec![birth(b,seed(a,&[("a",a)]),"b",&b_cut,&b_given)];
        b_rows.extend(result(b,"b","B"));
        let (c_cut, c_given) = cut(&["b","c"], &[("b","B",b)]);
        let rows = vec![birth(c,seed(b,&[("a",a),("b",b)]),"c",&c_cut,&c_given)];
        let chain = chain(&rows,&HashMap::from([(a,a_rows),(b,b_rows)]),&[program(),b_cut]).await.unwrap();
        let snapshot = fold_seeded(c,c_cut,&chain,&rows).unwrap();
        assert_eq!(snapshot.firings["a"][0].inherited_from,Some(a));
        assert_eq!(snapshot.firings["b"][0].inherited_from,Some(b));
        assert_eq!(snapshot.firings["b"][0].received.input["in"].as_ref(),&json!("A"));
    }

    #[tokio::test]
    async fn absent_and_cyclic_ancestors_are_refused() {
        let rows = vec![birth(run_id(2),seed(run_id(1),&[]),"a",&program(),&Given::default())];
        assert!(chain(&rows,&HashMap::new(),&[program()]).await.unwrap_err().to_string().contains("no journal rows"));
        let ancestor = vec![birth(run_id(1),seed(run_id(2),&[]),"a",&program(),&Given::default())];
        assert!(chain(&rows,&HashMap::from([(run_id(1),ancestor)]),&[program()]).await.unwrap_err().to_string().contains("loops"));
    }

    #[tokio::test]
    async fn a_fresh_run_reads_no_ancestors() {
        let rows = vec![birth(run_id(1),None,"a", &program(), &Given::default())];
        let chain = seed_chain(&rows, |_| async { bail!("unexpected journal read") },
            |_,_| async { bail!("unexpected definition read") }).await.unwrap();
        assert!(chain.is_empty());
    }

    #[tokio::test]
    async fn chosen_failed_result_is_refused_without_searching_for_older_success() {
        let parent = run_id(1);
        let mut parent_rows = vec![birth(parent,None,"a", &program(), &Given::default())];
        parent_rows.push(RunEvent::NodeStarted {run_id:parent,node_id:"a".into(),frames:vec![],at_unix:1});
        parent_rows.push(RunEvent::NodeFailed {run_id:parent,node_id:"a".into(),frames:vec![],error:"failed".into(),gave_up_wait:false,at_unix:2});
        let rows = vec![birth(run_id(2),seed(parent,&[("a",parent)]),"b",&program(),&Given::default())];
        let chain = chain(&rows,&HashMap::from([(parent,parent_rows)]),&[program()]).await.unwrap();
        assert!(fold_seeded(run_id(2),program(),&chain,&rows).err().unwrap().to_string().contains("no complete reusable result"));
    }

    /// What a snapshot says, in a form two folds can be compared by
    /// (pulse and record ids are minted by the fold, so they are left
    /// out).
    fn said(snap: &RunSnapshot) -> serde_json::Value {
        let firings: BTreeMap<&String, Vec<serde_json::Value>> = snap
            .firings
            .iter()
            .map(|(node, records)| (node, records.values().map(|record| {
                let mut record = serde_json::to_value(record).unwrap();
                record.as_object_mut().unwrap().remove("id");
                record
            }).collect()))
            .collect();
        let pulses: BTreeMap<&String, Vec<(&String, &serde_json::Value, bool)>> = snap
            .pulses
            .iter()
            .map(|(node, bucket)| (node, bucket.iter().map(|p| (&p.target_port, p.value.as_ref(), p.closed)).collect()))
            .collect();
        let mut absorbed: Vec<(&String, &String, &serde_json::Value, bool)> =
            snap.pulses.all_absorbed().map(|p| (&p.target_node, &p.target_port, p.value.as_ref(), p.closed)).collect();
        absorbed.sort_by_key(|row| serde_json::to_string(row).unwrap());
        json!({
            "firings": serde_json::to_value(firings).unwrap(),
            "pulses": serde_json::to_value(pulses).unwrap(),
            "absorbed": serde_json::to_value(absorbed).unwrap(),
            "corruptions": snap.corruptions.len(),
            "kicked": snap.kicked.len(),
        })
    }

    /// Folding a start and then the rest lands exactly where folding
    /// every event at once does, whatever the split, on a fresh run and on
    /// a seeded one.
    #[tokio::test]
    async fn a_start_then_the_rest_folds_like_the_whole_record() {
        let parent = run_id(1);
        let mut parent_rows = vec![birth(parent, None, "a", &program(), &Given::default())];
        parent_rows.extend(result(parent, "a", "A"));
        let fresh = {
            let mut rows = vec![birth(run_id(2), None, "a", &program(), &Given::default())];
            rows.extend(result(run_id(2), "a", "A"));
            rows.extend(result(run_id(2), "b", "B"));
            rows.extend(result(run_id(2), "c", "C"));
            rows
        };
        let (seeded_cut, seeded_given) = cut(&["a", "b", "c"], &[("a", "A", parent)]);
        let seeded = {
            let mut rows = vec![birth(run_id(3), seed(parent, &[("a", parent)]), "b", &seeded_cut, &seeded_given)];
            rows.extend(result(run_id(3), "b", "B"));
            rows.extend(result(run_id(3), "c", "C"));
            rows
        };
        let runs = HashMap::from([(parent, parent_rows)]);
        for (events, program) in [(fresh, program()), (seeded, seeded_cut)] {
            let run = events[0].run_id();
            let chain = chain(&events, &runs, &[self::program()]).await.unwrap();
            let whole = said(&fold_seeded(run, program.clone(), &chain, &events).unwrap());
            for split in 1..=events.len() {
                let mut live = LiveFold::start(run, program.clone(), &chain, &events[..split]).unwrap();
                live.catch_up(&events).unwrap();
                assert_eq!(said(&live.snapshot()), whole, "split at {split}");
            }
        }
    }

}
