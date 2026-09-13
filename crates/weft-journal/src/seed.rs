//! Reconstruct chosen history under each ancestor's immutable program.
//! History never runs through the child's scheduling rules. Only the chosen
//! sources' outputs cross into the child's executable selection.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use weft_core::primitive::ExecutionSnapshot;
use weft_core::project::ProjectDefinition;
use weft_core::Color;

use crate::events::{ExecEvent, Seed};
use crate::fold::{Fold, FoldEffects};

/// Full original context, including the birth row and unselected history.
#[derive(Debug, Clone)]
pub struct Ancestor {
    pub color: Color,
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
    pub fn materialize(&self) -> Result<BTreeMap<Color, Fold>> {
        let mut sources = BTreeMap::new();
        for ancestor in &self.ancestors {
            let mut fold = Fold::new(ancestor.color, ancestor.project.clone()).with_output_history();
            apply_history(&mut fold, &ancestor.rows, &sources)?;
            // An ancestor is a finished run being read for its outputs:
            // a row its own program refuses means those outputs cannot
            // be trusted, so the child refuses to inherit. The child's
            // OWN corruptions stay on its snapshot for the driver to
            // word and journal as the run's terminal.
            anyhow::ensure!(
                fold.snapshot().corruptions.is_empty(),
                "seed ancestor {} has corrupt history: {:?}",
                fold.color(),
                fold.snapshot().corruptions
            );
            sources.insert(ancestor.color, fold);
        }
        Ok(sources)
    }

}

/// Import from already reconstructed runs, so callers painting several
/// ancestors do not reconstruct the same history once per ancestor.
pub fn import_origins(fold: &mut Fold, seed: &Seed, sources: &BTreeMap<Color, Fold>, at_unix: u64) -> Result<FoldEffects> {
    let mut by_origin: BTreeMap<Color, BTreeSet<String>> = BTreeMap::new();
    for (node, origin) in &seed.origins { by_origin.entry(*origin).or_default().insert(node.clone()); }
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

fn apply_history(fold: &mut Fold, rows: &[ExecEvent], sources: &BTreeMap<Color, Fold>) -> Result<()> {
    let birth = rows.first().filter(|event| matches!(event, ExecEvent::ExecutionStarted { .. }))
        .ok_or_else(|| anyhow::anyhow!("run {} has no initial ExecutionStarted row", fold.color()))?;
    fold.apply(birth);
    if let Some(seed) = seed_of(rows) { import_origins(fold, seed, sources, birth.at_unix())?; }
    for row in &rows[1..] { fold.apply(row); }
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
    F: Fn(Color) -> Fut,
    Fut: Future<Output = Result<Vec<ExecEvent>>>,
    D: Fn(String, String) -> Def,
    Def: Future<Output = Result<Arc<ProjectDefinition>>>,
{
    let Some(seed) = seed_of(child_rows) else { return Ok(SeedChain::default()) };
    let mut next = Some(seed.parent);
    let mut seen = HashSet::new();
    if let Some(birth) = child_rows.first() { seen.insert(birth.color()); }
    let mut nearest_first = Vec::new();
    while let Some(color) = next {
        if !seen.insert(color) { bail!("the seed chain loops back to {color}"); }
        let rows = fetch_rows(color).await.with_context(|| format!("read seed run {color}"))?;
        anyhow::ensure!(!rows.is_empty(), "seed run {color} has no journal rows; choose another seed or run without --seed");
        let (project_id, hash) = match rows.first() {
            Some(ExecEvent::ExecutionStarted { project_id, definition_hash: Some(hash), .. }) => (project_id.clone(), hash.clone()),
            _ => bail!("seed run {color} has no initial program identity"),
        };
        anyhow::ensure!(rows.iter().all(|row| row.color() == color), "seed run {color} contains another run's rows");
        let project = fetch_definition(project_id, hash.clone()).await.with_context(|| format!("read original program of seed {color}"))?;
        anyhow::ensure!(weft_core::project::hash::compute_definition_hash(&project)? == hash,
            "original program of seed {color} does not match its recorded definition hash");
        next = seed_of(&rows).map(|seed| seed.parent);
        nearest_first.push(Ancestor { color, project, rows });
    }
    nearest_first.reverse();
    Ok(SeedChain { ancestors: nearest_first })
}

pub fn fold_seeded(
    color: Color,
    project: Arc<ProjectDefinition>,
    chain: &SeedChain,
    rows: &[ExecEvent],
) -> Result<ExecutionSnapshot> {
    let sources = chain.materialize()?;
    let mut fold = Fold::new(color, project);
    apply_history(&mut fold, rows, &sources)?;
    Ok(fold.into_snapshot())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use serde_json::json;
    use uuid::Uuid;
    use weft_core::project::selection::{RunSelection, SelectionBounds};

    fn color(n: u8) -> Color { Uuid::from_bytes([n; 16]) }

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

    fn birth(color: Color, seed: Option<Seed>, from: &str) -> ExecEvent {
        let mut selection = RunSelection::carve(&program(), &SelectionBounds { from: vec![from.into()], ..Default::default() }).unwrap();
        if let Some(seed) = &seed {
            selection.nodes.retain(|id| !seed.origins.contains_key(id));
            selection.suppliers.extend(seed.origins.keys().cloned());
        }
        ExecEvent::ExecutionStarted {
            color, project_id: Uuid::nil().to_string(), entry_node: from.into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some(weft_core::project::hash::compute_definition_hash(&program()).unwrap()),
            program: None, source_version: None, node_test: false, subgraph: Some(selection), seed, at_unix: 0,
        }
    }

    fn result(color: Color, node: &str, value: &str) -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeStarted { color, node_id:node.into(), frames:vec![], at_unix:1 },
            ExecEvent::PortEmitted { color, emission_id:Uuid::new_v4(), node_id:node.into(), frames:vec![],
                port:"out".into(), value:Arc::new(json!(value)), provided:false, at_unix:2 },
            ExecEvent::NodeCompleted { color, node_id:node.into(), frames:vec![], at_unix:3 },
        ]
    }

    async fn chain(rows: &[ExecEvent], runs: &HashMap<Color, Vec<ExecEvent>>) -> Result<SeedChain> {
        seed_chain(rows, |color| async move { Ok(runs.get(&color).cloned().unwrap_or_default()) },
            |_, _| async { Ok(program()) }).await
    }

    #[tokio::test]
    async fn history_before_the_child_cut_survives_and_only_the_frontier_is_pending() {
        let (parent, child) = (color(1), color(2));
        let mut parent_rows = vec![birth(parent, None, "a")];
        parent_rows.extend(result(parent, "a", "A"));
        parent_rows.extend(result(parent, "b", "B"));
        let rows = vec![birth(child, Some(Seed { parent, origins:BTreeMap::from([("a".into(),parent),("b".into(),parent)]) }), "c")];
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
        let (a,b,c) = (color(1),color(2),color(3));
        let mut a_rows = vec![birth(a,None,"a")];
        a_rows.extend(result(a,"a","A"));
        let mut b_rows = vec![birth(b,Some(Seed { parent:a, origins:BTreeMap::from([("a".into(),a)]) }),"b")];
        b_rows.extend(result(b,"b","B"));
        let rows = vec![birth(c,Some(Seed { parent:b, origins:BTreeMap::from([("a".into(),a),("b".into(),b)]) }),"c")];
        let chain = chain(&rows,&HashMap::from([(a,a_rows),(b,b_rows)])).await.unwrap();
        let snapshot = fold_seeded(c,program(),&chain,&rows).unwrap();
        assert_eq!(snapshot.executions["a"][0].inherited_from,Some(a));
        assert_eq!(snapshot.executions["b"][0].inherited_from,Some(b));
        assert_eq!(snapshot.executions["b"][0].received.input["in"].as_ref(),&json!("A"));
    }

    #[tokio::test]
    async fn absent_and_cyclic_ancestors_are_refused() {
        let rows = vec![birth(color(2),Some(Seed { parent:color(1), origins:BTreeMap::new() }),"a")];
        assert!(chain(&rows,&HashMap::new()).await.unwrap_err().to_string().contains("no journal rows"));
        let ancestor = vec![birth(color(1),Some(Seed { parent:color(2), origins:BTreeMap::new() }),"a")];
        assert!(chain(&rows,&HashMap::from([(color(1),ancestor)])).await.unwrap_err().to_string().contains("loops"));
    }

    #[tokio::test]
    async fn a_fresh_run_reads_no_ancestors() {
        let rows = vec![birth(color(1),None,"a")];
        let chain = seed_chain(&rows, |_| async { bail!("unexpected journal read") },
            |_,_| async { bail!("unexpected definition read") }).await.unwrap();
        assert!(chain.is_empty());
    }

    #[tokio::test]
    async fn chosen_failed_result_is_refused_without_searching_for_older_success() {
        let parent = color(1);
        let mut parent_rows = vec![birth(parent,None,"a")];
        parent_rows.push(ExecEvent::NodeStarted {color:parent,node_id:"a".into(),frames:vec![],at_unix:1});
        parent_rows.push(ExecEvent::NodeFailed {color:parent,node_id:"a".into(),frames:vec![],error:"failed".into(),at_unix:2});
        let rows = vec![birth(color(2),Some(Seed {parent,origins:BTreeMap::from([("a".into(),parent)])}),"b")];
        let chain = chain(&rows,&HashMap::from([(parent,parent_rows)])).await.unwrap();
        assert!(fold_seeded(color(2),program(),&chain,&rows).err().unwrap().to_string().contains("no complete reusable result"));
    }
}
