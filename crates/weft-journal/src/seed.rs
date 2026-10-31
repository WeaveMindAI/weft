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
use crate::traits::JournalRow;

/// One run a seeded run inherits from: its program and its first event,
/// its birth, with its place in its record. The rest of its record is read
/// a page at a time when the chain is reconstructed
/// ([`SeedChain::materialize`]), never held whole.
#[derive(Debug, Clone)]
pub struct Ancestor {
    pub run_id: RunId,
    pub project: Arc<ProjectDefinition>,
    pub birth: JournalRow,
}

#[derive(Debug, Clone, Default)]
pub struct SeedChain {
    /// Oldest first. Every origin must name a run already reconstructed.
    pub ancestors: Vec<Ancestor>,
    /// Per ancestor, the places the runs after it take from it: the ones
    /// each of their seeds names it the origin of, and the answers the run
    /// right after it takes. What its reconstruction keeps of it.
    pub handed_down: BTreeMap<RunId, BTreeSet<Located>>,
}

/// What the birth `birth` says its run inherits from, when it is one.
pub fn seed_of(birth: &RunEvent) -> Option<&Seed> {
    match birth {
        RunEvent::RunStarted { seed, .. } => seed.as_ref(),
        _ => None,
    }
}

impl SeedChain {
    /// The chain of a run seeded by `seed`, its `ancestors` oldest first:
    /// what each hands down is read off `seed` and the ancestors' births.
    pub fn new(ancestors: Vec<Ancestor>, seed: &Seed) -> Self {
        let mut handed_down: BTreeMap<RunId, BTreeSet<Located>> = BTreeMap::new();
        for seed in std::iter::once(seed).chain(ancestors.iter().filter_map(|ancestor| seed_of(&ancestor.birth.event))) {
            for (place, origin) in &seed.origins {
                handed_down.entry(*origin).or_default().insert(place.clone());
            }
            handed_down.entry(seed.parent).or_default().extend(seed.answers.iter().cloned());
        }
        Self { ancestors, handed_down }
    }

    pub fn is_empty(&self) -> bool { self.ancestors.is_empty() }

    /// Reconstruct the ancestors, oldest first, each read a page at a time
    /// through `page` (one page of a run's record after a row,
    /// `crate::decoded_page`) and folded keeping only what the runs after
    /// it take from it ([`Fold::keeping`]), so a long ancestor costs what
    /// its handed-down places hold, never its whole record.
    pub async fn materialize<P, Fut>(&self, page: &P) -> Result<BTreeMap<RunId, Fold>>
    where
        P: Fn(RunId, Option<i32>) -> Fut,
        Fut: Future<Output = Result<Vec<JournalRow>>>,
    {
        let mut sources = BTreeMap::new();
        for ancestor in &self.ancestors {
            let run_id = ancestor.run_id;
            let keeps = self.handed_down.get(&run_id).cloned().unwrap_or_default();
            // The birth imports from the ancestors before it, so the fold
            // and those travel together onto the blocking pool.
            let mut reading = (Fold::new(run_id, ancestor.project.clone()).keeping(keeps), sources, false);
            let mut after = None;
            loop {
                let rows = page(run_id, after).await.with_context(|| format!("read seed run {run_id}"))?;
                let Some(last) = rows.last().map(|row| row.seq) else { break };
                reading = crate::on_blocking_pool(move || -> Result<_> {
                    let (mut fold, sources, mut born) = reading;
                    let mut rows = rows.iter();
                    if !born {
                        let birth = rows.next().expect("a page with a last row has a first");
                        apply_birth(&mut fold, birth, Some(birth.position()), &sources)?;
                        born = true;
                    }
                    for row in rows {
                        anyhow::ensure!(row.event.run_id() == run_id, "seed run {run_id} contains another run's rows");
                        fold.apply_at(&row.event, Some(row.position()));
                    }
                    Ok((fold, sources, born))
                })
                .await?;
                after = Some(last);
            }
            let (mut fold, mut before, born) = reading;
            anyhow::ensure!(born, "seed run {run_id} has no journal rows; choose another seed or run without --seed");
            fold.read_to_the_end();
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
            before.insert(run_id, fold);
            sources = before;
        }
        Ok(sources)
    }
}

/// Import from already reconstructed runs, so callers painting several
/// ancestors do not reconstruct the same history once per ancestor.
/// All or nothing: a refused import leaves `fold` as it was, never holding
/// some of what it inherits as if it were all of it. It runs as the birth
/// is folded, when the fold holds the birth alone, so trying on a copy
/// costs little.
pub fn import_origins(fold: &mut Fold, seed: &Seed, sources: &BTreeMap<RunId, Fold>, at_unix: u64) -> Result<FoldEffects> {
    let mut staged = fold.clone();
    let mut by_origin: BTreeMap<RunId, BTreeSet<Located>> = BTreeMap::new();
    for (place, origin) in &seed.origins { by_origin.entry(*origin).or_default().insert(place.clone()); }
    for (origin, nodes) in by_origin {
        let source = sources.get(&origin).ok_or_else(|| anyhow::anyhow!("seed names origin {origin} outside its ancestor chain"))?;
        staged.inherit(source, &nodes)?;
    }
    if !seed.answers.is_empty() {
        let parent = sources.get(&seed.parent).ok_or_else(|| anyhow::anyhow!("seed parent {} is outside its ancestor chain", seed.parent))?;
        staged.take_answers(parent, &seed.answers);
    }
    let effects = staged.settle(at_unix);
    *fold = staged;
    Ok(effects)
}

/// Fold a run's first event, its birth, read at `at`, and import what it
/// inherits.
fn apply_birth(fold: &mut Fold, birth: &JournalRow, at: Option<crate::traits::Position>, sources: &BTreeMap<RunId, Fold>) -> Result<()> {
    anyhow::ensure!(matches!(birth.event, RunEvent::RunStarted { .. }), "run {} has no initial RunStarted row", fold.run_id());
    fold.apply_at(&birth.event, at);
    if let Some(seed) = seed_of(&birth.event) { import_origins(fold, seed, sources, birth.event.at_unix())?; }
    Ok(())
}

/// Read original graphs by the immutable identity on each ancestor's birth.
/// An absent ancestor or cyclic chain is an error, never a fresh-run fallback.
/// `birth` is the child's first event; `page` reads one page of a run's
/// record after a row, of which only each ancestor's first is read here.
pub async fn seed_chain<P, Fut, D, Def>(
    birth: Option<&RunEvent>,
    page: &P,
    fetch_definition: D,
) -> Result<SeedChain>
where
    P: Fn(RunId, Option<i32>) -> Fut,
    Fut: Future<Output = Result<Vec<JournalRow>>>,
    D: Fn(uuid::Uuid, String) -> Def,
    Def: Future<Output = Result<Arc<ProjectDefinition>>>,
{
    let Some(seed) = birth.and_then(seed_of) else { return Ok(SeedChain::default()) };
    let mut next = Some(seed.parent);
    let mut seen = HashSet::new();
    if let Some(birth) = birth { seen.insert(birth.run_id()); }
    let mut nearest_first = Vec::new();
    while let Some(run_id) = next {
        if !seen.insert(run_id) { bail!("the seed chain loops back to {run_id}"); }
        let first = page(run_id, None).await.with_context(|| format!("read seed run {run_id}"))?;
        let Some(birth) = first.into_iter().next() else {
            bail!("seed run {run_id} has no journal rows; choose another seed or run without --seed");
        };
        anyhow::ensure!(birth.event.run_id() == run_id, "seed run {run_id} contains another run's rows");
        let (project_id, hash) = match &birth.event {
            RunEvent::RunStarted { project_id, definition_hash: Some(hash), .. } => (*project_id, hash.clone()),
            _ => bail!("seed run {run_id} has no initial program identity"),
        };
        // Read BY its hash, which is the check: a stored program keeps the
        // hash it was made under through every upgrade of its JSON, so
        // hashing it again here could refuse an upgraded one
        // (`weft_core::project::hash::compute_definition_hash`).
        let project = fetch_definition(project_id, hash.clone()).await.with_context(|| format!("read original program of seed {run_id}"))?;
        next = seed_of(&birth.event).map(|seed| seed.parent);
        nearest_first.push(Ancestor { run_id, project, birth });
    }
    nearest_first.reverse();
    Ok(SeedChain::new(nearest_first, seed))
}

/// A run's fold, fed its record as it is read (a page at a time, then,
/// while its drive goes, only the rows that landed since), on top of what
/// it inherits (its seed chain, imported as its birth is folded). Rows go
/// in once each, in order: a row at or before the last one folded is not
/// this run's record read forward. Its readers (a worker carrying the run
/// on, a run's snapshot, its outputs) read values, never where the record
/// keeps them, so it folds without positions (`Fold::apply`). The
/// ancestors are reconstructed with theirs: what the run shows of them
/// names where their records keep it.
pub struct LiveFold {
    fold: Fold,
    /// The ancestors, reconstructed, until the birth imported from them.
    sources: Option<Arc<BTreeMap<RunId, Fold>>>,
    /// The place of the last event folded.
    folded: Option<crate::traits::Position>,
}

impl LiveFold {
    /// `fold`, with nothing folded yet, on top of its seed chain
    /// reconstructed through `page` ([`SeedChain::materialize`]).
    pub async fn new<P, Fut>(fold: Fold, chain: &SeedChain, page: &P) -> Result<Self>
    where
        P: Fn(RunId, Option<i32>) -> Fut,
        Fut: Future<Output = Result<Vec<JournalRow>>>,
    {
        Ok(Self::from(fold, Arc::new(chain.materialize(page).await?)))
    }

    /// `fold`, empty, fed on top of `sources`, its seed chain reconstructed
    /// (`SeedChain::materialize`), which it lets go of once its birth
    /// imported from them.
    pub fn from(fold: Fold, sources: Arc<BTreeMap<RunId, Fold>>) -> Self {
        Self { fold, sources: Some(sources), folded: None }
    }

    /// Fold `rows`, the run's next events in record order. The first event
    /// folded is the run's birth.
    pub fn apply<'a>(&mut self, rows: impl IntoIterator<Item = &'a JournalRow>) -> Result<()> {
        for row in rows {
            if let Some(folded) = self.folded {
                anyhow::ensure!(
                    row.position() > folded,
                    "event {}:{} of run {} is not past {}:{}, which its fold already took",
                    row.seq, row.index, self.fold.run_id(), folded.seq, folded.index
                );
            }
            match self.sources.take() {
                Some(sources) => apply_birth(&mut self.fold, row, None, &sources)?,
                None => {
                    self.fold.apply(&row.event);
                }
            }
            self.folded = Some(row.position());
        }
        Ok(())
    }

    /// The last record row folded, which a read of what landed since
    /// starts after (a read gives whole rows); `None` before any.
    pub fn folded(&self) -> Option<i32> {
        self.folded.map(|at| at.seq)
    }

    /// The run's state as the events so far say.
    pub fn snapshot(&self) -> RunSnapshot {
        self.fold.current_snapshot()
    }

    /// The run's state as the events folded say, the fold given up.
    pub fn into_snapshot(self) -> RunSnapshot {
        self.fold.into_snapshot()
    }

    /// The fold, its record folded. Refused when the record had no birth.
    pub fn into_fold(self) -> Result<Fold> {
        anyhow::ensure!(self.sources.is_none(), "run {} has no initial RunStarted row", self.fold.run_id());
        Ok(self.fold)
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

    /// `events` as a record whose every event is a row of its own.
    fn rows(events: &[RunEvent]) -> Vec<JournalRow> {
        events.iter().enumerate().map(|(seq, event)| JournalRow { seq: seq as i32, index: 0, event: event.clone() }).collect()
    }

    /// The records of `runs`, read a page of two rows at a time, with
    /// every page read noted.
    struct Records {
        runs: HashMap<RunId, Vec<RunEvent>>,
        read: std::sync::Mutex<Vec<(RunId, Option<i32>)>>,
    }

    impl Records {
        fn of(runs: HashMap<RunId, Vec<RunEvent>>) -> Self {
            Self { runs, read: Default::default() }
        }

        fn page(&self, run_id: RunId, after: Option<i32>) -> impl Future<Output = Result<Vec<JournalRow>>> {
            self.read.lock().unwrap().push((run_id, after));
            let page: Vec<JournalRow> = rows(&self.runs.get(&run_id).cloned().unwrap_or_default())
                .into_iter().filter(|row| after.is_none_or(|after| row.seq > after)).take(2).collect();
            async move { Ok(page) }
        }
    }

    /// Run `run_id`'s record `events` folded over `project` on top of
    /// `chain`, its ancestors' records read from `records`.
    async fn fold_seeded(run_id: RunId, project: Arc<ProjectDefinition>, chain: &SeedChain, records: &Records, events: &[RunEvent]) -> Result<RunSnapshot> {
        let mut live = LiveFold::new(Fold::new(run_id, project), chain, &|run, after| records.page(run, after)).await?;
        live.apply(&rows(events))?;
        Ok(live.into_snapshot())
    }

    /// The seed chain of `rows`, reading each ancestor's program among
    /// `programs` by its hash.
    async fn chain(child: &[RunEvent], records: &Records, programs: &[Arc<ProjectDefinition>]) -> Result<SeedChain> {
        seed_chain(child.first(), &|run, after| records.page(run, after),
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
        let records = Records::of(HashMap::from([(parent, parent_rows)]));
        let chain = chain(&rows, &records, &[program()]).await.unwrap();
        assert!(matches!(chain.ancestors[0].birth.event, RunEvent::RunStarted { .. }));
        assert_eq!(*records.read.lock().unwrap(), vec![(parent, None)], "the chain reads each ancestor's first page alone");
        assert_eq!(chain.handed_down[&parent], BTreeSet::from([Located::top("a"), Located::top("b")]));
        let snapshot = fold_seeded(child, cut, &chain, &records, &rows).await.unwrap();
        assert_eq!(
            *records.read.lock().unwrap(),
            vec![(parent, None), (parent, None), (parent, Some(1)), (parent, Some(3)), (parent, Some(5)), (parent, Some(6))],
            "the reconstruction reads the ancestor a page at a time"
        );
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
        let records = Records::of(HashMap::from([(a,a_rows),(b,b_rows)]));
        let chain = chain(&rows,&records,&[program(),b_cut]).await.unwrap();
        assert_eq!(chain.handed_down[&a], BTreeSet::from([Located::top("a")]), "the grandchild and its parent take `a` from its original run");
        assert_eq!(chain.handed_down[&b], BTreeSet::from([Located::top("b")]));
        let snapshot = fold_seeded(c,c_cut,&chain,&records,&rows).await.unwrap();
        assert_eq!(snapshot.firings["a"][0].inherited_from,Some(a));
        assert_eq!(snapshot.firings["b"][0].inherited_from,Some(b));
        assert_eq!(snapshot.firings["b"][0].received.input["in"].as_ref(),&json!("A"));
    }

    /// An ancestor is reconstructed keeping only what the runs after it
    /// take from it: a child that reuses `a` leaves `b` and `c` behind,
    /// their records and their outputs.
    #[tokio::test]
    async fn an_ancestor_keeps_only_what_is_handed_down() {
        let parent = run_id(1);
        let mut parent_rows = vec![birth(parent, None, "a", &program(), &Given::default())];
        parent_rows.extend(result(parent, "a", "A"));
        parent_rows.extend(result(parent, "b", "B"));
        parent_rows.extend(result(parent, "c", "C"));
        let (cut, given) = cut(&["a", "b", "c"], &[("a", "A", parent)]);
        let rows = vec![birth(run_id(2), seed(parent, &[("a", parent)]), "b", &cut, &given)];
        let records = Records::of(HashMap::from([(parent, parent_rows)]));
        let chain = chain(&rows, &records, &[program()]).await.unwrap();
        let sources = chain.materialize(&|run, after| records.page(run, after)).await.unwrap();
        let kept = sources[&parent].snapshot();
        assert_eq!(kept.firings.keys().collect::<Vec<_>>(), vec!["a"], "only the reused place's record is kept");
        let wires = sources[&parent].output_wires().unwrap();
        assert_eq!(wires.iter().map(|wire| wire.node.as_str()).collect::<Vec<_>>(), vec!["a"], "only its output history");
        let snapshot = fold_seeded(run_id(2), cut, &chain, &records, &rows).await.unwrap();
        assert_eq!(snapshot.firings["a"][0].inherited_from, Some(parent));
    }

    #[tokio::test]
    async fn absent_and_cyclic_ancestors_are_refused() {
        let rows = vec![birth(run_id(2),seed(run_id(1),&[]),"a",&program(),&Given::default())];
        assert!(chain(&rows,&Records::of(HashMap::new()),&[program()]).await.unwrap_err().to_string().contains("no journal rows"));
        let ancestor = vec![birth(run_id(1),seed(run_id(2),&[]),"a",&program(),&Given::default())];
        assert!(chain(&rows,&Records::of(HashMap::from([(run_id(1),ancestor)])),&[program()]).await.unwrap_err().to_string().contains("loops"));
    }

    #[tokio::test]
    async fn a_fresh_run_reads_no_ancestors() {
        let birth = birth(run_id(1),None,"a", &program(), &Given::default());
        let chain = seed_chain(Some(&birth), &|_, _| async { bail!("unexpected journal read") },
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
        let records = Records::of(HashMap::from([(parent,parent_rows)]));
        let chain = chain(&rows,&records,&[program()]).await.unwrap();
        assert!(fold_seeded(run_id(2),program(),&chain,&records,&rows).await.err().unwrap().to_string().contains("no complete reusable result"));
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
        let records = Records::of(HashMap::from([(parent, parent_rows)]));
        for (events, program) in [(fresh, program()), (seeded, seeded_cut)] {
            let run = events[0].run_id();
            let chain = chain(&events, &records, &[self::program()]).await.unwrap();
            let whole = said(&fold_seeded(run, program.clone(), &chain, &records, &events).await.unwrap());
            let rows = rows(&events);
            for split in 1..=rows.len() {
                let mut live = LiveFold::new(Fold::new(run, program.clone()), &chain, &|run, after| records.page(run, after)).await.unwrap();
                live.apply(&rows[..split]).unwrap();
                assert_eq!(live.folded(), Some(rows[split - 1].seq));
                live.apply(&rows[split..]).unwrap();
                assert_eq!(said(&live.snapshot()), whole, "split at {split}");
                assert!(live.apply(&rows[..1]).is_err(), "a row already folded is not this run's record read forward");
            }
        }
    }

}
