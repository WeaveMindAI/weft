//! How much memory painting a long run's record takes: a run of a loop
//! fed by a stream, `ITERATIONS` iterations of nine events each (the
//! default makes 270,000 events), each value `VALUE_BYTES` (a kilobyte by
//! default), read the way a replay reads it (a page at a time,
//! `weft_journal::record::read_page`'s rows, decoded), folded into the
//! run's projection and every event written by reference as the replay
//! writes it, then dropped. Prints the process's peak resident memory.
//!
//! The loop gathers what its body formats into its output list, which it
//! hands on as a value: that list grows with the run, item by item. With
//! `no-gather` as the third argument the loop gathers nothing, and what is
//! left is everything else a reader holds.
//!
//! `scripts/replay-memory.sh` runs it. It measures what one reader holds,
//! not a whole dispatcher: the run's record is generated here, never read
//! from Postgres.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use weft_core::live_event::IdentifiedEvent;
use weft_core::project::ProjectDefinition;
use weft_core::RunId;
use weft_dispatcher::projection::{ProgramLookup, RunProjector};
use weft_journal::record::{RawRecord, RunLogRow, PAGE_ROWS};
use weft_journal::RunEvent;

/// The system allocator, counting the bytes held: what the reader really
/// holds, beside the resident memory, which also counts what the allocator
/// keeps after it was freed.
struct Counting;

static HELD: AtomicUsize = AtomicUsize::new(0);
static MOST_HELD: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let held = HELD.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        MOST_HELD.fetch_max(held, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HELD.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn main() {
    let iterations: u32 = std::env::args().nth(1).map_or(30_000, |n| n.parse().expect("the number of iterations"));
    let value_bytes: usize = std::env::args().nth(2).map_or(1024, |n| n.parse().expect("the bytes of each value"));
    let gathers = match std::env::args().nth(3).as_deref() {
        None => true,
        Some("no-gather") => false,
        Some(other) => panic!("the third argument is `no-gather` or nothing, not {other:?}"),
    };
    let run_id = RunId::from_u128(1);
    let started = std::time::Instant::now();
    let mut projector = RunProjector::new(run_id, ProgramLookup::Found(program(gathers)), uuid::Uuid::nil());
    let (mut on_record, mut written, mut bytes) = (0usize, 0usize, 0usize);
    // The record is made a page at a time, as a reader gets it: the
    // measure is of the reader, never of a record held whole.
    let mut page = Vec::new();
    let mut paint = |page: &mut Vec<RunLogRow>| {
        let record = RawRecord { rows: std::mem::take(page) };
        for row in record.decode(run_id).expect("the record reads") {
            for event in IdentifiedEvent::projected(row.position(), projector.project(&row)) {
                let event = IdentifiedEvent { event_id: event.event_id, event: event.event.by_reference() };
                bytes += serde_json::to_vec(&event).expect("an event writes").len();
                written += 1;
            }
        }
    };
    for event in record(run_id, iterations, value_bytes) {
        page.push(RunLogRow::of(on_record as i32, std::slice::from_ref(&event)));
        on_record += 1;
        if page.len() == PAGE_ROWS as usize {
            paint(&mut page);
        }
    }
    paint(&mut page);
    println!("{on_record} events on record, {written} painted ({} MiB written by reference) in {:?}", bytes / (1 << 20), started.elapsed());
    println!("peak resident memory: {} MiB", peak_rss_kib() / 1024);
    println!("most memory held at once: {} MiB, held at the end: {} MiB", MOST_HELD.load(Ordering::Relaxed) >> 20, HELD.load(Ordering::Relaxed) >> 20);
}

/// The record of a run of [`program`] over `iterations` items, event by
/// event, each a row of its own.
fn record(run_id: RunId, iterations: u32, value_bytes: usize) -> impl Iterator<Item = RunEvent> {
    let value = move |i: u32| Arc::new(json!(format!("{i:08}").repeat(value_bytes.div_ceil(8))));
    let frame = |i: u32| vec![weft_core::frames::Frame::Loop { index: i }];
    let start = vec![
        RunEvent::RunStarted {
            run_id, project_id: uuid::Uuid::nil(), entry_node: "gen".into(), phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()), binary_hash: None, cut_from: None, source_version: None,
            run_kind: weft_core::run::RunKind::Project, seed: None, instance: None, stand_in: None, fired_trigger: None,
            settings: Default::default(), instance_values: Default::default(), picks: Default::default(), at_unix: 0, given: Default::default(),
        },
        RunEvent::NodeStarted { run_id, node_id: "gen".into(), frames: vec![], at_unix: 0 },
        RunEvent::NodeStarted { run_id, node_id: "lp__in".into(), frames: vec![], at_unix: 1 },
        RunEvent::LoopInstantiated { run_id, group_id: "lp".into(), parent_frames: vec![], at_unix: 1 },
    ];
    let iteration = move |i: u32| {
        let emission = uuid::Uuid::new_v4();
        let item = weft_core::run::emission::pulse_id(emission, "gen.rows->lp__in.rows", false);
        [
            RunEvent::PortEmitted { run_id, emission_id: emission, node_id: "gen".into(), frames: vec![], port: "rows".into(), value: Arc::new(json!(i)), at_unix: 1 },
            RunEvent::LoopIterationLaunched { run_id, group_id: "lp".into(), parent_frames: vec![], index: i, stream_pulse: Some(item.to_string()), at_unix: 1 },
            RunEvent::NodeStarted { run_id, node_id: "fmt".into(), frames: frame(i), at_unix: 2 },
            RunEvent::PortEmitted { run_id, emission_id: uuid::Uuid::new_v4(), node_id: "fmt".into(), frames: frame(i), port: "text".into(), value: value(i), at_unix: 2 },
            RunEvent::NodeCompleted { run_id, node_id: "fmt".into(), frames: frame(i), at_unix: 2 },
            RunEvent::NodeStarted { run_id, node_id: "lp__out".into(), frames: frame(i), at_unix: 3 },
            RunEvent::LoopOutFired { run_id, group_id: "lp".into(), parent_frames: vec![], index: i, at_unix: 3 },
            RunEvent::NodeCompleted { run_id, node_id: "lp__out".into(), frames: frame(i), at_unix: 3 },
            RunEvent::CostReported {
                run_id, node_id: "fmt".into(), frames: frame(i), cost_id: format!("c{i}"), service: "s".into(), model: None,
                amount_usd: Some(0.001), billed: false, origin: weft_core::CredentialOwner::Platform, metadata: Value::Null, charge: None, at_unix: 3,
            },
        ]
    };
    let end = vec![
        RunEvent::NodeCompleted { run_id, node_id: "gen".into(), frames: vec![], at_unix: 4 },
        RunEvent::LoopStreamEnded { run_id, group_id: "lp".into(), parent_frames: vec![], end: weft_core::generator::StreamEnd::Finished, at_unix: 4 },
        RunEvent::LoopTerminated { run_id, group_id: "lp".into(), parent_frames: vec![], reason: weft_core::primitive::LoopTerminationReason::OverExhausted, at_unix: 4 },
        RunEvent::NodeCompleted { run_id, node_id: "lp__in".into(), frames: vec![], at_unix: 4 },
        RunEvent::RunCompleted { run_id, at_unix: 5 },
    ];
    start.into_iter().chain((0..iterations).flat_map(iteration)).chain(end)
}

/// A stream feeding a loop whose body formats each item; the loop
/// gathers what it formatted when `gathers`.
fn program(gathers: bool) -> Arc<ProjectDefinition> {
    let node = |id: &str, ty: &str, inputs: Value, outputs: Value, scope: Value, boundary: Value, config: Value| {
        json!({
            "id": id, "nodeType": ty, "label": null, "config": config, "position": { "x": 0.0, "y": 0.0 },
            "inputs": inputs, "outputs": outputs, "features": {}, "scope": scope, "groupBoundary": boundary,
            "requiresInfra": false, "images": []
        })
    };
    let port = |name: &str, ty: &str, required: bool| json!({ "name": name, "portType": ty, "required": required });
    let edge = |source: &str, sh: &str, target: &str, th: &str| json!({ "id": format!("{source}.{sh}->{target}.{th}"), "source": source, "sourceHandle": sh, "target": target, "targetHandle": th, "via": [] });
    let edges: Vec<Value> = [edge("gen", "rows", "lp__in", "rows"), edge("lp__in", "rows", "fmt", "item")]
        .into_iter()
        .chain(gathers.then(|| edge("fmt", "text", "lp__out", "text")))
        .collect();
    Arc::new(
        serde_json::from_value(json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                node("gen", "T", json!([]), json!([port("rows", "Generator[Number]", true)]), json!([]), Value::Null, Value::Null),
                node("lp__in", "LoopIn", json!([port("rows", "Generator[Number]", true), port("_should_flow", "Boolean", false)]),
                    json!([port("rows", "Number", true), port("index", "Number", true)]), json!([]),
                    json!({ "groupId": "lp", "role": "In" }), json!({ "parallel": false, "over": ["rows"], "carry": [] })),
                node("fmt", "T", json!([port("item", "Number", true)]), json!([port("text", "String", true)]), json!(["lp"]), Value::Null, Value::Null),
                node("lp__out", "LoopOut",
                    if gathers { json!([port("text", "String", false), port("done", "Boolean", false)]) } else { json!([port("done", "Boolean", false)]) },
                    if gathers { json!([port("text", "List[String]", true)]) } else { json!([]) }, json!([]),
                    json!({ "groupId": "lp", "role": "Out" }), json!({ "parentId": null })),
            ],
            "edges": edges,
            "groups": [{ "id": "lp", "kind": "loop", "loopConfig": {}, "nodeIds": ["fmt"] }],
        }))
        .expect("the program reads"),
    )
}

/// The process's peak resident memory, in KiB (`VmHWM`).
fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("this measures on Linux")
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("the status names the peak")
}
