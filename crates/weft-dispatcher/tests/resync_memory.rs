//! What the dispatcher holds in memory while it brings back the shared
//! triggers of a large program (a resync, a maintenance lift), measured
//! with an allocator that counts live bytes and their peak.
//!
//! A measurement, not a check: ignored, and run by hand. The counter is
//! the whole process's, which is why it is the only test of its binary:
//!
//! ```text
//! cargo nextest run --workspace -E 'rdeps(weft-dispatcher) & test(resync_memory)' --run-ignored only --no-capture
//! ```
//!
//! The program is compiled from source by the real compiler and catalog:
//! `TRIGGERS` Route triggers sharing one PostgresDatabase, each answering
//! through a query, a Format and an ExecPython with a few kilobytes of
//! text each, the shape of a site's backend. Every step below is the code
//! a bring-back runs (`weft_core::run_spec`, `weft_dispatcher::store`,
//! `weft_journal::Fold`), called the way `weft_dispatcher::api::project`
//! calls it, and the program cache (`DispatcherState::programs`) filled
//! the two ways it grows: one cut per trigger fired, one program per
//! build.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use weft_core::frames::Located;
use weft_core::project::hash::ProgramIdentity;
use weft_core::ProjectDefinition;
use weft_dispatcher::store::TriggerBake;
use weft_journal::RunEvent;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let at = unsafe { System.alloc(layout) };
        if !at.is_null() {
            grew(layout.size());
        }
        at
    }

    unsafe fn dealloc(&self, at: *mut u8, layout: Layout) {
        unsafe { System.dealloc(at, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let at = unsafe { System.alloc_zeroed(layout) };
        if !at.is_null() {
            grew(layout.size());
        }
        at
    }

    unsafe fn realloc(&self, at: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(at, layout, size) };
        if !moved.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            grew(size);
        }
        moved
    }
}

fn grew(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// What one step left held (`held`) and the most it had live at once
/// (`peak`), both over what was live before it.
struct Took {
    held: isize,
    peak: usize,
}

fn measure<T>(step: impl FnOnce() -> T) -> (T, Took) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = step();
    let took = Took { held: LIVE.load(Ordering::Relaxed) as isize - before as isize, peak: PEAK.load(Ordering::Relaxed) - before };
    (out, took)
}

fn mib(bytes: impl TryInto<i64>) -> String {
    let bytes: i64 = bytes.try_into().unwrap_or(i64::MAX);
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn report(what: &str, took: &Took) {
    println!("{what:<62} held {:>10}   peak {:>10}", mib(took.held as i64), mib(took.peak as i64));
}

const TRIGGERS: usize = 63;

/// `words` words of plain text, distinct per `seed`.
fn text(seed: usize, words: usize) -> String {
    (0..words).map(|w| format!("w{seed}x{w}")).collect::<Vec<_>>().join(" ")
}

/// A site's backend: `TRIGGERS` routes on one database, each with a few
/// kilobytes of query, template and code.
fn source() -> String {
    let mut source = String::from("pg = PostgresDatabase { database: \"app\" }\n");
    for i in 0..TRIGGERS {
        let query = format!("select {} from t{i} where tenant = $tenant", text(i, 150).replace(' ', ", "));
        let template = format!("{} {{{{p}}}} {}", text(i + 1000, 200), text(i + 2000, 100));
        let code = format!("return {{'out': x + ' {}'}}", text(i + 3000, 250));
        source.push_str(&format!(
            "r{i} = Route {{ path: \"t{i}/items\", method: \"POST\" }}\n\
             q{i} = PostgresExecuteQuery(tenant: String) {{ query: \"{query}\", tenant: r{i}.path }}\n\
             q{i}.account = pg.access\n\
             f{i} = Format(p: String) {{ template: \"{template}\", p: r{i}.path }}\n\
             x{i} = ExecPython(x: String) -> (out: String) {{ code: \"{code}\", x: f{i}.text }}\n\
             a{i} = Reply {{ status: 200 }}\n\
             a{i}.body = x{i}.out\n"
        ));
    }
    source
}

fn program() -> ProjectDefinition {
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    weft_compiler::compile_program(&source(), uuid::Uuid::new_v4(), weft_compiler::CompileFs::disk(dir.path()), &catalog)
        .unwrap_or_else(|diagnostics| panic!("the fixture compiles: {diagnostics:?}"))
}

/// The setup run of every trigger, as its record reads once it completed:
/// its birth, one capture per trigger holding that trigger's settings, its
/// ending.
fn setup_record(project: &ProjectDefinition, registered: &str, cut_hash: &str) -> Vec<RunEvent> {
    let run_id = uuid::Uuid::new_v4();
    let mut events = vec![RunEvent::RunStarted {
        given: Default::default(),
        run_id,
        project_id: project.id,
        entry_node: "r0".into(),
        phase: weft_core::context::Phase::TriggerSetup,
        definition_hash: Some(cut_hash.into()),
        binary_hash: Some("binary".into()),
        source_version: Some("source".into()),
        run_kind: weft_core::run::RunKind::Project,
        cut_from: Some(registered.into()),
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        at_unix: 1,
        settings: weft_core::run_settings::RunSettings::bookkeeping(),
    }];
    for node in project.nodes.iter().filter(|node| node.features.is_trigger) {
        events.push(RunEvent::TriggerCaptured {
            run_id,
            node_id: node.id.clone(),
            spec: weft_core::primitive::SignalSpec::of_kind("timer", serde_json::json!({"spec": {"kind": "after", "duration_ms": 100}})),
            port_snapshot: serde_json::to_value(&node.config).unwrap(),
            at_unix: 2,
        });
    }
    events.push(RunEvent::RunCompleted { run_id, at_unix: 3 });
    events
}

#[test]
#[ignore = "a measurement: run by hand, alone (see the module doc)"]
fn resync_memory() {
    let built = program();
    let json = serde_json::to_string(&built).unwrap();
    let triggers: Vec<String> = built.nodes.iter().filter(|node| node.features.is_trigger).map(|node| node.id.clone()).collect();
    assert_eq!(triggers.len(), TRIGGERS);
    let registered_hash = weft_core::project::hash::compute_definition_hash(&built).unwrap();
    println!("program: {} nodes, {} triggers, stored as {} of JSON", built.nodes.len(), triggers.len(), mib(json.len()));
    drop(built);

    // One parse of the stored program: the unit every other number is read against.
    let (registered, took) = measure(|| serde_json::from_str::<ProjectDefinition>(&json).unwrap());
    report("the registered program, parsed (one read)", &took);

    // The database's saved outputs, as its copy being up gives them: a fire
    // reads them baked.
    let saved: weft_core::infra::bake::Saved = weft_core::project::infra_places(&registered)
        .into_iter()
        .filter_map(|place| {
            let node = registered.nodes.iter().find(|node| node.id == place.id)?;
            (!node.baked_outputs.is_empty()).then(|| (place, node.baked_outputs.iter().map(|port| (port.clone(), serde_json::Value::Null)).collect()))
        })
        .collect();
    let seeds: Vec<Located> = triggers.iter().map(|trigger| Located::top(trigger.as_str())).collect();
    let ((setup, _kicks), took) = measure(|| weft_core::run_spec::setup_cut(&registered, &seeds).unwrap());
    report("the setup cut of every trigger (setup_cut)", &took);

    let (_, took) = measure(|| {
        for trigger in &triggers {
            let fire = weft_core::run_spec::compute_trigger_fire(&registered, trigger, &serde_json::Value::Null, None, &saved);
            fire.unwrap();
        }
    });
    report("a fire cut per trigger, one at a time (cannot_come_on)", &took);

    let (_, took) = measure(|| {
        let cuts: Vec<String> = triggers.iter().map(|trigger| weft_core::run_spec::trigger_cut(&registered, trigger).unwrap().fingerprint).collect();
        cuts.len()
    });
    report("a trigger cut per trigger, fingerprints kept (trigger_cuts_of)", &took);

    let (stored, took) = measure(|| weft_journal::record::StoredProgram::of(registered.id, &setup).unwrap());
    report("the setup cut stored for its birth (runs_program)", &took);

    // Over a day of fires, the program cache keeps the program each run
    // ran, by its hash: a fire runs its trigger's cut, so every trigger's
    // cut is an entry of its own.
    let fill = |cache: &weft_core::content_cache::ContentCache<(uuid::Uuid, String), ProjectDefinition>| {
        cache.put((registered.id, registered_hash.clone()), Arc::new(serde_json::from_str::<ProjectDefinition>(&json).unwrap()));
        for trigger in &triggers {
            let fire = weft_core::run_spec::compute_trigger_fire(&registered, trigger, &serde_json::Value::Null, None, &saved).unwrap();
            let hash = weft_core::project::hash::compute_definition_hash(&fire.program).unwrap();
            // Read back from the store the way `DispatcherState::program` does.
            let read = serde_json::from_str::<ProjectDefinition>(&serde_json::to_string(&fire.program).unwrap()).unwrap();
            drop(fire);
            cache.put((registered.id, hash), Arc::new(read));
        }
    };
    // And over a day of builds, every build registers the program under a
    // hash of its own, which an activation, a resync and every run of it
    // reads.
    let builds = |cache: &weft_core::content_cache::ContentCache<(uuid::Uuid, String), ProjectDefinition>| {
        for build in 0..64 {
            cache.put((registered.id, format!("build-{build}")), Arc::new(serde_json::from_str::<ProjectDefinition>(&json).unwrap()));
        }
    };
    let by_count = weft_core::content_cache::ContentCache::new(64);
    let by_weight = weft_core::content_cache::ContentCache::weighed(weft_dispatcher::state::PROGRAMS_JSON_BYTES, weft_dispatcher::state::program_json_bytes);
    for (bound, cache) in [("by count (64, before)", &by_count), ("by weight (after)", &by_weight)] {
        let (_, took) = measure(|| fill(cache));
        report(&format!("the program cache once every trigger fired, {bound}"), &took);
        cache.forget_all();
        let (_, took) = measure(|| builds(cache));
        report(&format!("the program cache after 64 builds, {bound}"), &took);
        cache.forget_all();
    }

    // While anybody on the process follows the project (a setup's own wait
    // does), the live view keeps a projection of every run that writes a
    // row, each a fold over that run's program.
    let setup = Arc::new(setup);
    let (fold, took) = measure(|| weft_journal::Fold::new(uuid::Uuid::new_v4(), setup.clone()).showing());
    report("the live view's projection of the setup run", &took);
    drop(fold);
    let fires: Vec<Arc<ProjectDefinition>> = triggers
        .iter()
        .map(|trigger| Arc::new(weft_core::run_spec::compute_trigger_fire(&registered, trigger, &serde_json::Value::Null, None, &saved).unwrap().program))
        .collect();
    let (folds, took) = measure(|| fires.iter().map(|fire| weft_journal::Fold::new(uuid::Uuid::new_v4(), fire.clone()).showing()).collect::<Vec<_>>());
    report("the live view's projections of one fire of each trigger", &took);
    drop(folds);
    drop(fires);

    // The setup's record read whole, its bake, the earlier bake read back,
    // the two merged and written: what `run_trigger_setup` and
    // `finish_trigger_setup` hold at once.
    let identity = ProgramIdentity { definition_hash: registered_hash.clone(), binary_hash: "binary".into(), implementations: Default::default() };
    let record = setup_record(&registered, &registered_hash, &stored.definition_hash);
    let (_, took) = measure(|| {
        // What `events_log` hands back: the record, decoded.
        let events: Vec<RunEvent> = record.clone();
        let bake = TriggerBake::from_events(&events, &identity).unwrap().unwrap();
        let prior_json = serde_json::to_string(&bake).unwrap();
        let prior: TriggerBake = serde_json::from_str(&prior_json).unwrap();
        let merged = prior.refreshed_by(&bake);
        let written = serde_json::to_string(&merged).unwrap();
        (events.len(), written.len())
    });
    report("the setup's record, its bake, the prior, merged, written", &took);
}
