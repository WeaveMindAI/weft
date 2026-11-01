//! What the dispatcher holds in memory to answer `weft status` and to
//! plan `weft infra upgrade` for a large program, the way it did before a
//! build named a file's content (`weft_core::content`) and recorded its
//! triggers' cuts, and the way it does now, measured with an allocator
//! that counts live bytes and their peak.
//!
//! A measurement, not a check: ignored, and run by hand. The counter is
//! the whole process's, which is why it is the only test of its binary:
//!
//! ```text
//! cargo nextest run --workspace -E 'rdeps(weft-dispatcher) & test(deploy_memory)' --run-ignored only --no-capture
//! ```
//!
//! The program is compiled from source by the real compiler and catalog:
//! `TRIGGERS` Route triggers on one database, each answering through a
//! query, one of which reads a `SQL_BYTES` SQL file with `@file`, the
//! shape of the program whose deploy ran its dispatcher out of memory.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use weft_core::ProjectDefinition;

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

/// The most a step had live at once, over what was live before it.
fn peak_of<T>(step: impl FnOnce() -> T) -> (T, usize) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = step();
    (out, PEAK.load(Ordering::Relaxed) - before)
}

fn mib(bytes: usize) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

const TRIGGERS: usize = 82;
const SQL_BYTES: usize = 2 * 1024 * 1024;

/// `TRIGGERS` routes on one database, each answering through a query; the
/// first reads `sql/import.sql`.
fn source() -> String {
    let mut source = String::from("pg = PostgresDatabase { database: \"app\" }\n");
    for i in 0..TRIGGERS {
        let query = if i == 0 { "@file(\"sql/import.sql\")".to_string() } else { format!("\"select * from t{i} where tenant = $tenant\"") };
        source.push_str(&format!(
            "r{i} = Route {{ path: \"t{i}/items\", method: \"POST\" }}\n\
             q{i} = PostgresExecuteQuery(tenant: String) {{ query: {query}, tenant: r{i}.path }}\n\
             q{i}.account = pg.access\n\
             a{i} = Reply {{ status: 200 }}\n\
             a{i}.body = q{i}.rows\n"
        ));
    }
    source
}

/// The SQL file the first query reads: `SQL_BYTES` of inserts, then the
/// one placeholder its query takes.
fn sql() -> String {
    let line = "insert into import values (1, 'a row of the import');\n";
    format!("{}select $tenant;\n", line.repeat(SQL_BYTES / line.len()))
}

/// The program as a build makes it now: the SQL named by its content.
fn named() -> ProjectDefinition {
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("sql")).unwrap();
    std::fs::write(dir.path().join("sql/import.sql"), sql()).unwrap();
    weft_compiler::compile_program(&source(), uuid::Uuid::new_v4(), weft_compiler::CompileFs::disk(dir.path()), &catalog)
        .unwrap_or_else(|diagnostics| panic!("the fixture compiles: {diagnostics:?}"))
}

/// The same program as a build made it before: the SQL inlined where its
/// name is.
fn inlined(named: &ProjectDefinition, sql: &str) -> ProjectDefinition {
    let mut program = named.clone();
    let values = weft_core::content::refs_in_program(&program)
        .into_iter()
        .map(|content| (content.id(), std::sync::Arc::new(content.value_of(sql.as_bytes()).unwrap())))
        .collect();
    weft_core::content::resolve_program(&mut program, &values).unwrap();
    program
}

/// What one `weft status` did with the program before: read it whole from
/// the store, walk every trigger's cut (each building two programs and
/// hashing them) for the registered build, and hash it.
fn status_before(json: &str) -> usize {
    let program: ProjectDefinition = serde_json::from_str(json).unwrap();
    let cuts = weft_core::run_spec::trigger_cuts(&program);
    let hash = weft_core::project::hash::compute_definition_hash(&program).unwrap();
    cuts.len() + hash.len()
}

/// What one does now: read the program (a parse, once per process, then
/// kept), and the cuts its registration recorded.
fn status_now(json: &str, recorded: &weft_core::run_spec::TriggerCuts) -> usize {
    let program: ProjectDefinition = serde_json::from_str(json).unwrap();
    program.nodes.len() + recorded.len()
}

#[test]
#[ignore = "a measurement: run by hand, alone (see the module doc)"]
fn deploy_memory() {
    let named = named();
    let inlined = inlined(&named, &sql());
    let (named_json, inlined_json) = (serde_json::to_string(&named).unwrap(), serde_json::to_string(&inlined).unwrap());
    println!(
        "program: {} nodes, {TRIGGERS} triggers; stored as {} with the SQL inlined, {} with it named",
        named.nodes.len(),
        mib(inlined_json.len()),
        mib(named_json.len())
    );
    let recorded = weft_core::run_spec::trigger_cuts(&named);
    drop((named, inlined));

    let (_, before) = peak_of(|| status_before(&inlined_json));
    let (_, now) = peak_of(|| status_now(&named_json, &recorded));
    println!("one `weft status`, peak: before {}, now {}", mib(before), mib(now));

    // An upgrade read the program about four times and walked every cut
    // twice; now it reads it once and the recorded cuts.
    let (_, before) = peak_of(|| (0..4).map(|_| status_before(&inlined_json)).sum::<usize>());
    let (_, now) = peak_of(|| status_now(&named_json, &recorded));
    println!("one `weft infra upgrade`'s reads, peak: before {}, now {}", mib(before), mib(now));

    // The definition hash, of the program as it was and as it is.
    let parsed: ProjectDefinition = serde_json::from_str(&inlined_json).unwrap();
    let (_, before) = peak_of(|| weft_core::project::hash::compute_definition_hash(&parsed).unwrap());
    drop(parsed);
    let parsed: ProjectDefinition = serde_json::from_str(&named_json).unwrap();
    let (_, now) = peak_of(|| weft_core::project::hash::compute_definition_hash(&parsed).unwrap());
    println!("one definition hash, peak: of the inlined program {}, of the named one {}", mib(before), mib(now));
}
