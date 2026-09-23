//! Layer-3: cutting a run at a group inside an included file that is fed
//! from outside, the shape a connection wired into an include takes. The
//! compiler is real; the checks are on the selection the run is held to
//! and on whether the run is refused for an input left with nothing.
use std::collections::BTreeMap;

use weft_compiler::weft_compiler::compile;
use weft_compiler::CompileFs;
use weft_core::frames::Located;
use weft_core::run_spec::{refuse_unfed, resolve_spec, RunSpec};

/// `hear` holds a group `note` that reads both of `hear`'s inputs, and a
/// step `other` that only reads `db`. `db` also feeds `elsewhere`, a
/// branch outside `hear` altogether, and `after`, which reads `hear`'s
/// result AND `db`: so a run downstream of `hear.note` reaches `db` on its
/// own, as the program Tangle hit this in does. `text` comes out of a
/// group, `words`, the way a connection comes out of an `accounts` group.
const HEAR: &str = "Group(db: String, text: String) -> (out: String) {
  note = Group(db: String, text: String) -> (out: String) {
    f = Format(d: String, t: String) { template: \"{{d}}{{t}}\", d: self.db, t: self.text }
    self.out = f.text
  }
  other = Format(d: String) { template: \"{{d}}\", d: self.db }
  note.db = self.db
  note.text = self.text
  self.out = note.out
}
";

const MAIN: &str = "db = Text { value: \"db\" }
words = Group() -> (text: String) {
  w = Text { value: \"t\" }
  self.text = w.value
}
hear = @include(\"hear.weft\")
hear.db = db.value
hear.text = words.text
d = Debug { data: hear.out }
elsewhere = Debug { data: db.value }
after = Format(a: String, d: String) { template: \"{{a}}{{d}}\", a: hear.out, d: db.value }
";

fn program() -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("hear.weft"), HEAR).unwrap();
    let mut project = compile(MAIN, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    weft_compiler::enrich::enrich(&mut project, &catalog).expect("enrich");
    project
}

fn starts(names: &[&str]) -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
    names.iter().map(|name| (name.to_string(), BTreeMap::new())).collect()
}

/// A start is a boundary: `--target` keeps what the target needs from
/// the start down, and never what feeds the start, even when something
/// else in the run reaches it (`after` reads `db`). Without `feed`, the
/// start's own ports get nothing, and the run is refused.
#[test]
fn a_start_still_cuts_what_feeds_it_under_a_target() {
    let project = program();
    let spec = RunSpec { from: starts(&["hear.note"]), target: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    assert!(!resolved.selection.nodes.contains(&Located::top("db")), "what feeds the start stays out");
    assert!(refuse_unfed(&project, &resolved.selection, &spec).is_err());

    let spec = RunSpec { feed: vec!["hear.note".into()], ..spec };
    let resolved = resolve_spec(&spec, &project).unwrap();
    assert!(resolved.selection.nodes.contains(&Located::top("db")), "fed: one level up");
    refuse_unfed(&project, &resolved.selection, &spec).expect("fed");
}

/// A start upstream of another is never dropped: naming the feeders and
/// the group as starts runs all of them, and the target still keeps the
/// other branches `db` feeds out of the run.
#[test]
fn a_start_upstream_of_another_start_stays_in_the_cut() {
    let project = program();
    let spec = RunSpec { from: starts(&["db", "words.w", "hear.note"]), target: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    for kept in ["db", "words.w"] {
        assert!(resolved.selection.nodes.contains(&Located::top(kept)), "{kept} is a start and stays");
    }
    assert!(!resolved.selection.nodes.contains(&Located::top("elsewhere")), "the target drops db's other branch");
    refuse_unfed(&project, &resolved.selection, &spec).expect("the group is fed by the starts above it");
}

/// `feed` runs the node feeding each start port nobody handed, found
/// through the doors on the way (out of `words` too), and nothing beside
/// it: `db`'s other branch and `hear.other` stay out.
#[test]
fn feed_runs_what_feeds_a_start_and_nothing_else() {
    let project = program();
    let spec = RunSpec { from: starts(&["hear.note"]), feed: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    for fed in ["db", "words.w"] {
        assert!(resolved.selection.nodes.contains(&Located::top(fed)), "{fed} feeds the start and runs");
    }
    assert!(!resolved.selection.nodes.contains(&Located::top("elsewhere")));
    assert!(!resolved.selection.nodes.iter().any(|place| place.id.ends_with("other")), "the feeder runs, not its other consumers");
    refuse_unfed(&project, &resolved.selection, &spec).expect("every port of the start is fed");
}

/// A handed value wins, so the port it covers is not fed: only `words.w` runs,
/// and the run is still complete. (Aimed at the group: with no target,
/// `after` would run too and bring `db` along as its own input.)
#[test]
fn feed_leaves_a_handed_port_to_its_value() {
    let project = program();
    let from = BTreeMap::from([("hear.note".to_string(), BTreeMap::from([("db".to_string(), serde_json::json!("by hand"))]))]);
    let spec = RunSpec { from, feed: vec!["hear.note".into()], target: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    assert!(!resolved.selection.nodes.contains(&Located::top("db")), "db was handed, not fed");
    assert!(resolved.selection.nodes.contains(&Located::top("words.w")), "fed out through the group's door");
    refuse_unfed(&project, &resolved.selection, &spec).expect("db by hand, text by its feeder");
}

/// `feed` works on a `--group` start too, and names only starts.
#[test]
fn feed_on_a_group_start_and_only_on_starts() {
    let project = program();
    let spec = RunSpec { group: Some(("hear.note".into(), BTreeMap::new())), feed: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    assert!(resolved.selection.nodes.contains(&Located::top("db")));
    refuse_unfed(&project, &resolved.selection, &spec).expect("the group run is fed");

    let spec = RunSpec { target: vec!["hear.note".into()], feed: vec!["hear.note".into()], ..RunSpec::whole("x") };
    let refusal = resolve_spec(&spec, &project).unwrap_err().to_string();
    assert!(refusal.contains("only a start can be fed"), "{refusal}");
}

/// `feed` follows a value through as many doors as it crosses: out of a
/// group nested in another group, and out of a loop, which then runs
/// whole (a loop is never cut inside).
#[test]
fn feed_reaches_through_nested_groups_and_takes_a_loop_whole() {
    let src = "outer = Group() -> (text: String) {
  inner = Group() -> (text: String) {
    w = Text { value: \"t\" }
    self.text = w.value
  }
  self.text = inner.text
}
l = Loop(items: List[String]) -> (out: List[String | Null]) {
  over: [\"items\"]
  e = Format(i: String) { template: \"{{i}}\", i: self.items }
  self.out = e.text
}
l.items = [\"a\"]
use = Group(a: String, b: List[String | Null]) -> (o: String) {
  f = Format(a: String, b: List[String | Null]) { template: \"{{a}}{{b}}\", a: self.a, b: self.b }
  self.o = f.text
}
use.a = outer.text
use.b = l.out
d = Debug { data: use.o }
";
    let mut project = compile(src, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    weft_compiler::enrich::enrich(&mut project, &catalog).expect("enrich");
    let spec = RunSpec { from: starts(&["use"]), feed: vec!["use".into()], target: vec!["use".into()], ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).expect("the loop comes whole, so the cut is allowed");
    for fed in ["outer.inner.w", "l.e"] {
        assert!(resolved.selection.nodes.contains(&Located::top(fed)), "{fed} feeds `use` and runs");
    }
    refuse_unfed(&project, &resolved.selection, &spec).expect("both inputs are fed");
}

/// A start whose gate only comes from a trigger the run does not fire
/// would skip whatever it is fed, and the run would still say
/// "completed", so it is refused. Handing the gate a value, or firing the
/// trigger, lets it through.
#[test]
fn a_start_shut_by_an_unfired_trigger_is_refused() {
    let src = "tick = Cron { cron: \"0 0 * * * *\" }
direct = Format(p: String) { template: \"{{p}}\", p: tick.scheduledTime }
note = Group(a: String) -> (o: String) {
  f = Format(a: String) { template: \"{{a}}\", a: self.a }
  self.o = f.text
}
note._should_flow = direct.text
d = Debug { data: note.o }
";
    let mut project = compile(src, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    weft_compiler::enrich::enrich(&mut project, &catalog).expect("enrich");
    let handed = |ports: serde_json::Value| -> BTreeMap<String, serde_json::Value> { serde_json::from_value(ports).unwrap() };

    let spec = RunSpec { group: Some(("note".into(), handed(serde_json::json!({"a": "hi"})))), ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    let refusal = weft_core::run_spec::refuse_unrunnable(&project, &resolved.selection, &spec).unwrap_err().to_string();
    assert!(refusal.contains("note would skip") && refusal.contains("tick"), "{refusal}");

    let spec = RunSpec { group: Some(("note".into(), handed(serde_json::json!({"a": "hi", "_should_flow": true})))), ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    weft_core::run_spec::refuse_unrunnable(&project, &resolved.selection, &spec).expect("the handed gate opens it");

    let spec = RunSpec {
        from: BTreeMap::from([("note".to_string(), handed(serde_json::json!({"a": "hi"})))]),
        fire: Some(("tick".into(), serde_json::json!({"scheduledTime": "t", "actualTime": "t"}))),
        ..RunSpec::whole("x")
    };
    let resolved = resolve_spec(&spec, &project).unwrap();
    weft_core::run_spec::refuse_unrunnable(&project, &resolved.selection, &spec).expect("the fired trigger can open it");
}

fn enriched(src: &str) -> weft_core::ProjectDefinition {
    let mut project = compile(src, uuid::Uuid::new_v4(), CompileFs::none()).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    weft_compiler::enrich::enrich(&mut project, &catalog).expect("enrich");
    project
}

/// A group around the start that stays shut is named, and the fix is a
/// start at THAT group, with the flag the run used: a gate value handed
/// to the inner start does not reach the outer group's door.
#[test]
fn a_start_inside_a_shut_group_is_sent_to_the_group() {
    let project = enriched("tick = Cron { cron: \"0 0 * * * *\" }
direct = Format(p: String) { template: \"{{p}}\", p: tick.scheduledTime }
outer = Group(a: String) -> (o: String) {
  inner = Group(a: String) -> (o: String) {
    f = Format(a: String) { template: \"{{a}}\", a: self.a }
    self.o = f.text
  }
  inner.a = self.a
  self.o = inner.o
}
outer._should_flow = direct.text
d = Debug { data: outer.o }
");
    let ports = |value: serde_json::Value| -> BTreeMap<String, serde_json::Value> { serde_json::from_value(value).unwrap() };
    let spec = RunSpec {
        group: Some(("outer.inner".into(), ports(serde_json::json!({"a": "hi", "_should_flow": true})))),
        ..RunSpec::whole("x")
    };
    let resolved = resolve_spec(&spec, &project).unwrap();
    let refusal = weft_core::run_spec::refuse_unrunnable(&project, &resolved.selection, &spec).unwrap_err().to_string();
    assert!(refusal.contains("the group outer around it is shut") && refusal.contains("--group outer="), "{refusal}");

    let spec = RunSpec { from: BTreeMap::from([("outer".to_string(), ports(serde_json::json!({"a": "hi", "_should_flow": true})))]), ..RunSpec::whole("x") };
    let resolved = resolve_spec(&spec, &project).unwrap();
    weft_core::run_spec::refuse_unrunnable(&project, &resolved.selection, &spec).expect("the group's own gate is handed");
}
