//! Layer-3: a compiled program with one file included three times (two
//! chained sites at the top, one inside a loop), cut through its sites.
//! The compiler is real; the cut is the program the run runs.
use std::collections::BTreeSet;

use crate::rig::compile;
use weft_compiler::CompileFs;
use weft_core::frames::{Frame, Located};
use weft_core::project::cut::{self, SelectionBounds};
use weft_core::project::graph::ProjectGraph;
use weft_core::project::ProgramIndex;
use weft_core::run_spec::KickPlan;

const CLEAN: &str = r#"
Group(text: String) -> (out: String, count: Number) {
  strip = ExecPython(text: String) -> (out: String) { code: "x" }
  strip.text = self.text
  loud = ExecPython(text: String) -> (loud: String) { code: "x" }
  loud.text = strip.out
  letters = ExecPython(text: String) -> (items: List[String]) { code: "x" }
  letters.text = strip.out
  each = Loop(items: List[String]) -> (marks: List[Number | Null]) {
    parallel: false
    over: ["items"]
    mark = ExecPython(c: String) -> (n: Number) { code: "x" }
    mark.c = self.items
    self.marks = mark.n
  }
  each.items = letters.items
  tally = ExecPython(marks: List[Number | Null]) -> (count: Number) { code: "x" }
  tally.marks = each.marks
  self.out = loud.loud
  self.count = tally.count
}
"#;

const MAIN: &str = r#"
src = Text { value: " hello " }
one = @include("lib/clean.weft")
one.text = src.value
two = @include("lib/clean.weft")
two.text = one.out
out = Debug
out.data = two.out
items = ExecPython() -> (values: List[String]) { code: "x" }
outer = Loop(values: List[String]) -> (results: List[String | Null]) {
  parallel: false
  over: ["values"]
  call = @include("lib/clean.weft")
  call.text = self.values
  self.results = call.out
}
outer.values = items.values
total = Debug
total.data = outer.results
"#;

fn at(id: &str, path: &[&str]) -> Located {
    Located::new(id, path.iter().map(|s| s.to_string()).collect())
}

fn compiled() -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("lib")).unwrap();
    std::fs::write(dir.path().join("lib/clean.weft"), CLEAN).unwrap();
    compile(MAIN, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile")
}

fn walk(project: &weft_core::ProjectDefinition, bounds: SelectionBounds) -> Result<cut::Walk, String> {
    cut::walk(&ProjectGraph::new(project), &bounds)
}

fn call(site: &str) -> Vec<Frame> {
    vec![Frame::Call { site: site.into() }]
}

/// From a node in the first call to the same node in the second: the
/// rest of the first call on the way out and the second call's node; the
/// same id is two places, and the cut runs each call's own part of the
/// shared file (`GroupKind::Call::runs`).
#[test]
fn a_cut_from_one_call_to_the_next_holds_the_path_between_them() {
    let project = compiled();
    let walked = walk(&project, SelectionBounds { from: vec!["one.strip".into()], target: vec!["two.strip".into()], ..Default::default() }).unwrap();
    let expected: BTreeSet<Located> = [
        at("@lib:clean.strip", &["one"]), at("@lib:clean.loud", &["one"]), at("@lib:clean.strip", &["two"]),
    ].into();
    assert_eq!(walked.runs, expected, "{:?}", walked.runs);
    let program = cut::cut(&project, &walked, &mut Default::default()).expect("a cut");
    let index = ProgramIndex::build(&program);
    assert!(index.admits("@lib:clean.loud", &call("one")) && !index.admits("@lib:clean.loud", &call("two")));
    assert!(index.admits("@lib:clean.strip", &call("two")));
    let strip_to_loud = program.edges.iter().find(|e| e.source == "@lib:clean.strip" && e.target == "@lib:clean.loud").expect("the wire inside the file");
    assert!(index.frames_across(strip_to_loud, &call("one")).is_some());
    assert!(index.get_outgoing(&program, "@lib:clean.strip", &call("two")).iter().all(|edge| edge.target != "@lib:clean.loud"),
        "the second call's strip feeds nothing in the cut");
    // Before the second place: the node does not run.
    let before = walk(&project, SelectionBounds { from: vec!["one.strip".into()], before: vec!["two.strip".into()], ..Default::default() }).unwrap();
    assert!(before.runs.contains(&at("@lib:clean.loud", &["one"])) && !before.runs.contains(&at("@lib:clean.strip", &["two"])));
    // Up to a node in the second call: everything the top feeds it through.
    let to = walk(&project, SelectionBounds { target: vec!["two.strip".into()], ..Default::default() }).unwrap();
    assert!(to.runs.contains(&at("src", &[])) && to.runs.contains(&at("@lib:clean.loud", &["one"])) && !to.runs.iter().any(|p| p.id == "@lib:clean.loud" && p.path == ["two"]));
    let kicks = KickPlan::for_program(&cut::cut(&project, &to, &mut Default::default()).expect("a cut"), None, None);
    assert_eq!(kicks.iter().map(|kick| kick.node.as_str()).collect::<Vec<_>>(), vec!["src"]);
    // `--group one` runs the first call whole, spelled from main; the
    // loop `each` inside the file goes in with it.
    let grouped = walk(&project, SelectionBounds { group: Some("one".into()), ..Default::default() }).unwrap();
    for place in [at("@lib:clean.strip", &["one"]), at("@lib:clean.each.mark", &["one"]), at("@lib:clean.tally", &["one"])] {
        assert!(grouped.runs.contains(&place), "{place} in {:?}", grouped.runs);
    }
    assert!(!grouped.runs.iter().any(|p| p.id == "src" || p.id == "out" || p.path == ["two"]));
    // Up to a node of the call inside the loop: it reads what the loop
    // hands its body, so the run reaches the loop's start and not its end.
    let err = walk(&project, SelectionBounds { target: vec!["outer.call.strip".into()], ..Default::default() }).unwrap_err();
    assert!(err.contains("loop 'outer''s start but not its end"), "{err}");
    // Started inside that call, the run sits in the loop's iteration 0,
    // one call deeper.
    let inside = walk(&project, SelectionBounds { from: vec!["outer.call.strip".into()], target: vec!["outer.call.loud".into()], ..Default::default() }).unwrap();
    let program = cut::cut(&project, &inside, &mut Default::default()).expect("a cut");
    let index = ProgramIndex::build(&program);
    assert_eq!(index.frames_at(&program, &at("@lib:clean.strip", &["outer.call"])), vec![Frame::Loop { index: 0 }, Frame::Call { site: "outer.call".into() }]);
    let whole = weft_core::project::places::every_place(&project);
    assert!(whole.contains(&at("@lib:clean.each.mark", &["outer.call"])) && whole.contains(&at("@lib:clean.each.mark", &["one"])));
}

/// The loop `each` in the included file is one loop under both sites.
/// Part of its body under one site runs inside its iteration 0; a run that
/// also holds all of it under the other site is refused, naming both.
#[test]
fn an_included_loop_runs_whole_or_in_part_in_one_run_not_both() {
    let project = compiled();
    let part = walk(&project, SelectionBounds { from: vec!["two.each.mark".into()], target: vec!["two.each.mark".into()], ..Default::default() }).unwrap();
    let program = cut::cut(&project, &part, &mut Default::default()).expect("a cut");
    let index = ProgramIndex::build(&program);
    assert_eq!(index.frames_at(&program, &at("@lib:clean.each.mark", &["two"])), vec![Frame::Call { site: "two".into() }, Frame::Loop { index: 0 }]);
    let err = walk(&project, SelectionBounds {
        from: vec!["one.letters".into(), "two.each.mark".into()],
        target: vec!["one.tally".into(), "two.each.mark".into()],
        ..Default::default()
    })
    .unwrap_err();
    assert!(err.contains("all of loop 'one.each'") && err.contains("part of the body of loop 'two.each'"), "{err}");
}

/// A cut started inside an included file, frozen as an example, keeps its
/// start spelled through the site (`one.strip`, not the file's own id) and
/// its review focus the same way, reads back from the file it was written
/// to, and runs the same cut again with the frozen value handed to that one
/// call's node: nothing before the start, nothing past the target.
#[test]
fn a_frozen_cut_inside_a_call_reads_back_and_runs_that_call_again() {
    use weft_core::run_spec::{resolve_spec, Expected, FrozenFrom, RunSpec};
    let project = compiled();
    let frozen = RunSpec {
        from: [("one.strip".to_string(), [("text".to_string(), serde_json::json!(" frozen "))].into())].into(),
        target: vec!["two.strip".into()],
        frozen_from: Some(FrozenFrom { version: "v".into(), run_id: uuid::Uuid::nil(), definition_hash: "h".into() }),
        expected: Some(Expected { focus: vec!["two.strip".into()], ..Default::default() }),
        ..RunSpec::whole("inner")
    };
    let text = frozen.file_text().unwrap();
    let file: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(file["from"]["one.strip"]["text"], serde_json::json!(" frozen "), "{text}");
    assert_eq!(file["expected"]["focus"], serde_json::json!(["two.strip"]), "{text}");

    let replayed = RunSpec::parse_file(&text).unwrap();
    let resolved = resolve_spec(&replayed, &project, &Default::default()).expect("the frozen spec runs again");
    let expected: BTreeSet<Located> = [
        at("@lib:clean.strip", &["one"]), at("@lib:clean.loud", &["one"]), at("@lib:clean.strip", &["two"]),
    ].into();
    assert_eq!(resolved.runs, expected, "{:?}", resolved.runs);
    let handed = resolved.given.inputs.get(&at("@lib:clean.strip", &["one"])).expect("the start is handed its value");
    assert_eq!(handed.get("text"), Some(&serde_json::json!(" frozen ")));
    assert!(!resolved.runs.iter().any(|place| place.id == "src" || place.id == "out"), "{:?}", resolved.runs);
}
