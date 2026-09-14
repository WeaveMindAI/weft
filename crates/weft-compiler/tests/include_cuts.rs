//! Layer-3: a compiled program with one file included three times (two
//! chained sites at the top, one inside a loop), cut through its sites.
//! The compiler is real; the selection is what the run is held to.
use std::collections::BTreeSet;

use weft_compiler::weft_compiler::compile;
use weft_compiler::CompileFs;
use weft_core::frames::Located;
use weft_core::project::selection::{RunSelection, SelectionBounds};

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

/// From a node in the first call to the same node in the second: the
/// rest of the first call on the way out, the second site, and the
/// second call's gate; the same id is two places, and only the first
/// place has the backup.
#[test]
fn a_cut_from_one_call_to_the_next_holds_the_path_between_them() {
    let project = compiled();
    let cut = RunSelection::carve(&project, &SelectionBounds { from: vec!["one.strip".into()], target: vec!["two.strip".into()], ..Default::default() }).unwrap();
    let expected: BTreeSet<Located> = [
        at("@lib:clean.strip", &["one"]), at("@lib:clean.loud", &["one"]), at("@lib:clean__out", &["one"]), at("one__out", &[]),
        at("two__in", &[]), at("@lib:clean__in", &["two"]), at("@lib:clean.strip", &["two"]),
        at("one__in", &[]), at("@lib:clean__in", &["one"]),
    ].into();
    assert_eq!(cut.nodes, expected, "{:?}", cut.nodes);
    assert_eq!(cut.roots(&project), vec![at("one__in", &[])]);
    assert!(cut.has_edge(&project, &at("@lib:clean.strip", &["one"]), project.edges.iter().find(|e| e.source == "@lib:clean.strip" && e.target == "@lib:clean.loud").unwrap(), true));
    assert!(!cut.has_edge(&project, &at("@lib:clean.strip", &["two"]), project.edges.iter().find(|e| e.source == "@lib:clean.strip" && e.target == "@lib:clean.loud").unwrap(), true));
    // Before the second place: its gate opens, the node does not run.
    let before = RunSelection::carve(&project, &SelectionBounds { from: vec!["one.strip".into()], before: vec!["two.strip".into()], ..Default::default() }).unwrap();
    assert!(before.nodes.contains(&at("@lib:clean__in", &["two"])) && !before.nodes.contains(&at("@lib:clean.strip", &["two"])));
    // Up to a node in the second call: everything the top feeds it through.
    let to = RunSelection::carve(&project, &SelectionBounds { target: vec!["two.strip".into()], ..Default::default() }).unwrap();
    assert!(to.nodes.contains(&at("src", &[])) && to.nodes.contains(&at("@lib:clean.loud", &["one"])) && !to.nodes.iter().any(|p| p.id == "@lib:clean.loud" && p.path == ["two"]));
    assert_eq!(to.roots(&project), vec![at("src", &[])]);
    // `--group one` runs the first call whole, spelled from main; the
    // loop `each` inside the file goes in with it.
    let grouped = RunSelection::carve(&project, &SelectionBounds { group: Some("one".into()), ..Default::default() }).unwrap();
    for place in [at("one__in", &[]), at("@lib:clean.strip", &["one"]), at("@lib:clean.each.mark", &["one"]), at("@lib:clean.tally", &["one"]), at("one__out", &[])] {
        assert!(grouped.nodes.contains(&place), "{place} in {:?}", grouped.nodes);
    }
    assert!(!grouped.nodes.iter().any(|p| p.id == "src" || p.id == "out" || p.path == ["two"]));
    // The site inside the loop is cut with the loop.
    let err = RunSelection::carve(&project, &SelectionBounds { target: vec!["outer.call.strip".into()], ..Default::default() }).unwrap_err();
    assert!(err.contains("inside loop 'outer'"), "{err}");
    let whole = RunSelection::whole(&project);
    assert!(whole.nodes.contains(&at("@lib:clean.each.mark", &["outer.call"])) && whole.nodes.contains(&at("@lib:clean.each.mark", &["one"])));
}
