//! Layer-3: cutting a run inside a loop's body, one loop deep and two. The
//! compiler is real; the checks are on the walk, the program it cuts, the
//! frames that program runs its part at, and the refusal of a cut that
//! holds one end of a loop or leaves something it needs with nothing.
use std::collections::BTreeMap;

use crate::rig::{compile, enrich};
use weft_compiler::CompileFs;
use weft_core::frames::{Frame, Located};
use weft_core::project::cut::{self, SelectionBounds};
use weft_core::project::graph::ProjectGraph;
use weft_core::project::scopes::ScopeCopy;
use weft_core::project::ProgramIndex;
use weft_core::run_spec::{refuse_unfed, resolve_spec, RunSpec};

/// The body both loops below run: a group `prep` reading the item, a
/// stretch `cut1` then `split`, a loop `inner` wholly inside the body, and
/// `join` after it, which the loop gathers.
const BODY: &str = r#"
    prep = Group(text: String) -> (out: String) {
      clean = ExecPython(text: String) -> (out: String) { code: "x" }
      clean.text = self.text
      self.out = clean.out
    }
    prep.text = self.items
    cut1 = ExecPython(text: String) -> (out: String) { code: "x" }
    cut1.text = prep.out
    split = ExecPython(text: String) -> (parts: List[String]) { code: "x" }
    split.text = cut1.out
    inner = Loop(parts: List[String]) -> (lens: List[Number | Null]) {
      over: ["parts"]
      len = ExecPython(p: String) -> (n: Number) { code: "x" }
      len.p = self.parts
      self.lens = len.n
    }
    inner.parts = split.parts
    join = ExecPython(lens: List[Number | Null]) -> (out: String) { code: "x" }
    join.lens = inner.lens
    self.marks = join.out
"#;

/// `each` runs [`BODY`] at the top; `outer` runs it one loop deeper, in
/// `outer.mid`. `total` reads what `each` gathers.
fn program() -> weft_core::ProjectDefinition {
    let main = format!(
        r#"src = ExecPython() -> (values: List[String]) {{ code: "x" }}
each = Loop(items: List[String]) -> (marks: List[String | Null]) {{
    over: ["items"]
{BODY}
}}
each.items = src.values
total = ExecPython(marks: List[String | Null]) -> (out: String) {{ code: "x" }}
total.marks = each.marks
rows = ExecPython() -> (values: List[List[String]]) {{ code: "x" }}
outer = Loop(rows: List[List[String]]) -> (all: List[List[String | Null] | Null]) {{
  over: ["rows"]
  mid = Loop(items: List[String]) -> (marks: List[String | Null]) {{
    over: ["items"]
{BODY}
  }}
  mid.items = self.rows
  self.all = mid.marks
}}
outer.rows = rows.values
"#
    );
    let dir = tempfile::tempdir().unwrap();
    let mut project = compile(&main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    enrich(&mut project, &catalog).expect("enrich");
    project
}

fn walk(project: &weft_core::ProjectDefinition, bounds: SelectionBounds) -> Result<cut::Walk, String> {
    cut::walk(&ProjectGraph::new(project), &bounds)
}

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// The two loops a body sits in: `each` alone, and `outer` then
/// `outer.mid`. Each with the frames its body runs at in a cut of it.
fn bodies() -> [(&'static str, Vec<&'static str>, Vec<Frame>); 2] {
    let zero = || Frame::Loop { index: 0 };
    [("each", vec!["each"], vec![zero()]), ("outer.mid", vec!["outer", "outer.mid"], vec![zero(), zero()])]
}

/// Every cut flag is taken inside a body: a group, a stretch handed its
/// start, one holding a whole inner loop, one standing in at its start,
/// one stopping before a node, and one with the start's feeder. The cut
/// runs its part at iteration 0 of each loop around it, opened with the
/// run, and holds no end of those loops.
#[test]
fn every_cut_flag_works_inside_a_body_one_and_two_loops_deep() {
    let project = program();
    for (body, loops, zero) in bodies() {
        let at = |name: &str| Located::top(format!("{body}.{name}"));
        let spelled = |name: &str| format!("{body}.{name}");
        let cuts = [
            SelectionBounds { group: Some(spelled("prep")), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("cut1")]), target: names(&[&spelled("split")]), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("cut1")]), target: names(&[&spelled("join")]), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("prep")]), target: names(&[&spelled("split")]), ..Default::default() },
            SelectionBounds { emit: names(&[&spelled("cut1")]), target: names(&[&spelled("split")]), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("cut1")]), before: names(&[&spelled("join")]), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("split")]), feed: names(&[&spelled("split")]), target: names(&[&spelled("split")]), ..Default::default() },
            SelectionBounds { from: names(&[&spelled("inner.len")]), target: names(&[&spelled("inner.len")]), ..Default::default() },
        ];
        for bounds in cuts {
            let walked = walk(&project, bounds.clone()).unwrap_or_else(|why| panic!("{bounds:?}: {why}"));
            let program = cut::cut(&project, &walked, &mut Default::default()).expect("a cut");
            let index = ProgramIndex::build(&program);
            for around in &loops {
                assert!(index.scopes().get(around).is_some_and(|scope| scope.entered), "{bounds:?}: '{around}' entered");
                assert!(!program.nodes.iter().any(|node| node.id == weft_core::project::loop_in_id(around)), "{bounds:?}: no start of '{around}'");
            }
            let first = walked.kept().into_iter().next().expect("the cut keeps something");
            assert!(index.frames_at(&program, &first).starts_with(&zero), "{bounds:?}: {first} runs inside iteration 0");
        }
        // The stretch that holds `inner` holds it whole, both ends, and its
        // start fires at the body's iteration 0.
        let with_inner = walk(&project, SelectionBounds { from: names(&[&spelled("cut1")]), target: names(&[&spelled("join")]), ..Default::default() }).unwrap();
        for member in ["inner__in", "inner__out", "inner.len", "join"] {
            assert!(with_inner.runs.contains(&at(member)), "{member} in {:?}", with_inner.runs);
        }
        let program = cut::cut(&project, &with_inner, &mut Default::default()).unwrap();
        let index = ProgramIndex::build(&program);
        assert!(!index.scopes().get(&spelled("inner")).unwrap().entered, "a loop held whole is launched");
        assert_eq!(index.frames_at(&program, &at("inner__in")), zero);
        // The cut wholly inside `inner`: one more iteration 0.
        let deepest = walk(&project, SelectionBounds { from: names(&[&spelled("inner.len")]), target: names(&[&spelled("inner.len")]), ..Default::default() }).unwrap();
        let program = cut::cut(&project, &deepest, &mut Default::default()).unwrap();
        let index = ProgramIndex::build(&program);
        let deeper: Vec<Frame> = zero.iter().cloned().chain([Frame::Loop { index: 0 }]).collect();
        assert_eq!(index.frames_at(&program, &at("inner.len")), deeper);
    }
}

/// The run opens the loops around its part itself, outermost first: the
/// top one at its start, and each one inside it once the one around it is
/// open. The group around the part is decided inside the innermost.
#[test]
fn the_loops_around_a_cut_open_outermost_first() {
    let project = program();
    let walked = walk(&project, SelectionBounds { group: Some("outer.mid.prep".into()), ..Default::default() }).unwrap();
    let program = cut::cut(&project, &walked, &mut Default::default()).unwrap();
    let index = ProgramIndex::build(&program);
    let zero = |n: usize| vec![Frame::Loop { index: 0 }; n];
    assert_eq!(index.scopes().top_copies(), vec![ScopeCopy::new("outer", zero(1))]);
    let below_outer = index.scopes().children_of(&ScopeCopy::new("outer", zero(1)));
    assert_eq!(below_outer, vec![ScopeCopy::new("outer.mid", zero(2))]);
    assert!(index.scopes().children_of(&ScopeCopy::new("outer.mid", zero(2))).contains(&ScopeCopy::new("outer.mid.prep", zero(2))));
}

/// A cut that holds one end of a loop is refused, naming the loop and the
/// end it lacks: a node of the body reading the item reaches the loop's
/// start, and so does a fed start whose feeder is the loop's start. A
/// bound inside a loop the run reaches the end of, and so runs whole, is
/// refused naming the bound and the loop.
#[test]
fn a_cut_holding_one_end_of_a_loop_is_refused_naming_it() {
    let project = program();
    let start_only = walk(&project, SelectionBounds { target: names(&["each.split"]), ..Default::default() }).unwrap_err();
    assert!(start_only.contains("loop 'each''s start but not its end") && start_only.contains("or neither"), "{start_only}");
    let fed = walk(&project, SelectionBounds { group: Some("each.prep".into()), feed: names(&["each.prep"]), ..Default::default() }).unwrap_err();
    assert!(fed.contains("loop 'each''s start but not its end"), "{fed}");
    let deep = walk(&project, SelectionBounds { target: names(&["outer.mid.split"]), ..Default::default() }).unwrap_err();
    assert!(deep.contains("start but not its end"), "{deep}");
    let whole = walk(&project, SelectionBounds { from: names(&["each.cut1"]), target: names(&["total"]), ..Default::default() }).unwrap_err();
    assert!(whole.contains("'each.cut1' is inside loop 'each'") && whole.contains("runs the loop whole"), "{whole}");
    let no_end = walk(&project, SelectionBounds { from: names(&["each.cut1"]), ..Default::default() }).unwrap_err();
    assert!(no_end.contains("'each.cut1' is inside loop 'each'"), "{no_end}");
    // The loop itself, and a run through it, cut nothing in half.
    walk(&project, SelectionBounds { group: Some("each".into()), ..Default::default() }).expect("the loop whole");
    walk(&project, SelectionBounds { target: names(&["total"]), ..Default::default() }).expect("through the loop");
    // A run started from a spec, the way the install starts one, is
    // refused the same way.
    let spec = RunSpec { target: names(&["each.split"]), ..RunSpec::whole("x") };
    let refused = resolve_spec(&spec, &project, &Default::default()).unwrap_err();
    assert!(refused.errors.iter().any(|error| error.contains("loop 'each''s start but not its end")), "{:?}", refused.errors);
}

/// What the loop would hand the part of its body a run cuts (the item,
/// here through the group's port) is a value from outside the cut: handed
/// at the start, the run goes; left with nothing, it is refused naming the
/// port that needs it.
#[test]
fn what_the_loop_would_hand_in_is_handed_or_refused() {
    let project = program();
    let start = |ports: BTreeMap<String, serde_json::Value>| RunSpec {
        group: Some(("each.prep".into(), ports)),
        ..RunSpec::whole("x")
    };
    let unhanded = start(BTreeMap::new());
    let resolved = resolve_spec(&unhanded, &project, &Default::default()).expect("resolves");
    let refused = refuse_unfed(&project, &resolved, &unhanded).unwrap_err();
    assert!(refused.errors.iter().any(|error| error.contains("each.prep.clean.text")), "{:?}", refused.errors);
    let handed = start(BTreeMap::from([("text".to_string(), serde_json::json!("a clip"))]));
    let resolved = resolve_spec(&handed, &project, &Default::default()).expect("resolves");
    refuse_unfed(&project, &resolved, &handed).expect("handed");

    let stretch = RunSpec { from: BTreeMap::from([("each.cut1".to_string(), BTreeMap::new())]), target: names(&["each.split"]), ..RunSpec::whole("x") };
    let resolved = resolve_spec(&stretch, &project, &Default::default()).expect("resolves");
    let refused = refuse_unfed(&project, &resolved, &stretch).unwrap_err();
    assert!(refused.errors.iter().any(|error| error.contains("each.cut1.text")), "{:?}", refused.errors);
}
