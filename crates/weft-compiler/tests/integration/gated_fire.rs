//! Layer-3: a fire of a route whose work hangs off it through a group's
//! `_should_flow` alone, the shape an API builder writes. The compiler is
//! real; the check is on the program the fire runs: the whole group comes
//! along with the route, and nothing behind the other route does.
use crate::rig::{compile, enrich};
use weft_compiler::CompileFs;
use weft_core::run_spec::compute_trigger_fire;

const DEEP: &str = "Group() {
  body = ExecPython() -> (body: JsonDict) { code: \"x\" }
  answer = Reply
  answer.body = body.body
}
";

const MAIN: &str = "grouped = Route { path: \"grouped\", method: \"POST\", auth: \"none\" }
work = Group() {
  body = ExecPython() -> (body: JsonDict) { code: \"x\" }
  answer = Reply
  answer.body = body.body
}
work._should_flow = grouped.method
included = Route { path: \"included\", method: \"POST\", auth: \"none\" }
deep = @include(\"deep.weft\")
deep._should_flow = included.method
";

fn program() -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("deep.weft"), DEEP).unwrap();
    let mut project = compile(MAIN, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile");
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    enrich(&mut project, &catalog).expect("enrich");
    project
}

/// Each route's fire runs the work its gate opens, kept behind that gate,
/// and none of the other route's.
#[test]
fn a_fire_runs_the_group_its_route_gates() {
    let project = program();
    for (route, inside, other) in [("grouped", ["work.body", "work.answer"], "deep"), ("included", ["body", "answer"], "work")] {
        let fire = compute_trigger_fire(&project, route, &serde_json::json!({}), None, &Default::default()).unwrap();
        let runs = weft_core::project::places::running_places(&fire.program);
        let spelled: Vec<String> = runs.iter().map(|place| weft_core::project::address_of(&fire.program, &place.id, &place.path)).collect();
        for member in inside {
            let member = if route == "included" { format!("deep.{member}") } else { member.to_string() };
            assert!(spelled.contains(&member), "{route}: {member} runs: {spelled:?}");
        }
        assert!(!spelled.iter().any(|place| place.starts_with(&format!("{other}."))), "{route}: {spelled:?}");
        assert!(fire.program.edges.iter().any(|edge| edge.source == route && edge.is_group_gate(&fire.program)), "{route}: the gate is kept");
    }
}
