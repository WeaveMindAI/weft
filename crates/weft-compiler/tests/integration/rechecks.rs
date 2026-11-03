//! Layer-3: the rules a build cannot decide, because their answer leans
//! on a value that arrives on a wire, recorded on the node of the
//! program it makes (`NodeDefinition::recheck`) and asked on the values
//! that arrive (`weft_core::rules::recheck`). The real compiler and the
//! standard catalog: FetchToStorage refuses a `url` that is no web
//! address.
use weft_compiler::CompileFs;

fn built(main: &str) -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
    weft_compiler::compile_program(main, uuid::Uuid::nil(), CompileFs::disk(dir.path()), &catalog).expect("compiles")
}

fn fetch(program: &weft_core::ProjectDefinition) -> &weft_core::project::NodeDefinition {
    program.nodes.iter().find(|n| n.id == "fetch").expect("fetch")
}

/// A wired `url` leaves the rule to the firing: it is recorded, refuses
/// a value that is no web address with the rule's own message, and lets
/// one that is through.
#[test]
fn a_rule_on_a_wired_value_is_recorded_and_asked_when_it_arrives() {
    let program = built("src = Text { value: \"x\" }\nfetch = FetchToStorage\nfetch.url = src.value\n");
    let node = fetch(&program);
    assert_eq!(node.recheck.len(), 1, "{:?}", node.recheck);
    let arrived = |url: &str| serde_json::json!({ "url": url }).as_object().cloned().unwrap();
    match weft_core::rules::recheck(node, &program, &arrived("ftp://files")) {
        Err(weft_core::error::WeftError::Input(m)) => {
            assert_eq!(m, "FetchToStorage 'fetch' has a `url` that is not a web address. Start it with http:// or https://.")
        }
        other => panic!("a url that is no web address fails the firing, got {other:?}"),
    }
    assert!(weft_core::rules::recheck(node, &program, &arrived("https://example.com/a.png")).is_ok());
}

/// A written `url` is decided when the program compiles: nothing is
/// recorded, and a firing pays nothing.
#[test]
fn a_rule_on_a_written_value_is_decided_at_compile_time() {
    let program = built("fetch = FetchToStorage { url: \"https://example.com/a.png\" }\n");
    assert!(fetch(&program).recheck.is_empty());
}
