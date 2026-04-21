//! End-to-end compile + enrich tests against the stdlib catalog.

use weft_compiler::enrich::enrich;
use weft_compiler::weft_compiler::compile;
use weft_stdlib::StdlibCatalog;

#[test]
fn enrich_text_debug_chain() {
    let source = r#"
# Project: Pure

greeting = Text { value: "hello" }
out = Debug

out.value = greeting.value
"#;
    let mut project = compile(source, uuid::Uuid::new_v4()).expect("compile");
    enrich(&mut project, &StdlibCatalog).expect("enrich");

    let text = project.nodes.iter().find(|n| n.id == "greeting").unwrap();
    assert_eq!(text.node_type, "Text");
    assert_eq!(text.outputs.len(), 1);
    assert_eq!(text.outputs[0].name, "value");

    let debug = project.nodes.iter().find(|n| n.id == "out").unwrap();
    assert_eq!(debug.node_type, "Debug");
    assert_eq!(debug.inputs.len(), 1);
    assert_eq!(debug.inputs[0].name, "value");
}

#[test]
fn enrich_rejects_unknown_node_type() {
    let source = r#"
# Project: Bad

bad = NotARealNode
"#;
    let mut project = compile(source, uuid::Uuid::new_v4()).expect("compile");
    let err = enrich(&mut project, &StdlibCatalog).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("NotARealNode"), "expected NotARealNode in error, got: {msg}");
}
