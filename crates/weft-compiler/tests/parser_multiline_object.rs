//! Grammar pin: a MULTI-LINE `{...}` object config value parses in EVERY value
//! position: canonical multi-line body, one-liner body, anon one-liner body,
//! and a connection-origin field line. The structured editor's containment gate
//! RELIES on this (it admits newlines inside a balanced brace-run:
//! `edit/ops.rs reject_uncontained_value`), so if the grammar ever narrows,
//! this fails first and points at the gate to re-tighten.

use weft_compiler::weft_compiler::*;
use weft_compiler::CompileFs;

/// Parse `src` leniently and return node config key `k` (with no diagnostics).
fn config_k(src: &str) -> serde_json::Value {
    let (project, errs) =
        compile_lenient(src, uuid::Uuid::new_v4(), CompileFs::none(), IncludeMode::Interface, None);
    assert!(errs.is_empty(), "expected a clean parse for:\n{src}\ngot: {errs:?}");
    project
        .nodes
        .iter()
        .find_map(|n| n.config.get("k").cloned())
        .unwrap_or_else(|| panic!("config `k` missing after parsing:\n{src}"))
}

#[test]
fn a_multiline_object_value_parses_in_every_position() {
    let ml = "{\n    \"a\": 1,\n    \"b\": {\"c\": true}\n  }";
    let expected = serde_json::json!({"a": 1, "b": {"c": true}});

    assert_eq!(config_k(&format!("t = Text {{\n  k: {ml}\n}}\n")), expected, "canonical body");
    assert_eq!(config_k(&format!("t = Text {{ k: {ml} }}\n")), expected, "one-liner body");
    assert_eq!(
        config_k(&format!("t = Text {{\n  value: \"x\"\n}}\nt.k = {ml}\n")),
        expected,
        "connection-origin field"
    );
    assert_eq!(
        config_k(&format!("op = ExecPython() -> (a: Number) {{ k: {ml} }}\n")),
        expected,
        "anon one-liner body"
    );
    // Compact stays legal too (the small-value form the webview emits).
    assert_eq!(
        config_k("t = Text {\n  value: \"x\"\n}\nt.k = {\"a\": 1}\n"),
        serde_json::json!({"a": 1}),
        "compact on a connection line"
    );
}
